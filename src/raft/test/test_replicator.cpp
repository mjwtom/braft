// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Jingwei Ma (majingwei@baidu.com)
// Date: 2024/05/30 17:03:46

#include <gtest/gtest.h>
#include <base/string_printf.h>
#include <base/memory/scoped_ptr.h>
#include "raft/log.h"
#include "raft/io_uring.h"
#include "raft/node.h"
#include "raft/test/util.h"

namespace baidu {
namespace hpn {
namespace polar {
DECLARE_string(lcore_mask);
DECLARE_string(pci_bus_list);
DECLARE_string(server_ports_list);
DECLARE_string(host_ip);
DECLARE_int32(client_ports_start);
DECLARE_int32(client_ports_end);
DECLARE_int32(client_ports_num);
DECLARE_string(socket_mem);
}
}
}
namespace raft {
extern BvarAdder<int64_t> g_redundant_get;
DECLARE_int32(raft_max_entries_size);
DECLARE_bool(raft_trace_append_entry_latency);
DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
DECLARE_bool(raft_use_iouring);
}

class MockRaftServiceAppend : public raft::RaftService {
public:
    MockRaftServiceAppend() {}
    void append_entries(google::protobuf::RpcController* controller,
            const raft::AppendEntriesRequest* request,
            raft::AppendEntriesResponse* response,
            google::protobuf::Closure* done) override {
        rpc::Controller* cntl =
            static_cast<rpc::Controller*>(controller);
        rpc::ClosureGuard done_guard(done);
        int64_t index = request->prev_log_index();
        LOG(NOTICE) << "recv append entry, first: " << index + 1
            << ", entries: " << request->entries_size();
        // Parse request
        base::IOBuf data_buf;
        data_buf.swap(cntl->request_attachment());
        for (int i = 0; i < request->entries_size(); i++) {
            index++;
            const raft::EntryMeta entry = request->entries(i);
            if ((raft::EntryType)entry.type() != raft::ENTRY_TYPE_UNKNOWN) {
                raft::LogEntry* log_entry = new raft::LogEntry();
                log_entry->AddRef();
                log_entry->id.term = entry.term();
                log_entry->id.index = index;
                log_entry->type = (raft::EntryType)entry.type();
                if (entry.peers_size() > 0) {
                    log_entry->peers = new std::vector<raft::PeerId>;
                    for (int i = 0; i < entry.peers_size(); i++) {
                        log_entry->peers->push_back(entry.peers(i));
                    }
                    CHECK_EQ(log_entry->type, raft::ENTRY_TYPE_CONFIGURATION);
                    if (entry.old_peers_size() > 0) {
                        log_entry->old_peers = new std::vector<raft::PeerId>;
                        for (int i = 0; i < entry.old_peers_size(); i++) {
                            log_entry->old_peers->push_back(entry.old_peers(i));
                        }
                    }
                    if (entry.has_user_conf()) {
                        log_entry->user_conf = entry.user_conf();
                    }
                } else {
                    CHECK_NE((raft::EntryType)entry.type(), raft::ENTRY_TYPE_CONFIGURATION);
                }
                if (entry.has_data_len()) {
                    int len = entry.data_len();
                    data_buf.cutn(&log_entry->data, len);
                }

                std::unique_lock<raft_mutex_t> lck(_mutex);
                ASSERT_TRUE(_entries.insert(std::make_pair(index, log_entry)).second);
            }
        }
        response->set_term(1);
        response->set_success(true);
    }

    void wait_entries(const size_t n) {
        do {
            std::unique_lock<raft_mutex_t> lck(_mutex);
            if (_entries.size() >= n) {
                return;
            }
            lck.unlock();
            bthread_usleep(1000);
        } while (true);
    }

    void clean_entries() {
        std::unique_lock<raft_mutex_t> lck(_mutex);
        for (auto& iter : _entries) {
            iter.second->Release();
        }
        _entries.clear();
    }

private:
    raft_mutex_t _mutex;
    std::map<int64_t, raft::LogEntry*> _entries;
};

class MockReplicator : public raft::Replicator {
public:
    MockReplicator() : _install_called(false) {}
    virtual void _install_snapshot() override {
        LOG(NOTICE) << "install snapshot called";
        _install_called.store(true, std::memory_order_release);
        ASSERT_EQ(0, raft::raft_bthread_id_unlock(_id)) << "Fail to unlock " << _id;
    }

    std::atomic<bool> _install_called;
};

class ReplicatorTest : public testing::Test {
    ReplicatorTest() : _heartbeat_timeout_ms(300),
    _election_timeout_ms(3000), _cq(false), _storage("./data") {}
protected:
    void SetUp() {
        base::EndPoint addr;
        generate_random_addr(&addr);
        system("rm -rf ./data");
        google::SetCommandLineOption("crash_on_fatal_log", "true");
        google::SetCommandLineOption("verbose", "99");
        google::SetCommandLineOption("min_log_level", "0");
        raft::FLAGS_raft_max_entries_size = 32;
        raft::FLAGS_raft_max_parallel_append_entries_rpc_num = 16;
        raft::FLAGS_raft_sync = false;
        baidu::hpn::polar::FLAGS_lcore_mask = "0x0F0000";
        baidu::hpn::polar::FLAGS_host_ip = base::ip2str(addr.ip).c_str();
        baidu::hpn::polar::FLAGS_server_ports_list = std::to_string(addr.port);
        baidu::hpn::polar::FLAGS_client_ports_start = 9100;
        baidu::hpn::polar::FLAGS_client_ports_end = 9200;
        baidu::hpn::polar::FLAGS_client_ports_num = 50;
        baidu::hpn::polar::FLAGS_socket_mem = "512,512";
        ASSERT_EQ(0, _server.AddService(&_mock_service,
                    rpc::SERVER_DOESNT_OWN_SERVICE));
        rpc::ServerOptions server_options;
        LOG(NOTICE) << "raft use_polar: " << raft::FLAGS_raft_use_polar;
        server_options.use_polar = raft::FLAGS_raft_use_polar;
        ASSERT_EQ(_server.Start(addr, &server_options), 0);
        raft::FLAGS_raft_use_iouring = false;

        raft::BallotBoxOptions ballot_box_options;
        ballot_box_options.waiter = &_fsm_caller;
        ballot_box_options.closure_queue = &_cq;
        ASSERT_EQ(0, _ballot_box.init(ballot_box_options));

        _log_manager = new MockLogManager;
        raft::LogManagerOptions log_opt;
        log_opt.log_storage = &_storage;
        log_opt.configuration_manager = &_config_manager;
        log_opt.fsm_caller = &_fsm_caller;
        ASSERT_EQ(0, _log_manager->init(log_opt));

        // init replicator
        raft::ReplicatorGroupOptions rg_options;
        rg_options.heartbeat_timeout_ms = _heartbeat_timeout_ms;
        rg_options.election_timeout_ms = _election_timeout_ms;
        rg_options.log_manager = _log_manager;
        rg_options.ballot_box = &_ballot_box;
        rg_options.node = nullptr;
        rg_options.snapshot_throttle = nullptr;
        _group.init(raft::NodeId("test",
            raft::PeerId(_server.listen_address())), rg_options);
    }
    void TearDown() {
        _server.Stop(0);
        _server.Join();
    }
    
    int start(raft::ReplicatorId* id) {
        raft::ReplicatorOptions options;
        options.dynamic_heartbeat_timeout_ms = &_heartbeat_timeout_ms;
        options.election_timeout_ms = &_election_timeout_ms;
        options.group_id = "test";
        options.server_id = raft::PeerId(_server.listen_address());
        options.peer_id = raft::PeerId(_server.listen_address());
        options.log_manager = _log_manager;
        options.ballot_box = &_ballot_box;
        options.node = &_node;
        options.term = 1;
        options.snapshot_storage = nullptr;
        options.snapshot_throttle = nullptr;
        options.replicator_status = new raft::ReplicatorStatus;

        // copied
        raft::Replicator* r = new MockReplicator();
        r->_polar_idx = options.node->polar_worker_index();
        rpc::ChannelOptions channel_opt;
        raft::fill_rpc_channel_option(&channel_opt);
        //channel_opt.connect_timeout_ms = *options.heartbeat_timeout_ms;
        channel_opt.protocol = "baidu_std";
        channel_opt.timeout_ms = -1; // We don't need RPC timeout
        if (r->_sending_channel.Init(options.peer_id.addr, &channel_opt) != 0) {
            LOG(ERROR) << "Fail to init sending channel"
                   << ", group " << options.group_id;
            delete r;
            return -1;
        }
        channel_opt.protocol = "fb_rpc";
        channel_opt.max_retry = 0;
        if (r->_fb_sending_channel.Init(options.peer_id.addr, &channel_opt) != 0) {
            LOG(ERROR) << "Fail to init fb sending channel"
                   << ", group " << options.group_id;
            delete r;
            return -1;
        }
        LOG(INFO) << "node " << options.group_id << ":" << options.server_id
              << " init channel success";

        r->_options = options;
        r->_next_index = r->_options.log_manager->last_log_index() + 1;
        if (raft::raft_bthread_id_create(&r->_id, r,
                    raft::Replicator::_on_error) != 0) {
            LOG(ERROR) << "Fail to create bthread_id"
                   << ", group " << options.group_id;
            delete r;
            return -1;
        }

        // bind lifecycle with node, AddRef
        // Replicator stop is async
        options.node->AddRef();
        options.replicator_status->AddRef();

        raft::raft_bthread_id_lock(r->_id, NULL);
        if (id) {
            *id = r->_id.value;
        }
        LOG(INFO) << "Node " << r->_options.group_id << ":" << r->_options.server_id
            << " started replicator=" << r->_id << "@" << r->_options.peer_id;
        r->_catchup_closure = NULL;
        r->_update_last_rpc_send_timestamp_ms(base::monotonic_time_ms());
        r->_start_heartbeat_timer(base::gettimeofday_us());
        // Note: r->_id is unlock in _send_empty_entries, don't touch r ever after
        r->send_empty_entries(false);
        return 0;
    }

    int stop(const raft::ReplicatorId& id) {
        return raft::Replicator::stop(id);
    }

private:
    int _heartbeat_timeout_ms;
    int _election_timeout_ms;
    rpc::Server _server;
    MockRaftServiceAppend _mock_service;
    raft::ClosureQueue _cq;
    raft::FSMCaller _fsm_caller;
    raft::BallotBox _ballot_box;
    raft::ConfigurationManager _config_manager;
    raft::SegmentLogStorage _storage;
    MockLogManager* _log_manager;
    raft::NodeImpl _node;
    raft::ReplicatorGroup _group;
};

class SyncClosure : public raft::LogManager::StableClosure {
public:
    SyncClosure() {
        _butex = bthread::butex_create_checked<base::atomic<int> >();
        *_butex = 0;
    }
    ~SyncClosure() {
        bthread::butex_destroy(_butex);
    }
    void Run() {
        _butex->store(1);
        bthread::butex_wake(_butex);
    }
    void reset() {
        status().reset();
        *_butex = 0;
    }
    void join() {
        while (*_butex != 1) {
            bthread::butex_wait(_butex, 0, NULL);
        }
    }
private:
    base::atomic<int> *_butex;
};

TEST_F(ReplicatorTest, pipeline) {
    raft::ReplicatorId id;
    ASSERT_EQ(0, start(&id));
    _log_manager->_pending_get = true;
    const size_t N = 1000;
    SyncClosure c[N];
    for (size_t i = 0; i < N; ++i) {
        int64_t index = i + 1;
        std::vector<raft::LogEntry*> entries;
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", index);
        entry->data.append(buf);
        entry->id.index = index;
        entry->id.term = i;
        entries.push_back(entry);
        _log_manager->append_entries(&entries, &c[i]);
    }
    for (auto& cc : c) {
        cc.join();
        ASSERT_TRUE(cc.status().ok()) << cc.status();
    }
    ASSERT_EQ(0, raft::Replicator::_continue_sending((void*)id, 0));
    raft::FLAGS_raft_max_entries_size = 64;
    ASSERT_EQ(0, raft::Replicator::_continue_sending((void*)id, 0));
    raft::FLAGS_raft_max_entries_size = 155;
    _log_manager->_pending_get = false;
    _log_manager->wake_up();
    ASSERT_EQ(0, raft::Replicator::_continue_sending((void*)id, 0));
    _mock_service.wait_entries(N);
    ASSERT_EQ(N, _mock_service._entries.size());
    for (auto& iter : _mock_service._entries) {
        auto entry = iter.second;
        ASSERT_EQ(iter.first, entry->id.index);
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello_%ld", entry->id.index);
        ASSERT_EQ(data_buf, entry->data.to_string());
    }
    _mock_service.clean_entries();
    ASSERT_EQ(0, stop(id));
}

TEST_F(ReplicatorTest, cancel_rpc) {
    // TODO: wait polar async call on_error
}

TEST_F(ReplicatorTest, wait_canceled) {
    // TODO: cancel wait_id
}

TEST_F(ReplicatorTest, read_meets_truncate) {
    raft::ReplicatorId id;
    _log_manager->_pending_get = true;
    ASSERT_EQ(0, start(&id));
    const size_t N = 1000;
    SyncClosure c[N];
    for (size_t i = 0; i < N; ++i) {
        int64_t index = i + 1;
        std::vector<raft::LogEntry*> entries;
        raft::LogEntry* entry = new raft::LogEntry;
        entry->AddRef();
        entry->type = raft::ENTRY_TYPE_DATA;
        std::string buf;
        base::string_printf(&buf, "hello_%lu", index);
        entry->data.append(buf);
        entry->id.index = index;
        entry->id.term = i;
        entries.push_back(entry);
        _log_manager->append_entries(&entries, &c[i]);
    }
    for (auto& cc : c) {
        cc.join();
        ASSERT_TRUE(cc.status().ok()) << cc.status();
    }
    bthread_usleep(1000*100); // wait for get entries
    _log_manager->_pending_get = false;
    std::vector<raft::LogEntry*> entries;
    size_t read_size = N/2;
    size_t has_size = read_size/4;
    entries.resize(read_size);
    // entries before truncate
    for (int i = 0; i < 2; i ++) {
        entries[i] = nullptr;
    }
    for (int i = 2; i < has_size; i ++) {
        raft::SynchronizedClosure done;
        entries[i] = nullptr;
        _log_manager->get_entry(i+1, &entries[i], &done);
        done.wait();
        ASSERT_TRUE(done.status().ok()) << done.status();
    }
    std::unique_lock<raft::Mutex> lck(_log_manager->_mutex);
    _log_manager->truncate_prefix(has_size*2+1, lck);
    // entries in hole
    for (int i = has_size; i < has_size*2; i ++) {
        raft::SynchronizedClosure done;
        entries[i] = nullptr;
        _log_manager->get_entry(i+1, &entries[i], &done);
        done.wait();
        ASSERT_FALSE(done.status().ok()) << i;
    }
    // entries kept
    for (int i = has_size*2; i < read_size; i ++) {
        raft::SynchronizedClosure done;
        entries[i] = nullptr;
        _log_manager->get_entry(i+1, &entries[i], &done);
        done.wait();
        ASSERT_TRUE(done.status().ok()) << done.status();
    }
    bthread_id_t dummy_id = { id };
    raft::Replicator::send_entries_rpc(dummy_id, entries);
    for (auto& entry : entries) {
        if (!entry) {
            continue;
        }
        entry->Release();
    }
    raft::Replicator* r = NULL;
    // already stopped
    ASSERT_EQ(raft::raft_bthread_id_lock(dummy_id, (void**)&r), 0);
    MockReplicator* mr = dynamic_cast<MockReplicator*>(r);
    ASSERT_EQ(raft::raft_bthread_id_unlock(dummy_id), 0);
    while (!mr->_install_called.load(std::memory_order_acquire)) {
        bthread_usleep(10000);
    }
    ASSERT_EQ(0, stop(id));
}

