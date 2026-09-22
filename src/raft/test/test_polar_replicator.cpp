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

#define RUN(expr) RAFT_SYNC_BTHREAD_RUN_ON_POLAR(expr, 0)

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
DECLARE_bool(raft_use_polar);
DECLARE_bool(raft_use_iouring);
}

class MockRaftServiceAppend : public raft::RaftService {
public:
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

class ReplicatorTest : public testing::Test {
    ReplicatorTest() : _heartbeat_timeout_ms(300),
    _election_timeout_ms(3000), _cq(nullptr), _storage("./data") {}
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
        raft::FLAGS_raft_use_polar = true;
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
        server_options.use_polar = raft::FLAGS_raft_use_polar;
        ASSERT_EQ(_server.Start(addr, &server_options), 0);
        raft::FLAGS_raft_use_iouring = false;
        raft::SynchronizedClosure done;
        // some platform not support iouring, we need pthread worker
        auto l = [&] {
            scoped_refptr<raft::IOWorker> worker = raft::g_raft_io_man->get_or_create_worker();
            if (!worker) {
                done.status().set_error(EIO, "create worker error");
            }
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        raft::FLAGS_raft_use_iouring = true;
        auto cq_init = [&] { _cq = new raft::ClosureQueue(false); };
        RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cq_init(), 0);

        raft::BallotBoxOptions ballot_box_options;
        ballot_box_options.waiter = &_fsm_caller;
        ballot_box_options.closure_queue = _cq;

        done.reset();
        auto l2 = [&] {
            _ballot_box = new raft::BallotBox;
            ASSERT_EQ(0, _ballot_box->init(ballot_box_options));
            _log_manager = new MockLogManager;
            raft::LogManagerOptions log_opt;
            log_opt.log_storage = &_storage;
            log_opt.configuration_manager = &_config_manager;
            if (_log_manager->init(log_opt) != 0) {
                done.status().set_error(EIO, "disk_manager init error");
            }
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());

        // init replicator
        raft::ReplicatorGroupOptions rg_options;
        rg_options.heartbeat_timeout_ms = _heartbeat_timeout_ms;
        rg_options.election_timeout_ms = _election_timeout_ms;
        rg_options.log_manager = _log_manager;
        rg_options.ballot_box = _ballot_box;
        rg_options.node = nullptr;
        rg_options.snapshot_throttle = nullptr;
        _group.init(raft::NodeId("test",
            raft::PeerId(_server.listen_address())), rg_options);
    }
    void TearDown() {
        raft::SynchronizedClosure done;
        auto l = [&] {
            _log_manager->stop_disk_thread();
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
        done.wait();
        _server.Stop(0);
        _server.Join();
        auto l2 = [=] {
            delete _ballot_box;
            delete _cq;
        };
        RUN(l2());
        raft::FLAGS_raft_use_polar = false;
    }

    int start(raft::ReplicatorId* id) {
        raft::ReplicatorOptions options;
        options.dynamic_heartbeat_timeout_ms = &_heartbeat_timeout_ms;
        options.election_timeout_ms = &_election_timeout_ms;
        options.group_id = "test";
        options.server_id = raft::PeerId(_server.listen_address());
        options.peer_id = raft::PeerId(_server.listen_address());
        options.log_manager = _log_manager;
        options.ballot_box = _ballot_box;
        options.node = &_node;
        options.term = 1;
        options.snapshot_storage = nullptr;
        options.snapshot_throttle = nullptr;
        options.replicator_status = new raft::ReplicatorStatus;
        raft::SynchronizedClosure done;
        auto l = [&] {
            if (raft::Replicator::start(options, id) != 0) {
                done.status().set_error(EINVAL, "start replicator error");
            }
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
        done.wait();
        return done.status().error_code();
    }
    
    int stop(const raft::ReplicatorId& id) {
        raft::SynchronizedClosure done;
        auto l = [&] {
            if (raft::Replicator::stop(id) != 0) {
                done.status().set_error(EINVAL, "stop replicator error");
            }
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
        done.wait();
        return done.status().error_code();
    }

private:
    int _heartbeat_timeout_ms;
    int _election_timeout_ms;
    rpc::Server _server;
    MockRaftServiceAppend _mock_service;
    raft::ClosureQueue* _cq;
    raft::FSMCaller _fsm_caller;
    raft::BallotBox* _ballot_box;
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
    auto l = [&] {
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
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    for (auto& cc : c) {
        cc.join();
        ASSERT_TRUE(cc.status().ok()) << cc.status();
    }
    raft::SynchronizedClosure done;
    auto l2 = [&] {
        ASSERT_EQ(0, raft::Replicator::_continue_sending((void*)id, 0));
        done.Run();
    };
    auto l3 = l2;
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    done.wait();
    raft::FLAGS_raft_max_entries_size = 64;
    done.reset();
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), 0);
    done.wait();
    done.reset();
    raft::FLAGS_raft_max_entries_size = 155;
    auto l4 = [&] {
        _log_manager->_pending_get = false;
        _log_manager->wake_up();
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l4)), 0);
    done.wait();
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
    ASSERT_GT(raft::g_redundant_get.get_value(), 0);
    ASSERT_EQ(0, stop(id));
}

TEST_F(ReplicatorTest, cancel_rpc) {
    // TODO: wait polar async call on_error
}

TEST_F(ReplicatorTest, wait_canceled) {
    // TODO: cancel wait_id
}

