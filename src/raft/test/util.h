// Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Pengfei Zheng (zhengpengfei@baidu.com)

#ifndef PUBLIC_RAFT_TEST_UTIL_H
#define PUBLIC_RAFT_TEST_UTIL_H

#include <arpa/inet.h>
#include "raft/node.h"
#include "raft/closure_helper.h"
#include "raft/enum.pb.h"
#include "raft/errno.pb.h"
#include "raft/snapshot_throttle.h" 
#include "raft/snapshot_executor.h"

bool g_dont_print_apply_log = false;
namespace baidu {
namespace hpn {
namespace polar {
DECLARE_int32(polar_thread_num);
}
}
}

void generate_random_addr(base::EndPoint* addr) {
    CHECK(addr);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sock_addr;

    while (true) {
        sock_addr.sin_family = AF_INET;
        int port = base::fast_rand_in(8500, 8999);
        sock_addr.sin_port = htons(port); // short, network byte order
        sock_addr.sin_addr.s_addr = inet_addr(base::my_ip_cstr());
        memset(sock_addr.sin_zero, '\0', sizeof(sock_addr.sin_zero));
        int ret = bind(fd, (struct sockaddr *)&sock_addr, sizeof(struct sockaddr_in));
        if (ret != 0) {
            continue;
        }
        socklen_t len = sizeof(sock_addr);
        ret = getsockname(fd, (struct sockaddr *)&sock_addr, &len);
        if (ret != 0) {
            continue;
        }
        break;
    }

    addr->ip = base::my_ip();
    addr->port = ntohs(sock_addr.sin_port);
    close(fd);
}

void run_polar_or_bthread(const std::function<void()>& func, int polar_idx) {
    if (raft::FLAGS_raft_use_polar) {
        RAFT_SYNC_BTHREAD_RUN_ON_POLAR(func(), polar_idx);
    } else {
        func();
    }
}

class MockFSM : public raft::StateMachine {
public:
    MockFSM(const base::EndPoint& address_)
        : address(address_)
        , applied_index(0)
        , snapshot_index(0)
        , _on_start_following_times(0)
        , _on_stop_following_times(0)
        , _leader_term(-1)
        , _on_leader_start_closure(NULL)
    {
        pthread_mutex_init(&mutex, NULL);
    }
    virtual ~MockFSM() {
        pthread_mutex_destroy(&mutex);
    }

    base::EndPoint address;
    std::vector<base::IOBuf> logs;
    pthread_mutex_t mutex;
    int64_t applied_index;
    int64_t snapshot_index;
    int64_t _on_start_following_times;
    int64_t _on_stop_following_times;
    volatile int64_t _leader_term;
    raft::Closure* _on_leader_start_closure;

    void lock() {
        pthread_mutex_lock(&mutex);
    }

    void unlock() {
        pthread_mutex_unlock(&mutex);
    }

    bool try_lock() {
        return pthread_mutex_trylock(&mutex) == 0;
    }

    void set_on_leader_start_closure(raft::Closure* closure) {
        _on_leader_start_closure = closure;
    }

    void on_leader_start(int64_t term) {
        LOG(TRACE) << "address " << address << " on_leader_start";
        _leader_term = term;
        if (_on_leader_start_closure) {
            LOG(NOTICE) << "addr " << address << " before leader start closure";
            _on_leader_start_closure->Run();
            LOG(NOTICE) << "addr " << address << " after leader start closure";
            _on_leader_start_closure = NULL;
        }
    }
    void on_leader_stop(const raft::LeaderChangeContext&) {
        LOG(TRACE) << "address " << address << " on_leader_stop";
        _leader_term = -1;
    }

    bool is_leader() { return _leader_term > 0; }

    virtual void on_apply(raft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            LOG_IF(TRACE, !g_dont_print_apply_log) << "addr " << address 
                                                   << " apply " << iter.index()
                                                   << " data " << iter.data();
            ::rpc::ClosureGuard guard(iter.done());
            lock();
            logs.push_back(iter.data());
            applied_index = iter.index();
            unlock();
        }
    }

    virtual void on_shutdown() {
        LOG(TRACE) << "addr " << address << " shutdown";
    }

    virtual void on_snapshot_save(raft::SnapshotWriter* writer, raft::Closure* done) {
        std::string file_path = writer->get_path();
        file_path.append("/data");
        rpc::ClosureGuard done_guard(done);

        LOG(NOTICE) << "on_snapshot_save to " << file_path;

        int fd = ::creat(file_path.c_str(), 0644);
        if (fd < 0) {
            LOG(ERROR) << "create file failed, path: " << file_path << " err: " << berror();
            done->status().set_error(EIO, "Fail to create file");
            return;
        }
        lock();
        // write snapshot and log to file
        for (size_t i = 0; i < logs.size(); i++) {
            base::IOBuf data = logs[i];
            int len = data.size();
            int ret = write(fd, &len, sizeof(int));
            CHECK_EQ(ret, 4);
            data.cut_into_file_descriptor(fd, len);
        }
        ::close(fd);
        snapshot_index = applied_index;
        unlock();
        writer->add_file("data");
    }

    virtual void on_snapshot_load(raft::SnapshotReader* reader, raft::Closure* done) override {
        rpc::ClosureGuard done_guard(done);
        std::string file_path = reader->get_path();
        file_path.append("/data");

        LOG(INFO) << "on_snapshot_load from " << file_path;

        int fd = ::open(file_path.c_str(), O_RDONLY);
        if (fd < 0) {
            LOG(ERROR) << "creat file failed, path: " << file_path << " err: " << berror();
            done->status().set_error(EIO, "get open file");
            return;
        }

        lock();
        logs.clear();
        while (true) {
            int len = 0;
            int ret = read(fd, &len, sizeof(int));
            if (ret <= 0) {
                break;
            }

            base::IOPortal data;
            data.append_from_file_descriptor(fd, len);
            logs.push_back(data);
        }

        ::close(fd);
        unlock();
    }

    virtual void on_start_following(const raft::LeaderChangeContext& start_following_context) {
        LOG(TRACE) << "address " << address << " start following new leader: " 
                   <<  start_following_context;
        ++_on_start_following_times;
    }

    virtual void on_stop_following(const raft::LeaderChangeContext& stop_following_context) {
        LOG(TRACE) << "address " << address << " stop following old leader: " 
                   <<  stop_following_context;
        ++_on_stop_following_times;
    }

    virtual void on_configuration_committed(const ::raft::Configuration& conf, int64_t index) {
        LOG(TRACE) << "address " << address << " commit conf: " << conf << " at index " << index;
    }
};

inline void parse_string(const std::string& str, std::vector<std::string>& str_vec) {
    if (str.empty()) {
        return;
    }
    base::StringSplitter splitter(str.c_str(), '#');
    while (splitter) {
        std::string str(splitter.field(), splitter.length());
        str_vec.push_back(str);
        splitter++;
    }
}

inline std::string generate_user_conf_str(std::vector<std::string>& str_vec) {
    std::stringstream ss;
    ss << str_vec[0];
    for (size_t i = 1; i < str_vec.size(); ++i) {
        ss << "#" << str_vec[i];
    }
    return ss.str();
}

class TwoStageFSM : public MockFSM {
    TwoStageFSM(const base::EndPoint& address_)
        : MockFSM(address_) {}
    virtual void notify_configuration_change_stage1(std::string* user_conf,
                                            raft::Closure* done) {
        std::vector<std::string> str_vec;
        parse_string(*user_conf, str_vec);
        str_vec.erase(str_vec.begin());
        *user_conf = generate_user_conf_str(str_vec);
        if (done) {
            done->Run();
        }
    }
};

class ExpectClosure : public raft::Closure {
public:
    void Run() {
        if (_expect_err_code >= 0) {
            CHECK_EQ(status().error_code(), _expect_err_code)
                << _pos << " : " << status();
            ASSERT_EQ(status().error_code(), _expect_err_code)
                << _pos << " : " << status();
        }
        if (_st) {
            *_st = status();
        }
        if (_cond) {
            _cond->signal();
        }
        delete this;
    }
private:
    ExpectClosure(bthread::CountdownEvent* cond, int expect_err_code, const char* pos)
        : _cond(cond), _expect_err_code(expect_err_code), _pos(pos), _st(nullptr) {}

    ExpectClosure(bthread::CountdownEvent* cond, const char* pos)
        : _cond(cond), _expect_err_code(-1), _pos(pos), _st(nullptr) {}

    ExpectClosure(bthread::CountdownEvent* cond, base::Status* st, const char* pos)
        : _cond(cond), _expect_err_code(-1), _pos(pos), _st(st) {}

    bthread::CountdownEvent* _cond;
    int _expect_err_code;
    const char* _pos;
    base::Status* _st;
};

typedef ExpectClosure ShutdownClosure;
typedef ExpectClosure ApplyClosure;
typedef ExpectClosure AddPeerClosure;
typedef ExpectClosure RemovePeerClosure;
typedef ExpectClosure SnapshotClosure;

#define NEW_SHUTDOWNCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_APPLYCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_ADDPEERCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_REMOVEPEERCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))
#define NEW_SNAPSHOTCLOSURE(arg...) \
        (new ExpectClosure(arg, __FILE__ ":" BAIDU_SYMBOLSTR(__LINE__)))

static raft::BvarAdder<int64_t> g_test_log_manager_count("raft_test_log_manager_count");

class MockLogManager : public raft::LogManager {
public:
    MockLogManager() : _closure_mutex(raft::FLAGS_raft_use_polar), _pending_last_log_id(false), _pending_get(false),
        _pending_append_to_storage(false) {
        g_test_log_manager_count << 1;
    }
    ~MockLogManager() {
        g_test_log_manager_count << -1;
    }
    virtual void last_log_id(raft::LogId* log_id, raft::Closure* done) override {
        raft::Closure* closure = raft::NewCallback(this, &MockLogManager::run_closure,
                _pending_last_log_id.load(std::memory_order_acquire), done);
        LOG_IF(NOTICE, _fsm_caller && _pending_last_log_id.load(std::memory_order_acquire))
            << "Node " << _fsm_caller->node_id()
            << " last_log will pending closure";
        raft::LogManager::last_log_id(log_id, closure);
    }
    void run_closure(bool pending, raft::Closure* done, const base::Status& st) {
        CHECK(done);
        rpc::ClosureGuard done_guard(done);
        if (!st.ok()) {
            done->status() = st;
        }
        if (pending) {
            std::unique_lock lck(_closure_mutex);
            LOG_IF(NOTICE, _fsm_caller) << "Node " << _fsm_caller->node_id()
                << " pending closure";
            _pending_closures.push_back(done_guard.release());
            return;
        }
    };

    virtual void get_entry(const int64_t index,
            raft::LogEntry** entry, raft::Closure* done) override {
        raft::Closure* closure = raft::NewCallback(this, &MockLogManager::run_closure,
                _pending_get.load(std::memory_order_acquire), done);
        LOG_IF(NOTICE, _fsm_caller && _pending_get.load(std::memory_order_acquire))
            << "Node " << _fsm_caller->node_id()
            << " get_entry will pending closure";
        raft::LogManager::get_entry(index, entry, closure);
    }

    virtual void append_to_storage(const std::vector<raft::LogEntry*>& to_append,
            raft::IOMetric* metric, raft::Closure* done) override {
        if (_pending_append_to_storage) {
            std::unique_lock lck(_closure_mutex);
            _pending_closures.push_back(done);
            return;
        }
        raft::LogManager::append_to_storage(to_append, metric, done);
    }

    bool has_pending() {
        std::unique_lock lck(_closure_mutex);
        return !_pending_closures.empty();
    }

    void wake_up() {
        std::unique_lock lck(_closure_mutex);
        LOG_IF(NOTICE, _fsm_caller) << "Node " << _fsm_caller->node_id()
                << " run " << _pending_closures.size() << " pending closure";
        for (auto& c : _pending_closures) {
            c->Run();
        }
        _pending_closures.clear();
    }

    raft::Mutex _closure_mutex;
    std::atomic<bool> _pending_last_log_id; // for vote, we have to wait pre-vote happen then block vote
    std::atomic<bool> _pending_get;
    std::atomic<bool> _pending_append_to_storage;
    std::deque<google::protobuf::Closure*> _pending_closures;
};

class MockNodeImpl : public raft::NodeImpl {
public:
    MockNodeImpl(const raft::GroupId& group_id, const raft::PeerId& peer_id) :
        raft::NodeImpl(group_id, peer_id) {}
    virtual int init_log_storage() override {
        CHECK(_fsm_caller);
        if (_options.log_storage) {
            _log_storage = _options.log_storage;
        } else {
            _log_storage = raft::LogStorage::create(_options.log_uri);
        }
        if (!_log_storage) {
            LOG(ERROR) << "node " << _group_id << ":" << _server_id
                       << " find log storage failed, uri " << _options.log_uri;
            return -1;
        }
        _log_manager = new MockLogManager();
        raft::LogManagerOptions log_manager_options;
        log_manager_options.log_storage = _log_storage;
        log_manager_options.configuration_manager = _config_manager;
        log_manager_options.fsm_caller = _fsm_caller;
        return _log_manager->init(log_manager_options);
    }
};

class Cluster {
public:
    Cluster(const std::string& name, const std::vector<raft::PeerId>& peers,
            int32_t election_timeout_ms = 3000, int max_clock_drift_ms = 1000,
            bool use_two_stages = false)
        : _name(name), _peers(peers) 
        , _election_timeout_ms(election_timeout_ms)
        , _max_clock_drift_ms(max_clock_drift_ms)
        , _use_two_stages(use_two_stages) {

        int64_t throttle_throughput_bytes = 10 * 1024 * 1024;
        int64_t check_cycle = 10;
        _throttle = new raft::ThroughputSnapshotThrottle(throttle_throughput_bytes, check_cycle);
        if (raft::FLAGS_raft_use_polar) {
            _thread_index = base::fast_rand() % baidu::hpn::polar::FLAGS_polar_thread_num;
        }
    }
    ~Cluster() {
        stop_all();
    }

    void init_node(const base::EndPoint listen_addr, bool empty_peers,
              int snapshot_interval_s,
              raft::Closure* leader_start_closure, raft::Closure* finish_closure) {
        CHECK(finish_closure);
        rpc::ClosureGuard done_guard(finish_closure);

        raft::NodeOptions options;
        options.election_timeout_ms = _election_timeout_ms;
        options.max_clock_drift_ms = _max_clock_drift_ms;
        options.use_two_stages = _use_two_stages;
        options.snapshot_interval_s = snapshot_interval_s;
        if (!empty_peers) {
            options.initial_conf = raft::Configuration(_peers);
        }
        raft::Node* node = new raft::Node(_name, raft::PeerId(listen_addr, 0));
        // replace impl
        delete node->_impl;
        node->_impl = new MockNodeImpl(_name, raft::PeerId(listen_addr, 0));
        // force let first schedule in timeout, avoid first scheduling in rand (0, timeout)
        // cause fail in following operations
        node->_impl->_snapshot_timer._first_schedule = false;
        MockFSM* fsm;
        if (_use_two_stages) {
            fsm = new TwoStageFSM(listen_addr);
        } else {
            fsm = new MockFSM(listen_addr);
        }
        if (leader_start_closure) {
            fsm->set_on_leader_start_closure(leader_start_closure);
        }
        options.fsm = fsm;
        options.node_owns_fsm = true;
        base::string_printf(&options.log_uri, "local://./data/%s/log",
                            base::endpoint2str(listen_addr).c_str());
        base::string_printf(&options.stable_uri, "local://./data/%s/stable",
                            base::endpoint2str(listen_addr).c_str());
        base::string_printf(&options.snapshot_uri, "local://./data/%s/snapshot",
                            base::endpoint2str(listen_addr).c_str());
        
        scoped_refptr<raft::SnapshotThrottle> tst(_throttle);
        options.snapshot_throttle = &tst;

        options.catchup_margin = 2;
        
        int ret = node->init(options);
        if (ret != 0) {
            LOG(WARNING) << "init_node failed, server: " << listen_addr;
            delete node;
            finish_closure->status().set_error(ret, "failed");
        } else {
            LOG(NOTICE) << "init node " << listen_addr;
        }

        {
            std::lock_guard<raft_mutex_t> guard(_mutex);
            _nodes.push_back(node);
            _fsms.push_back(fsm);
        }
    }
    
    int start(const base::EndPoint& listen_addr, bool empty_peers = false,
              int snapshot_interval_s = 30,
              raft::Closure* leader_start_closure = NULL) {
        if (_server_map[listen_addr] == NULL) {
            rpc::Server* server = new rpc::Server();
            rpc::ServerOptions server_options;
            server_options.use_polar = raft::FLAGS_raft_use_polar;
            if (raft::add_service(server, listen_addr) != 0
                    || server->Start(listen_addr, &server_options) != 0) {
                            // ^^^^^ do not start server in polling thread, deadlock may happen
                LOG(ERROR) << "Fail to start raft service";
                delete server;
                return -1;
            }
            LOG(NOTICE) << "Start server: " << listen_addr
                << ", FLAGS_polar_thread_num: " << baidu::hpn::polar::FLAGS_polar_thread_num
                << ", polar worker num: " << baidu::hpn::polar::poll_thread_num();
            _server_map[listen_addr] = server;
        }
        raft::SynchronizedClosure waiter;
        do {
            int thread_index = -1;
            if (raft::FLAGS_raft_use_polar) {
                std::lock_guard<raft_mutex_t> guard(_mutex);
                thread_index = _thread_index % baidu::hpn::polar::poll_thread_num();
                _thread_index++;
                CHECK_GE(thread_index, 0);
            }
            RAFT_SYNC_BTHREAD_RUN_ON_POLAR(init_node(listen_addr, empty_peers, snapshot_interval_s,
                                            leader_start_closure, &waiter), thread_index);
        } while (0);
        waiter.wait();
        if (!empty_peers) {
            check_node_ready(listen_addr);
        }
        return waiter.status().error_code();
    }


    int stop(const base::EndPoint& listen_addr) {
        bthread::CountdownEvent cond;
        raft::Node* node = remove_node(listen_addr);
        if (node) {
            raft::SynchronizedClosure done;
            RAFT_BTHREAD_RUN_ON_POLAR(node->shutdown(&done), node->polar_worker_index());
            done.wait();
            done.reset();
            RAFT_BTHREAD_RUN_ON_POLAR(node->join(&done), node->polar_worker_index());
            LOG(WARNING) << "waiting " << listen_addr << " stop";
            done.wait();
        }

        if (_server_map[listen_addr] != NULL) {
            rpc::Server* server = _server_map[listen_addr];
            server->Stop(1);
            server->Join();
            delete server;
            _server_map.erase(listen_addr);
        }
        _server_map.erase(listen_addr);
        RAFT_SYNC_BTHREAD_RUN_ON_POLAR(delete node, node->polar_worker_index());
        LOG(WARNING) << listen_addr << " stoped";
        return node ? 0 : -1;
    }

    void stop_all() {
        std::vector<base::EndPoint> addrs;
        all_nodes(&addrs);
        for (size_t i = 0; i < addrs.size(); i++) {
            stop(addrs[i]);
        }
    }

    void clean(const base::EndPoint& listen_addr) {
        std::string data_path;
        base::string_printf(&data_path, "./data/%s",
                            base::endpoint2str(listen_addr).c_str());

        if (!base::DeleteFile(base::FilePath(data_path), true)) {
            LOG(ERROR) << "delete path failed, path: " << data_path;
        }
    }

    raft::Node* leader() {
        std::lock_guard<raft_mutex_t> guard(_mutex);
        raft::Node* node = NULL;
        for (size_t i = 0; i < _nodes.size(); i++) {
            bool is_leader = false;
            int64_t term = -1;
            auto func = [&] {
                is_leader = _nodes[i]->is_leader();
                term = _nodes[i]->_impl->_current_term;
            };
            RAFT_SYNC_BTHREAD_RUN_ON_POLAR(func(), _nodes[i]->polar_worker_index());
            if (is_leader && _fsms[i]->_leader_term == term) {
                node = _nodes[i];
                break;
            }
        }
        return node;
    }

    void followers(std::vector<raft::Node*>* nodes) {
        nodes->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            bool is_leader;
            auto func = [&] {
                is_leader = _nodes[i]->is_leader();
            };
            RAFT_SYNC_BTHREAD_RUN_ON_POLAR(func(), _nodes[i]->polar_worker_index());
            if (!is_leader) {
                nodes->push_back(_nodes[i]);
            }
        }
    }

    void all_nodes(std::vector<raft::Node*>* nodes) {
        nodes->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            nodes->push_back(_nodes[i]);
        }
    }

    raft::Node* find_node(const raft::PeerId& peer_id) {
        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            if (peer_id == _nodes[i]->node_id().peer_id) {
                return _nodes[i];
            }
        }
        return NULL;
    }

    void wait_leader() {
        while (true) {
            raft::Node* node = leader();
            if (node) {
                return;
            } else {
                usleep(100 * 1000);
            }
        }
    }

    void check_node_status() {
        std::vector<raft::Node*> nodes;
        {
            std::lock_guard<raft_mutex_t> guard(_mutex);
            for (size_t i = 0; i < _nodes.size(); i++) {
                nodes.push_back(_nodes[i]);
            }
        }
        for (const auto& node : _nodes) {
            raft::NodeStatus status;
            bool is_leader;
            int round = 0;
            while (round < 100) {
                // since on leader start and on leader stop executed in execution queue
                // is_leader may not exact what use see
                // wait 10s
                auto func = [&] {
                    node->get_status(&status);
                    is_leader = node->is_leader();
                };
                RAFT_SYNC_BTHREAD_RUN_ON_POLAR(func(), node->polar_worker_index());
                if ((is_leader && (status.state == raft::STATE_LEADER)) ||
                            ((!is_leader) && (status.state != raft::STATE_LEADER))) {

                    break;
                }
                bthread_usleep(100 * 1000);
                round ++;
            }
            if (is_leader) {
                ASSERT_EQ(status.state, raft::STATE_LEADER) << node->node_id();
            } else {
                ASSERT_NE(status.state, raft::STATE_LEADER) << node->node_id();
                ASSERT_EQ(status.stable_followers.size(), 0u) << node->node_id();
            }
        }
    }

    void check_node_ready(const base::EndPoint addr, int wait_num = 5) {
        do {
            raft::Node* ld = leader();
            if (ld == NULL) {
                return;
            }
            std::vector<raft::PeerId> peers;
            RAFT_SYNC_BTHREAD_RUN_ON_POLAR(ld->list_peers(&peers), ld->polar_worker_index());
            for (auto& peer : peers) {
                if (peer.addr == addr) {
                    return;
                }
            }
            bthread_usleep(1000L * 1000);
            wait_num--;
        } while (wait_num >= 0);
        LOG(WARNING) << "failed to check_node_ready, not found " << addr;
    }
    void check_node_ready(const raft::PeerId peer) {
        check_node_ready(peer.addr);
    }

    void ensure_leader(const base::EndPoint& expect_addr) {
CHECK:
        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            raft::PeerId leader_id;
            auto func = [&] {
                leader_id = _nodes[i]->leader_id();
            };
            RAFT_SYNC_BTHREAD_RUN_ON_POLAR(func(), _nodes[i]->polar_worker_index());
            if (leader_id.addr != expect_addr) {
                goto WAIT;
            }
        }

        return;
WAIT:
        usleep(100 * 1000);
        goto CHECK;
    }

    bool check_fsm() {
        bool ret = true;
        MockFSM* first = _fsms[0];
        std::unique_lock<MockFSM> first_lock(*first);
        for (size_t i = 1; ret && i < _fsms.size(); i++) {
            MockFSM* fsm = _fsms[i];
            std::unique_lock<MockFSM> fsm_lock(*fsm);
            if (first->logs.size() != fsm->logs.size()) {
                LOG(INFO) << "logs size not match, "
                        << " addr: " << first->address << " vs "
                        << fsm->address << ", log num "
                        << first->logs.size() << " vs " << fsm->logs.size();
                ret = false;
                break;
            }
            for (size_t j = 0; ret && j < first->logs.size(); j++) {
                base::IOBuf& first_data = first->logs[j];
                base::IOBuf& fsm_data = fsm->logs[j];
                if (first_data.to_string() != fsm_data.to_string()) {
                    LOG(INFO) << "log data of index=" << j << " not match, "
                            << " addr: " << first->address << " vs "
                            << fsm->address << ", data ("
                            << first_data.to_string() << ") vs " 
                            << fsm_data.to_string() << ")";
                    ret = false;
                    break;
                }
            }
        }
        return ret;
    }

    bool ensure_same(int wait_time_s = -1) {
        {
            std::unique_lock<raft_mutex_t> guard(_mutex);
            if (_fsms.size() <= 1) {
                return true;
            }
            LOG(INFO) << "_fsms.size()=" << _fsms.size();

            int nround = 0;
            bool check = true;
            do {
                if (check == false) {
                    bthread_usleep(1000UL * 1000 * 1);
                    ++nround;
                    if (wait_time_s > 0 && nround > wait_time_s) {
                        return false;
                    }
                    LOG(WARNING) << "check fsm failed, retry " << nround;
                }
                check = check_fsm();
            } while (check == false);
        }
        // check success
        check_node_status();
        LOG(NOTICE) << "check fsm success";
        return true;
    }

private:
    void all_nodes(std::vector<base::EndPoint>* addrs) {
        addrs->clear();

        std::lock_guard<raft_mutex_t> guard(_mutex);
        for (size_t i = 0; i < _nodes.size(); i++) {
            addrs->push_back(_nodes[i]->node_id().peer_id.addr);
        }
    }

    raft::Node* remove_node(const base::EndPoint& addr) {
        std::lock_guard<raft_mutex_t> guard(_mutex);

        // remove node
        raft::Node* node = NULL;
        std::vector<raft::Node*> new_nodes;
        for (size_t i = 0; i < _nodes.size(); i++) {
            if (addr.port == _nodes[i]->node_id().peer_id.addr.port) {
                node = _nodes[i];
            } else {
                new_nodes.push_back(_nodes[i]);
            }
        }
        _nodes.swap(new_nodes);

        // remove fsm
        std::vector<MockFSM*> new_fsms;
        for (size_t i = 0; i < _fsms.size(); i++) {
            if (_fsms[i]->address != addr) {
                new_fsms.push_back(_fsms[i]);
            }
        }
        _fsms.swap(new_fsms);

        return node;
    }

    std::string _name;
    std::vector<raft::PeerId> _peers;
    std::vector<raft::Node*> _nodes;
    std::vector<MockFSM*> _fsms;
    std::map<base::EndPoint, rpc::Server*> _server_map;
    int32_t _election_timeout_ms;
    int32_t _max_clock_drift_ms;
    bool _use_two_stages;
    raft_mutex_t _mutex;
    scoped_refptr<raft::SnapshotThrottle> _throttle;
    int _thread_index;
};

class MockRaftService : public raft::RaftServiceImpl {
public:
    MockRaftService() : raft::RaftServiceImpl(base::EndPoint()), _error(0), _us_sleep(0) {}
    void append_entries(google::protobuf::RpcController* controller,
            const raft::AppendEntriesRequest* request,
            raft::AppendEntriesResponse* response, google::protobuf::Closure* done) override {
        rpc::ClosureGuard done_guard(done);
        rpc::Controller* cntl =
        static_cast<rpc::Controller*>(controller);
        if (request->entries_size() != 0) {
            // only mock send entries reponse error, since we meet deadlock here
            if (_error != 0) {
                LOG(ERROR) << "mock error " << _error;
                cntl->SetFailed(_error, "busy");
                return;
            }
            if (_us_sleep > 0) {
                bthread_usleep(_us_sleep);
            }
        }
        raft::RaftServiceImpl::append_entries(controller, request, response, done_guard.release());
    }
private:
    int32_t _error;
    uint64_t _us_sleep;
};

#endif // ~PUBLIC_RAFT_TEST_UTIL_H
