// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Jingwei Ma (majingwei@baidu.com)
// Date: 2024/05/30 17:03:46

#include <gtest/gtest.h>
#include <base/string_printf.h>
#include <base/memory/scoped_ptr.h>
#include "raft/fsm_caller.h"
#include "raft/raft.h"
#include "raft/log.h"
#include "raft/configuration.h"
#include "raft/log_manager.h"
#include "raft/io_uring.h"
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
DECLARE_bool(raft_trace_append_entry_latency);
DECLARE_bool(raft_use_polar);
DECLARE_bool(raft_use_iouring);
}

class FSMCallerTest : public testing::Test {
protected:
    void SetUp() {
        base::EndPoint addr;
        generate_random_addr(&addr);
        raft::FLAGS_raft_sync = false;
        raft::FLAGS_raft_use_polar = true;
        baidu::hpn::polar::FLAGS_lcore_mask = "0x0F0000";
        baidu::hpn::polar::FLAGS_host_ip = base::ip2str(addr.ip).c_str();
        baidu::hpn::polar::FLAGS_server_ports_list = std::to_string(addr.port);
        baidu::hpn::polar::FLAGS_client_ports_start = 9100;
        baidu::hpn::polar::FLAGS_client_ports_end = 9200;
        baidu::hpn::polar::FLAGS_client_ports_num = 50;
        baidu::hpn::polar::FLAGS_socket_mem = "512,512";
        rpc::ServerOptions server_options;
        server_options.use_polar = raft::FLAGS_raft_use_polar;
        ASSERT_EQ(_server.Start(addr, &server_options), 0);
        google::SetCommandLineOption("crash_on_fatal_log", "true");
        google::SetCommandLineOption("min_log_level", "0");
        google::SetCommandLineOption("verbose", "99");
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
    }
    void TearDown() {}
private:
    rpc::Server _server;
};

class OrderedStateMachine : public raft::StateMachine {
public:
    OrderedStateMachine() 
        : _expected_next(0)
        , _stopped(false)
        , _on_leader_start_times(0)
        , _on_leader_stop_times(0)
        , _on_snapshot_save_times(0)
        , _on_snapshot_load_times(0)
    {}
    void on_apply(raft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            std::string expected;
            base::string_printf(&expected, "hello_%lu", _expected_next++);
            ASSERT_EQ(expected, iter.data().to_string());
            if (iter.done()) {
                ASSERT_TRUE(iter.done()->status().ok()) << "index=" << iter.index();
                iter.done()->Run();
            }
        }
    }
    void on_shutdown() {
        _stopped = true;
    }
    void on_snapshot_save(raft::SnapshotWriter* /*writer*/, raft::Closure* done) {
        ++_on_snapshot_save_times;
        done->Run();
    }
    void on_snapshot_load(raft::SnapshotReader* /*reader*/, raft::Closure* done) {
        ++_on_snapshot_load_times;
        done->Run();
    }
    void on_leader_start() {
        _on_leader_start_times++;
    }
    void on_leader_stop() {
        _on_leader_stop_times++;
    }
    void join() {
        while (!_stopped) {
            bthread_usleep(100);
        }
    }
private:
    uint64_t _expected_next;
    bool _stopped;
    int _on_leader_start_times;
    int _on_leader_stop_times;
    int _on_snapshot_save_times;
    int _on_snapshot_load_times;
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

TEST_F(FSMCallerTest, sanity) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    const size_t N = 1000;
    SyncClosure c[N];
    raft::ClosureQueue* cq = NULL;
    auto cq_init = [&] { cq = new raft::ClosureQueue(false); };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cq_init(), 0);
    raft::FSMCaller* caller = new raft::FSMCaller;
    OrderedStateMachine fsm;
    fsm._expected_next = 0;
    scoped_refptr<raft::LogManager> lm;
    auto l = [&] {
        lm = new raft::LogManager();
        raft::LogManagerOptions log_opt;
        log_opt.log_storage = storage.get();
        log_opt.configuration_manager = cm.get();
        ASSERT_EQ(0, lm->init(log_opt));

        raft::FSMCallerOptions opt;
        opt.log_manager = lm.get();
        opt.after_shutdown = NULL;
        opt.fsm = &fsm;
        opt.closure_queue = cq;

        ASSERT_EQ(0, caller->init(opt));

        for (size_t i = 0; i < N; ++i) {
            std::vector<raft::LogEntry*> entries;
            raft::LogEntry* entry = new raft::LogEntry;
            entry->AddRef();
            entry->type = raft::ENTRY_TYPE_DATA;
            std::string buf;
            base::string_printf(&buf, "hello_%lu", i);
            entry->data.append(buf);
            entry->id.index = i + 1;
            entry->id.term = i;
            entries.push_back(entry);
            lm->append_entries(&entries, &c[i]);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    for (auto& cc : c) {
        cc.join();
        ASSERT_TRUE(cc.status().ok()) << cc.status();
    }
    SyncClosure c2;
    auto l2 = [&] {
        cq->reset_first_index(N);
        cq->append_pending_closure(&c2);
        ASSERT_EQ(0, caller->on_committed(N));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    c2.join();
    auto l3 = [&] {
        ASSERT_EQ(0, caller->shutdown());
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), 0);
    fsm.join();
    ASSERT_EQ(fsm._expected_next, N);
    raft::SynchronizedClosure done;
    auto release = [&] {
        lm = nullptr;
        delete caller;
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(release)), 0);
    done.wait();
    auto cq_delete = [=] { delete cq; };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cq_delete(), 0);
}

TEST_F(FSMCallerTest, get_entry_fail) {
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    const size_t N = 500;
    SyncClosure c[N];
    raft::ClosureQueue* cq = NULL;
    auto cq_init = [&] { cq = new raft::ClosureQueue(false); };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cq_init(), 0);
    raft::FSMCaller* caller = new raft::FSMCaller;
    OrderedStateMachine fsm;
    fsm._expected_next = 0;
    scoped_refptr<raft::LogManager> lm;
    auto l = [&] {
        lm = new raft::LogManager();
        raft::LogManagerOptions log_opt;
        log_opt.log_storage = storage.get();
        log_opt.configuration_manager = cm.get();
        ASSERT_EQ(0, lm->init(log_opt));

        raft::FSMCallerOptions opt;
        opt.log_manager = lm.get();
        opt.after_shutdown = NULL;
        opt.fsm = &fsm;
        opt.closure_queue = cq;

        ASSERT_EQ(0, caller->init(opt));

        for (size_t i = 0; i < N; ++i) {
            std::vector<raft::LogEntry*> entries;
            raft::LogEntry* entry = new raft::LogEntry;
            entry->AddRef();
            entry->type = raft::ENTRY_TYPE_DATA;
            std::string buf;
            base::string_printf(&buf, "hello_%lu", i);
            entry->data.append(buf);
            entry->id.index = i + 1;
            entry->id.term = i;
            entries.push_back(entry);
            lm->append_entries(&entries, &c[i]);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    for (auto& cc : c) {
        cc.join();
        ASSERT_TRUE(cc.status().ok()) << cc.status();
    }
    SyncClosure c2;
    auto l2 = [&] {
        cq->reset_first_index(N);
        cq->append_pending_closure(&c2);
        ASSERT_EQ(0, caller->on_committed(N));
    };

    // make fail when get_entry
    storage->_first_log_index.store(storage->_last_log_index.load() + 1);

    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    c2.join();
    auto l3 = [&] {
        ASSERT_EQ(0, caller->shutdown());
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), 0);
    fsm.join();
    raft::SynchronizedClosure done;
    auto release = [&] {
        lm = nullptr;
        delete caller;
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(release)), 0);
    done.wait();
    auto cq_delete = [=] { delete cq; };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cq_delete(), 0);
}

TEST_F(FSMCallerTest, on_leader_start_and_stop) {
    scoped_refptr<raft::LogManager> lm;
    OrderedStateMachine fsm;
    fsm._expected_next = 0;
    raft::ClosureQueue* cq = NULL;
    auto cq_init = [&] { cq = new raft::ClosureQueue(false); };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cq_init(), 0);
    raft::FSMCallerOptions opt;
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = cq;
    raft::FSMCaller* caller = new raft::FSMCaller;
    auto l = [&] {
        lm = new raft::LogManager();
        opt.log_manager = lm.get();
        ASSERT_EQ(0, caller->init(opt));
        base::Status status;
        caller->_leader_start_notified = true;
        ASSERT_EQ(0, caller->on_leader_stop(status));
        caller->shutdown();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    fsm.join();
    ASSERT_EQ(0, fsm._on_leader_start_times);
    ASSERT_EQ(1, fsm._on_leader_stop_times);
    ASSERT_FALSE(caller->_leader_start_notified);
    raft::SynchronizedClosure done;
    auto release = [&] {
        lm = nullptr;
        delete caller;
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(release)), 0);
    done.wait();
    auto cq_delete = [=] { delete cq; };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cq_delete(), 0);
}

class DummySnapshotReader : public raft::SnapshotReader {
public:
    DummySnapshotReader(raft::SnapshotMeta* meta) 
        : _meta(meta)
    {
    };
    ~DummySnapshotReader() {}
    std::string generate_uri_for_copy() { return std::string(); }
    void list_files(std::vector<std::string>*) {}
    int get_file_meta(const std::string&, google::protobuf::Message*) { return 0; }
    std::string get_path() { return std::string(); }
    int load_meta(raft::SnapshotMeta* meta) {
        *meta = *_meta;
        return 0;
    }
private:
    raft::SnapshotMeta* _meta;
};

class DummySnapshoWriter : public raft::SnapshotWriter {
public:
    DummySnapshoWriter() {}
    ~DummySnapshoWriter() {}
    int save_meta(const raft::SnapshotMeta&) {
        EXPECT_TRUE(false) << "Should never be called";
        return 0;
    }
    std::string get_path() { return std::string(); }
    int add_file(const std::string&, const google::protobuf::Message*) { return 0;}
    int remove_file(const std::string&) { return 0; }
    void list_files(std::vector<std::string>*) {}
    int get_file_meta(const std::string&, google::protobuf::Message*) { return 0; }
private:
};

class MockSaveSnapshotClosure : public raft::SaveSnapshotClosure {
public:
    MockSaveSnapshotClosure(raft::SnapshotWriter* writer, 
                            raft::SnapshotMeta *expected_meta) 
        : _start_times(0)
        , _writer(writer)
        , _expected_meta(expected_meta)
    {
    }
    ~MockSaveSnapshotClosure() {}
    void Run() {
        ASSERT_TRUE(status().ok()) << status();
    }
    raft::SnapshotWriter* start(const raft::SnapshotMeta& meta) {
        EXPECT_EQ(meta.last_included_index(), 
                    _expected_meta->last_included_index());
        EXPECT_EQ(meta.last_included_term(), 
                    _expected_meta->last_included_term());
        ++_start_times;
        return _writer;
    }
private:
    int _start_times;
    raft::SnapshotWriter* _writer;
    raft::SnapshotMeta* _expected_meta;
};

class MockLoadSnapshotClosure : public raft::LoadSnapshotClosure {
public:
    MockLoadSnapshotClosure(raft::SnapshotReader* reader)
        : _start_times(0)
        , _reader(reader)
    {}
    ~MockLoadSnapshotClosure() {}
    void Run() {
        ASSERT_TRUE(status().ok()) << status();
    }
    raft::SnapshotReader* start() {
        ++_start_times;
        return _reader;
    }
private:
    int _start_times;
    raft::SnapshotReader* _reader;
};

TEST_F(FSMCallerTest, snapshot) {
    raft::SnapshotMeta snapshot_meta;
    snapshot_meta.set_last_included_index(0);
    snapshot_meta.set_last_included_term(0);
    DummySnapshotReader dummy_reader(&snapshot_meta);
    DummySnapshoWriter dummy_writer;
    MockSaveSnapshotClosure save_snapshot_done(&dummy_writer, &snapshot_meta);
    system("rm -rf ./data");
    scoped_ptr<raft::ConfigurationManager> cm(
                                new raft::ConfigurationManager);
    scoped_ptr<raft::SegmentLogStorage> storage(
                                new raft::SegmentLogStorage("./data"));
    scoped_refptr<raft::LogManager> lm;
    raft::LogManagerOptions log_opt;
    log_opt.log_storage = storage.get();
    log_opt.configuration_manager = cm.get();

    OrderedStateMachine fsm;
    fsm._expected_next = 0;
    raft::ClosureQueue* cq = NULL;
    auto cq_init = [&] { cq = new raft::ClosureQueue(false); };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cq_init(), 0);
    raft::FSMCallerOptions opt;
    opt.after_shutdown = NULL;
    opt.fsm = &fsm;
    opt.closure_queue = cq;
    MockLoadSnapshotClosure load_snapshot_done(&dummy_reader);
    raft::FSMCaller* caller = new raft::FSMCaller;
    auto l = [&] {
        lm = new raft::LogManager();
        ASSERT_EQ(0, lm->init(log_opt));
        opt.log_manager = lm.get();
        ASSERT_EQ(0, caller->init(opt));
        ASSERT_EQ(0, caller->on_snapshot_save(&save_snapshot_done));
        ASSERT_EQ(0, caller->on_snapshot_load(&load_snapshot_done));
        ASSERT_EQ(0, caller->shutdown());
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    fsm.join();
    ASSERT_EQ(1, fsm._on_snapshot_save_times);
    ASSERT_EQ(1, fsm._on_snapshot_load_times);
    ASSERT_EQ(1, save_snapshot_done._start_times);
    ASSERT_EQ(1, load_snapshot_done._start_times);
    raft::SynchronizedClosure done;
    auto release = [&] {
        lm = nullptr;
        delete caller;
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(release)), 0);
    done.wait();
    auto cq_delete = [=] { delete cq; };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cq_delete(), 0);
}

