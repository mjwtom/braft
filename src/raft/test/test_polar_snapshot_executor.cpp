// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2016/07/27 20:13:42

#include <gtest/gtest.h>
#include <base/atomicops.h>
#include "baidu/hpn/rpc/polar/polar_helper.h"
#include "raft/snapshot_executor.h"
#include "raft/fsm_caller.h"
#include "raft/util.h"
#include "raft/raft.h"
#include "raft/file_service.h"
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
DECLARE_int32(polar_thread_num);
}
}
}
namespace raft {

class MockFileServiceImpl : public FileServiceImpl {
public:
    MockFileServiceImpl() : _get_file_called(false) {}
    void get_file(::google::protobuf::RpcController* controller,
                  const ::raft::GetFileRequest* request,
                  ::raft::GetFileResponse* response,
                  ::google::protobuf::Closure* done) {
        LOG(NOTICE) << "get_file, reader id: " << request->reader_id();
        if (!_get_file_called) {
            done->Run();
            _get_file_called = true;
        }
        // rpc::ClosureGuard done_guard(done);
        // FileServiceImpl::get_file(controller, request, response, done_guard.release());
    }

    std::atomic<bool> _get_file_called;
};

class SnapshotExecutorTest : public testing::Test {
protected:
    void SetUp() {
        system("rm -rf .data");
        base::EndPoint addr;
        generate_random_addr(&addr);
        google::SetCommandLineOption("crash_on_fatal_log", "true");
        google::SetCommandLineOption("verbose", "99");
        google::SetCommandLineOption("min_log_level", "0");
        FLAGS_raft_use_polar = true;
        baidu::hpn::polar::FLAGS_lcore_mask = "0x0F0000";
        baidu::hpn::polar::FLAGS_host_ip = base::ip2str(addr.ip).c_str();
        baidu::hpn::polar::FLAGS_server_ports_list = std::to_string(addr.port);
        baidu::hpn::polar::FLAGS_client_ports_start = 9100;
        baidu::hpn::polar::FLAGS_client_ports_end = 9200;
        baidu::hpn::polar::FLAGS_client_ports_num = 50;
        baidu::hpn::polar::FLAGS_socket_mem = "512,512";
        baidu::hpn::polar::FLAGS_polar_thread_num = 1;
        baidu::hpn::rpc::polar::GlobalPolarInitializeOrDie();
        ASSERT_EQ(0, raft::add_service(&_server, addr));
        ASSERT_EQ(_server.RemoveService(&_file_service), 0);
        ASSERT_EQ(_server.AddService(&_file_service, rpc::SERVER_DOESNT_OWN_SERVICE), 0);
        rpc::ServerOptions server_options;
        server_options.use_polar = raft::FLAGS_raft_use_polar;
        ASSERT_EQ(0, _server.Start(addr, &server_options));
    }
    void TearDown() {
        _server.Stop(0);
        _server.Join();
    }
    MockFileServiceImpl _file_service;
    rpc::Server _server;
};

class MockFSMCaller : public raft::FSMCaller {
protected:
    RAFT_MOCK int on_committed(int64_t /*committed_index*/) { return 0; }
    RAFT_MOCK int on_snapshot_load(LoadSnapshotClosure* done) {
        _snapshot_load_times.fetch_add(1);
        raft::raft_run_closure_background(done);
        return 0;
    }
    RAFT_MOCK int on_snapshot_save(SaveSnapshotClosure* done) {
        _snapshot_save_times.fetch_add(1);
        raft::raft_run_closure_background(done);
        return 0;
    }
    RAFT_MOCK int on_error(const Error& /*e*/) {
        _on_error_times.fetch_add(1);
        return 0;
    }
    base::atomic<int> _on_error_times;
    base::atomic<int> _snapshot_load_times;
    base::atomic<int> _snapshot_save_times;
};

class MockLogManager : public raft::LogManager {
protected:
    // Notify the log manager about the latest snapshot, which indicates the
    // logs which can be safely truncated.
    RAFT_MOCK void set_snapshot(const SnapshotMeta* /*meta*/,
                                const int64_t first_snapshot_index) {
        _set_times.fetch_add(1);
    }

    // We don't delete all the logs before last snapshot to avoid installing
    // snapshot on slow replica. Call this method to drop all the logs before
    // last snapshot immediately.
    RAFT_MOCK void clear_bufferred_logs() {
        _clear_timers.fetch_add(1);
    }

    base::atomic<int> _set_times;
    base::atomic<int> _clear_timers;
};

class MockSnapshotReader : public raft::SnapshotReader {
public:
    MockSnapshotReader(const std::string& path)
        : _path(path)
    {}
    virtual ~MockSnapshotReader() {}

    // Load meta from 
    virtual int load_meta(SnapshotMeta* meta) {
        return 0;
    }

    // Generate uri for other peers to copy this snapshot.
    // Return an empty string if some error has occcured
    virtual std::string generate_uri_for_copy() {
        return "remote://ip:port/reader_id";    
    }

    void list_files(std::vector<std::string> *files) {
        return;
    }

    virtual std::string get_path() { return _path; }

private:
    std::string _path;
};

class MockSnapshotStorage;

class MockSnapshotCopier : public raft::SnapshotCopier {
friend class MockSnapshotStorage;
public:
    MockSnapshotCopier();
    virtual ~MockSnapshotCopier() {}
    // Cancel the copy job
    virtual void cancel();
    // Block the thread until this copy job finishes, or some error occurs.
    virtual void join();
    // Get the the SnapshotReader which represents the copied Snapshot
    virtual SnapshotReader* get_reader();
    // Show Copy Progress
    virtual int percent() { return 0; }

    void start();
    
    static void* start_copy(void* arg);
    
private:
    bthread_t _tid;
    MockSnapshotStorage* _storage;
    SnapshotReader* _reader;
};

class MockSnapshotStorage : public raft::SnapshotStorage {
friend class MockSnapshotCopier;
public:
    MockSnapshotStorage(const std::string& path)
        : _path(path)
        , _last_snapshot_index(0)
    {}

    virtual ~MockSnapshotStorage() {}

    // Initialize
    virtual int init() { return 0; }

    // create new snapshot writer
    virtual SnapshotWriter* create() {
        return NULL; 
    }

    // close snapshot writer
    virtual int close(SnapshotWriter* writer) {
        return 0;
    }

    // get lastest snapshot reader
    virtual SnapshotReader* open() { 
        MockSnapshotReader* reader = new MockSnapshotReader(_path);
        return reader; 
    }

    // close snapshot reader
    virtual int close(SnapshotReader* reader) { 
        delete reader;
        return 0; 
    }

    // Copy snapshot from uri and open it as a SnapshotReader
    virtual SnapshotReader* copy_from(const std::string& uri) {
        return NULL;
    }

    virtual SnapshotCopier* start_to_copy_from(const std::string& uri, Closure* done = nullptr) {
        MockSnapshotCopier* copier = new MockSnapshotCopier();
        copier->_storage = this;
        copier->start();
        return copier;
    }

    virtual int close(SnapshotCopier* copier) {
        delete copier;
        return 0;
    }

    virtual SnapshotStorage* new_instance(const std::string& uri) const {
        return NULL;
    }
    
    virtual base::Status gc_instance(const std::string& uri) const {
        return base::Status::OK();
    }
    
private:
    std::string _path;
    int64_t _last_snapshot_index;
};

MockSnapshotCopier::MockSnapshotCopier() 
    : _tid(INVALID_BTHREAD)
    , _storage(NULL)
    , _reader(NULL)
{}

void MockSnapshotCopier::cancel() {}

void MockSnapshotCopier::join() {
    bthread_join(_tid, NULL);
}

SnapshotReader* MockSnapshotCopier::get_reader() { return _reader; }

void MockSnapshotCopier::start() {
    LOG(INFO) << "In MockSnapshotCopier::start()"; 
    _reader = _storage->open();
    if (raft_start_background(
                &_tid, NULL, start_copy, this) != 0) {
        LOG(INFO) << "Fail to start bthread."; 
    } 
}

void* MockSnapshotCopier::start_copy(void* arg) {
    usleep(5 * 1000 * 1000);
    return NULL;
}

void write_file(const std::string& file, const std::string& content) {
    base::ScopedFILE fp(fopen(file.c_str(), "w"));
    ASSERT_TRUE(fp) << berror();
    fprintf(fp.get(), "%s", content.c_str());
}

std::string read_file(const std::string& file) {
    base::ScopedFILE fp(fopen(file.c_str(), "r"));
    char buf[1024];
    fscanf(fp.get(), "%s", buf);
    return buf;
}

class SyncClosure : public google::protobuf::Closure {
protected:
    SyncClosure() {
    }
    void Run() {
        _event.signal();
    }
    void wait() {
        _event.wait();
    }
    bthread::CountdownEvent _event;
};

struct InstallArg {
    SnapshotExecutor* e;
    InstallSnapshotRequest request;
    InstallSnapshotResponse response;
    rpc::Controller cntl;
    SyncClosure done;
};

void* install_thread(void* arg) {
    InstallArg* ia = (InstallArg*)arg;
    ia->e->install_snapshot(
            &ia->cntl, &ia->request, &ia->response, &ia->done);
    return NULL;
}

TEST_F(SnapshotExecutorTest, DISABLED_retry_request) {
    MockFSMCaller fsm_caller;
    MockLogManager* log_manager = new MockLogManager;
    SnapshotExecutorOptions options;
    options.init_term = 1;
    options.addr = _server.listen_address();
    options.node = NULL;
    options.fsm_caller = &fsm_caller;
    options.log_manager = log_manager;
    options.uri = "local://.data/snapshot0";
    SnapshotExecutor executor;
    ASSERT_EQ(0, executor.init(options));
    LocalSnapshotStorage storage1(".data/snapshot1");
    storage1.set_server_addr(_server.listen_address());
    ASSERT_EQ(0, storage1.init());
    SnapshotWriter* writer = storage1.create();
    ASSERT_TRUE(writer);
    std::string file_path = writer->get_path() + "/data";
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "dd if=/dev/zero of=%s bs=1M count=128",
             file_path.c_str());
    ASSERT_EQ(0, system(cmd));
    writer->add_file("data");
    SnapshotMeta meta;
    meta.set_last_included_index(1);
    meta.set_last_included_term(1);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage1.close(writer));
    SnapshotReader* reader= storage1.open();
    std::string uri = reader->generate_uri_for_copy();
    const size_t N = 10;
    bthread_t tids[N];
    InstallArg args[N];
    for (size_t i = 0; i < N; ++i) {
        args[i].e = &executor;
        args[i].request.set_group_id("test");
        args[i].request.set_term(1);
        args[i].request.mutable_meta()->CopyFrom(meta);
        args[i].request.set_uri(uri);
    }
    for (size_t i = 0; i < N; ++i) {
        raft_start_background(&tids[i], NULL, install_thread, &args[i]);
    }
    for (size_t i = 0; i < N; ++i) {
        bthread_join(tids[i], NULL);
    }
    size_t suc = 0;
    for (size_t i = 0; i < N; ++i) {
        suc += !args[i].cntl.Failed();
        if (args[i].cntl.Failed()) {
            ASSERT_EQ(EINTR, args[i].cntl.ErrorCode());
        }
    }
    ASSERT_EQ(0, storage1.close(reader));
    ASSERT_EQ(1u, suc);
    reader = executor.snapshot_storage()->open();
    ASSERT_EQ(0, reader->get_file_meta("data", NULL));
    ASSERT_EQ(0, executor.snapshot_storage()->close(reader));
}

TEST_F(SnapshotExecutorTest, interrupt_installing) {
    SnapshotExecutor* executor = nullptr;
    InstallArg arg;
    LocalSnapshotStorage storage1(".data/snapshot1");
    SnapshotReader* reader = nullptr;
    auto l = [&] {
        executor = new SnapshotExecutor;
        ASSERT_NE(executor, nullptr);
        MockFSMCaller fsm_caller;
        MockLogManager* log_manager = new MockLogManager;
        SnapshotExecutorOptions options;
        options.init_term = 1;
        options.addr = _server.listen_address();
        options.node = NULL;
        options.fsm_caller = &fsm_caller;
        options.log_manager = log_manager;
        options.uri = "local://.data/snapshot0";
        ASSERT_EQ(0, executor->init(options));
        storage1.set_server_addr(_server.listen_address());
        ASSERT_EQ(0, storage1.init());
        SnapshotWriter* writer = storage1.create();
        ASSERT_TRUE(writer);
        std::string file_path = writer->get_path() + "/data";
        char cmd[1024];
        snprintf(cmd, sizeof(cmd), "dd if=/dev/zero of=%s bs=1M count=128",
                 file_path.c_str());
        ASSERT_EQ(0, system(cmd));
        writer->add_file("data");
        SnapshotMeta meta;
        meta.set_last_included_index(1);
        meta.set_last_included_term(1);
        ASSERT_EQ(0, writer->save_meta(meta));
        ASSERT_EQ(0, storage1.close(writer));
        reader = storage1.open();
        std::string uri = reader->generate_uri_for_copy();
        arg.e = executor;
        arg.request.set_group_id("test");
        arg.request.set_term(1);
        arg.request.mutable_meta()->CopyFrom(meta);
        LOG(NOTICE) << "generate uri: " << uri;
        arg.request.set_uri(uri);
        executor->install_snapshot(&arg.cntl, &arg.request,
                &arg.response, &arg.done);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
    while (!_file_service._get_file_called) {
        bthread_usleep(100);
    }
    auto l2 = [&] {
        executor->interrupt_downloading_snapshot(2);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
    arg.done.wait();
    ASSERT_TRUE(arg.cntl.Failed());
    if (arg.cntl.Failed()) {
        LOG(ERROR) << "error: " << arg.cntl.ErrorText();
    } else {
        LOG(INFO) << "success.";
    }
    ASSERT_EQ(ECANCELED, arg.cntl.ErrorCode());
    ASSERT_EQ(0, storage1.close(reader));
    SyncClosure done;
    auto release = [&] {
        delete executor;
        executor = nullptr;
        done.Run();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(release)), 0);
    done.wait();
}

TEST_F(SnapshotExecutorTest, DISABLED_retry_install_snapshot) {
    MockFSMCaller fsm_caller;
    MockLogManager* log_manager = new MockLogManager;

    SnapshotExecutorOptions options;
    options.init_term = 1;
    options.addr = _server.listen_address();
    options.node = NULL;
    options.fsm_caller = &fsm_caller;
    options.log_manager = log_manager;
    
    SnapshotExecutor executor;
    executor._log_manager = options.log_manager;
    executor._fsm_caller = options.fsm_caller;
    executor._node = options.node;
    executor._term = options.init_term;
    executor._usercode_in_pthread = options.usercode_in_pthread;
    MockSnapshotStorage* storage0 = new MockSnapshotStorage(".data/snapshot0");
    executor._snapshot_storage = storage0;

    // target snapshot_storage
    MockSnapshotStorage storage1(".data/snapshot1");
    
    SnapshotMeta meta;
    meta.set_last_included_index(1);
    meta.set_last_included_term(1);
    SnapshotReader* reader= storage1.open();
    std::string uri = reader->generate_uri_for_copy();    
    // using bthreads to simulate install_snapshot requests
    const size_t N = 10;
    bthread_t tids[N];
    InstallArg args[N];
    for (size_t i = 0; i < N; ++i) {
        args[i].e = &executor;
        args[i].request.set_group_id("test");
        args[i].request.set_term(1);
        args[i].request.mutable_meta()->CopyFrom(meta);
        args[i].request.set_uri(uri);
    }
    for (size_t i = 0; i < N; ++i) {
        raft_start_background(&tids[i], NULL, install_thread, &args[i]);
    }
    for (size_t i = 0; i < N; ++i) {
        bthread_join(tids[i], NULL);
    }
    size_t suc = 0;
    for (size_t i = 0; i < N; ++i) {
        LOG(INFO) << "Try number: " << i << "------------------------"; 
        if (args[i].cntl.Failed()) {
            LOG(ERROR) << "Result, Error: " << args[i].cntl.ErrorText();
        } else {
            suc += 1;
            LOG(INFO) << "Result, Success.";
        }
    }
    ASSERT_EQ(1, suc);
    ASSERT_EQ(0, storage1.close(reader));
}

TEST_F(SnapshotExecutorTest, DISABLED_retry_request_with_throttle) {
    MockFSMCaller fsm_caller;
    MockLogManager* log_manager = new MockLogManager;
    SnapshotExecutorOptions options;
    options.init_term = 1;
    options.addr = _server.listen_address();
    options.node = NULL;
    options.fsm_caller = &fsm_caller;
    options.log_manager = log_manager;
    options.uri = "local://.data/snapshot0";

    int64_t throttle_throughput_bytes = 100 * 1024 * 1024;
    int64_t check_cycle = 10;
    raft::ThroughputSnapshotThrottle* throttle = 
        new raft::ThroughputSnapshotThrottle(throttle_throughput_bytes, check_cycle);
    scoped_refptr<raft::SnapshotThrottle> tst(throttle);
    options.snapshot_throttle = tst;

    SnapshotExecutor executor;
    ASSERT_EQ(0, executor.init(options));
    LocalSnapshotStorage storage1(".data/snapshot1");
    storage1.set_server_addr(_server.listen_address());
    ASSERT_EQ(0, storage1.init());
    SnapshotWriter* writer = storage1.create();
    ASSERT_TRUE(writer);
    std::string file_path = writer->get_path() + "/data";
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "dd if=/dev/zero of=%s bs=1M count=128",
             file_path.c_str());
    ASSERT_EQ(0, system(cmd));
    writer->add_file("data");
    SnapshotMeta meta;
    meta.set_last_included_index(1);
    meta.set_last_included_term(1);
    ASSERT_EQ(0, writer->save_meta(meta));
    ASSERT_EQ(0, storage1.close(writer));
    SnapshotReader* reader= storage1.open();
    std::string uri = reader->generate_uri_for_copy();
    const size_t N = 10;
    bthread_t tids[N];
    InstallArg args[N];
    for (size_t i = 0; i < N; ++i) {
        args[i].e = &executor;
        args[i].request.set_group_id("test");
        args[i].request.set_term(1);
        args[i].request.mutable_meta()->CopyFrom(meta);
        args[i].request.set_uri(uri);
    }
    for (size_t i = 0; i < N; ++i) {
        raft_start_background(&tids[i], NULL, install_thread, &args[i]);
    }
    for (size_t i = 0; i < N; ++i) {
        bthread_join(tids[i], NULL);
    }
    size_t suc = 0;
    for (size_t i = 0; i < N; ++i) {
        LOG(INFO) << "Try number: " << i << "------------------------"; 
        if (args[i].cntl.Failed()) {
            LOG(ERROR) << "Result, Error: " << args[i].cntl.ErrorText();
        } else {
            suc += 1;
            LOG(INFO) << "Result, Success.";
        }
    }
    ASSERT_EQ(0, storage1.close(reader));
    ASSERT_EQ(1u, suc);
    reader = executor.snapshot_storage()->open();
    ASSERT_EQ(0, reader->get_file_meta("data", NULL));
    ASSERT_EQ(0, executor.snapshot_storage()->close(reader));
}

}  // namespace raft
