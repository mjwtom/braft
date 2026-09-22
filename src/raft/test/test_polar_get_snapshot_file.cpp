// Copyright (c) 2024 Baidu.com, Inc. All Rights Reserved

// Author: Wei Li (liwei20@baidu.com)
// Date: 2024/05/20 15:13:45

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <base/logging.h>
#include <base/file_util.h>

#include "raft/file_service.h"
#include "raft/util.h"
#include "raft/file_system_adaptor.h"
#include "raft/snapshot_throttle.h"
#include "raft/snapshot.h"
#include "baidu/hpn/polar/global.h"
#include "baidu/hpn/rpc/polar/polar_helper.h"

#define RUN(expr) RAFT_SYNC_BTHREAD_RUN_ON_POLAR(expr, 0)

namespace logging {
DECLARE_int32(verbose);
DECLARE_int32(min_log_level);
}

namespace raft {
DECLARE_bool(raft_file_check_hole);
DECLARE_bool(raft_use_polar);
DECLARE_bool(raft_enable_throttle_when_install_snapshot);
}

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

class MockFileAdaptor : public raft::FileAdaptor {
public:
    MockFileAdaptor(raft::FileAdaptor* file) : _file(file) {}
    virtual ~MockFileAdaptor() {}

    virtual void read(base::IOPortal* portal,
                            off_t offset,
                            size_t size,
                            size_t *nread,
                            raft::Closure* done) override {
        ASSERT_TRUE(nread);
        LOG(NOTICE) << "mock async_read file";
        _file->read(portal, offset, size, nread, done);
        LOG(NOTICE) << "mock async_read file run done";
    }
    virtual ssize_t size() override { return _file->size(); }
    virtual bool close() override { return _file->close(); }
    virtual void write(const base::IOBuf& data, off_t offset, raft::Closure* done) override {
        CHECK(done);
        done->status().set_error(EIO, "Mock write failed");
    }
    virtual bool sync() override { return false; }

private:
    raft::FileAdaptor* _file;
};

class MockFileSystemAdaptor : public raft::PosixFileSystemAdaptor {
public:
    MockFileSystemAdaptor() : PosixFileSystemAdaptor() {}
    virtual ~MockFileSystemAdaptor() {}

    virtual raft::FileAdaptor* open(const std::string& path, int oflag,
                                    const ::google::protobuf::Message* file_meta,
                                    base::File::Error* e) override {
        LOG(NOTICE) << "mock open file";

        raft::FileAdaptor* file
            = raft::PosixFileSystemAdaptor::open(path, oflag, file_meta, nullptr);
        return new MockFileAdaptor(file);
    };
};

class GetSnapshotFileTest : public testing::Test {
protected:
    void SetUp() {
        auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG(NOTICE) << "We are in " << test_info->test_case_name() << "." << test_info->name();
        raft::FLAGS_raft_use_polar = true;
        baidu::hpn::polar::FLAGS_polar_thread_num = 1;
        baidu::hpn::polar::FLAGS_lcore_mask = "0x00F000";
        baidu::hpn::polar::FLAGS_host_ip = "127.0.0.1";
        baidu::hpn::polar::FLAGS_server_ports_list = "8535,8536,8537,8538";
        baidu::hpn::polar::FLAGS_client_ports_start = 9200;
        baidu::hpn::polar::FLAGS_client_ports_end = 9300;
        baidu::hpn::polar::FLAGS_client_ports_num = 50;
        baidu::hpn::polar::FLAGS_socket_mem = "512,512";
        logging::FLAGS_verbose = 90;
        logging::FLAGS_min_log_level = 0;
        raft::FLAGS_raft_enable_throttle_when_install_snapshot = true;
        ::system("rm -rf data");
        ::system("mkdir data");

        _fs = new MockFileSystemAdaptor;
        _throttle = new raft::ThroughputSnapshotThrottle(30, 10);
        rpc::ServerOptions server_options;
        server_options.use_polar = raft::FLAGS_raft_use_polar;
        ASSERT_EQ(0, _server.AddService(raft::file_service(), 
                                        rpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, _server.Start(8535, &server_options));
    }
    void TearDown() {
        _server.Stop(0);
        _server.Join();
    }
    rpc::Server _server;
    scoped_refptr<raft::FileSystemAdaptor> _fs;
    scoped_refptr<raft::ThroughputSnapshotThrottle> _throttle;
};

TEST_F(GetSnapshotFileTest, sanity) {
    raft::LocalSnapshotMetaTable meta_table;
    raft::LocalFileMeta meta;
    ASSERT_EQ(0, meta_table.add_file("c", meta));
    scoped_refptr<raft::SnapshotFileReader> reader;
    auto l = [&] {
        reader = new raft::SnapshotFileReader(0, _fs.get(), "data/a", _throttle.get());
    };
    RUN(l());
    reader->set_meta_table(meta_table);
    int64_t reader_id = 0;
    ASSERT_EQ(0, raft::file_service_add(reader.get(), &reader_id));

    std::string uri;
    base::string_printf(&uri, "remote://127.0.0.1:8535/%ld", reader_id);
    raft::RemoteFileCopier copier;
    raft::FileSystemAdaptor* fs = raft::default_file_system();
    ASSERT_EQ(0, copier.init(uri, fs, NULL));

    // normal copy dir
    ASSERT_EQ(0, system("mkdir data/a; mkdir data/a/b; echo '123' > data/a/c"));
    ASSERT_TRUE(base::CreateDirectory(base::FilePath("./data/b")));
    ASSERT_EQ(0, copier.copy_to_file("c", "./data/b/c", NULL));
    int ret = system("diff ./data/a/c ./data/b/c");
    ASSERT_EQ(0, ret);

    ASSERT_EQ(0, raft::file_service_remove(reader_id));
    RUN(reader.release());
}
