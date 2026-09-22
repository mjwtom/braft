// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: Zhangyi Chen (chenzhangyi01@baidu.com)
// Date: 2015/11/06 15:40:45

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <base/logging.h>
#include <base/file_util.h>

#include "raft/file_service.h"
#include "raft/util.h"
#include "raft/remote_file_copier.h"
#include "raft/file_system_adaptor.h"
#include "raft/test/util.h"

namespace raft {
DECLARE_bool(raft_file_check_hole);
DECLARE_bool(enable_crc_install_snapshot);
DECLARE_bool(raft_enable_crc_test);
DECLARE_int32(raft_crc_wrong_checksum_rate);
DECLARE_int32(raft_crc_wrong_checksum_flip_interval_s);
extern BvarAdder<uint64_t> g_remote_copier_crc_error;
}

class FileServiceTest : public testing::Test {
protected:
    void SetUp() {
        auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG(NOTICE) << "We are in " << test_info->test_case_name()
            << "." << test_info->name();
        generate_random_addr(&_addr);
        logging::FLAGS_verbose = 90;
        ASSERT_EQ(0, _server.AddService(raft::file_service(), 
                                        rpc::SERVER_DOESNT_OWN_SERVICE));
        ASSERT_EQ(0, _server.Start(_addr, NULL));
    }
    void TearDown() {
        _server.Stop(0);
        _server.Join();
    }
    rpc::Server _server;
    base::EndPoint _addr;
};

TEST_F(FileServiceTest, sanity) {
    raft::FileSystemAdaptor* fs = raft::default_file_system();
    scoped_refptr<raft::LocalDirReader> reader(new raft::LocalDirReader(-1, fs, "a"));
    int64_t reader_id = 0;
    ASSERT_EQ(0, raft::file_service_add(reader.get(), &reader_id));
    std::string uri;
    raft::RemoteFileCopier copier;
    base::string_printf(&uri, "local://127.0.0.1:%d/123456", _addr.port);
    ASSERT_NE(0, copier.init(uri, fs, NULL));
    uri.clear();
    base::string_printf(&uri, "remote://127.0.0.1:%d//123456", _addr.port);
    ASSERT_NE(0, copier.init(uri, fs, NULL));
    uri.clear();
    base::string_printf(&uri, "remote://127.0.1:%d//123456", _addr.port);
    ASSERT_NE(0, copier.init(uri, fs, NULL));
    ASSERT_NE(0, copier.init("remote://127.0.0.1//123456", fs, NULL));
    uri = "remote://"+std::string(base::endpoint2str(_addr).c_str())+"/" + std::to_string(reader_id);
    ASSERT_EQ(0, copier.init(uri, fs, NULL));

    // normal copy dir
    system("chmod -R 755 ./a; chmod -R 755 ./b");
    ASSERT_EQ(0, system("rm -rf a; rm -rf b; mkdir a; mkdir a/b; echo '123' > a/c"));
    ASSERT_TRUE(base::CreateDirectory(base::FilePath("./b")));
    ASSERT_EQ(0, copier.copy_to_file("c", "./b/c", NULL));
    base::IOBuf c_data;
    ASSERT_EQ(0, copier.copy_to_iobuf("c", &c_data, NULL));
    ASSERT_TRUE(c_data.equals("123\n")) << c_data.to_string();
    // Copy Directory is not allowed
    ASSERT_NE(0, copier.copy_to_file("b", "./b/b", NULL));

    // Copy non-existed file
    ASSERT_NE(0, copier.copy_to_file("d", "./b/d", NULL));

    // src no permission read
    ASSERT_EQ(0, system("chmod 000 a/c"));
    ASSERT_NE(0, copier.copy_to_file("c", "./b/cc", NULL));
    ASSERT_EQ(0, system("chmod -R 755 ./a"));

    ASSERT_EQ(0, raft::file_service_remove(reader_id));

    // Copy after reader is remove
    ASSERT_NE(0, copier.copy_to_file("c", "./b/d", NULL));
    ASSERT_EQ(0, system("rm -rf a; rm -rf b;"));
}

TEST_F(FileServiceTest, hole_file) {
    int ret = 0;
    ASSERT_EQ(0, system("rm -rf a; rm -rf b; rm -rf c; mkdir a;"));

    LOG(INFO) << "build hole file";
    int fd = ::open("./a/hole.data", O_CREAT | O_TRUNC | O_WRONLY, 0644);
    ASSERT_GE(fd, 0);
    for (int i = 0; i < 1000; i++) {
        char buf[16*1024] = {0};
        snprintf(buf, sizeof(buf), "hello %d", i);
        ssize_t nwritten = pwrite(fd, buf, strlen(buf), 128 * 1024 * i);
        ASSERT_EQ(static_cast<size_t>(nwritten), strlen(buf));
    }
    ::close(fd);
    raft::FileSystemAdaptor* fs = raft::default_file_system();
    scoped_refptr<raft::LocalDirReader> reader(new raft::LocalDirReader(-1, fs, "a"));
    int64_t reader_id = 0;
    ASSERT_EQ(0, raft::file_service_add(reader.get(), &reader_id));

    raft::RemoteFileCopier copier;
    std::string uri = "remote://"+std::string(base::endpoint2str(_addr).c_str())+"/"+std::to_string(reader_id);
    // normal init
    raft::FLAGS_raft_file_check_hole = false;
    ASSERT_EQ(0, copier.init(uri, fs, NULL));
    ASSERT_TRUE(base::CreateDirectory(base::FilePath("./b")));
    ASSERT_EQ(0, copier.copy_to_file("hole.data", "./b/hole.data", NULL));
    ret = system("diff ./a/hole.data ./b/hole.data");
    ASSERT_EQ(0, ret);

    raft::FLAGS_raft_file_check_hole = true;
    ASSERT_TRUE(base::CreateDirectory(base::FilePath("./c")));
    ASSERT_EQ(0, copier.copy_to_file("hole.data", "./c/hole.data", NULL));
    ret = system("diff ./a/hole.data ./c/hole.data");
    ASSERT_EQ(0, ret);
}

TEST_F(FileServiceTest, crc_error) {
    raft::FLAGS_enable_crc_install_snapshot = true;
    raft::FLAGS_raft_enable_crc_test = true;
    raft::FLAGS_raft_crc_wrong_checksum_rate = 100;
    raft::FLAGS_raft_crc_wrong_checksum_flip_interval_s = -1;
    int ret = 0;
    ASSERT_EQ(0, system("rm -rf a; rm -rf b; rm -rf c; mkdir a;"));

    int fd = ::open("./a/file.data", O_CREAT | O_TRUNC | O_WRONLY, 0644);
    ASSERT_GE(fd, 0);
    for (int i = 0; i < 1000; i++) {
        char buf[16*1024] = {0};
        snprintf(buf, sizeof(buf), "hello %d", i);
        ssize_t nwritten = pwrite(fd, buf, strlen(buf), 128 * 1024 * i);
        ASSERT_EQ(static_cast<size_t>(nwritten), strlen(buf));
    }
    ::close(fd);
    raft::FileSystemAdaptor* fs = raft::default_file_system();
    scoped_refptr<raft::LocalDirReader> reader(new raft::LocalDirReader(-1, fs, "a"));
    int64_t reader_id = 0;
    ASSERT_EQ(0, raft::file_service_add(reader.get(), &reader_id));

    raft::RemoteFileCopier copier;
    std::string uri;
    base::string_printf(&uri, "remote://%s/%ld", base::endpoint2str(_addr).c_str(), reader_id);
    // normal init
    ASSERT_EQ(0, copier.init(uri, fs, NULL));
    ASSERT_TRUE(base::CreateDirectory(base::FilePath("./b")));
    raft::g_remote_copier_crc_error.reset();
    auto l = [&] {
        while (raft::g_remote_copier_crc_error.get_value() == 0) {
            bthread_usleep(1000);
        }
        raft::FLAGS_raft_crc_wrong_checksum_rate = 0;
    };
    raft::run_closure_in_bthread(baidu::hpn::rpc::NewCallback(std::move(l)));
    ASSERT_EQ(0, copier.copy_to_file("file.data", "./b/file.data", NULL));
    ret = system("diff ./a/file.data ./b/file.data");
    ASSERT_EQ(0, ret);
    ASSERT_NE(0, raft::g_remote_copier_crc_error.get_value());
}

class MockFileAdaptor : public raft::PosixFileAdaptor {
public:
    MockFileAdaptor(int fd, const std::string& path) : raft::PosixFileAdaptor(fd, path), _writing(false) {}
    void write(const base::IOBuf& data, off_t offset, raft::Closure* done) override {
        LOG(NOTICE) << "write: " << _path << " offset: " << offset
            << ", length: " << data.length() << " done: " << done;
        auto l = [=] (const base::Status& st) {
            _writing.store(true, std::memory_order_release);
            _cond.wait(); // mock io hang
            done->status().set_error(st.error_code(), st.error_str());
            done->Run();
        };
        raft::PosixFileAdaptor::write(data, offset,
                raft::create_raft_closure(std::move(l)));
    }
private:
    std::atomic<bool> _writing;
    bthread::CountdownEvent _cond;
};

class MockFileSystem : public raft::PosixFileSystemAdaptor {
public:
    virtual raft::FileAdaptor* open(const std::string& path, int oflag,
                              const ::google::protobuf::Message* file_meta,
                              base::File::Error* e) override {
        raft::PosixFileAdaptor* pfa = static_cast<raft::PosixFileAdaptor*>(
            raft::PosixFileSystemAdaptor::open(path, oflag, file_meta, e));
        MockFileAdaptor* mock_fa = new MockFileAdaptor(pfa->_fd->fd(), pfa->_path);
        pfa->_fd->_fd = -1; // in case of release
        delete pfa;
        return mock_fa;
    }
};

TEST_F(FileServiceTest, cancel_with_io) {
    ASSERT_EQ(0, system("rm -rf a; rm -rf b; rm -rf c; mkdir a;"));

    LOG(INFO) << "build hole file";
    int fd = ::open("./a/hole.data", O_CREAT | O_TRUNC | O_WRONLY, 0644);
    ASSERT_GE(fd, 0);
    for (int i = 0; i < 1000; i++) {
        char buf[16*1024] = {0};
        snprintf(buf, sizeof(buf), "hello %d", i);
        ssize_t nwritten = pwrite(fd, buf, strlen(buf), 128 * 1024 * i);
        ASSERT_EQ(static_cast<size_t>(nwritten), strlen(buf));
    }
    ::close(fd);
    static scoped_refptr<MockFileSystem> fs = new MockFileSystem;
    scoped_refptr<raft::LocalDirReader> reader(new raft::LocalDirReader(-1, fs.get(), "a"));
    int64_t reader_id = 0;
    ASSERT_EQ(0, raft::file_service_add(reader.get(), &reader_id));

    raft::RemoteFileCopier copier;
    std::string uri = "remote://"+std::string(base::endpoint2str(_addr).c_str())+"/"+std::to_string(reader_id);
    // normal init
    raft::FLAGS_raft_file_check_hole = false;
    ASSERT_EQ(0, copier.init(uri, fs, NULL));
    ASSERT_TRUE(base::CreateDirectory(base::FilePath("./b")));
    scoped_refptr<raft::RemoteFileCopier::Session> session
        = copier.start_to_copy_to_file("hole.data", "./b/hole.data", NULL);
    MockFileAdaptor* f = dynamic_cast<MockFileAdaptor*>(session->_file);
    ASSERT_TRUE(f);
    while (!f->_writing.load(std::memory_order_acquire)) {
        bthread_usleep(100);
    }
    session->cancel();
    ASSERT_TRUE(session->_finished);
    f->_cond.signal();
    while (session->_file) {
        bthread_usleep(100);
    }
}

