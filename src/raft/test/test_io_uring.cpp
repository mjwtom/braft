// Copyright (c) 2023 Baidu.com, Inc. All Rights Reserved

// Author: Jingwei Ma (majingwei@baidu.com)
// Date: 2023/08/04 12:15:37

#include <random>
#include <filesystem>
#include <gtest/gtest.h>
#include "raft/io_uring.h"
#include "raft/util.h"
#include "raft/rtc_helper.h"
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
DECLARE_int32(uring_max_event);
DECLARE_bool(raft_segment_direct_io);
DECLARE_uint64(raft_dio_align_unit_size);
DECLARE_bool(raft_use_iouring);
DEFINE_uint64(raft_dio_align_unit_size, 4096, "align unit size for aio");

class UringTest : public testing::Test {
protected:
    void SetUp() {
        google::SetCommandLineOption("crash_on_fatal_log", "true");
        base::EndPoint addr;
        generate_random_addr(&addr);
        FLAGS_raft_use_polar = true;
        FLAGS_raft_use_iouring = true; // for kernel not support io uring if you like to test this, change this flag
        FLAGS_uring_max_event = 4;
        baidu::hpn::polar::FLAGS_lcore_mask = "0x0F0000";
        baidu::hpn::polar::FLAGS_host_ip = base::ip2str(addr.ip).c_str();
        baidu::hpn::polar::FLAGS_server_ports_list = std::to_string(addr.port);
        baidu::hpn::polar::FLAGS_client_ports_start = 9100;
        baidu::hpn::polar::FLAGS_client_ports_end = 9200;
        baidu::hpn::polar::FLAGS_client_ports_num = 50;
        baidu::hpn::polar::FLAGS_socket_mem = "512,512";
        rpc::ServerOptions server_options;
        server_options.use_polar = FLAGS_raft_use_polar;

        ASSERT_EQ(_server.Start(addr, &server_options), 0);
    }
    void TearDown() {}

private:
    rpc::Server _server;
};

static thread_local std::random_device g_random_device;
static thread_local std::mt19937_64 g_random_engine(g_random_device());

TEST_F(UringTest, direct_io) {
    size_t write_size = 8192;
    std::string file_name = "test_file";
    int mode = O_RDWR | O_CREAT | O_TRUNC | O_DIRECT;
    int fd = open(file_name.c_str(), mode, 0644);
    ASSERT_GT(fd, 0);
    LOG(NOTICE) << "open " << file_name << " with fd: " << fd;
    void* write_buf = nullptr;
    ASSERT_EQ(posix_memalign(&write_buf, FLAGS_raft_dio_align_unit_size, write_size), 0);
    ASSERT_TRUE(write_buf !=  nullptr);
    size_t mask = FLAGS_raft_dio_align_unit_size - 1;
    ASSERT_EQ(FLAGS_raft_dio_align_unit_size & mask, 0);
    std::uniform_int_distribution<int16_t> distribution(INT8_MIN, INT8_MAX);
    for (size_t i = 0; i < write_size; i ++) {
        ((uint8_t*)write_buf)[i] = static_cast<int8_t>(distribution(g_random_engine));
    }
    base::IOBuf data;
    data.append_user_data(write_buf, write_size, ::free);
    raft::SynchronizedClosure done;
    auto lwrite = [&] {
        raft::g_raft_io_man->write(fd, 0, data, &done);
    };
    if (raft::FLAGS_raft_use_polar) {
        raft::run_closure_in_polar(create_pb_closure(std::move(lwrite)), 0);
    } else {
        raft::raft_run_closure_background(create_pb_closure(std::move(lwrite)));
    }
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ::close(fd);
    fd = -1;
    base::IOPortal in;
    // open again;
    fd = open(file_name.c_str(), O_RDONLY, 0644);
    ASSERT_GT(fd, 0);
    done.reset();
    auto lread = [&] {
        raft::g_raft_io_man->read(fd, 0, &in, write_size, &done);
    };
    if (raft::FLAGS_raft_use_polar) {
        raft::run_closure_in_polar(create_pb_closure(std::move(lread)), 0);
    } else {
        raft::raft_run_closure_background(create_pb_closure(std::move(lread)));
    }
    done.wait();
    ASSERT_TRUE(done.status().ok());
    EXPECT_EQ(in.to_string(), data.to_string());
    ::close(fd);
    std::filesystem::remove_all(std::filesystem::path(file_name));
}

TEST_F(UringTest, uring_full) {
    const size_t io_count = 2 * FLAGS_uring_max_event;
    size_t write_size = base::IOBuf::DEFAULT_BLOCK_SIZE * 2 * IOV_MAX; // exceeds MAX_IOV
    std::string file_name = "test_file";
    int mode = O_RDWR | O_CREAT | O_TRUNC;
    int fd = open(file_name.c_str(), mode, 0644);
    ASSERT_GT(fd, 0);
    LOG(NOTICE) << "open " << file_name << " with fd: " << fd;
    void* write_buf = nullptr;
    ASSERT_EQ(posix_memalign(&write_buf, FLAGS_raft_dio_align_unit_size, write_size), 0);
    ASSERT_TRUE(write_buf !=  nullptr);
    size_t mask = FLAGS_raft_dio_align_unit_size - 1;
    ASSERT_EQ(FLAGS_raft_dio_align_unit_size & mask, 0);
    std::uniform_int_distribution<int16_t> distribution(INT8_MIN, INT8_MAX);
    base::IOBuf buf[io_count];
    for (size_t j = 0; j < io_count; j++) {
        for (size_t i = 0; i < write_size; i ++) {
            ((uint8_t*)write_buf)[i] = static_cast<int8_t>(distribution(g_random_engine));
        }
        buf[j].append(write_buf, write_size);
    }
    raft::SynchronizedClosure done;
    raft::ClosureGroup* group_done = new ClosureGroup(io_count, &done);
    auto lwrite = [&] {
        for (size_t i = 0; i < io_count; i++) {
            raft::g_raft_io_man->write(fd, i * write_size, buf[i], group_done->sub_done(i));
        }
    };
    if (raft::FLAGS_raft_use_polar) {
        raft::run_closure_in_polar(create_pb_closure(std::move(lwrite)), 0);
    } else {
        raft::raft_run_closure_background(create_pb_closure(std::move(lwrite)));
    }
    done.wait();
    LOG(NOTICE) << "finish write";
    ASSERT_TRUE(done.status().ok());
    ::close(fd);
    free(write_buf);
    fd = -1;
    base::IOPortal in[io_count];
    // open again;
    fd = open(file_name.c_str(), O_RDONLY, 0644);
    ASSERT_GT(fd, 0);
    done.reset();
    group_done = new ClosureGroup(io_count, &done);
    auto lread = [&] {
        for (size_t i = 0; i < io_count; i++) {
            raft::g_raft_io_man->read(fd, i*write_size, &in[i], write_size, group_done->sub_done(i));
        }
    };
    if (raft::FLAGS_raft_use_polar) {
        raft::run_closure_in_polar(create_pb_closure(std::move(lread)), 0);
    } else {
        raft::raft_run_closure_background(create_pb_closure(std::move(lread)));
    }
    done.wait();
    LOG(NOTICE) << "finish write";
    ASSERT_TRUE(done.status().ok()) << done.status();
    ::close(fd);
    //std::filesystem::remove_all(std::filesystem::path(file_name));
    for (size_t i = 0; i < io_count; i ++) {
        EXPECT_EQ(in[i].to_string(), buf[i].to_string());
    }
}

static void random_binary(base::IOBuf& data, uint64_t len) {
    LOG(NOTICE) << "start generate " << len / (1024 * 1024L) << "MB random data";
    std::uniform_int_distribution<int16_t> distribution(INT8_MIN, INT8_MAX);
    for (uint64_t i = 0; i < len; i++) {
        CHECK_EQ(0, data.push_back(static_cast<int8_t>(distribution(g_random_engine))));
        if (i % (10 * 1024 * 1024L) == 0) {
            LOG(NOTICE) << "generate " << i / (1024 * 1024L) << "MB random data";
        }
    }
    LOG(NOTICE) << "generate " << len / (1024 * 1024L) << "MB random data";
}

TEST_F(UringTest, read_file_end) {
    size_t write_size = 8192;
    std::string file_name = "test_file";
    int mode = O_RDWR | O_CREAT | O_TRUNC;
    int fd = open(file_name.c_str(), mode, 0644);
    ASSERT_GT(fd, 0);
    LOG(NOTICE) << "open " << file_name << " with fd: " << fd;
    raft::SynchronizedClosure done;
    base::IOBuf write_buf;
    random_binary(write_buf, write_size);
    auto lwrite = [&] {
        raft::g_raft_io_man->write(fd, 0, write_buf, &done);
    };
    if (raft::FLAGS_raft_use_polar) {
        raft::run_closure_in_polar(create_pb_closure(std::move(lwrite)), 0);
    } else {
        raft::raft_run_closure_background(create_pb_closure(std::move(lwrite)));
    }
    done.wait();
    ASSERT_TRUE(done.status().ok());
    base::IOPortal in;
    done.reset();
    off_t offset = write_size;
    auto lread = [&] {
        raft::g_raft_io_man->read(fd, offset, &in, write_size, &done);
    };
    if (raft::FLAGS_raft_use_polar) {
        raft::run_closure_in_polar(create_pb_closure(std::move(lread)), 0);
    } else {
        raft::raft_run_closure_background(create_pb_closure(std::move(lread)));
    }
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(in.size(), 0);
    done.reset();
    offset = write_size / 2;
    auto lread2 = [&] {
        raft::g_raft_io_man->read(fd, offset, &in, write_size, &done);
    };
    if (raft::FLAGS_raft_use_polar) {
        raft::run_closure_in_polar(create_pb_closure(std::move(lread2)), 0);
    } else {
        raft::raft_run_closure_background(create_pb_closure(std::move(lread2)));
    }
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(in.size(), write_size - offset);
    write_buf.pop_front(offset);
    EXPECT_EQ(in.to_string(), write_buf.to_string());
    ::close(fd);
    std::filesystem::remove_all(std::filesystem::path(file_name));
}

struct CheckArg {
    CheckArg() : fd(-1), in_flight_read(0), in_flight_write(0), round(0) {}
    int fd;
    std::atomic<size_t> in_flight_read;
    std::atomic<size_t> in_flight_write;
    std::atomic<size_t> round;
    bthread::CountdownEvent cond;
    std::map<off_t, uint32_t> crc_map;
    std::map<off_t, uint32_t> read_map;
    std::map<off_t, uint32_t> write_map;
};

static int check_data(void* arg) {
    CheckArg* check_arg = (CheckArg*)arg;
    EXPECT_TRUE(check_arg);
    const size_t in_flight_io = 32;
    auto& crc_map = check_arg->crc_map;
    auto& read_map = check_arg->read_map;
    auto& write_map = check_arg->write_map;
    auto& in_flight_read = check_arg->in_flight_read;
    auto& in_flight_write = check_arg->in_flight_write;
    LOG(INFO) << " in_flight_read: " << in_flight_read.load(std::memory_order_relaxed)
        << " in_flight_write: " << in_flight_write.load(std::memory_order_relaxed);
    int fd = check_arg->fd;
    size_t len = 67;
    std::uniform_int_distribution<int16_t> distribution(INT16_MIN, INT16_MAX);
    off_t offset = static_cast<uint16_t>(distribution(g_random_engine)) * len;
    bool should_write = (static_cast<uint16_t>(distribution(g_random_engine)) % 2 == 0);
    auto iter = crc_map.find(offset);
    do {
        if (should_write) {
            break;
        }
        if (in_flight_read.load(std::memory_order_relaxed) > in_flight_io) {
            break;
        }
        if (iter == crc_map.end()) {
            break;
        }
        uint32_t check_crc = iter->second;
        iter = read_map.find(offset);
        if (iter != read_map.end()) {
            break;
        }
        iter = write_map.find(offset);
        if (iter != write_map.end()) {
            break;
        }
        base::IOPortal* in = new base::IOPortal;
        auto check = [=] (const base::Status& st) {
            auto& tmp_map = check_arg->read_map;
            EXPECT_TRUE(st.ok());
            uint32_t read_crc = 0;
            CHECK(raft::get_crc32((raft::CrcType)raft::FLAGS_crc32_type, *in, &read_crc));
            auto iter2 = tmp_map.find(offset);
            EXPECT_TRUE(iter2 != tmp_map.end());
            EXPECT_EQ(iter2->second, read_crc);
            tmp_map.erase(iter2);
            delete in;
            check_arg->round.fetch_add(1, std::memory_order_relaxed);
            check_arg->in_flight_read.fetch_sub(1, std::memory_order_release);
        };
        EXPECT_TRUE(read_map.insert(std::make_pair(offset, check_crc)).second);
        in_flight_read.fetch_add(1, std::memory_order_release);
        raft::g_raft_io_man->read(fd, offset, in, len,
                raft::create_raft_closure(std::move(check)));
    } while (0);
    if (in_flight_write.load(std::memory_order_relaxed) > in_flight_io) {
        LOG(INFO) << "write exceeds: " << in_flight_write
            << " in_flight_read: " << in_flight_read
            << " in_flight_write: " << in_flight_write;
        return 0;
    }
    iter = read_map.find(offset);
    if (iter != read_map.end()) {
        return 0;
    }
    iter = write_map.find(offset);
    if (iter != write_map.end()) {
        return 0;
    }
    base::IOBuf* w_data = new base::IOBuf;
    random_binary(*w_data, len);
    uint32_t write_crc = 0;
    CHECK(raft::get_crc32((raft::CrcType)raft::FLAGS_crc32_type, *w_data, &write_crc));
    auto write = [=] (const base::Status& st) {
        auto& tmp_map = check_arg->write_map;
        EXPECT_TRUE(st.ok());
        auto iter2 = tmp_map.find(offset);
        EXPECT_TRUE(iter2 != tmp_map.end()) << "offset: " << offset;
        EXPECT_EQ(iter2->second, write_crc);
        tmp_map.erase(iter2);
        check_arg->crc_map[offset] = write_crc;
        delete w_data;
        check_arg->round.fetch_add(1, std::memory_order_relaxed);
        check_arg->in_flight_write.fetch_sub(1, std::memory_order_release);
    };
    EXPECT_TRUE(write_map.insert(std::make_pair(offset, write_crc)).second);
    in_flight_write.fetch_add(1, std::memory_order_release);
    raft::g_raft_io_man->write(fd, offset, *w_data, raft::create_raft_closure(std::move(write)));
    return 0;
}

void check_destroy(void* arg) {
    CheckArg* check_arg = (CheckArg*)arg;
    LOG(NOTICE) << "end poller";
    check_arg->cond.signal();
}

TEST_F(UringTest, check_data) {
    std::string file_name = "test_file";
    int mode = O_RDWR | O_CREAT | O_TRUNC;
    int fd = open(file_name.c_str(), mode, 0644);
    ASSERT_GT(fd, 0);
    LOG(NOTICE) << "open " << file_name << " with fd: " << fd;

    const size_t check_round = 1000*1000; // one million round
    CheckArg check_arg;
    check_arg.fd = fd;
    baidu::hpn::polar::Poller p;
    p.fn = check_data;
    p.destroy = check_destroy;
    p.ctx = &check_arg;
    p.name = "test_uring_check_data";
    ASSERT_EQ(0, baidu::hpn::polar::poll_thread_add_poller(p, 0));
    LOG(NOTICE) << "add poller: " << p.name << " to polar: " << 0;
    while (check_arg.round.load(std::memory_order_relaxed) < check_round) {
        bthread_usleep(1000);
    }
    ASSERT_EQ(0, baidu::hpn::polar::poll_thread_remove_poller(p.name, 0));
    LOG(NOTICE) << "remove poller: " << p.name << " from polar: " << 0;
    check_arg.cond.wait();
    while (check_arg.in_flight_read.load(std::memory_order_relaxed) +
            check_arg.in_flight_write.load(std::memory_order_relaxed) > 0) {
        bthread_usleep(1000);
    }
}

class MockIOManager : public IOManager {
    MockIOManager() : _fail(false) {}
    ~MockIOManager() {}
    virtual scoped_refptr<IOWorker> get_or_create_worker() override {
        if (_fail) {
            return nullptr;
        }
        return IOManager::get_or_create_worker();
    }
private:
    std::atomic<bool> _fail;
};

TEST_F(UringTest, init) {
    MockIOManager manager;
    EXPECT_EQ(manager.init(), 0);
    manager._fail = true;
    EXPECT_NE(manager.init(), 0);
}

TEST_F(UringTest, sync) {
    FLAGS_raft_use_polar = false;
    const ssize_t data_size = 8192;
    MockIOManager manager;
    std::string file_name = "test_file";
    int mode = O_RDWR | O_CREAT | O_TRUNC;
    int fd = open(file_name.c_str(), mode, 0644);
    ASSERT_GE(fd, 0);
    base::IOBuf write_data;
    for (size_t i = 0; i < data_size; i++) {
        write_data.append("c");
    }
    ASSERT_EQ(data_size, file_pwrite(write_data, fd, 0));
    SynchronizedClosure done;
    manager.sync(fd, &done);
    done.wait();
    EXPECT_FALSE(done.status().ok());
    
    EXPECT_EQ(manager.init(), 0);
    done.reset();
    manager.sync(fd, &done);
    done.wait();
    EXPECT_TRUE(done.status().ok());
    ::close(fd);
    fd = 100; // wrong fd
    done.reset();
    manager.sync(fd, &done);
    done.wait();
    EXPECT_FALSE(done.status().ok());
    FLAGS_raft_use_polar = true;
}

} // namespace raft

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
