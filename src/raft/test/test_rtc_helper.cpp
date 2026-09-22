/*
 * =====================================================================================
 *
 *       Filename:  test_util.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2015年11月06日 17时48分54秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  majingwei@baidu.com
 *        Company:  Baidu, Inc
 *
 * =====================================================================================
 */

#include <gtest/gtest.h>
#include <base/logging.h>
#include <base/files/scoped_temp_dir.h>
#include <baidu/hpn/rpc/acceptor.h>

#include "raft/util.h"
#include "raft/rtc_helper.h"
#include "raft/test/util.h"


namespace baidu {
namespace hpn {
namespace polar {
DECLARE_string(lcore_mask);
DECLARE_string(host_ip);
DECLARE_string(server_ports_list);
DECLARE_int32(server_port_start);
DECLARE_int32(server_port_end);
DECLARE_int32(client_ports_start);
DECLARE_int32(client_ports_end);
DECLARE_int32(client_ports_num);
DECLARE_string(socket_mem);
}
}
}

namespace raft {
DECLARE_bool(raft_use_polar);

class RtcSuits : public testing::Test {
protected:
    void SetUp() {
        base::EndPoint addr;
        generate_random_addr(&addr);
        raft::FLAGS_raft_use_polar = true;
        rpc::ServerOptions server_options;
        server_options.use_polar = raft::FLAGS_raft_use_polar;
        server_options.idle_timeout_sec = 1;
        baidu::hpn::polar::FLAGS_lcore_mask = "0x03";
        baidu::hpn::polar::FLAGS_host_ip = base::ip2str(addr.ip).c_str();
        baidu::hpn::polar::FLAGS_server_ports_list = std::to_string(addr.port);
        baidu::hpn::polar::FLAGS_client_ports_start = 9100;
        baidu::hpn::polar::FLAGS_client_ports_end = 9200;
        baidu::hpn::polar::FLAGS_client_ports_num = 50;
        baidu::hpn::polar::FLAGS_socket_mem = "512,512";

        ASSERT_EQ(_server.Start(addr, &server_options), 0);
    }
    void TearDown() {
        // we have to wait for poler server start successfully before stop
        auto* ctx = _server._am->_polar_acceptor->_ctxs;
        auto ctx_num = _server._am->_polar_acceptor->_poll_thread_num;
        while (true) {
            bool all_ready = true;
            for (int i = 0; i < ctx_num; i++) {
                if (ctx[i].Status() != baidu::hpn::rpc::polar::PolarAcceptor::ACCEPTOR_RUNNING) {
                    all_ready = false;
                }
            }
            if (all_ready) {
                break;
            }
            bthread_usleep(100);
        }
        _server.Stop(200);
        _server.Join();
    }
    rpc::Server _server;
};

static std::atomic<int> g_on_error(0);

static int on_error(bthread_id_t id, void* arg, int error_code) {
    raft::raft_bthread_id_unlock(id);
    LOG(NOTICE) << "on error: " << error_code;
    g_on_error = error_code;
    return 0;
}

static int raft_id_func(void* arg) {
    bthread::CountdownEvent* event = (bthread::CountdownEvent*)arg;
    for (size_t i = 0; i < 10; i++) {
        EXPECT_TRUE(event != nullptr);
        bthread_id_t id;
        void* data = malloc(1024);
        EXPECT_EQ(raft::raft_bthread_id_create(&id, data, on_error), 0);
        void* check_data = nullptr;
        EXPECT_EQ(raft::raft_bthread_id_lock(id, &check_data), 0);
        EXPECT_EQ(check_data, data);
        EXPECT_EQ(raft::raft_bthread_id_unlock(id), 0);
        EXPECT_EQ(raft::raft_bthread_id_error(id, EEXIST), 0);
        EXPECT_EQ(g_on_error, EEXIST);
        EXPECT_EQ(raft::raft_bthread_id_error(id, 0), 0);
        EXPECT_EQ(g_on_error, 0);
        
        EXPECT_EQ(raft::raft_bthread_id_lock(id, &check_data), 0);
        EXPECT_EQ(raft::raft_bthread_id_error(id, EEXIST), 0) << "round: " << i;
        EXPECT_EQ(g_on_error, 0); // on_error not executed until no lock
        EXPECT_EQ(raft::raft_bthread_id_unlock(id), 0);
        EXPECT_EQ(g_on_error, EEXIST); // on_error executed by unlock
        
        EXPECT_EQ(raft::raft_bthread_id_lock(id, &check_data), 0);
        EXPECT_EQ(raft::raft_bthread_id_unlock_and_destroy(id), 0);
        check_data = nullptr;
        EXPECT_NE(raft::raft_bthread_id_lock(id, &check_data), 0);
        bthread_id_t saved_id = id;
        EXPECT_EQ(raft::raft_bthread_id_create(&id, data, on_error), 0);
        EXPECT_NE(id, saved_id);
        EXPECT_EQ(raft::raft_bthread_id_lock(id, &check_data), 0);
        EXPECT_EQ(raft::raft_bthread_id_unlock_and_destroy(id), 0);
    }
    event->signal();
    return 0;
}

TEST_F(RtcSuits, raft_id_func) {
    bthread::CountdownEvent event;
    baidu::hpn::polar::CallBack callback;
    callback.fn = raft_id_func;
    callback.ctx = &event;
    int polar_worker_index = 0;
    ASSERT_EQ(baidu::hpn::polar::poll_thread_add_dynamic_callback(callback,
                -1, &polar_worker_index), 0);
    event.wait();
}

struct ApplyContex {
    ApplyContex() : num(0), limit(0), received(nullptr), event(nullptr) {}
    bthread::ExecutionQueueId<ApplyContex> apply_queue_id;
    int num;
    int limit;
    std::vector<int>* received;
    bthread::CountdownEvent* event;
};

bthread::CountdownEvent g_stop_cond;
static int execute_applying_tasks(
        void* meta, bthread::TaskIterator<ApplyContex>& iter) {
    if (iter.is_queue_stopped()) {
        g_stop_cond.signal();
        LOG(NOTICE) << "stopped";
        return 0;
    }
    for (; iter; ++iter) {
        ApplyContex& ctx = *iter;
        EXPECT_TRUE(ctx.received);
        EXPECT_TRUE(ctx.event);
        if (ctx.num < ctx.limit) {
            ctx.received->push_back(ctx.num);
            ctx.num ++;
            EXPECT_EQ(raft::raft_execution_queue_execute(ctx.apply_queue_id, ctx), 0);
            continue;
        }
        ctx.event->signal();
    }
    return 0;
}

TEST_F(RtcSuits, raft_eq_bthread) {
    std::vector<int> received;
    raft::FLAGS_raft_use_polar = false;
    bthread::ExecutionQueueId<ApplyContex> apply_queue_id;
    bthread::ExecutionQueueOptions opt;
    opt.max_tasks_size = 256;
    ASSERT_EQ(raft::raft_execution_queue_start(&apply_queue_id,
                &opt, execute_applying_tasks, nullptr), 0);
    bthread::CountdownEvent event;
    ApplyContex ctx;
    ctx.apply_queue_id = apply_queue_id;
    ctx.num = 0;
    ctx.limit = 4;
    ctx.received = &received;
    ctx.event = &event;
    ASSERT_EQ(raft::raft_execution_queue_execute(apply_queue_id, ctx), 0);
    event.wait();
    EXPECT_EQ(received.size(), ctx.limit);
    EXPECT_TRUE(std::is_sorted(received.begin(), received.end()));
    g_stop_cond.reset();
    raft::raft_execution_queue_stop(apply_queue_id);
    g_stop_cond.wait();
    g_stop_cond.reset();
    raft::raft_execution_queue_join(apply_queue_id);
    ASSERT_NE(raft::raft_execution_queue_execute(apply_queue_id, ctx), 0);
    raft::FLAGS_raft_use_polar = true;
}

TEST_F(RtcSuits, raft_eq_polar) {
    // in case of reuse execution_queue resource, we do this several times
    for (size_t i = 0; i < 10; i++) {
        std::vector<int> received;
        bthread::ExecutionQueueId<ApplyContex> apply_queue_id;
        bthread::ExecutionQueueOptions opt;
        opt.max_tasks_size = 256;
        raft::SynchronizedClosure done;
        auto l = [&] {
            raft::raft_execution_queue_start(&apply_queue_id, &opt,
                    execute_applying_tasks, nullptr);
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        bthread::CountdownEvent event;
        ApplyContex ctx;
        ctx.apply_queue_id = apply_queue_id;
        ctx.num = 0;
        ctx.limit = 4;
        ctx.received = &received;
        ctx.event = &event;
        auto l_run = [&] {
            ASSERT_EQ(raft::raft_execution_queue_execute(apply_queue_id, ctx), 0);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l_run)), 0);
        event.wait();
        EXPECT_EQ(received.size(), ctx.limit);
        EXPECT_TRUE(std::is_sorted(received.begin(), received.end()));

        g_stop_cond.reset();
        auto l2 = [&] {
            raft::raft_execution_queue_stop(apply_queue_id);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), 0);
        g_stop_cond.wait();
        g_stop_cond.reset();

        done.reset();
        auto l_run2 = [&] {
            ASSERT_NE(raft::raft_execution_queue_execute(apply_queue_id, ctx), 0);
            done.status().set_error(EINVAL, "raft execution queue should not work");
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l_run2)), 0);
        done.wait();
        ASSERT_FALSE(done.status().ok());
        
        done.reset();
        auto l3 = [&] {
            raft::raft_execution_queue_join(apply_queue_id);
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        
        done.reset();
        auto start_again = [&] {
            raft::raft_execution_queue_start(&apply_queue_id, &opt,
                    execute_applying_tasks, nullptr);
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(start_again)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
        
        g_stop_cond.reset();
        auto stop_again = [&] {
            raft::raft_execution_queue_stop(apply_queue_id);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(stop_again)), 0);
        g_stop_cond.wait();
        
        done.reset();
        auto join_again = [&] {
            raft::raft_execution_queue_join(apply_queue_id);
            done.Run();
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(join_again)), 0);
        done.wait();
        ASSERT_TRUE(done.status().ok());
    }
}

TEST_F(RtcSuits, counter) {
    Counter c1(true);
    c1 ++;
    ASSERT_EQ(c1, 1);
    ++ c1;
    ASSERT_EQ(c1, 2);
    c1 --;
    ASSERT_EQ(c1, 1);
    -- c1;
    ASSERT_EQ(c1, 0);

    Counter c2(false);
    c2 ++;
    ASSERT_EQ(c2, 1);
    ++ c2;
    ASSERT_EQ(c2, 2);
    c2 --;
    ASSERT_EQ(c2, 1);
    -- c2;
    ASSERT_EQ(c2, 0);
}

static std::atomic<int32_t> g_dummy_ref_count(0);

class DummyRefCounted : public raft::RaftRefCounted<DummyRefCounted> {
public:
    DummyRefCounted(bool shared) : raft::RaftRefCounted<DummyRefCounted>(shared) {
        g_dummy_ref_count.fetch_add(1, std::memory_order_relaxed);
    }
    ~DummyRefCounted() {
        g_dummy_ref_count.fetch_sub(1, std::memory_order_relaxed);
    }
private:
friend class raft::RaftRefCounted<raft::DummyRefCounted>;
};

TEST_F(RtcSuits, ref_counted) {
    scoped_refptr<DummyRefCounted> ptr1 = new DummyRefCounted(true);
    ASSERT_TRUE(ptr1->has_one_ref());
    scoped_refptr<DummyRefCounted> ptr2 = new DummyRefCounted(false);
    ASSERT_TRUE(ptr1->has_one_ref());
    ASSERT_EQ(g_dummy_ref_count.load(std::memory_order_relaxed), 2);
    ptr1 = nullptr;
    ptr2 = nullptr;
    ASSERT_EQ(g_dummy_ref_count.load(std::memory_order_relaxed), 0);
}

}  // namespace raft to use operator
