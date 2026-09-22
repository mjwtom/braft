// libraft - Quorum-based replication of states across machines.
// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved

// Author: WangYao (fisherman), wangyao02@baidu.com
// Date: 2015/10/08 17:00:05

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <base/logging.h>
#include <base/files/file_path.h>
#include <base/file_util.h>
#include <bthread.h>
#include <bthread/countdown_event.h>
#include "raft/log.h"
#include "raft/test/util.h"
#include "baidu/hpn/polar/global.h"
#include "baidu/hpn/rpc/polar/polar_helper.h"

#define LEADER_RUN(expr) RAFT_SYNC_BTHREAD_RUN_ON_POLAR(expr, leader->polar_worker_index())
#define LEADER_ASYNC_RUN(expr) RAFT_BTHREAD_RUN_ON_POLAR(expr, leader->polar_worker_index())
#define NODE_RUN(expr) RAFT_SYNC_BTHREAD_RUN_ON_POLAR(expr, node->polar_worker_index())
#define NODE_ASYNC_RUN(expr) RAFT_BTHREAD_RUN_ON_POLAR(expr, node->polar_worker_index())
#define FOLLOWER_RUN(expr) RAFT_SYNC_BTHREAD_RUN_ON_POLAR(expr, follower->polar_worker_index())

namespace raft {
extern BvarAdder<int64_t> g_num_nodes;
DECLARE_int32(raft_max_parallel_append_entries_rpc_num);
DECLARE_bool(raft_enable_append_entries_cache);
DECLARE_int32(raft_max_append_entries_cache_size);
DECLARE_int32(raft_retry_replicate_interval_ms);
DECLARE_int32(raft_max_entries_size);
DECLARE_bool(raft_use_polar);
DECLARE_bool(raft_io_with_fb);
DECLARE_bool(raft_use_iouring);
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
namespace rpc {
DECLARE_bool(hpn_fixed_protocol_on_channel_socket);
}
}
}

class NodeTest : public testing::TestWithParam<std::tuple<const char*, bool>> {
protected:
    void SetUp() {
        raft::FLAGS_raft_use_polar = true;
        raft::FLAGS_raft_use_iouring = false;
        g_dont_print_apply_log = false;
        logging::FLAGS_verbose = 0;
        google::SetCommandLineOption("hpn_builtin_services_authorization", "");
        // google::SetCommandLineOption("raft_use_iouring", "true");
        google::SetCommandLineOption("crash_on_fatal_log", "true");
        google::SetCommandLineOption("min_log_level", "0");
        google::SetCommandLineOption("verbose", "99");
        google::SetCommandLineOption("hpn_health_check_interval", "1");
        auto [config, use_fb] = GetParam();
        if (config == std::string("NoReplication")) {
            raft::FLAGS_raft_max_parallel_append_entries_rpc_num = 1;
            raft::FLAGS_raft_enable_append_entries_cache = false;
        } else if (config == std::string("NoCache")) {
            raft::FLAGS_raft_max_parallel_append_entries_rpc_num = 32;
            raft::FLAGS_raft_enable_append_entries_cache = false;
        } else if (config == std::string("HasCache")) {
            raft::FLAGS_raft_max_parallel_append_entries_rpc_num = 32;
            raft::FLAGS_raft_enable_append_entries_cache = true;
            raft::FLAGS_raft_max_append_entries_cache_size = 8;
        }
        use_fb = false; // TODO: remove this after flatbuffer fixed
        raft::FLAGS_raft_use_flatbuffer = use_fb;
        baidu::hpn::rpc::FLAGS_hpn_fixed_protocol_on_channel_socket = !use_fb;
        baidu::hpn::polar::FLAGS_polar_thread_num = 1;
        auto test_info = ::testing::UnitTest::GetInstance()->current_test_info();
        LOG(NOTICE) << "We are in " << test_info->test_case_name()
                    << "." << test_info->name()
                    << "/" << config << "/" << (int)use_fb;
        LOG(NOTICE) << "FLAGS_polar_thread_num: " << baidu::hpn::polar::FLAGS_polar_thread_num
            << ", polar worker num: " << baidu::hpn::polar::poll_thread_num();

        baidu::hpn::polar::FLAGS_lcore_mask = "0x0F0000";
        baidu::hpn::polar::FLAGS_host_ip = "127.0.0.1";
        baidu::hpn::polar::FLAGS_server_ports_list = "8435,8436,8437,8438";
        baidu::hpn::polar::FLAGS_client_ports_start = 9100;
        baidu::hpn::polar::FLAGS_client_ports_end = 9200;
        baidu::hpn::polar::FLAGS_client_ports_num = 50;
        baidu::hpn::polar::FLAGS_socket_mem = "512,512";
        baidu::hpn::rpc::polar::GlobalPolarInitializeOrDie();

        ::system("rm -rf data");
        LOG(INFO) << "SetUp rm -rf data";
        ASSERT_EQ(0, raft::g_num_nodes.get_value());
    }

    void TearDown() {
        ::system("rm -rf data");
        LOG(INFO) << "TearDown rm -rf data";
        // Sleep for a while to wait all timer has stopped
        if (raft::g_num_nodes.get_value() != 0) {
            usleep(1000 * 1000);
            ASSERT_EQ(0, raft::g_num_nodes.get_value());
        }
    }
};

TEST_P(NodeTest, InitShutdown) {
    rpc::Server server;
    int ret = raft::add_service(&server, "0.0.0.0:8435");
    ASSERT_EQ(0, ret);
    rpc::ServerOptions server_options;
    server_options.use_polar = raft::FLAGS_raft_use_polar;
    ASSERT_EQ(0, server.Start("0.0.0.0:8435", &server_options));

    raft::Node* node = nullptr;
    MockFSM* mock_fsm = nullptr;

    raft::NodeOptions options;
    mock_fsm = new MockFSM(base::EndPoint());
    options.fsm = mock_fsm;
    options.log_uri = "local://./data/log";
    options.stable_uri = "local://./data/stable";
    options.snapshot_uri = "local://./data/snapshot";

    node = new raft::Node("unittest", raft::PeerId(base::EndPoint(base::get_host_ip(), 8435), 0));
    ret = RAFT_SYNC_BTHREAD_RUN_ON_POLAR(node->init(options), 0);
    ASSERT_EQ(0, ret);

    NODE_RUN(node->shutdown(NULL));

    raft::SynchronizedClosure join_done;
    NODE_ASYNC_RUN(node->join(&join_done));
    join_done.wait();

    bthread::CountdownEvent cond;
    base::IOBuf data;
    data.append("hello");
    raft::Task task;
    task.data = &data;
    task.done = NEW_APPLYCLOSURE(&cond);
    NODE_RUN(node->apply(task));
    cond.wait();

    auto release = [&] {
        delete node;
        delete mock_fsm;
    };
    NODE_RUN(release());
    server.Stop(200);
    server.Join();
}

TEST_P(NodeTest, Server) {
    rpc::Server server1;
    rpc::Server server2;
    ASSERT_EQ(0, raft::add_service(&server1, "0.0.0.0:8435"));
    ASSERT_EQ(0, raft::add_service(&server1, "0.0.0.0:8435"));
    ASSERT_EQ(0, raft::add_service(&server2, "0.0.0.0:8346"));

    rpc::ServerOptions server_options;
    server_options.use_polar = raft::FLAGS_raft_use_polar;
    server1.Start("0.0.0.0:8435", &server_options);
    server2.Start("0.0.0.0:8346", &server_options);
    server1.Stop(200);
    server1.Join();
    server2.Stop(200);
    server2.Join();
}

TEST_P(NodeTest, SingleNode) {
    rpc::Server server;
    int ret = raft::add_service(&server, 8435);
    rpc::ServerOptions server_options;
    server_options.use_polar = raft::FLAGS_raft_use_polar;
    server.Start(8435, &server_options);
    ASSERT_EQ(0, ret);

    raft::PeerId peer;
    peer.addr.ip = base::get_host_ip();
    peer.addr.port = 8435;
    peer.idx = 0;
    std::vector<raft::PeerId> peers;
    peers.push_back(peer);

    raft::Node* node = nullptr;
    MockFSM* mock_fsm = nullptr;

    raft::NodeOptions options;
    mock_fsm = new MockFSM(base::EndPoint());
    options.fsm = mock_fsm;
    options.election_timeout_ms = 300;
    options.initial_conf = raft::Configuration(peers);
    options.log_uri = "local://./data/log";
    options.stable_uri = "local://./data/stable";
    options.snapshot_uri = "local://./data/snapshot";

    node = new raft::Node("unittest", peer);
    ret = RAFT_SYNC_BTHREAD_RUN_ON_POLAR(node->init(options), 0);
    ASSERT_EQ(ret, 0);

    int target_tidx = node->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);
        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
        NODE_RUN(node->apply(task));
    }
    cond.wait();

    cond.reset(1);
    NODE_ASYNC_RUN(node->shutdown(NEW_SHUTDOWNCLOSURE(&cond, 0)));
    cond.wait();

    raft::SynchronizedClosure join_done;
    NODE_ASYNC_RUN(node->join(&join_done));
    join_done.wait();

    auto release = [&] {
        delete node;
        delete mock_fsm;
    };
    NODE_RUN(release());
    server.Stop(200);
    server.Join();
}

TEST_P(NodeTest, NoLeader) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    cluster.start(peers[0].addr);

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(1, nodes.size());

    raft::Node* follower = nodes[0];
    int target_tidx = follower->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, EPERM);
            follower->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // add peer1
    raft::PeerId peer3;
    peer3.addr.ip = base::get_host_ip();
    peer3.addr.port = 8435 + 3;
    peer3.idx = 0;

    cond.reset(1);
    auto add_peer = [&] {
        follower->add_peer(peer3, NEW_ADDPEERCLOSURE(&cond, EPERM));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(add_peer)), target_tidx);
    cond.wait();
    LOG(NOTICE) << "add peer " << peer3;

    // remove peer1
    raft::PeerId peer0;
    peer0.addr.ip = base::get_host_ip();
    peer0.addr.port = 8435 + 0;
    peer0.idx = 0;

    cond.reset(1);
    auto remove_peer = [&] {
        follower->remove_peer(peer0, NEW_REMOVEPEERCLOSURE(&cond, EPERM));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(remove_peer)), target_tidx);
    cond.wait();
    LOG(NOTICE) << "remove peer " << peer0;
}

TEST_P(NodeTest, TripleNode) {
    // send_entries prepare entry failed and on_error called
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    auto l = [=] {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        raft::Task task;
        task.data = &data;
        leader->apply(task);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l(), target_tidx);

    ASSERT_TRUE(cluster.ensure_same());

    {
        raft_channel_t channel;
        rpc::ChannelOptions options;
        options.protocol = rpc::PROTOCOL_HTTP;

        if (channel.Init(leader->node_id().peer_id.addr, &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
        }

        {
            rpc::Controller cntl;
            cntl.http_request().uri() = "/raft_stat";
            cntl.http_request().set_method(rpc::HTTP_METHOD_GET);

            channel.CallMethod(NULL, &cntl, NULL, NULL, NULL/*done*/);

            LOG(NOTICE) << "http return: \n" << cntl.response_attachment();
        }

        {
            rpc::Controller cntl;
            cntl.http_request().uri() = "/raft_stat/unittest";
            cntl.http_request().set_method(rpc::HTTP_METHOD_GET);

            channel.CallMethod(NULL, &cntl, NULL, NULL, NULL/*done*/);

            LOG(NOTICE) << "http return: \n" << cntl.response_attachment();
        }
    }

    // stop cluster
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    cluster.stop_all();
    LOG(WARNING) << "cluster all stoped";
}

TEST_P(NodeTest, LeaderFail) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
             base::IOBuf data;
             char data_buf[128];
             snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
             data.append(data_buf);

             raft::Task task;
             task.data = &data;
             task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
             leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // stop leader
    base::EndPoint old_leader = leader->node_id().peer_id.addr;
    LOG(WARNING) << "stop leader " << leader->node_id();
    cluster.stop(leader->node_id().peer_id.addr);

    // apply something when follower
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    target_tidx = nodes[0]->polar_worker_index();
    CHECK_GE(target_tidx, 0);
    cond.reset(10);
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "follower apply: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, -1);
            nodes[0]->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // elect new leader
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "elect new leader " << leader->node_id();
    target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // old leader restart
    ASSERT_EQ(0, cluster.start(old_leader));
    LOG(WARNING) << "restart old leader " << old_leader;

    // apply something
    cond.reset(10);
    for (int i = 20; i < 30; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // stop and clean old leader
    LOG(WARNING) << "stop old leader " << old_leader;
    cluster.stop(old_leader);
    LOG(WARNING) << "clean old leader data " << old_leader;
    cluster.clean(old_leader);

    sleep(2);
    // restart old leader
    ASSERT_EQ(0, cluster.start(old_leader));
    LOG(WARNING) << "restart old leader " << old_leader;

    cluster.ensure_same();

    cluster.stop_all();
}

TEST_P(NodeTest, configuration_change_use_two_stages) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers, 3000, 1000, true);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // start peer1
    raft::PeerId peer1;
    peer1.addr.ip = base::get_host_ip();
    peer1.addr.port = 8348;
    peer1.idx = 0;
    ASSERT_EQ(0, cluster.start(peer1.addr, true));
    LOG(NOTICE) << "start peer " << peer1;
    // wait until started successfully
    usleep(1000* 1000);

    // add peer1
    int64_t epoch = 1;
    std::stringstream ss1;
    ss1 << ++epoch;
    ss1 << "#" << ++epoch;
    LOG(NOTICE) << "add peer " << peer1;
    std::string conf_str = ss1.str();
    cond.reset(1);
    auto l = [&] {
        leader->add_peer(peers, peer1, conf_str, NEW_ADDPEERCLOSURE(&cond, 0));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();
    cluster.check_node_ready(peer1);
    std::vector<raft::PeerId> old_peers;
    std::vector<raft::PeerId> new_peers;
    std::string user_conf;
    auto c = [&] {
        leader->get_configuration(&old_peers, &new_peers, &user_conf);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(c(), target_tidx);
    ASSERT_EQ(epoch, atoll(user_conf.c_str()));
    peers.push_back(peer1);
    LOG(NOTICE) << "add peer " << peer1 << " done";

    cluster.ensure_same();
    // apply something
    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(3, nodes.size());
    const raft::PeerId follower_id = nodes[0]->node_id().peer_id;
    const base::EndPoint follower_addr = follower_id.addr;
    LOG(WARNING) << "stop and clean follower " << follower_addr;
    cluster.stop(follower_addr);
    cluster.clean(follower_addr);
    //remove follower
    LOG(WARNING) << "remove follower " << follower_addr;
    std::stringstream ss2;
    ss2 << ++epoch;
    ss2 << "#" << ++epoch;
    conf_str = ss2.str();
    cond.reset(1);
    auto l2 = [&] {
        leader->remove_peer(peers, follower_id, conf_str, NEW_REMOVEPEERCLOSURE(&cond, 0));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), target_tidx);
    cond.wait();
    old_peers.clear();
    new_peers.clear();
    auto c2 = [&] {
        leader->get_configuration(&old_peers, &new_peers, &user_conf);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(c2(), target_tidx);
    ASSERT_EQ(epoch, atoll(user_conf.c_str()));
    LOG(WARNING) << "remove follower " << follower_addr << " done";

    // apply something
    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();
    cluster.ensure_same();
    cluster.stop_all();
}

TEST_P(NodeTest, JoinNode) {
    std::vector<raft::PeerId> peers;
    raft::PeerId peer0;
    peer0.addr.ip = base::get_host_ip();
    peer0.addr.port = 8435;
    peer0.idx = 0;

    // start cluster
    peers.push_back(peer0);
    Cluster cluster("unittest", peers);
    ASSERT_EQ(0, cluster.start(peer0.addr));
    LOG(NOTICE) << "start single cluster " << peer0;

    cluster.wait_leader();

    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    ASSERT_EQ(leader->node_id().peer_id, peer0);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    // apply something
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
        LEADER_RUN(leader->apply(task));
    }
    cond.wait();

    // start peer1
    raft::PeerId peer1;
    peer1.addr.ip = base::get_host_ip();
    peer1.addr.port = 8435 + 1;
    peer1.idx = 0;
    ASSERT_EQ(0, cluster.start(peer1.addr, true));
    LOG(NOTICE) << "start peer " << peer1;
    // wait until started successfully
    usleep(1000* 1000);

    // add peer1
    cond.reset(1);
    LEADER_RUN(leader->add_peer(peer1, NEW_ADDPEERCLOSURE(cond_ptr, 0)));
    cond.wait();
    LOG(NOTICE) << "add peer " << peer1;

    cluster.ensure_same();

    // add peer2 when peer not start
    raft::PeerId peer2;
    peer2.addr.ip = base::get_host_ip();
    peer2.addr.port = 8435 + 2;
    peer2.idx = 0;

    cond.reset(1);
    peers.push_back(peer1);
    LEADER_RUN(leader->add_peer(peer2, NEW_ADDPEERCLOSURE(cond_ptr, raft::ECATCHUP)));
    cond.wait();

    // start peer2 after some seconds wait 
    sleep(2);
    ASSERT_EQ(0, cluster.start(peer2.addr, true));
    LOG(NOTICE) << "start peer " << peer2;

    usleep(3000 * 1000L);

    raft::PeerId peer4("192.168.1.1:1234");

    // re add peer2
    cond.reset(2);
    // {peer0,peer1} add peer2
    LEADER_RUN(leader->add_peer(peer2, NEW_ADDPEERCLOSURE(cond_ptr, 0)));
    LOG(NOTICE) << "re add peer2 " << peer2;
    // concurrent configration change
    LEADER_RUN(leader->add_peer(peer4, NEW_ADDPEERCLOSURE(cond_ptr, EBUSY)));
    LOG(NOTICE) << "concurrent configration change";
    /*
    // new peer equal old configuration
    leader->add_peer(peers, peer1, NEW_ADDPEERCLOSURE(&cond, EBUSY));
    */
    cond.wait();
    cluster.check_node_ready(peer2);

    cond.reset(1);
    // retry add_peer direct ok
    LEADER_RUN(leader->add_peer(peer2, NEW_ADDPEERCLOSURE(cond_ptr, 0)));
    LOG(NOTICE) << "retry add_peer direct peer2 " << peer2;
    /*
    // {peer0, peer1, peer2} can't accept peers{peer0, peer1}, must skip same check
    leader->add_peer(peer1, NEW_ADDPEERCLOSURE(&cond, EINVAL));
    */
    cond.wait();

    cluster.ensure_same();

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_P(NodeTest, Leader_step_down_during_install_snapshot) {
    std::vector<raft::PeerId> peers;
    raft::PeerId peer0;
    peer0.addr.ip = base::get_host_ip();
    peer0.addr.port = 8435;
    peer0.idx = 0;

    // start cluster
    peers.push_back(peer0);
    Cluster cluster("unittest", peers);
    ASSERT_EQ(0, cluster.start(peer0.addr));
    LOG(NOTICE) << "start single cluster " << peer0;

    cluster.wait_leader();

    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    ASSERT_EQ(leader->node_id().peer_id, peer0);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    // apply something
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data; 
            std::string data_buf;
            data_buf.resize(256 * 1024, 'a');
            data.append(data_buf);
            
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // trigger leader snapshot
    LOG(WARNING) << "trigger leader snapshot ";
    cond.reset(1);
    auto func1 = [&] { leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func1)), target_tidx);
    cond.wait();

    cond.reset(10);
    // apply something
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            std::string data_buf;
            data_buf.resize(256 * 1024, 'b');
            data.append(data_buf);
            
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();
    
    // trigger leader snapshot again to compact logs
    LOG(WARNING) << "trigger leader snapshot again";
    cond.reset(1);
    auto func2 = [&] { leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func2)), target_tidx);
    cond.wait();

    // start peer1
    raft::PeerId peer1;
    peer1.addr.ip = base::get_host_ip();
    peer1.addr.port = 8435 + 1;
    peer1.idx = 0;
    ASSERT_EQ(0, cluster.start(peer1.addr, true));
    LOG(NOTICE) << "start peer " << peer1;
    // wait until started successfully
    usleep(1000* 1000);

    // add peer1, leader step down while caught_up
    cond.reset(1);
    LOG(NOTICE) << "add peer: " << peer1;
    auto func3 = [&] { leader->add_peer(peer1, NEW_ADDPEERCLOSURE(&cond, EPERM)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func3)), target_tidx);
    usleep(500 * 1000);

    {
        raft_channel_t channel;
        rpc::ChannelOptions options;
        options.protocol = rpc::PROTOCOL_HTTP;
        if (channel.Init(leader->node_id().peer_id.addr, &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
        }
        {
            rpc::Controller cntl;
            cntl.http_request().uri() = "/raft_stat/unittest";
            cntl.http_request().set_method(rpc::HTTP_METHOD_GET);
            channel.CallMethod(NULL, &cntl, NULL, NULL, NULL/* done*/);
            LOG(NOTICE) << "http return: \n" << cntl.response_attachment();
        }
    }

    LOG(NOTICE) << "leader " << leader->node_id() 
                << " step_down because of some error";
    base::Status status;
    status.set_error(raft::ERAFTTIMEDOUT, "Majority of the group dies");
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(leader->_impl->step_down(leader->_impl->_current_term, false, status), target_tidx);
    cond.wait(); 
    
    // add peer1 again, success 
    LOG(NOTICE) << "add peer again: " << peer1;
    cond.reset(1);
    cluster.wait_leader();
    leader = cluster.leader();
    auto func4 = [&] { leader->add_peer(peer1, NEW_ADDPEERCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func4)), target_tidx);
    cond.wait(); 
    cluster.check_node_ready(peer1);
    
    cluster.ensure_same();
    
    LOG(TRACE) << "stop cluster";
    cluster.stop_all();
}

TEST_P(NodeTest, Report_error_during_install_snapshot) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }
    
    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            std::string data_buf;
            data_buf.resize(256 * 1024, 'a');
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    cluster.ensure_same();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    // stop follower
    LOG(WARNING) << "stop follower";
    base::EndPoint follower_addr = nodes[0]->node_id().peer_id.addr;
    cluster.stop(follower_addr);

    // apply something
    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        auto l = [=] {
            base::IOBuf data;
            std::string data_buf;
            data_buf.resize(256 * 1024, 'b');
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // trigger leader snapshot
    LOG(WARNING) << "trigger leader snapshot ";
    cond.reset(1);
    auto func1 = [&] { leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func1)), target_tidx);
    cond.wait();

    // apply something
    cond.reset(10);
    for (int i = 20; i < 30; i++) {
        auto l = [=] {
            base::IOBuf data;
            std::string data_buf;
            data_buf.resize(256 * 1024, 'c');
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // trigger leader snapshot again to compact logs
    LOG(WARNING) << "trigger leader snapshot again";
    cond.reset(1);
    auto func2 = [&] { leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func2)), target_tidx);
    cond.wait();

    LOG(WARNING) << "restart follower";
    ASSERT_EQ(0, cluster.start(follower_addr));
    usleep(1*1000*1000);
    
    // trigger newly-started follower report_error when install_snapshot
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(cluster._nodes.back()->_impl->_snapshot_executor->report_error(EIO, "%s", 
                                                    "Fail to close writer"), target_tidx);
    
    sleep(2);
    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_P(NodeTest, RemoveFollower) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    // apply something
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    cluster.ensure_same();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    const raft::PeerId follower_id = nodes[0]->node_id().peer_id;
    const base::EndPoint follower_addr = follower_id.addr;
    // stop follower
    LOG(WARNING) << "stop and clean follower " << follower_addr;
    cluster.stop(follower_addr);
    cluster.clean(follower_addr);

    // remove follower
    LOG(WARNING) << "remove follower " << follower_addr;
    cond.reset(1);
    auto l = [&] {
        leader->remove_peer(follower_id, NEW_REMOVEPEERCLOSURE(&cond, 0));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();

    //stop and clean one follower
    //LOG(WARNING) << "stop follower " << follower_addr;
    //cluster.stop(follower_addr);
    //LOG(WARNING) << "clean follower data " << follower_addr;
    //cluster.clean(follower_addr);

    // apply something
    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    cluster.followers(&nodes);
    ASSERT_EQ(1, nodes.size());

    peers.clear();
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        if (peer.addr != follower_addr) {
            peers.push_back(peer);
        }
    }

    // start follower
    LOG(WARNING) << "start follower " << follower_addr;
    ASSERT_EQ(0, cluster.start(follower_addr));

    // re add follower fail when leader step down
    LOG(WARNING) << "add follower " << follower_addr;
    cond.reset(1);
    auto l2 = [&] {
        leader->add_peer(follower_id, NEW_ADDPEERCLOSURE(&cond, 0));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), target_tidx);
    cond.wait();

    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    cluster.ensure_same();
}

TEST_P(NodeTest, RemoveLeader) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    // apply something
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    base::EndPoint old_leader_addr = leader->node_id().peer_id.addr;
    LOG(WARNING) << "remove leader " << old_leader_addr;
    cond.reset(1);
    auto l = [&] {
        leader->remove_peer(leader->node_id().peer_id, NEW_REMOVEPEERCLOSURE(&cond, 0));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();

    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    LOG(INFO) << "here";
    cond.wait();

    LOG(WARNING) << "stop and clear leader " << old_leader_addr;
    cluster.stop(old_leader_addr);
    cluster.clean(old_leader_addr);

    LOG(WARNING) << "start old leader " << old_leader_addr;
    cluster.start(old_leader_addr);

    LOG(WARNING) << "add old leader " << old_leader_addr;
    cond.reset(1);
    peers.clear();
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        if (peer.addr != old_leader_addr) {
            peers.push_back(peer);
        }
    }
    auto l2 = [&] {
        leader->add_peer(raft::PeerId(old_leader_addr, 0), NEW_ADDPEERCLOSURE(&cond, 0));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), target_tidx);
    cond.wait();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    cluster.ensure_same();
}

TEST_P(NodeTest, restart_without_stable_meta) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    cluster.ensure_same();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    // stop follower
    base::EndPoint follower_addr = nodes[0]->node_id().peer_id.addr;
    LOG(WARNING) << "stop follower: " << follower_addr;
    cluster.stop(follower_addr);
    
    ::system(base::string_printf("rm -rf ./data/%s/stable/*", 
                                 base::endpoint2str(follower_addr).c_str()).c_str());
    
    LOG(INFO) << "restart follower: " << follower_addr;
    ASSERT_EQ(0, cluster.start(follower_addr));
    
    // wait new leader elected
    bthread_usleep(3000*1000);
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(INFO) << "leader is " << leader->node_id();
    target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    for (int i = 0; i < 10; i++) {
        bool rc = false;
        while(true) {
            cond.reset(1);
            base::Status st;
            base::Status* st_ptr = &st;
            auto l = [=] {
                if (!leader->is_leader()) {
                    return false;
                }
                base::IOBuf data;
                char data_buf[128];
                snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
                data.append(data_buf);

                raft::Task task;
                task.data = &data;
                task.done = NEW_APPLYCLOSURE(cond_ptr, st_ptr);
                leader->apply(task);
                return true;
            };
            rc = RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l(), target_tidx);
            if (!rc) {
                cluster.wait_leader();
                leader = cluster.leader();
                ASSERT_TRUE(leader != NULL);
                LOG(INFO) << "leader changes to " << leader->node_id();
                target_tidx = leader->polar_worker_index();
                CHECK_GE(target_tidx, 0);
                continue;
            }
            cond.wait();
            if (!st.ok()) {
                LOG(INFO) << leader->node_id() << " apply failed and retry, rc: " << st.error_code() << " " << st;
                continue;
            }
            break;
        }
    }

    cluster.ensure_same();
    
    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_P(NodeTest, TriggerVote) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    // apply something
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    cluster.ensure_same();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    auto l = [&] {
        leader->vote(1000);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l(), leader->polar_worker_index());

    auto l0 = [&] {
        nodes[0]->vote(200);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l0(), nodes[0]->polar_worker_index());

    auto l1 = [&] {
        nodes[1]->vote(600);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l1(), nodes[0]->polar_worker_index());

    LOG(WARNING) << "trigger vote";

    // max wait 5 seconds for new leader
    int count = 0;
    raft::NodeId old_leader = leader->node_id();
    while (1) {
        cluster.wait_leader();
        leader = cluster.leader();
        ASSERT_TRUE(leader != NULL);
        if (leader->node_id() == old_leader && count < 5) {
            count++;
            sleep(1);
        } else {
            break;
        }
    }
    LOG(WARNING) << "new leader is " << leader->node_id();
}

TEST_P(NodeTest, PreVote) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    bthread::CountdownEvent cond(10);
    // apply something
    auto l = [&] {
        for (int i = 0; i < 10; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();

    cluster.ensure_same();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());
    const raft::PeerId follower_id = nodes[0]->node_id().peer_id;
    const base::EndPoint follower_addr = follower_id.addr;

    const int64_t saved_term = leader->_impl->_current_term;
    //remove follower
    LOG(WARNING) << "remove follower " << follower_addr;
    cond.reset(1);
    auto l2 = [&] {
        leader->remove_peer(follower_id, NEW_REMOVEPEERCLOSURE(&cond, 0));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), target_tidx);
    cond.wait();

    // apply something
    cond.reset(10);
    auto l3 = [&] {
        for (int i = 10; i < 20; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), target_tidx);
    cond.wait();

    sleep(2);

    //add follower
    LOG(WARNING) << "add follower " << follower_addr;
    peers.clear();
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        if (peer.addr != follower_addr) {
            peers.push_back(peer);
        }
    }
    cond.reset(1);
    auto l4 = [&] {
        leader->add_peer(follower_id, NEW_REMOVEPEERCLOSURE(&cond, 0));
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l4)), target_tidx);
    cond.wait();

    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);

    ASSERT_EQ(saved_term, leader->_impl->_current_term);
}

TEST_P(NodeTest, Vote_timedout) {
    google::SetCommandLineOption("raft_step_down_when_vote_timedout", "true");
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 2; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers, 500);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr, false, 1));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    usleep(1000 * 1000);
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_FALSE(nodes.empty());
    // stop follower, only one node left 
    const base::EndPoint follower_addr = nodes[0]->_impl->_server_id.addr;
    cluster.stop(follower_addr);
    
    // wait old leader to step down 
    usleep(2000 * 1000);
    // trigger old leader to vote, expecting fail when vote timedout
    auto l = [&] {
        std::unique_lock<raft::Mutex> lck(leader->_impl->_mutex);
        leader->_impl->elect_self(&lck);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l(), target_tidx);
    usleep(3000 * 1000);
   
    // start the stopped follower
    LOG(WARNING) << "restart follower";
    cluster.start(follower_addr);
    usleep(2000 * 1000);

    ASSERT_TRUE(cluster.ensure_same(5));
    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
    google::SetCommandLineOption("raft_step_down_when_vote_timedout", "false");
}
TEST_P(NodeTest, SetPeer1) {
    // bootstrap from null
    Cluster cluster("unittest", std::vector<raft::PeerId>());
    raft::PeerId boot_peer;
    boot_peer.addr.ip = base::get_host_ip();
    boot_peer.addr.port = 8435;
    boot_peer.idx = 0;

    ASSERT_EQ(0, cluster.start(boot_peer.addr));
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(1, nodes.size());

    std::vector<raft::PeerId> peers;
    peers.push_back(boot_peer);
    auto l = [&] {
        return nodes[0]->reset_peers(raft::Configuration(peers)).ok();
    };
    bool rc = RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l(), nodes[0]->polar_worker_index());
    ASSERT_TRUE(rc);

    cluster.wait_leader();
}

TEST_P(NodeTest, SetPeer2) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    std::cout << "Here" << std::endl;
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    base::EndPoint leader_addr = leader->node_id().peer_id.addr;
    LOG(WARNING) << "leader is " << leader->node_id();
    std::cout << "Here" << std::endl;
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    // apply something
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();
    std::cout << "Here" << std::endl;

    // check follower
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());
    raft::PeerId follower_peer1 = nodes[0]->node_id().peer_id;
    raft::PeerId follower_peer2 = nodes[1]->node_id().peer_id;

    LOG(WARNING) << "stop and clean follower " << follower_peer1;
    cluster.stop(follower_peer1.addr);
    cluster.clean(follower_peer1.addr);
    cluster.ensure_same(5);
    cluster.wait_leader();
    ASSERT_EQ(cluster.leader()->node_id(), leader->node_id());

    std::cout << "Here" << std::endl;
    // apply something
    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
        LEADER_RUN(leader->apply(task));
    }
    cond.wait();
    
    std::cout << "Here" << std::endl;
    //set peer when no quorum die
    std::vector<raft::PeerId> new_peers;
    LOG(WARNING) << "set peer to " << leader_addr;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        if (peer != follower_peer1) {
            new_peers.push_back(peer);
        }
    }
    /*
    std::cout << "Here" << std::endl;
    ASSERT_EQ(EINVAL, leader->set_peer(peers, new_peers));
    std::cout << "Here 7" << std::endl;
    */
    LOG(WARNING) << "stop and clean follower " << follower_peer2;
    cluster.stop(follower_peer2.addr);
    cluster.clean(follower_peer2.addr);

    std::cout << "Here 8" << std::endl;
    // leader will stepdown, become follower
    sleep(2);

    new_peers.clear();
    new_peers.push_back(raft::PeerId(leader_addr, 0));

    // new peers equal current conf
    auto l = [&] {
        return leader->reset_peers(raft::Configuration(peers)).ok();
    };
    bool rc = RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l(), target_tidx);
    ASSERT_TRUE(rc);
    /*
    // old peers not match current conf
    ASSERT_EQ(EINVAL, leader->set_peer(new_peers, new_peers));
    // new_peers not include in current conf
    new_peers.clear();
    new_peers.push_back(raft::PeerId(leader_addr, 1));
    ASSERT_EQ(EINVAL, leader->set_peer(peers, new_peers));
    */

    // set peer when quorum die
    LOG(WARNING) << "set peer to " << leader_addr;
    new_peers.clear();
    new_peers.push_back(raft::PeerId(leader_addr, 0));
    auto l2 = [&] {
        return leader->reset_peers(raft::Configuration(new_peers)).ok();
    };
    rc = RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l2(), target_tidx);
    ASSERT_TRUE(rc);
    std::cout << "Here 9" << std::endl;

    cluster.wait_leader();
    std::cout << "Here 10" << std::endl;
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    ASSERT_EQ(leader->node_id().peer_id.addr, leader_addr);
    target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    LOG(WARNING) << "start old follower " << follower_peer1;
    ASSERT_EQ(0, cluster.start(follower_peer1.addr, true));
    LOG(WARNING) << "start old follower " << follower_peer2;
    ASSERT_EQ(0, cluster.start(follower_peer2.addr, true));
    usleep(3000 * 1000L);

    LOG(WARNING) << "add old follower " << follower_peer1;
    cond.reset(1);
    LEADER_RUN(leader->add_peer(follower_peer1, NEW_ADDPEERCLOSURE(&cond, 0)));
    cluster.check_node_ready(follower_peer1);
    cond.wait();

    LOG(WARNING) << "add old follower " << follower_peer2;
    cond.reset(1);
    new_peers.push_back(follower_peer1);
    LEADER_RUN(leader->add_peer(follower_peer2, NEW_ADDPEERCLOSURE(&cond, 0)));
    cond.wait();
    cluster.check_node_ready(follower_peer2);

    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    cluster.ensure_same();
}

TEST_P(NodeTest, RestoreSnapshot) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    base::EndPoint leader_addr = leader->node_id().peer_id.addr;
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    cluster.ensure_same();

    // trigger leader snapshot
    LOG(WARNING) << "trigger leader snapshot ";
    cond.reset(1);
    auto func = [=] { leader->snapshot(NEW_SNAPSHOTCLOSURE(cond_ptr, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func)), target_tidx);
    cond.wait();

    // stop leader
    LOG(WARNING) << "stop leader";
    cluster.stop(leader->node_id().peer_id.addr);

    sleep(2);

    LOG(WARNING) << "restart leader";
    ASSERT_EQ(0, cluster.start(leader_addr));

    cluster.ensure_same();

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_P(NodeTest, InstallSnapshot) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    cluster.ensure_same();

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    // stop follower
    LOG(WARNING) << "stop follower";
    base::EndPoint follower_addr = nodes[0]->node_id().peer_id.addr;
    cluster.stop(follower_addr);

    // apply something
    cond.reset(10);
    for (int i = 10; i < 20; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // trigger leader snapshot
    LOG(WARNING) << "trigger leader snapshot ";
    cond.reset(1);
    {
        auto l = [=] { leader->snapshot(NEW_SNAPSHOTCLOSURE(cond_ptr, 0)); };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // apply something
    cond.reset(10);
    for (int i = 20; i < 30; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // trigger leader snapshot again to compact logs
    LOG(WARNING) << "trigger leader snapshot again";
    cond.reset(1);
    {
        auto l = [=] { leader->snapshot(NEW_SNAPSHOTCLOSURE(cond_ptr, 0)); };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    LOG(WARNING) << "restart follower";
    ASSERT_EQ(0, cluster.start(follower_addr));

    sleep(2);

    cluster.ensure_same();

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_P(NodeTest, NoSnapshot) {
    rpc::Server server;
    rpc::ServerOptions server_options;
    server_options.use_polar = raft::FLAGS_raft_use_polar;
    int ret = raft::add_service(&server, "0.0.0.0:8435");
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, server.Start(8435, &server_options));

    raft::PeerId peer;
    peer.addr.ip = base::get_host_ip();
    peer.addr.port = 8435;
    peer.idx = 0;
    std::vector<raft::PeerId> peers;
    peers.push_back(peer);
    int target_tidx = 0;

    raft::NodeOptions options;
    options.election_timeout_ms = 300;
    options.initial_conf = raft::Configuration(peers);
    options.fsm = new MockFSM(base::EndPoint());
    options.log_uri = "local://./data/log";
    options.stable_uri = "local://./data/stable";

    raft::Node* node = new raft::Node("unittest", peer);
    ASSERT_EQ(0, RAFT_SYNC_BTHREAD_RUN_ON_POLAR(node->init(options), target_tidx));

    // wait node elect to leader
    sleep(2);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            node->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    // trigger snapshot, not expect ret
    cond.reset(1);
    auto func2 = [=] { node->snapshot(NEW_SNAPSHOTCLOSURE(cond_ptr, -1)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func2)), target_tidx);
    cond.wait();

    // shutdown
    cond.reset(1);
    auto func3 = [=] { node->shutdown(NEW_SHUTDOWNCLOSURE(cond_ptr, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func3)), target_tidx);
    cond.wait();

    cond.reset();
    auto release = [&] {
        delete node;
        cond.signal();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(release)), 0);
    cond.wait();
    // stop
    server.Stop(200);
    server.Join();
}

TEST_P(NodeTest, AutoSnapshot) {
    rpc::Server server;
    rpc::ServerOptions server_options;
    server_options.use_polar = raft::FLAGS_raft_use_polar;
    int ret = raft::add_service(&server, "0.0.0.0:8435");
    ASSERT_EQ(0, ret);
    ASSERT_EQ(0, server.Start(8435, &server_options));

    raft::PeerId peer;
    peer.addr.ip = base::get_host_ip();
    peer.addr.port = 8435;
    peer.idx = 0;
    std::vector<raft::PeerId> peers;
    peers.push_back(peer);
    int target_tidx = 0;

    raft::NodeOptions options;
    options.election_timeout_ms = 300;
    options.initial_conf = raft::Configuration(peers);
    options.fsm = new MockFSM(base::EndPoint());
    options.log_uri = "local://./data/log";
    options.stable_uri = "local://./data/stable";
    options.snapshot_uri = "local://./data/snapshot";
    options.snapshot_interval_s = 10;

    raft::Node* node = new raft::Node("unittest", peer);
    ASSERT_EQ(0, RAFT_SYNC_BTHREAD_RUN_ON_POLAR(node->init(options), target_tidx));

    // wait node elect to leader
    sleep(2);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            node->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    sleep(10);
    ASSERT_GT(static_cast<MockFSM*>(options.fsm)->snapshot_index, 0);

    // shutdown
    cond.reset(1);
    auto func = [=] { node->shutdown(NEW_SHUTDOWNCLOSURE(cond_ptr, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func)), target_tidx);
    cond.wait();

    cond.reset();
    auto release = [&] {
        delete node;
        cond.signal();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(release)), 0);
    cond.wait();
    // stop
    server.Stop(200);
    server.Join();
}

TEST_P(NodeTest, LeaderShouldNotChange) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader0 = cluster.leader();
    ASSERT_TRUE(leader0 != NULL);
    LOG(WARNING) << "leader is " << leader0->node_id();
    const int64_t saved_term = leader0->_impl->_current_term;
    usleep(5000 * 1000);
    cluster.wait_leader();
    raft::Node* leader1 = cluster.leader();
    LOG(WARNING) << "leader is " << leader1->node_id();
    ASSERT_EQ(leader0, leader1);
    ASSERT_EQ(saved_term, leader1->_impl->_current_term);
    cluster.stop_all();
}

TEST_P(NodeTest, RecoverFollower) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr, false, 1));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();

    usleep(1000 * 1000);
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_FALSE(nodes.empty());
    const base::EndPoint follower_addr = nodes[0]->_impl->_server_id.addr;
    cluster.stop(follower_addr);

    // apply something
    bthread::CountdownEvent cond(10);
    auto l = [&] {
        for (int i = 0; i < 10; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();
    auto l2 = [&] {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        raft::Task task;
        task.data = &data;
        leader->apply(task);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l2(), target_tidx);
    // wait leader to compact logs
    usleep(5000 * 1000);

    // Start the stopped follower, expecting that leader would recover it
    LOG(WARNING) << "restart follower";
    cluster.start(follower_addr);
    LOG(WARNING) << "restart follower done";
    LOG(WARNING) << "here";
    ASSERT_TRUE(cluster.ensure_same(5));
    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_P(NodeTest, leader_transfer) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr, false, 1));
    }
    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    raft::PeerId target = nodes[0]->node_id().peer_id;
    ASSERT_EQ(0, LEADER_RUN(leader->transfer_leadership_to(target)));
    usleep(500 * 1000);
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_EQ(target, leader->node_id().peer_id);
    ASSERT_TRUE(cluster.ensure_same(5));
    cluster.stop_all();
}

TEST_P(NodeTest, leader_transfer_before_log_is_compleleted) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers, 5000);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr, false, 1));
    }
    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    raft::PeerId target = nodes[0]->node_id().peer_id;
    cluster.stop(target.addr);
    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();
    ASSERT_EQ(0, RAFT_SYNC_BTHREAD_RUN_ON_POLAR(leader->transfer_leadership_to(target), target_tidx));
    cond.reset(1);
    auto l = [=] {
        raft::Task task;
        base::IOBuf data;
        data.resize(5, 'a');
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(cond_ptr, EBUSY);
        leader->apply(task);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();
    cluster.start(target.addr);
    usleep(5000 * 1000);
    LOG(INFO) << "here";
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_EQ(target, leader->node_id().peer_id);
    ASSERT_TRUE(cluster.ensure_same(5));
    cluster.stop_all();
}

TEST_P(NodeTest, leader_transfer_resume_on_failure) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr, false, 1));
    }
    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    raft::PeerId target = nodes[0]->node_id().peer_id;
    cluster.stop(target.addr);
    // apply something
    bthread::CountdownEvent cond(10);
    auto l = [&] {
        for (int i = 0; i < 10; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();
    ASSERT_EQ(0, RAFT_SYNC_BTHREAD_RUN_ON_POLAR(leader->transfer_leadership_to(target), target_tidx));
    raft::Node* saved_leader = leader;
    cond.reset(1);
    auto l2 = [&] {
        raft::Task task;
        base::IOBuf data;
        data.resize(5, 'a');
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, EBUSY);
        leader->apply(task);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), target_tidx);
    cond.wait();
    //cluster.start(target.addr);
    usleep(1000 * 1000);
    LOG(INFO) << "here";
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_EQ(saved_leader, leader);
    LOG(INFO) << "restart the target follower";
    cluster.start(target.addr);
    usleep(1000 * 1000);
    cond.reset(1);
    auto l3 = [&] {
        raft::Task task;
        base::IOBuf data;
        data.resize(5, 'a');
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        leader->apply(task);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), target_tidx);
    cond.wait();
    ASSERT_TRUE(cluster.ensure_same(10));
    cluster.stop_all();
}

class MockFSM1 : public MockFSM {
protected:
    MockFSM1() : MockFSM(base::EndPoint()) {}
    virtual void on_snapshot_load(raft::SnapshotReader* reader, raft::Closure* done) override {
        rpc::ClosureGuard done_guard(done);
        (void)reader;
        CHECK(done);
        done->status().set_error(EIO, "load failed");
    }
};

TEST_P(NodeTest, shutdown_and_join_work_after_init_fails) {
    rpc::ServerOptions server_options;
    server_options.use_polar = raft::FLAGS_raft_use_polar;
    rpc::Server server;
    int ret = raft::add_service(&server, 5006);
    server.Start(5006, &server_options);
    ASSERT_EQ(0, ret);

    raft::PeerId peer;
    peer.addr.ip = base::get_host_ip();
    peer.addr.port = 5006;
    peer.idx = 0;
    std::vector<raft::PeerId> peers;
    peers.push_back(peer);

    int target_tidx = 0;
    raft::NodeOptions options;
    options.election_timeout_ms = 300;
    options.initial_conf = raft::Configuration(peers);
    options.fsm = new MockFSM1();
    options.log_uri = "local://./data/log";
    options.stable_uri = "local://./data/stable";
    options.snapshot_uri = "local://./data/snapshot";
    raft::Node* node = new raft::Node("unittest", peer);
    ret = RAFT_SYNC_BTHREAD_RUN_ON_POLAR(node->init(options), target_tidx);
    ASSERT_EQ(0, ret);
    sleep(1);
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);
        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        NODE_RUN(node->apply(task));
    }
    cond.wait();
    LOG(INFO) << "begin to save snapshot";
    NODE_RUN(node->snapshot(NULL));
    LOG(INFO) << "begin to shutdown";
    NODE_RUN(node->shutdown(NULL));
    raft::SynchronizedClosure join_done;
    NODE_ASYNC_RUN(node->join(&join_done));
    join_done.wait();
    NODE_RUN(delete node);
    
    options.election_timeout_ms = 300;
    options.initial_conf = raft::Configuration(peers);
    options.fsm = new MockFSM1();
    options.log_uri = "local://./data/log";
    options.stable_uri = "local://./data/stable";
    options.snapshot_uri = "local://./data/snapshot";
    node = new raft::Node("unittest", peer);
    LOG(INFO) << "node init again";
    ret = RAFT_SYNC_BTHREAD_RUN_ON_POLAR(node->init(options), target_tidx);
    ASSERT_NE(0, ret);
    NODE_RUN(node->shutdown(NULL));
    join_done.reset();
    NODE_ASYNC_RUN(node->join(&join_done));
    join_done.wait();
    NODE_RUN(delete node);

    server.Stop(200);
    server.Join();
}

TEST_P(NodeTest, shutting_leader_triggers_timeout_now) {
    google::SetCommandLineOption("raft_sync", "false");
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }
    // start cluster
    Cluster cluster("unittest", peers, 1000);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(INFO) << "shutdown leader" << leader->node_id();
    LEADER_RUN(leader->shutdown(NULL));
    raft::SynchronizedClosure join_done;
    LEADER_ASYNC_RUN(leader->join(&join_done));
    join_done.wait();
    LOG(INFO) << "join";
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    google::SetCommandLineOption("raft_sync", "true");
}

TEST_P(NodeTest, removing_leader_triggers_timeout_now) {
    google::SetCommandLineOption("raft_sync", "false");
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }
    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    raft::PeerId old_leader_id = leader->node_id().peer_id;
    bthread::CountdownEvent cond;
    LOG(WARNING) << "remove leader " << old_leader_id;
    LEADER_RUN(leader->remove_peer(old_leader_id, NEW_REMOVEPEERCLOSURE(&cond, 0)));
    cond.wait();
    usleep(1000 * 1000);
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    ASSERT_NE(old_leader_id, leader->node_id().peer_id);
    google::SetCommandLineOption("raft_sync", "true");
}

TEST_P(NodeTest, transfer_should_work_after_install_snapshot) {
    std::vector<raft::PeerId> peers;
    for (size_t i = 0; i < 3; ++i) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 8435 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }
    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size() - 1; i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(1, nodes.size());
    raft::PeerId follower = nodes[0]->node_id().peer_id;
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(leader->transfer_leadership_to(follower), target_tidx);
    usleep(2000 * 1000);
    leader = cluster.leader();
    while (leader == nullptr) {
        leader = cluster.leader();
    }
    ASSERT_EQ(follower, leader->node_id().peer_id);
    cond.reset(1);
    auto func1 = [&] { leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func1)), target_tidx);
    cond.wait();
    cond.reset(1);
    auto func2 = [&] { leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func2)), target_tidx);
    cond.wait();

    // Start the last peer which should be recover with snapshot
    raft::PeerId last_peer = peers.back(); 
    cluster.start(last_peer.addr);
    usleep(5000 * 1000);

    ASSERT_EQ(0, LEADER_RUN(leader->transfer_leadership_to(last_peer)));
    usleep(2000 * 1000);
    cluster.check_node_ready(last_peer);
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    ASSERT_EQ(last_peer, leader->node_id().peer_id);
}

TEST_P(NodeTest, DISABLED_read_committed_user_log) {
                    // ^^ not supported in polar mode
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader_first
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();

    // apply something
    bthread::CountdownEvent cond(10);
    auto l = [&] {
        for (int i = 0; i < 10; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();
    sleep(2);

    auto l2 = [&] {
        // index == 1 is a CONFIGURATION log, so real_index will be 2 when returned.
        int64_t index = 1;
        raft::UserLog* user_log = new raft::UserLog();
        base::Status status = leader->read_committed_user_log(index, user_log);
        ASSERT_EQ(0, status.error_code());
        ASSERT_EQ(2, user_log->log_index());
        LOG(INFO) << "read local committed user log from leader:" << leader->node_id() << ", index:"
            << index << ", real_index:" << user_log->log_index() << ", data:" << user_log->log_data() 
            << ", status:" << status; 

        // index == 5 is a DATA log(a user log)
        index = 5;
        user_log->reset();
        status = leader->read_committed_user_log(index, user_log);
        ASSERT_EQ(0, status.error_code());
        ASSERT_EQ(5, user_log->log_index());
        LOG(INFO) << "read local committed user log from leader:" << leader->node_id() << ", index:"
            << index << ", real_index:" << user_log->log_index() << ", data:" << user_log->log_data() 
            << ", status:" << status; 

        // index == 15 is greater than last_committed_index
        index = 15;
        user_log->reset();
        status = leader->read_committed_user_log(index, user_log);
        ASSERT_EQ(raft::ENOMOREUSERLOG, status.error_code());
        LOG(INFO) << "read local committed user log from leader:" << leader->node_id() << ", index:"
            << index << ", real_index:" << user_log->log_index() << ", data:" << user_log->log_data() 
            << ", status:" << status; 

        // index == 0, invalid request index.
        index = 0;
        user_log->reset();
        status = leader->read_committed_user_log(index, user_log);
        ASSERT_EQ(EINVAL, status.error_code());
        LOG(INFO) << "read local committed user log from leader:" << leader->node_id() << ", index:"
            << index << ", real_index:" << user_log->log_index() << ", data:" << user_log->log_data() 
            << ", status:" << status; 
        delete user_log;
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l2(), target_tidx);

    // trigger leader snapshot for the first time
    LOG(WARNING) << "trigger leader snapshot ";
    cond.reset(1);
    auto l3 = [&] { leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), target_tidx);
    cond.wait();

    // remove and add a peer to add two CONFIGURATION logs
    std::vector<raft::Node*> followers;
    cluster.followers(&followers);
    raft::PeerId follower_test = followers[0]->node_id().peer_id;
    cond.reset(1);
    auto l4 = [&] { leader->remove_peer(follower_test, NEW_REMOVEPEERCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l4)), target_tidx);
    cond.wait();
    std::vector<raft::PeerId> new_peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;

        if (peer != follower_test) {
            new_peers.push_back(peer);
        }
    }
    cond.reset(1);
    auto l5 = [&] { leader->add_peer(follower_test, NEW_REMOVEPEERCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l5)), target_tidx);
    cond.wait();

    // apply something again
    cond.reset(10);
    auto l6 = [&] {
        for (int i = 10; i < 20; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l6)), target_tidx);
    cond.wait();

    // trigger leader snapshot for the second time, after this the log of index 1~11 will be deleted.
    LOG(WARNING) << "trigger leader snapshot ";
    cond.reset(1);
    auto l7 = [&] { leader->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0)); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l7)), target_tidx);
    cond.wait();

    auto l8 = [&] {
        // index == 5 log has been deleted in log_storage.
        int64_t index = 5;
        raft::UserLog* user_log = new raft::UserLog();
        base::Status status = leader->read_committed_user_log(index, user_log);
        ASSERT_EQ(raft::ELOGDELETED, status.error_code());
        LOG(INFO) << "read local committed user log from leader:" << leader->node_id() << ", index:"
            << index << ", real_index:" << user_log->log_index() << ", data:" << user_log->log_data() 
            << ", status:" << status; 

        // index == 12 and index == 13 are 2 CONFIGURATION logs, so real_index will be 14 when returned.
        index = 12;
        user_log->reset();
        status = leader->read_committed_user_log(index, user_log);
        ASSERT_EQ(0, status.error_code());
        ASSERT_EQ(14, user_log->log_index());
        LOG(INFO) << "read local committed user log from leader:" << leader->node_id() << ", index:"
            << index << ", real_index:" << user_log->log_index() << ", data:" << user_log->log_data() 
            << ", status:" << status; 

        // now index == 15 is a user log
        index = 15;
        user_log->reset();
        status = leader->read_committed_user_log(index, user_log);
        ASSERT_EQ(0, status.error_code());
        ASSERT_EQ(15, user_log->log_index());
        LOG(INFO) << "read local committed user log from leader:" << leader->node_id() << ", index:"
            << index << ", real_index:" << user_log->log_index() << ", data:" << user_log->log_data() 
            << ", status:" << status; 
        delete(user_log);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l8(), target_tidx);

    cluster.ensure_same();
    cluster.stop_all();
}

TEST_P(NodeTest, DISABLED_boostrap_with_snapshot) {
    auto l = [&] {
        base::EndPoint addr;
        ASSERT_EQ(0, base::str2endpoint("127.0.0.1:5006", &addr));
        MockFSM fsm(addr);
        for (char c = 'a'; c <= 'z'; ++c) {
            base::IOBuf buf;
            buf.resize(100, c);
            fsm.logs.push_back(buf);
        }
        raft::BootstrapOptions boptions;
        boptions.last_log_index = fsm.logs.size();
        boptions.log_uri = "local://./data/log";
        boptions.stable_uri = "local://./data/stable";
        boptions.snapshot_uri = "local://./data/snapshot";
        boptions.group_conf.add_peer(raft::PeerId(addr));
        boptions.node_owns_fsm = false;
        boptions.fsm = &fsm;
        ASSERT_EQ(0, raft::bootstrap(boptions));
        rpc::Server server;
        ASSERT_EQ(0, raft::add_service(&server, addr));
        ASSERT_EQ(0, server.Start(addr, NULL));
        raft::Node node("test", raft::PeerId(addr));
        raft::NodeOptions options;
        options.log_uri = "local://./data/log";
        options.stable_uri = "local://./data/stable";
        options.snapshot_uri = "local://./data/snapshot";
        options.node_owns_fsm = false;
        options.fsm = &fsm;
        ASSERT_EQ(0, node.init(options));
        ASSERT_EQ(26u, fsm.logs.size());
        for (char c = 'a'; c <= 'z'; ++c) {
            std::string expected;
            expected.resize(100, c);
            ASSERT_TRUE(fsm.logs[c - 'a'].equals(expected));
        }
        while (!node.is_leader()) {
            usleep(1000);
        }
        node.shutdown(NULL);
        node.join(nullptr);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l(), 0);
}

TEST_P(NodeTest, DISABLED_boostrap_without_snapshot) {
    auto l = [&] {
        base::EndPoint addr;
        ASSERT_EQ(0, base::str2endpoint("127.0.0.1:5006", &addr));
        raft::BootstrapOptions boptions;
        boptions.last_log_index = 0;
        boptions.log_uri = "local://./data/log";
        boptions.stable_uri = "local://./data/stable";
        boptions.snapshot_uri = "local://./data/snapshot";
        boptions.group_conf.add_peer(raft::PeerId(addr));
        ASSERT_EQ(0, raft::bootstrap(boptions));
        rpc::Server server;
        ASSERT_EQ(0, raft::add_service(&server, addr));
        ASSERT_EQ(0, server.Start(addr, NULL));
        raft::Node node("test", raft::PeerId(addr));
        raft::NodeOptions options;
        options.log_uri = "local://./data/log";
        options.stable_uri = "local://./data/stable";
        options.snapshot_uri = "local://./data/snapshot";
        options.node_owns_fsm = false;
        MockFSM fsm(addr);
        options.fsm = &fsm;
        ASSERT_EQ(0, node.init(options));
        while (!node.is_leader()) {
            usleep(1000);
        }
        node.shutdown(NULL);
        node.join(nullptr);
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l(), 0);
}

TEST_P(NodeTest, change_peers) {
    std::vector<raft::PeerId> peers;
    raft::PeerId peer0;
    peer0.addr.ip = base::my_ip();
    peer0.addr.port = 8435;
    peer0.idx = 0;

    // start cluster
    peers.push_back(peer0);
    Cluster cluster("unittest", peers);
    ASSERT_EQ(0, cluster.start(peer0.addr));
    LOG(NOTICE) << "start single cluster " << peer0;
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    int target_tidx = leader->polar_worker_index();
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();

    for (int i = 1; i < 10; ++i) {
        raft::PeerId peer = peer0;
        peer.addr.port += i;
        ASSERT_EQ(0, cluster.start(peer.addr, true));
    }
    for (int i = 0; i < 9; ++i) {
        cluster.wait_leader();
        raft::Node* leader = cluster.leader();
        raft::PeerId peer = peer0;
        peer.addr.port += i;
        ASSERT_EQ(leader->node_id().peer_id, peer);
        peer.addr.port += 1;
        raft::Configuration conf;
        conf.add_peer(peer);
        raft::SynchronizedClosure done;
        auto l = [&] { leader->change_peers(conf, &done); };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
        done.wait();
        ASSERT_TRUE(done.status().ok()) << done.status();
        cluster.check_node_ready(peer);
    }
    ASSERT_TRUE(cluster.ensure_same());
}

TEST_P(NodeTest, change_peers_add_multiple_node) {
    std::vector<raft::PeerId> peers;
    raft::PeerId peer0;
    peer0.addr.ip = base::my_ip();
    peer0.addr.port = 8435;
    peer0.idx = 0;

    // start cluster
    peers.push_back(peer0);
    Cluster cluster("unittest", peers);
    ASSERT_EQ(0, cluster.start(peer0.addr));
    LOG(NOTICE) << "start single cluster " << peer0;
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    int target_tidx = leader->polar_worker_index();
    CHECK_GE(target_tidx, 0);
    bthread::CountdownEvent cond(10);
    bthread::CountdownEvent* cond_ptr = &cond;
    for (int i = 0; i < 10; i++) {
        auto l = [=] {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(cond_ptr, 0);
            leader->apply(task);
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    }
    cond.wait();
    raft::Configuration conf;
    for (int i = 0; i < 3; ++i) {
        raft::PeerId peer = peer0;
        peer.addr.port += i;
        conf.add_peer(peer);
    }
    cluster.wait_leader();
    raft::SynchronizedClosure done;
    auto func1 = [&] { leader->change_peers(conf, &done); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func1)), target_tidx);
    done.wait();
    ASSERT_EQ(raft::ECATCHUP, done.status().error_code()) << done.status();
    raft::PeerId peer = peer0;
    peer.addr.port++;
    cluster.start(peer.addr, false);
    done.reset();
    auto func2 = [&] { leader->change_peers(conf, &done); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func2)), target_tidx);
    done.wait();
    ASSERT_EQ(raft::ECATCHUP, done.status().error_code()) << done.status();
    peer.addr.port++;
    cluster.start(peer.addr, false);
    cluster.check_node_ready(peer);
    done.reset();
    auto func3 = [&] { leader->change_peers(conf, &done); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(func3)), target_tidx);
    done.wait();
    ASSERT_TRUE(done.status().ok()) << done.status();
    ASSERT_TRUE(cluster.ensure_same());
}

TEST_P(NodeTest, change_peers_steps_down_in_joint_consensus) {
    std::vector<raft::PeerId> peers;
    raft::PeerId peer0("127.0.0.1:5006");
    raft::PeerId peer1("127.0.0.1:5007");
    raft::PeerId peer2("127.0.0.1:5008");
    raft::PeerId peer3("127.0.0.1:5009");

    // start cluster
    peers.push_back(peer0);
    Cluster cluster("unittest", peers);
    ASSERT_EQ(0, cluster.start(peer0.addr));
    LOG(NOTICE) << "start single cluster " << peer0;
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_NE(leader, nullptr);
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);
        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        LEADER_RUN(leader->apply(task));
    }
    cond.wait();
    cluster.start(peer1.addr, false);
    cluster.start(peer2.addr, false);
    cluster.start(peer3.addr, false);
    raft::Configuration conf;
    conf.add_peer(peer0);
    conf.add_peer(peer1);
    conf.add_peer(peer2);
    conf.add_peer(peer3);
    raft::SynchronizedClosure done;
    LEADER_RUN(leader->change_peers(conf, &done));
    done.wait();
    ASSERT_TRUE(done.status().ok());
    ASSERT_EQ(0, cluster.stop(peer3.addr));
    conf.remove_peer(peer0);
    conf.remove_peer(peer1);

    // Change peers to [peer2, peer3], which must fail since peer3 is stopped
    done.reset();
    LEADER_RUN(leader->change_peers(conf, &done));
    done.wait();
    ASSERT_EQ(EPERM, done.status().error_code());
    LOG(INFO) << done.status();
    ASSERT_FALSE(leader->_impl->_conf.stable());
    cluster.start(peer3.addr, false);
    while (true) {
        cluster.wait_leader();
        leader = cluster.leader();
        if (leader && conf.contains(leader->node_id().peer_id)) {
            break;
        }
        bthread_usleep(100);
    }
    ASSERT_NE(leader, nullptr);
    base::Status st = LEADER_RUN(leader->list_peers(&peers));
    ASSERT_TRUE(st.ok()) << "list_peer error:" << st;
    ASSERT_TRUE(conf.equals(peers));
    ASSERT_TRUE(leader->_impl->_conf.stable() || !leader->_impl->_conf_ctx.is_busy());
    int wait_count = 1000;
    while (leader->_impl->_conf_ctx.is_busy() && wait_count > 0) {
        LOG(WARNING) << "wait util stable stage finish";
        usleep(5 * 1000);
        --wait_count;
    }
    ASSERT_TRUE(!leader->_impl->_conf_ctx.is_busy());
}

struct ChangeArg {
    Cluster* c;
    std::vector<raft::PeerId> peers;
    volatile bool stop;
    bool dont_remove_first_peer;
};

static void* change_routine(void* arg) {
    ChangeArg* ca = (ChangeArg*)arg;
    while (!ca->stop) {
        ca->c->wait_leader();
        raft::Node* leader = ca->c->leader();
        if (!leader) {
            continue;
        }
        // Randomly select peers
        raft::Configuration conf;
        if (ca->dont_remove_first_peer) {
            conf.add_peer(ca->peers[0]);
        }
        for (size_t i = 0; i < ca->peers.size(); ++i) {
            bool select = base::fast_rand_less_than(64) < 32;
            if (select) {
                conf.add_peer(ca->peers[i]);
            }
        }
        if (conf.empty()) {
            LOG(WARNING) << "No peer has been selected";
            // Bad luck here
            continue;
        }
        raft::SynchronizedClosure done;
        LEADER_RUN(leader->change_peers(conf, &done));
        done.wait();
        EXPECT_TRUE(done.status().ok());
    }
    return NULL;
}

TEST_P(NodeTest, change_peers_chaos_with_snapshot) {
    g_dont_print_apply_log = false;
    google::SetCommandLineOption("raft_sync", "false");
    ASSERT_FALSE(google::SetCommandLineOption("crash_on_fatal_log", "true").empty());
    std::vector<raft::PeerId> peers;
    // start cluster
    peers.push_back(raft::PeerId("127.0.0.1:5006"));
    Cluster cluster("unittest", peers);
    cluster.start(peers.front().addr, false, 1);
    for (int i = 1; i < 10; ++i) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
        cluster.start(peers.back().addr, true, 1);
    }
    ChangeArg arg;
    arg.c = &cluster;
    arg.peers = peers;
    arg.stop = false;
    arg.dont_remove_first_peer = false;
    pthread_t tid;
    ASSERT_EQ(0, pthread_create(&tid, NULL, change_routine, &arg));
    for (int i = 0; i < 1000;) {
        cluster.wait_leader();
        raft::Node* leader = cluster.leader();
        if (!leader) {
            continue;
        }
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);
        raft::Task task;
        raft::SynchronizedClosure done;
        task.data = &data;
        task.done = &done;
        LEADER_RUN(leader->apply(task));
        done.wait();
        if (done.status().ok()) {
            ++i;
        } else {
            EXPECT_EQ(EPERM, done.status().error_code());
        }
    }
    arg.stop = true;
    pthread_join(tid, NULL);
    google::SetCommandLineOption("raft_sync", "true");
}

TEST_P(NodeTest, change_peers_chaos_without_snapshot) {
    // g_dont_print_apply_log = true;
    google::SetCommandLineOption("raft_sync", "false");
    ASSERT_FALSE(google::SetCommandLineOption("crash_on_fatal_log", "true").empty());
    std::vector<raft::PeerId> peers;
    // start cluster
    peers.push_back(raft::PeerId("127.0.0.1:5006"));
    Cluster cluster("unittest", peers);
    cluster.start(peers.front().addr, false, 100000);
    for (int i = 1; i < 10; ++i) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
        cluster.start(peers.back().addr, true, 10000);
    }
    ChangeArg arg;
    arg.c = &cluster;
    arg.peers = peers;
    arg.stop = false;
    arg.dont_remove_first_peer = true;
    pthread_t tid;
    ASSERT_EQ(0, pthread_create(&tid, NULL, change_routine, &arg));
    for (int i = 0; i < 10000;) {
        cluster.wait_leader();
        raft::Node* leader = cluster.leader();
        if (!leader) {
            continue;
        }
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);
        raft::Task task;
        raft::SynchronizedClosure done;
        task.data = &data;
        task.done = &done;
        LEADER_RUN(leader->apply(task));
        done.wait();
        if (done.status().ok()) {
            ++i;
        } else {
            EXPECT_EQ(EPERM, done.status().error_code());
        }
    }
    arg.stop = true;
    pthread_join(tid, NULL);
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    raft::SynchronizedClosure done;
    LEADER_RUN(leader->change_peers(raft::Configuration(peers), &done));
    done.wait();
    ASSERT_TRUE(done.status().ok()) << done.status();
    cluster.ensure_same(5);
    google::SetCommandLineOption("raft_sync", "true");
}

class AppendEntriesSyncClosure : public google::protobuf::Closure {
public:
    AppendEntriesSyncClosure() {
        _cntl = new rpc::Controller;
    }
    ~AppendEntriesSyncClosure() {
        if (_cntl) {
            delete _cntl;
        }
    }
    void Run() {
        _event.signal();
    }
    void wait() {
        _event.wait();
    }
    raft::AppendEntriesRequest& request() { return _request; }
    raft::AppendEntriesResponse& response() { return _response; }
    rpc::Controller& cntl() {
        return *_cntl;
    }

private:
    bthread::CountdownEvent _event;
    raft::AppendEntriesRequest _request;
    raft::AppendEntriesResponse _response;
    rpc::Controller* _cntl;
};

void follower_append_entries(
        const raft::AppendEntriesRequest& request_template, int entry_size,
        int64_t prev_log_index, AppendEntriesSyncClosure& closure, raft::Node* node) {
    raft::AppendEntriesRequest& request = closure.request();
    request.CopyFrom(request_template);
    request.set_prev_log_index(prev_log_index);
    for (int i = 0; i < entry_size; ++i) {
        raft::EntryMeta em;
        base::IOBuf data;
        data.append("hello");
        em.set_data_len(data.size());
        em.set_type(raft::ENTRY_TYPE_DATA);
        em.set_term(request_template.term());
        request.add_entries()->Swap(&em);
        closure.cntl().request_attachment().append(data);
    }
    node->_impl->handle_append_entries_request<raft::ProtocolPB>(
            &closure.cntl(), &closure.request(), &closure.response(), &closure);
}

TEST_P(NodeTest, follower_handle_out_of_order_append_entries) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers, 3000);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    cluster.ensure_same();

    std::vector<raft::Node*> followers;
    cluster.followers(&followers);

    int target_tidx = followers[0]->polar_worker_index();
    while (true) {
        int64_t local_index = -1;
        auto l = [&]() {
            followers[0]->_impl->_mutex.lock();
            local_index = followers[0]->_impl->_log_manager->last_log_index();
            followers[0]->_impl->_mutex.unlock();
        };
        RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l(), target_tidx);
        if (local_index == 0) {
            bthread_usleep(10*1000);
            continue;
        } else {
            break;
        }
    }

    int64_t local_index;
    int64_t term;
    std::string group_id;
    std::string server_id;
    std::string peer_id;
    int64_t committed_index;

    auto ll1 = [&]() {
        followers[0]->_impl->_mutex.lock();
        local_index = followers[0]->_impl->_log_manager->last_log_index();
        term = followers[0]->_impl->_current_term;
        group_id = followers[0]->_impl->_group_id;
        server_id = followers[0]->_impl->_leader_id.to_string();
        peer_id = "";
        committed_index = followers[0]->_impl->_ballot_box->last_committed_index();
        followers[0]->_impl->_mutex.unlock();
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(ll1(), target_tidx);
    int32_t max_append_entries_cache_size = raft::FLAGS_raft_max_append_entries_cache_size;
    if (!raft::FLAGS_raft_enable_append_entries_cache) {
        max_append_entries_cache_size = 0;
    }

    // Create a template
    raft::AppendEntriesRequest request_template;
    request_template.set_term(term);
    request_template.set_group_id(group_id);
    request_template.set_server_id(server_id);
    request_template.set_peer_id(peer_id);
    // request_template.set_prev_log_index(local_index);
    request_template.set_prev_log_term(term);
    request_template.set_committed_index(committed_index);

    // Fill the entire cache
    std::deque<AppendEntriesSyncClosure*> out_of_order_closures;
    auto l2 = [&] {
        for (int32_t i = 0; i < max_append_entries_cache_size / 2; ++i) {
            out_of_order_closures.push_back(new AppendEntriesSyncClosure());
            AppendEntriesSyncClosure& closure = *out_of_order_closures.back();
            follower_append_entries(
                    request_template, 1, local_index + 1 + i,
                    closure, followers[0]);
        }
        for (int32_t i = max_append_entries_cache_size - 1; i >= max_append_entries_cache_size / 2; --i) {
            out_of_order_closures.push_back(new AppendEntriesSyncClosure());
            AppendEntriesSyncClosure& closure = *out_of_order_closures.back();
            follower_append_entries(
                    request_template, 1, local_index + 1 + i,
                    closure, followers[0]);
        }
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_log_manager->last_log_index(), local_index);
        ASSERT_TRUE(followers[0]->_impl->_append_entries_cache == NULL ||
                    followers[0]->_impl->_append_entries_cache->_rpc_map.size() ==
                    size_t(max_append_entries_cache_size));
        followers[0]->_impl->_mutex.unlock();
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l2(), target_tidx);

    // Fill another out-of-order request, be rejected
    AppendEntriesSyncClosure closure1;
    auto l3 = [&] {
        follower_append_entries(
            request_template, 1, local_index + 1 + max_append_entries_cache_size,
            closure1, followers[0]);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), target_tidx);
    closure1.wait();
    ASSERT_FALSE(closure1.response().success());

    // Let all out-of-order entries be handled
    AppendEntriesSyncClosure closure2;
    auto l4 = [&] {
        follower_append_entries(
            request_template, 1, local_index, closure2, followers[0]);
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l4)), target_tidx);
    closure2.wait();
    ASSERT_TRUE(closure2.response().success());
    for (auto& c : out_of_order_closures) {
        c->wait();
        ASSERT_TRUE(c->response().success());
    }
    out_of_order_closures.clear();

    auto ll2 = [&] {
        local_index += max_append_entries_cache_size + 1;
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_log_manager->last_log_index(), local_index);
        ASSERT_TRUE(followers[0]->_impl->_append_entries_cache == NULL);
        followers[0]->_impl->_mutex.unlock();
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(ll2(), target_tidx);

    if (max_append_entries_cache_size <= 1) {
        LOG(WARNING) << "cluster stop";
        cluster.stop_all();
        return;
    }

    // Overlap out-of-order requests
    AppendEntriesSyncClosure closure3;
    auto l5 = [&] {
        follower_append_entries(
            request_template, 3, local_index + 5,
            closure3, followers[0]);
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->_rpc_map.size(), 1);
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->first_index(), local_index + 5 + 1);
        followers[0]->_impl->_mutex.unlock();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l5)), target_tidx);

    AppendEntriesSyncClosure closure4;
    auto l6 = [&] {
        follower_append_entries(
            request_template, 2, local_index + 5,
            closure4, followers[0]);
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->_rpc_map.size(), 1);
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->first_index(), local_index + 5 + 1);
        followers[0]->_impl->_mutex.unlock();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l6)), target_tidx);
    closure3.wait();
    ASSERT_FALSE(closure3.response().success());

    AppendEntriesSyncClosure closure5;
    auto l7 = [&] {
        follower_append_entries(
            request_template, 2, local_index + 6,
            closure5, followers[0]);
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->_rpc_map.size(), 1);
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->first_index(), local_index + 6 + 1);
        followers[0]->_impl->_mutex.unlock();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l7)), target_tidx);
    closure4.wait();
    ASSERT_FALSE(closure4.response().success());

    AppendEntriesSyncClosure closure6;
    auto l71 = [&] {
        follower_append_entries(
                request_template, 3, local_index + 4,
                closure6, followers[0]);
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->_rpc_map.size(), 1);
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->first_index(), local_index + 4 + 1);
        followers[0]->_impl->_mutex.unlock();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l71)), target_tidx);
    closure5.wait();
    ASSERT_FALSE(closure5.response().success());

    // Wait until timeout
    closure6.wait();
    ASSERT_FALSE(closure6.response().success());
    auto ll3 = [&] {
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_log_manager->last_log_index(), local_index);
        ASSERT_TRUE(followers[0]->_impl->_append_entries_cache == NULL);
        followers[0]->_impl->_mutex.unlock();
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(ll3(), target_tidx);

    // Part of cache continuous
    AppendEntriesSyncClosure closure7;
    auto l72 = [&] {
        follower_append_entries(
                request_template, 3, local_index + 5,
                closure7, followers[0]);
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->_rpc_map.size(), 1);
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->first_index(), local_index + 5 + 1);
        followers[0]->_impl->_mutex.unlock();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l72)), target_tidx);

    AppendEntriesSyncClosure closure8;
    auto l73 = [&] {
        follower_append_entries(
                request_template, 2, local_index + 2,
                closure8, followers[0]);
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->_rpc_map.size(), 2);
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->first_index(), local_index + 2 + 1);
        followers[0]->_impl->_mutex.unlock();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l73)), target_tidx);

    AppendEntriesSyncClosure closure9;
    auto l74 = [&] {
        follower_append_entries(
                request_template, 1, local_index + 1,
                closure9, followers[0]);
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->_rpc_map.size(), 3);
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->first_index(), local_index + 1 + 1);
        followers[0]->_impl->_mutex.unlock();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l74)), target_tidx);

    AppendEntriesSyncClosure closure10;
    auto l75 = [&] {
        follower_append_entries(
                request_template, 1, local_index,
                closure10, followers[0]);
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->_rpc_map.size(), 1);
        ASSERT_EQ(followers[0]->_impl->_append_entries_cache->first_index(), local_index + 5 + 1);
        followers[0]->_impl->_mutex.unlock();
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l75)), target_tidx);

    closure10.wait();
    closure9.wait();
    closure8.wait();
    ASSERT_TRUE(closure10.response().success());
    ASSERT_TRUE(closure9.response().success());
    ASSERT_TRUE(closure8.response().success());
    auto ll4 = [&] {
        local_index += 2 + 2;
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_log_manager->last_log_index(), local_index);
        followers[0]->_impl->_mutex.unlock();
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(ll4(), target_tidx);

    // Wait util timeout
    closure7.wait();
    ASSERT_FALSE(closure7.response().success());
    auto ll5 = [&] {
        followers[0]->_impl->_mutex.lock();
        ASSERT_EQ(followers[0]->_impl->_log_manager->last_log_index(), local_index);
        ASSERT_TRUE(followers[0]->_impl->_append_entries_cache == NULL);
        followers[0]->_impl->_mutex.unlock();
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(ll5(), target_tidx);

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_P(NodeTest, readonly) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();
    ASSERT_GE(target_tidx, 0);

    // apply something
    bthread::CountdownEvent cond(10);
    int start_index = 0;
    for (int i = start_index; i < start_index + 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i);
        data.append(data_buf);

        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        LEADER_RUN(leader->apply(task));
    }
    cond.wait();

    // let leader leave readonly mode, accept user logs
    LEADER_RUN(leader->leave_readonly_mode());
    ASSERT_FALSE(LEADER_RUN(leader->readonly()));
    cond.reset(10);
    start_index += 10;
    for (int i = start_index; i < start_index + 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i);
        data.append(data_buf);

        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        LEADER_RUN(leader->apply(task));
    }
    cond.wait();

    std::vector<raft::Node*> followers;
    cluster.followers(&followers);
    ASSERT_EQ(2, followers.size());

    // Let follower 0 enter readonly mode, still can accept user logs
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(followers[0]->enter_readonly_mode(),
                                    followers[0]->polar_worker_index());
    LOG(NOTICE) << "follower0 " << followers[0]->node_id() << " enter readonly mode ";
    bthread_usleep(2000 * 1000); // wait a while for heartbeat
    cond.reset(10);
    start_index += 10;
    for (int i = start_index; i < start_index + 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i);
        data.append(data_buf);

        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        LEADER_RUN(leader->apply(task));
    }
    cond.wait();

    // Let follower 1 enter readonly mode, majority readonly, reject user logs
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(followers[1]->enter_readonly_mode(),
                                    followers[1]->polar_worker_index());
    LOG(NOTICE) << "follower1 " << followers[1]->node_id() << " enter readonly mode ";
    int retry = 5;
    while (!LEADER_RUN(leader->readonly()) && --retry >= 0) {
        bthread_usleep(1000 * 1000);
    }
    ASSERT_TRUE(LEADER_RUN(leader->readonly()));
    cond.reset(10);
    start_index += 10;
    for (int i = start_index; i < start_index + 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i);
        data.append(data_buf);

        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, raft::EREADONLY);
        LEADER_RUN(leader->apply(task));
    }
    cond.wait();

    // Add a new follower
    raft::PeerId peer3;
    peer3.addr.ip = base::get_host_ip();
    peer3.addr.port = 5006 + 3;
    peer3.idx = 0;
    ASSERT_EQ(0, cluster.start(peer3.addr, true));
    bthread_usleep(3000* 1000);
    cond.reset(1);
    LEADER_RUN(leader->add_peer(peer3, NEW_ADDPEERCLOSURE(&cond, 0)));
    LOG(NOTICE) << "add peer " << peer3;
    cond.wait();
    cluster.check_node_ready(peer3);

    // Trigger follower 0 do snapshot
    cond.reset(1);
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(followers[0]->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0)),
        followers[0]->polar_worker_index());
    LOG(NOTICE) << followers[0]->node_id() << " trigger snapshot";
    cond.wait();

    // 2/4 readonly, leader still in readonly
    retry = 5;
    while (!LEADER_RUN(leader->readonly()) && --retry >= 0) {
        bthread_usleep(1000 * 1000);
    }
    ASSERT_TRUE(LEADER_RUN(leader->readonly()));
    start_index += 10;
    cond.reset(10);
    for (int i = start_index; i < start_index + 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i);
        data.append(data_buf);

        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, raft::EREADONLY);
        LEADER_RUN(leader->apply(task));
    }
    cond.wait();

    // Remove follower 0
    cond.reset(1);
    LEADER_RUN(leader->remove_peer(followers[0]->node_id().peer_id, NEW_REMOVEPEERCLOSURE(&cond, 0)));
    LOG(NOTICE) << "remove peer " << followers[0]->node_id();
    cond.wait();
    cluster.stop(followers[0]->node_id().peer_id.addr);

    // 1/3 readonly, leader leave Readonly
    retry = 5;
    while (LEADER_RUN(leader->readonly()) && --retry >= 0) {
        bthread_usleep(1000 * 1000);
    }
    ASSERT_TRUE(!LEADER_RUN(leader->readonly()));
    cond.reset(10);
    start_index += 10;
    for (int i = start_index; i < start_index + 10; i++) {
        base::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i);
        data.append(data_buf);

        raft::Task task;
        task.data = &data;
        task.done = NEW_APPLYCLOSURE(&cond, 0);
        LEADER_RUN(leader->apply(task));
    }
    cond.wait();

    // Follower 1 leave readonly, catch up logs
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(followers[1]->leave_readonly_mode(),
                                    followers[1]->polar_worker_index());
    LOG(NOTICE) << followers[1]->node_id() << " leave readonly";
    cluster.ensure_same();

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}

TEST_P(NodeTest, append_entries_when_follower_is_in_error_state) {
    std::vector<raft::PeerId> peers;
    // five nodes
    for (int i = 0; i < 5; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();
    int target_tidx = leader->polar_worker_index();

    // apply something
    bthread::CountdownEvent cond(10);
    auto l = [&] {
        for (int i = 0; i < 10; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();

    // set the first Follower to Error state
    std::vector<raft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(nodes.size(), 4);
    base::EndPoint error_follower = nodes[0]->node_id().peer_id.addr;
    raft::Node* error_follower_node = nodes[0];
    LOG(WARNING) << "set follower error " << nodes[0]->node_id();
    raft::NodeImpl *node_impl = nodes[0]->_impl;
    auto l2 = [&] {
        node_impl->AddRef();
        raft::Error e;
        e.set_type(raft::ERROR_TYPE_STATE_MACHINE);
        e.status().set_error(EINVAL, "Follower has something wrong");
        node_impl->on_error(e);
        node_impl->Release();
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l2(), node_impl->polar_worker_index());

    // increase term  by stopping leader and electing a new leader again
    base::EndPoint old_leader = leader->node_id().peer_id.addr;
    LOG(WARNING) << "stop leader " << leader->node_id();
    cluster.stop(old_leader);
    // elect new leader
    cluster.wait_leader();
    leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "elect new leader " << leader->node_id();

    // apply something again
    cond.reset(10);
    auto l3 = [&] {
        for (int i = 10; i < 20; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l3)), target_tidx);
    cond.wait();

    sleep(2);
    // stop error follower
    LOG(WARNING) << "stop wrong follower " << error_follower_node->node_id();
    cluster.stop(error_follower);

    sleep(5);
    // restart error follower
    ASSERT_EQ(0, cluster.start(error_follower));
    LOG(WARNING) << "restart error follower " << error_follower;

    // restart old leader
    ASSERT_EQ(0, cluster.start(old_leader));
    LOG(WARNING) << "restart old leader " << old_leader;

    cluster.ensure_same();

    cluster.stop_all();
}

TEST_P(NodeTest, on_start_following_and_on_stop_following) {
    std::vector<raft::PeerId> peers;
    // five nodes
    for (int i = 0; i < 5; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader_first
    cluster.wait_leader();
    raft::Node* leader_first = cluster.leader();
    ASSERT_TRUE(leader_first != NULL);
    int32_t target_tidx = leader_first->polar_worker_index();
    LOG(WARNING) << "leader_first is " << leader_first->node_id()
                 << ", election_timeout is "
                 << leader_first->_impl->_options.election_timeout_ms;

    // apply something
    bthread::CountdownEvent cond(10);
    auto l = [&] {
        for (int i = 0; i < 10; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader_first->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();

    // check _on_start_following_times and _on_stop_following_times
    std::vector<raft::Node*> followers_first;
    cluster.followers(&followers_first);
    ASSERT_EQ(followers_first.size(), 4);
    // leader_first's _on_start_following_times and _on_stop_following_times should both be 0.
    ASSERT_EQ(static_cast<MockFSM*>(leader_first->_impl->_options.fsm)->_on_start_following_times, 0);
    ASSERT_EQ(static_cast<MockFSM*>(leader_first->_impl->_options.fsm)->_on_stop_following_times, 0);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(static_cast<MockFSM*>(followers_first[i]->_impl->_options.fsm)->_on_start_following_times, 1);
        ASSERT_EQ(static_cast<MockFSM*>(followers_first[i]->_impl->_options.fsm)->_on_stop_following_times, 0);
    }

    // stop old leader and elect a new one
    base::EndPoint leader_first_endpoint = leader_first->node_id().peer_id.addr;
    LOG(WARNING) << "stop leader_first " << leader_first->node_id();
    cluster.stop(leader_first_endpoint);
    // elect new leader
    cluster.wait_leader();
    raft::Node* leader_second = cluster.leader();
    ASSERT_TRUE(leader_second != NULL);
    LOG(WARNING) << "elect new leader " << leader_second->node_id();
    target_tidx = leader_second->polar_worker_index();

    // apply something
    cond.reset(10);
    auto l2 = [&] {
        for (int i = 0; i < 10; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader_second->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), target_tidx);
    cond.wait();

    // check _on_start_following_times and _on_stop_following_times again
    std::vector<raft::Node*> followers_second;
    cluster.followers(&followers_second);
    ASSERT_EQ(followers_second.size(), 3);
    // leader_second's _on_start_following_times and _on_stop_following_times should both be 1.
    // When it was still in follower state, it would do handle_election_timeout and
    // trigger on_stop_following when not receiving heartbeat for a long
    // time(election_timeout_ms).
    ASSERT_GE(static_cast<MockFSM*>(leader_second->_impl->_options.fsm)->_on_start_following_times, 1);
    ASSERT_GE(static_cast<MockFSM*>(leader_second->_impl->_options.fsm)->_on_stop_following_times, 1);
    for (int i = 0; i < 3; i++) {
        // Firstly these followers have a leader, but it stops and a candidate
        // sends request_vote_request to them, which triggers on_stop_following.
        // When the candidate becomes new leader, on_start_following is triggled
        // again so _on_start_following_times increase by 1.
        ASSERT_GE(static_cast<MockFSM*>(followers_second[i]->_impl->_options.fsm)->_on_start_following_times, 2);
        ASSERT_GE(static_cast<MockFSM*>(followers_second[i]->_impl->_options.fsm)->_on_stop_following_times, 1);
    }

    // transfer leadership to a follower
    raft::PeerId target = followers_second[0]->node_id().peer_id;
    auto l3 = [&] {
        ASSERT_EQ(0, leader_second->transfer_leadership_to(target));
    };
    RAFT_SYNC_BTHREAD_RUN_ON_POLAR(l3(), target_tidx);
    usleep(10 * 1000);
    cluster.wait_leader();
    raft::Node* leader_third = cluster.leader();
    ASSERT_EQ(target, leader_third->node_id().peer_id);
    target_tidx = leader_third->polar_worker_index();

    // apply something
    cond.reset(10);
    auto l4 = [&] {
        for (int i = 0; i < 10; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);
            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader_third->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l4)), target_tidx);
    cond.wait();

    // check _on_start_following_times and _on_stop_following_times again
    std::vector<raft::Node*> followers_third;
    cluster.followers(&followers_third);
    ASSERT_EQ(followers_second.size(), 3);
    // leader_third's _on_start_following_times and _on_stop_following_times should both be 2.
    // When it was still in follower state, it would do handle_timeout_now_request and
    // trigger on_stop_following when leader_second transferred leadership to it.
    ASSERT_GE(static_cast<MockFSM*>(leader_third->_impl->_options.fsm)->_on_start_following_times, 2);
    ASSERT_GE(static_cast<MockFSM*>(leader_third->_impl->_options.fsm)->_on_stop_following_times, 2);
    for (int i = 0; i < 3; i++) {
        // leader_second became follower when it transferred leadership to target,
        // and when it receives leader_third's append_entries_request on_start_following is triggled.
        if (followers_third[i]->node_id().peer_id == leader_second->node_id().peer_id) {
            ASSERT_GE(static_cast<MockFSM*>(followers_third[i]->_impl->_options.fsm)->_on_start_following_times, 2);
            ASSERT_GE(static_cast<MockFSM*>(followers_third[i]->_impl->_options.fsm)->_on_stop_following_times, 1);
            continue;
        }
        // other followers just lose the leader_second and get leader_third, so _on_stop_following_times and
        // _on_start_following_times both increase by 1.
        ASSERT_GE(static_cast<MockFSM*>(followers_third[i]->_impl->_options.fsm)->_on_start_following_times, 3);
        ASSERT_GE(static_cast<MockFSM*>(followers_third[i]->_impl->_options.fsm)->_on_stop_following_times, 2);
    }

    cluster.ensure_same();

    cluster.stop_all();
}

TEST_P(NodeTest, RestartWithoutLog) {
    std::vector<raft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != NULL);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    int32_t target_tidx = leader->polar_worker_index();
    auto l = [&] {
        for (int i = 0; i < 10; i++) {
            base::IOBuf data;
            char data_buf[128];
            snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
            data.append(data_buf);

            raft::Task task;
            task.data = &data;
            task.done = NEW_APPLYCLOSURE(&cond, 0);
            leader->apply(task);
        }
    };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    cond.wait();

    cluster.ensure_same();
    
    std::vector<raft::Node*> nodes;
    std::vector<std::string> log_paths;
    cluster.all_nodes(&nodes);
    cond.reset(nodes.size());
    for (auto& node : nodes) {
        int32_t polar_index = leader->polar_worker_index();
        auto l2 = [&] {
            node->snapshot(NEW_SNAPSHOTCLOSURE(&cond, 0));
        };
        raft::run_closure_in_polar(raft::create_pb_closure(std::move(l2)), polar_index);
        raft::SegmentLogStorage* storage = dynamic_cast<raft::SegmentLogStorage*>(
                node->_impl->_log_manager->_log_storage);
        log_paths.push_back(storage->_path);
    }
    cond.wait();

    cluster.stop_all();
    for (auto& path : log_paths) {
        LOG(NOTICE) << "delete log path: " << path;
        ASSERT_TRUE(base::DeleteFile(base::FilePath(path), true));
    }
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }
    cluster.wait_leader();
    cluster.stop_all();
}

TEST_P(NodeTest, shrink_to_one_node) {
    std::vector<raft::PeerId> peers;
    // five nodes
    for (int i = 0; i < 5; i++) {
        raft::PeerId peer;
        peer.addr.ip = base::get_host_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;
        peers.push_back(peer);
    }

    // start cluster
    Cluster cluster("unittest", peers);
    for (size_t i = 0; i < peers.size(); i++) {
        ASSERT_EQ(0, cluster.start(peers[i].addr));
    }

    // elect leader_first
    cluster.wait_leader();
    raft::Node* leader = cluster.leader();
    int32_t target_tidx = leader->polar_worker_index();

    raft::Configuration conf;
    conf.add_peer(LEADER_RUN(leader->leader_id()));
    raft::SynchronizedClosure done;
    auto l = [&] { leader->change_peers(conf, &done); };
    raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);
    done.wait();
    ASSERT_TRUE(done.status().ok());
}

INSTANTIATE_TEST_CASE_P(NodeTestWithoutPipelineReplication,
                        NodeTest,
                        ::testing::Combine(::testing::Values("NoReplcation"), ::testing::Bool()));

INSTANTIATE_TEST_CASE_P(NodeTestWithPipelineReplication,
                        NodeTest,
                        ::testing::Combine(::testing::Values("NoCache", "HasCache"), ::testing::Bool()));
