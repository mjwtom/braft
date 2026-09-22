// Copyright (c) 2026 Baidu.com, Inc. All Rights Reserved

// Author: Jingwei Ma (majingwei@baidu.com)
// Date: 2026/01/05 15:40:45

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <base/logging.h>
#include <baidu/hpn/rpc/server.h>
#include <baidu/hpn/rpc/channel.h>
#include "raft/raft.h"
#include "raft/node.h"
#include "raft/raft_service.h"
#include "raft/node_manager.h"

class RaftServiceTest : public testing::Test {
protected:
    void SetUp() {
        logging::FLAGS_verbose = 90;
        _listen_address.ip = base::my_ip();
        _listen_address.port = 60006;
        ASSERT_EQ(0, _server.AddService(
                    new raft::RaftServiceImpl(_listen_address),
                    baidu::hpn::rpc::SERVER_OWNS_SERVICE));
        ASSERT_EQ(0, _server.Start(_listen_address, NULL));
        
        // Initialize sending channel
        baidu::hpn::rpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = -1;
        _sending_channel.Init(_listen_address, &channel_opt);
    }
    void TearDown() {
        _server.Stop(0);
        _server.Join();
    }
    baidu::hpn::rpc::Server _server;
    base::EndPoint _listen_address;
    baidu::hpn::rpc::Channel _sending_channel;
};

TEST_F(RaftServiceTest, crc_error) {
    raft::NodeImpl node;
    node._group_id = "test_group";
    node._server_id = std::string("127.0.0.1:123:1");
    ASSERT_NE(0, raft::global_node_manager->_nodes.Modify(raft::NodeManager::_add_node, &node));
    baidu::hpn::rpc::Controller cntl;
    cntl.set_log_id(base::fast_rand());
    raft::AppendEntriesRequest request;
    raft::AppendEntriesResponse response;
    request.set_term(1);
    request.set_group_id(node._group_id);
    request.set_server_id(node._server_id.to_string());
    request.set_peer_id(node._server_id.to_string());
    request.set_prev_log_index(100);
    request.set_prev_log_term(1);
    request.set_committed_index(100);
    request.set_crc32_type(static_cast<uint32_t>(raft::CrcType::ISCSI));
    request.set_crc32(123);
    baidu::hpn::rpc::Channel channel;
    baidu::hpn::rpc::ChannelOptions channel_opt;
    channel_opt.timeout_ms = -1; // We don't need RPC timeout
    ASSERT_EQ(0, channel.Init(_listen_address, &channel_opt));
    raft::RaftService_Stub stub(&_sending_channel);
    stub.append_entries(&cntl, &request, &response, nullptr);
    ASSERT_TRUE(cntl.Failed());
    ASSERT_EQ(raft::ECRCERROR, cntl.ErrorCode()) << cntl.ErrorText();
}

