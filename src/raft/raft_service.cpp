// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
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

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)

#include <base/logging.h>
#include "raft/rpc.h"
#include "raft/raft_service.h"
#include "raft/raft.h"
#include "raft/node.h"
#include "raft/node_manager.h"

#define RAFT_CHECK_AND_GET_NODE() \
    rpc::ClosureGuard done_guard(done); \
    rpc::Controller* cntl = static_cast<rpc::Controller*>(cntl_base); \
    PeerId peer_id; \
    if (0 != peer_id.parse(request->peer_id())) { \
        cntl->SetFailed(EINVAL, "peer_id invalid"); \
        return; \
    } \
    scoped_refptr<NodeImpl> node = global_node_manager->get_node(request->group_id(), peer_id); \
    if (node.get() == nullptr) { \
        cntl->SetFailed(ENOENT, "peer_id not exist"); \
        return; \
    }

namespace raft {

DEFINE_bool(raft_enable_fb_verify, true, "enable appned_entries fb verify");
DEFINE_bool(raft_enable_fb_print_hex, true, "enable print appned_entries hex");

RaftServiceImpl::~RaftServiceImpl() {
    global_node_manager->remove_address(_addr);
}

void RaftServiceImpl::pre_vote(google::protobuf::RpcController* cntl_base,
                          const RequestVoteRequest* request,
                          RequestVoteResponse* response,
                          google::protobuf::Closure* done) {
    RAFT_CHECK_AND_GET_NODE();
    RAFT_DISPATCH_POLAR(node, handle_pre_vote_request,
                        cntl, request, response, done_guard);
}

void RaftServiceImpl::request_vote(google::protobuf::RpcController* cntl_base,
                          const RequestVoteRequest* request,
                          RequestVoteResponse* response,
                          google::protobuf::Closure* done) {
    RAFT_CHECK_AND_GET_NODE();
    RAFT_DISPATCH_POLAR(node, handle_request_vote_request,
                        cntl, request, response, done_guard);
}

void RaftServiceImpl::append_entries(google::protobuf::RpcController* cntl_base,
                            const AppendEntriesRequest* request,
                            AppendEntriesResponse* response,
                            google::protobuf::Closure* done) {
    RAFT_CHECK_AND_GET_NODE();
    if (request->has_crc32_type() && request->has_crc32()) {
        uint32_t crc_code = 0;
        bool ok = get_crc32((CrcType)request->crc32_type(), cntl->request_attachment(), &crc_code);
        if (!ok || crc_code != request->crc32()) {
            LOG(WARNING) << "send_log crc error, "
                         << cntl->remote_side() << " --> " << _addr
                         << " " << (CrcType)request->crc32_type()
                         << " (remote:" << request->crc32() << ") (local:" << crc_code
                         << ") group_id: " << node->node_id().group_id
                         << " attachment_size: " << cntl->request_attachment().size()
                         << " call_id: " << cntl->call_id()
                         << " log_id: " << cntl->log_id();
            cntl->SetFailed(ECRCERROR, "crc error");
            return;
        }
    }
    RAFT_DISPATCH_POLAR(node, handle_append_entries_request<ProtocolPB>,
                        cntl, request, response, done_guard);
}

void RaftServiceImpl::install_snapshot(google::protobuf::RpcController* cntl_base,
                              const InstallSnapshotRequest* request,
                              InstallSnapshotResponse* response,
                              google::protobuf::Closure* done) {
    RAFT_CHECK_AND_GET_NODE();
    RAFT_DISPATCH_POLAR(node, handle_install_snapshot_request,
                        cntl, request, response, done_guard);
}

void RaftServiceImpl::timeout_now(::google::protobuf::RpcController* cntl_base,
                                  const ::raft::TimeoutNowRequest* request,
                                  ::raft::TimeoutNowResponse* response,
                                  ::google::protobuf::Closure* done) {
    RAFT_CHECK_AND_GET_NODE();
    RAFT_DISPATCH_POLAR(node, handle_timeout_now_request,
                        cntl, request, response, done_guard);
}

void RaftServiceImpl::explore(::google::protobuf::RpcController* cntl_base,
                              const ::raft::ExploreRequest* request,
                              ::raft::ExploreResponse* response,
                              ::google::protobuf::Closure* done) {
    RAFT_CHECK_AND_GET_NODE();
    RAFT_DISPATCH_POLAR(node, handle_explore,
                        cntl, request, response, done_guard);
}

void RaftServiceImpl::read_committed_logs(::google::protobuf::RpcController* cntl_base,
                                          const ::raft::ReadCommittedLogsRequest* request,
                                          ::raft::ReadCommittedLogsResponse* response,
                                          ::google::protobuf::Closure* done) {
    RAFT_CHECK_AND_GET_NODE();
    RAFT_DISPATCH_POLAR(node, handle_read_committed_logs,
                        cntl, request, response, done_guard);
}

void RaftServiceFBImpl::append_entries(::flatbuffers::brpc::RpcController* cntl_base,
                        const ::flatbuffers::brpc::Message* request_base,
                        ::flatbuffers::brpc::Message* response_base,
                        ::google::protobuf::Closure* done) {
    rpc::ClosureGuard done_guard(done);
    rpc::Controller* cntl = (rpc::Controller*)cntl_base;
    const AppendEntriesFBRequest* request =
        (const AppendEntriesFBRequest*)request_base->GetRoot<fb::AppendEntriesRequest>();

    PeerId peer_id;
    if (0 != peer_id.parse(request->peer_id()->str())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    scoped_refptr<NodeImpl> node =
        global_node_manager->get_node(request->group_id(), peer_id);
    if (node.get() == nullptr) {
        cntl->SetFailed(ENOENT, "peer_id not exist");
        return;
    }

    fbrpc::MessageBuilder build;
    build.ForceDefaults(true);
    auto rsp = fb::CreateAppendEntriesResponse(build);
    build.Finish(rsp);
    *response_base = build.ReleaseMessage();

    if (FLAGS_raft_enable_fb_verify && !request_base->Verify<fb::AppendEntriesRequest>()) {
        LOG(WARNING) << "recv unexpected raft.fb.append_entries, message data size: "
                     << request_base->size() << " addr: " << (void*)request_base
                     << ", hex: " << fb_hexdump(request_base);
        cntl->SetFailed(EINVAL, "unexpected request");
        return;
    }

    AppendEntriesFBResponse* response =
            (AppendEntriesFBResponse*)response_base->GetMutableRoot<fb::AppendEntriesResponse>();
    RAFT_DISPATCH_POLAR(node, handle_append_entries_request<ProtocolFB>,
                        cntl, request, response, done_guard);
}

}  // namespace raft
