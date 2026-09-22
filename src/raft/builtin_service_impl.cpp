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

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)
//          Ge,Jun(gejun@baiud.com)

#include "raft/builtin_service_impl.h"
#include "raft/replicator.h"
#include "raft/node_manager.h"
#include "raft/learner.h"

namespace raft {

void RaftStatImpl::GetTabInfo(rpc::TabInfoList* info_list) const {
    rpc::TabInfo* info = info_list->add();
    info->tab_name = "raft";
    info->path = "/raft_stat";
}

void continuously_describe_nodes(bool html, std::string prev_group_id,
        std::vector<scoped_refptr<NodeImpl>>* nodes,
        base::IOBufBuilder* os, rpc::Controller* cntl, google::protobuf::Closure* done) {
    CHECK(nodes);
    CHECK(os);
    rpc::ClosureGuard done_guard(done);
    if (nodes->empty()) {
        // all nodes finished describe
        if (html) {
            *os << "</body></html>";
        }
        os->move_to(cntl->response_attachment());
        delete os;
        delete nodes;
        return;
    }
    const char *newline = html ? "<br>" : "\r\n";
    scoped_refptr<NodeImpl> node = *nodes->begin();
    if (node->polar_worker_index() != current_polar_idx()) {
        // ^^^ it is ok to do dispatch here, anywhere better?
        google::protobuf::Closure* polar_done = rpc::NewCallback(continuously_describe_nodes, html, prev_group_id,
                nodes, os, cntl, done_guard.release());
        run_closure_in_polar(polar_done, node->polar_worker_index());
        return;
    }
    nodes->erase(nodes->begin());
    const NodeId node_id = node->node_id();
    std::string group_id = node_id.group_id;
    if (group_id != prev_group_id) {
        if (html) {
            *os << "<h1>" << group_id << "</h1>";
        } else {
            *os << "[" << group_id << "]" << newline;
        }
        prev_group_id = group_id;
    }
    node->describe(*os, html);
    *os << newline;
    continuously_describe_nodes(html, prev_group_id, nodes, os, cntl, done_guard.release());
}

void RaftStatImpl::default_method(::google::protobuf::RpcController* controller,
                              const ::raft::IndexRequest* /*request*/,
                              ::raft::IndexResponse* /*response*/,
                              ::google::protobuf::Closure* done) {
    rpc::ClosureGuard done_guard(done);
    rpc::Controller* cntl = (rpc::Controller*)controller;
    std::string group_id = cntl->http_request().unresolved_path();
    std::vector<scoped_refptr<NodeImpl> > nodes;
    std::vector<scoped_refptr<LearnerImpl> > learner_nodes;
    if (group_id.empty()) {
        global_node_manager->get_all_nodes(&nodes);
        global_node_manager->get_all_learner_nodes(&learner_nodes);
    } else {
        global_node_manager->get_nodes_by_group_id(group_id, &nodes);
        global_node_manager->get_learners_by_group_id(group_id, &learner_nodes);
    }
    const bool html = rpc::UseHTML(cntl->http_request());
    if (html) {
        cntl->http_response().set_content_type("text/html");
    } else {
        cntl->http_response().set_content_type("text/plain");
    }
    base::IOBufBuilder* os = new base::IOBufBuilder;
    if (html) {
        *os << "<!DOCTYPE html><html><head>\n"
           << "<script language=\"javascript\" type=\"text/javascript\" src=\"/js/jquery_min\"></script>\n"
           << rpc::TabsHead() << "</head><body>";
        cntl->server()->PrintTabsBody(*os, "raft");
    }
    if (nodes.empty() && learner_nodes.empty()) {
        if (html) {
            *os << "</body></html>";
        }
        os->move_to(cntl->response_attachment());
        delete os;
        return;
    }

    std::string prev_group_id;

    if (FLAGS_raft_use_polar) {
        LOG(NOTICE) << "recv describe node from: " << cntl->remote_side() << ", nodes: " << nodes.size();
        SamePollerClosure* same_closure = new SamePollerClosure(done_guard.release());
        std::vector<scoped_refptr<NodeImpl>>* node_vector = 
            new std::vector<scoped_refptr<NodeImpl>>;
        node_vector->insert(node_vector->end(), nodes.begin(), nodes.end());
        // node_vector->insert(node_vector->end(), learner_nodes.begin(), learner_nodes.end()); // learner not supported
        continuously_describe_nodes(html, prev_group_id, node_vector, os, cntl, same_closure);
        return;
    }

    const char *newline = html ? "<br>" : "\r\n";

    if (!nodes.empty()) {
        for (size_t i = 0; i < nodes.size(); ++i) {
            const NodeId node_id = nodes[i]->node_id();
            group_id = node_id.group_id;
            if (group_id != prev_group_id) {
                if (html) {
                    *os << "<h1>" << group_id << "</h1>";
                } else {
                    *os << "[" << group_id << "]" << newline;
                }
                prev_group_id = group_id;
            }
            nodes[i]->describe(*os, html);
            *os << newline;
        }
    }

    if (!learner_nodes.empty()) {
        for (size_t i = 0; i < learner_nodes.size(); ++i) {
            const NodeId node_id = learner_nodes[i]->node_id();
            group_id = node_id.group_id;
            if (group_id != prev_group_id) {
                if (html) {
                    *os << "<h1>" << group_id << "</h1>";
                } else {
                    *os << "[" << group_id << "]" << newline;
                }
                prev_group_id = group_id;
            }
            learner_nodes[i]->describe(*os, html);
            *os << newline;
        }
    }
    if (html) {
        *os << "</body></html>";
    }
    os->move_to(cntl->response_attachment());
    delete os;
}

}  // namespace raft
