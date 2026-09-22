// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
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

#ifndef  PUBLIC_RAFT_CLI_H
#define  PUBLIC_RAFT_CLI_H

#include "raft/raft.h"
#include "raft/log.h"

namespace raft {
namespace cli {

struct CliOptions {
    int timeout_ms;
    int max_retry;
    CliOptions() : timeout_ms(-1), max_retry(3) {}
};

// Add a new peer into the replicating group which consists of |conf|.
// Returns OK on success, error information otherwise.
base::Status add_peer(const GroupId& group_id, const Configuration& conf,
                      const PeerId& peer_id, const CliOptions& options);

// Remove a peer from the replicating group which consists of |conf|.
// Returns OK on success, error information otherwise.
base::Status remove_peer(const GroupId& group_id, const Configuration& conf,
                         const PeerId& peer_id, const CliOptions& options);

// Gracefully change the peers of the replication group.
base::Status change_peers(const GroupId& group_id, const Configuration& conf, 
                          const Configuration& new_peers,
                          const CliOptions& options);

// Reset the peer set of the target peer
base::Status reset_peer(const GroupId& group_id, const PeerId& peer_id,
                        const Configuration& new_conf, const CliOptions& options);

// Ask the peer to dump a snapshot immediately.
base::Status snapshot(const GroupId& group_id, const PeerId& peer_id,
                      const CliOptions& options);

// Transfer the leader of the replication group to the target peer
base::Status transfer_leader(const GroupId& group_id, const Configuration& conf,
                             const PeerId& peer, const CliOptions& options);

int parse_log(const std::string log_file,
              const std::string& out_log_file,
              int64_t first_index,
              std::map<int64_t, std::pair<uint32_t, Segment::EntryHeader>>* index_header_map);

int modify_log(const std::string log_file,
               int64_t first_index,
               int64_t modify_index,
               const std::string& modify_header_field_name,
               int64_t modify_header_field_value,
               const std::string& out_log_file);

}  // namespace cli
}  // namespace raft

#endif  //PUBLIC_RAFT_CLI_H
