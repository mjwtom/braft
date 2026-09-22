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

#include <base/raw_pack.h>              // base::RawPacker
#include "raft/local_storage.pb.h"
#include "raft/cli.h"

#include "raft/cli.pb.h"                // CliService_Stub
#include "raft/util.h"

namespace raft {
namespace cli {

static base::Status get_leader(const GroupId& group_id, const Configuration& conf,
                        PeerId* leader_id) {
    if (conf.empty()) {
        return base::Status(EINVAL, "Empty group configuration");
    }
    // Construct a brpc naming service to access all the nodes in this group
    base::Status st(-1, "Fail to get leader of group %s", group_id.c_str());
    leader_id->reset();
    for (Configuration::const_iterator
            iter = conf.begin(); iter != conf.end(); ++iter) {
        raft_channel_t channel;
        rpc::ChannelOptions channel_options;
        if (channel.Init(iter->addr, &channel_options) != 0) {
            return base::Status(-1, "Fail to init channel to %s",
                                     iter->to_string().c_str());
        }
        CliService_Stub stub(&channel);
        GetLeaderRequest request;
        GetLeaderResponse response;
        rpc::Controller cntl;
        request.set_group_id(group_id);
        request.set_peer_id(iter->to_string());
        stub.get_leader(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            if (st.ok()) {
                st.set_error(cntl.ErrorCode(), "[%s] %s",
                            base::endpoint2str(cntl.remote_side()).c_str(),
                            cntl.ErrorText().c_str());
            } else {
                std::string saved_et = st.error_str();
                st.set_error(cntl.ErrorCode(), "%s, [%s] %s",  saved_et.c_str(),
                            base::endpoint2str(cntl.remote_side()).c_str(),
                            cntl.ErrorText().c_str());
                }
            continue;
        }
        leader_id->parse(response.leader_id());
    }
    if (leader_id->is_empty()) {
        return st;
    }
    return base::Status::OK();
}

base::Status add_peer(const GroupId& group_id, const Configuration& conf,
                       const PeerId& peer_id, const CliOptions& options) {
    PeerId leader_id;
    base::Status st = get_leader(group_id, conf, &leader_id);
    RAFT_RETURN_IF(!st.ok(), st);
    raft_channel_t channel;
    rpc::ChannelOptions channel_options;
    if (channel.Init(leader_id.addr, &channel_options) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }
    AddPeerRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    request.set_peer_id(peer_id.to_string());
    AddPeerResponse response;
    rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.add_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group `" << group_id
              << "' changed from " << old_conf
              << " to " << new_conf;
    return base::Status::OK();
}

base::Status remove_peer(const GroupId& group_id, const Configuration& conf,
                         const PeerId& peer_id, const CliOptions& options) {
    PeerId leader_id;
    base::Status st = get_leader(group_id, conf, &leader_id);
    RAFT_RETURN_IF(!st.ok(), st);
    raft_channel_t channel;
    rpc::ChannelOptions channel_options;
    if (channel.Init(leader_id.addr, &channel_options) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }
    RemovePeerRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    request.set_peer_id(peer_id.to_string());
    RemovePeerResponse response;
    rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.remove_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group `" << group_id
              << "' changed from " << old_conf
              << " to " << new_conf;
    return base::Status::OK();
}

base::Status reset_peer(const GroupId& group_id, const PeerId& peer_id,
                         const Configuration& new_conf,
                         const CliOptions& options) {
    if (new_conf.empty()) {
        return base::Status(EINVAL, "new_conf is empty");
    }
    raft_channel_t channel;
    rpc::ChannelOptions channel_options;
    if (channel.Init(peer_id.addr, &channel_options) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                peer_id.to_string().c_str());
    }
    rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    ResetPeerRequest request;
    request.set_group_id(group_id);
    request.set_peer_id(peer_id.to_string());
    for (Configuration::const_iterator
            iter = new_conf.begin(); iter != new_conf.end(); ++iter) {
        request.add_new_peers(iter->to_string());
    }
    ResetPeerResponse response;
    CliService_Stub stub(&channel);
    stub.reset_peer(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return base::Status::OK();
}

base::Status snapshot(const GroupId& group_id, const PeerId& peer_id,
                      const CliOptions& options) {
    raft_channel_t channel;
    rpc::ChannelOptions channel_options;
    if (channel.Init(peer_id.addr, &channel_options) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                peer_id.to_string().c_str());
    }
    rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    SnapshotRequest request;
    request.set_group_id(group_id);
    request.set_peer_id(peer_id.to_string());
    SnapshotResponse response;
    CliService_Stub stub(&channel);
    stub.snapshot(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return base::Status::OK();
}

base::Status change_peers(const GroupId& group_id, const Configuration& conf,
                           const Configuration& new_peers,
                           const CliOptions& options) {
    PeerId leader_id;
    base::Status st = get_leader(group_id, conf, &leader_id);
    RAFT_RETURN_IF(!st.ok(), st);
    LOG(INFO) << "conf=" << conf << " leader=" << leader_id
              << " new_peers=" << new_peers;
    raft_channel_t channel;
    rpc::ChannelOptions channel_options;
    if (channel.Init(leader_id.addr, &channel_options) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }

    ChangePeersRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    for (Configuration::const_iterator
            iter = new_peers.begin(); iter != new_peers.end(); ++iter) {
        request.add_new_peers(iter->to_string());
    }
    ChangePeersResponse response;
    rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);

    CliService_Stub stub(&channel);
    stub.change_peers(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    Configuration old_conf;
    for (int i = 0; i < response.old_peers_size(); ++i) {
        old_conf.add_peer(response.old_peers(i));
    }
    Configuration new_conf;
    for (int i = 0; i < response.new_peers_size(); ++i) {
        new_conf.add_peer(response.new_peers(i));
    }
    LOG(INFO) << "Configuration of replication group `" << group_id
              << "' changed from " << old_conf
              << " to " << new_conf;
    return base::Status::OK();
}

base::Status transfer_leader(const GroupId& group_id, const Configuration& conf,
                              const PeerId& peer, const CliOptions& options) {
    PeerId leader_id;
    base::Status st = get_leader(group_id, conf, &leader_id);
    RAFT_RETURN_IF(!st.ok(), st);
    if (leader_id == peer) {
        LOG(INFO) << "peer " << peer << " is already the leader";
        return base::Status::OK();
    }
    raft_channel_t channel;
    rpc::ChannelOptions channel_options;
    if (channel.Init(leader_id.addr, &channel_options) != 0) {
        return base::Status(-1, "Fail to init channel to %s",
                                leader_id.to_string().c_str());
    }
    TransferLeaderRequest request;
    request.set_group_id(group_id);
    request.set_leader_id(leader_id.to_string());
    if (!peer.is_empty()) {
        request.set_peer_id(peer.to_string());
    }
    TransferLeaderResponse response;
    rpc::Controller cntl;
    cntl.set_timeout_ms(options.timeout_ms);
    cntl.set_max_retry(options.max_retry);
    CliService_Stub stub(&channel);
    stub.transfer_leader(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return base::Status(cntl.ErrorCode(), cntl.ErrorText());
    }
    return base::Status::OK();
}

const static size_t ENTRY_HEADER_SIZE = 24;

int parse_log(const std::string log_file,
              const std::string& out_log_file,
              int64_t first_index,
              std::map<int64_t, std::pair<uint32_t, Segment::EntryHeader>>* index_header_map) {
    if (log_file.empty()) {
        LOG(ERROR) << "log_file is empty";
        return -1;
    }
    int fd = ::open(log_file.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Fail to open log_file " << log_file << ", " << berror();
        return -1;
    }

    struct stat st_buf;
    if (fstat(fd, &st_buf) != 0) {
        LOG(ERROR) << "Fail to get the stat of log_file " << log_file << ", " << berror();
        ::close(fd);
        fd = -1;
        return -1;
    }

    int fd_out = ::open(out_log_file.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd_out < 0) {
        ::close(fd);
        fd = -1;
        LOG(ERROR) << "Fail to open out_log_file " << out_log_file << ", " << berror();
        return -1;
    }

    std::string filename;
    std::size_t pos = log_file.rfind('/');
    if (pos != std::string::npos) {
        filename = log_file.substr(pos + 1);
    } else {
        filename = log_file;
    }

    int match = 0;
    int64_t last_index = 0;
    int64_t parsed_first_index = 0;
    match = sscanf(filename.c_str(), "log_%020ld_%020ld", &parsed_first_index, &last_index);
    if (match != 2) {
        match = sscanf(filename.c_str(), "log_inprogress_%020ld", &parsed_first_index);
        if (match != 1) {
            parsed_first_index = first_index;
            LOG(WARNING) << "can not parse first_index from log_file name, use first_index param";
        }
    }

    std::ostringstream oss;

    int64_t file_size = st_buf.st_size;
    int64_t entry_off = 0;
    for (int64_t i = parsed_first_index; entry_off < file_size; i++) {
        base::IOPortal buf;
        ssize_t n = file_pread(&buf, fd, entry_off, ENTRY_HEADER_SIZE);
        if (n != ENTRY_HEADER_SIZE) {
            LOG(ERROR) << "read head failed, rc: " << n
                       << ", offset: " << entry_off << ", to_read_len: " << ENTRY_HEADER_SIZE;
            break;
        }
        char header_buf[ENTRY_HEADER_SIZE];
        const char *p = (const char *)buf.fetch(header_buf, ENTRY_HEADER_SIZE);
        int64_t term = 0;
        uint32_t meta_field;
        uint32_t data_len = 0;
        uint32_t data_checksum = 0;
        uint32_t header_checksum = 0;
        base::RawUnpacker(p).unpack64((uint64_t&)term)
                            .unpack32(meta_field)
                            .unpack32(data_len)
                            .unpack32(data_checksum)
                            .unpack32(header_checksum);
        Segment::EntryHeader header;
        header.term = term;
        header.type = meta_field >> 24;
        header.checksum_type = (meta_field << 8) >> 24;
        header.data_len = data_len;
        header.data_checksum = data_checksum;
        if (!verify_checksum(header.checksum_type,
                            p, ENTRY_HEADER_SIZE - 4, header_checksum)) {
            LOG(ERROR) << "Found corrupted header at offset=" << entry_off
                       << ", header=" << header;
            break;
        }
        oss << "index: " << i << " offset: " << entry_off
            << " header_checksum: " << header_checksum << " header: " << header;

        n = file_pread(&buf, fd, entry_off + ENTRY_HEADER_SIZE, data_len);
        if (n != data_len) {
            LOG(ERROR) << "read data failed, rc: " << n
                       << ", offset: " << entry_off + ENTRY_HEADER_SIZE << ", to_read_len: " << data_len;
            break;
        }

        buf.pop_front(ENTRY_HEADER_SIZE);
        if (!verify_checksum(header.checksum_type, buf, header.data_checksum)) {
            LOG(ERROR) << "Found corrupted data at offset=" 
                       << entry_off + ENTRY_HEADER_SIZE
                       << " header=" << header;
            break;
        }

        if (header.type == ENTRY_TYPE_CONFIGURATION) {
            base::Status status;
            ConfigurationPBMeta meta;
            base::IOBufAsZeroCopyInputStream wrapper(buf);
            if (!meta.ParseFromZeroCopyStream(&wrapper)) {
                LOG(ERROR) << "Fail to parse ConfigurationPBMeta, offset="
                       << entry_off + ENTRY_HEADER_SIZE
                       << " header=" << header;
                break;
            }
            oss << " peers:";
            for (int j = 0; j < meta.peers_size(); ++j) {
                oss << " " << PeerId(meta.peers(j));
            }
            if (meta.old_peers_size() > 0) {
                oss << " old_peers:";
                for (int k = 0; k < meta.old_peers_size(); k++) {
                    oss << " " << PeerId(meta.old_peers(k));
                }
            }
            if (meta.has_user_conf()) {
                oss << " user_conf_len: " << meta.user_conf().length();
            }
        }

        if (index_header_map) {
            (*index_header_map)[i] = std::make_pair(header_checksum, std::move(header));
        }

        entry_off += (ENTRY_HEADER_SIZE + header.data_len);
        oss << "\n";
    }
    if (entry_off != file_size) {
        oss << "Corrupted !!\n";
    }

    ::close(fd);
    fd = -1;

    base::IOBuf out_buf;
    out_buf.append(oss.str());

    file_pwrite(out_buf, fd_out, 0);
    fdatasync(fd_out);

    ::close(fd_out);
    fd_out = -1;

    if (entry_off != file_size) {
        return 1;
    } else {
        return 0;
    }
}

int modify_log(const std::string log_file,
               int64_t first_index,
               int64_t modify_index,
               const std::string& modify_header_field_name,
               int64_t modify_header_field_value,
               const std::string& out_log_file) {
    if (log_file.empty()) {
        LOG(ERROR) << "log_file is empty";
        return -1;
    }
    int fd = ::open(log_file.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(ERROR) << "Fail to open log_file " << log_file << ", " << berror();
        return -1;
    }

    struct stat st_buf;
    if (fstat(fd, &st_buf) != 0) {
        LOG(ERROR) << "Fail to get the stat of log_file " << log_file << ", " << berror();
        ::close(fd);
        fd = -1;
        return -1;
    }

    int out_fd = ::open(out_log_file.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (out_fd < 0) {
        LOG(ERROR) << "Fail to open out_log_file " << out_log_file << ", " << berror();
        ::close(fd);
        fd = -1;
        return -1;
    }

    std::string filename;
    std::size_t pos = log_file.rfind('/');
    if (pos != std::string::npos) {
        filename = log_file.substr(pos + 1);
    } else {
        filename = log_file;
    }

    int match = 0;
    int64_t last_index = 0;
    int64_t parsed_first_index = 0;
    match = sscanf(filename.c_str(), "log_%020ld_%020ld", &parsed_first_index, &last_index);
    if (match != 2) {
        match = sscanf(filename.c_str(), "log_inprogress_%020ld", &parsed_first_index);
        if (match != 1) {
            parsed_first_index = first_index;
            LOG(WARNING) << "can not parse first_index from log_file name, use first_index param";
        }
    }

    base::IOBuf out_buf;
    int64_t file_size = st_buf.st_size;
    int64_t entry_off = 0;
    for (int64_t i = parsed_first_index; entry_off < file_size; i++) {
        base::IOPortal buf;
        ssize_t n = file_pread(&buf, fd, entry_off, ENTRY_HEADER_SIZE);
        if (n != ENTRY_HEADER_SIZE) {
            LOG(ERROR) << "read head failed, rc: " << n
                       << ", offset: " << entry_off << ", to_read_len: " << ENTRY_HEADER_SIZE;
            break;
        }
        char header_buf[ENTRY_HEADER_SIZE];
        const char *p = (const char *)buf.fetch(header_buf, ENTRY_HEADER_SIZE);
        int64_t term = 0;
        uint32_t meta_field;
        uint32_t data_len = 0;
        uint32_t data_checksum = 0;
        uint32_t header_checksum = 0;
        base::RawUnpacker(p).unpack64((uint64_t&)term)
                            .unpack32(meta_field)
                            .unpack32(data_len)
                            .unpack32(data_checksum)
                            .unpack32(header_checksum);
        Segment::EntryHeader header;
        header.term = term;
        header.type = meta_field >> 24;
        header.checksum_type = (meta_field << 8) >> 24;
        header.data_len = data_len;
        header.data_checksum = data_checksum;

        if (i == modify_index) {
            if (modify_header_field_name == "header_checksum") {
                LOG(WARNING) << "index: " << i << " offset: " << entry_off
                             << " modify header field: header_checksum from " << header_checksum
                             << " to " << modify_header_field_value;
                header_checksum = modify_header_field_value;
            } else if (modify_header_field_name == "data_checksum") {
                LOG(WARNING) << "index: " << i << " offset: " << entry_off
                             << " modify header field: data_checksum from " << header.data_checksum
                             << " to " << modify_header_field_value;
                header.data_checksum = modify_header_field_value;
            } else {
                LOG(ERROR) << "not support modify field: \"" << modify_header_field_name << "\"";
            }

            char header_buf[ENTRY_HEADER_SIZE];
            const uint32_t meta_field = (header.type << 24 ) | (header.checksum_type << 16);
            base::RawPacker packer(header_buf);
            packer.pack64(header.term)
                  .pack32(meta_field)
                  .pack32(header.data_len)
                  .pack32(header.data_checksum)
                  .pack32(header_checksum);
            out_buf.append(header_buf, ENTRY_HEADER_SIZE);
            buf.pop_front(ENTRY_HEADER_SIZE);
        } else {
            buf.cutn(&out_buf, ENTRY_HEADER_SIZE);
        }

        n = file_pread(&buf, fd, entry_off + ENTRY_HEADER_SIZE, data_len);
        if (n != data_len) {
            LOG(ERROR) << "read data failed, rc: " << n
                       << ", offset: " << entry_off + ENTRY_HEADER_SIZE << ", to_read_len: " << data_len;
            break;
        }

        buf.cutn(&out_buf, data_len);

        entry_off += (ENTRY_HEADER_SIZE + header.data_len);
    }

    ::close(fd);
    fd = -1;

    file_pwrite(out_buf, out_fd, 0);
    fdatasync(out_fd);
    ::close(out_fd);
    out_fd = -1;

    return 0;
}

}  // namespace cli
}  // namespace raft
