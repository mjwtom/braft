// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved

// Author: Yuanbo Wang (wangyuanbo@baidu.com)
// Date: 2025/02/07 12:00:00

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <base/logging.h>

#include "raft/protocol.h"

class ProtocolTest : public testing::Test {
public:
    void SetUp() {}
    void TearDown() {}
};

void init_entry_meta(::flatbuffers::brpc::Message* base,
                    int64_t term, int type,
                    const std::vector<std::string>& peers,
                    const std::vector<std::string>& old_peers,
                    const std::string& user_conf, int64_t data_len) {
    fbrpc::MessageBuilder build;
    build.ForceDefaults(true);

    auto fb_peers = raft::string_vector_to_fb(&build, peers);
    auto fb_old_peers = raft::string_vector_to_fb(&build, old_peers);
    auto fb_user_conf = raft::string_to_fb_bytes(&build, user_conf);
    auto msg = raft::fb::CreateEntryMeta(build, term, (raft::fb::EntryType)type, fb_peers, data_len, fb_old_peers, fb_user_conf);

    build.Finish(msg);
    *base = build.ReleaseMessage();
}

TEST_F(ProtocolTest, EntryMeta) {
    std::vector<std::string> peers, old_peers;
    std::string user_conf;
    ::flatbuffers::brpc::Message* base = new fbrpc::Message();
    init_entry_meta(base, 0, 0, peers, old_peers, user_conf, 0);
    raft::EntryMetaFB* msg = (raft::EntryMetaFB*)base->GetMutableRoot<raft::fb::EntryMeta>();
    CHECK_EQ(msg->GetTypeName(), "raft.fb.EntryMeta");
    CHECK_EQ(msg->peers_size(), 0);
    CHECK_EQ(msg->old_peers_size(), 0);
    CHECK_EQ(msg->has_user_conf(), false);
    CHECK_EQ(msg->has_data_len(), false);
    delete base;
    
    for (int i = 0; i < 10; ++i) {
        peers.push_back("peer" + std::to_string(i));
        old_peers.push_back("old_peer" + std::to_string(i));
    }
    user_conf = "user_conf";
    base = new fbrpc::Message();
    init_entry_meta(base, 10, 1, peers, old_peers, user_conf, 100);
    msg = (raft::EntryMetaFB*)base->GetMutableRoot<raft::fb::EntryMeta>();
    CHECK_EQ(msg->GetTypeName(), "raft.fb.EntryMeta");
    CHECK_EQ(msg->peers_size(), peers.size());
    for (int i = 0; i < peers.size(); ++i) {
        CHECK_EQ(msg->peers(i), peers[i]);
    }
    CHECK_EQ(msg->old_peers_size(), old_peers.size());
    for (int i = 0; i < old_peers.size(); ++i) {
        CHECK_EQ(msg->old_peers(i), old_peers[i]);
    }
    CHECK_EQ(msg->has_user_conf(), true);
    CHECK_EQ(msg->user_conf(), user_conf);
    CHECK_EQ(msg->has_data_len(), true);
    CHECK_EQ(msg->data_len(), 100);
    delete base;
}

typedef raft::EntryMetaUnit<raft::ProtocolFB> EntryMetaUnit;
fbrpc::Message* init_append_entries_request(bool has_entries) {
    fbrpc::MessageBuilder build;
    build.ForceDefaults(true);
    fbrpc::Message* msg = nullptr;
    if (has_entries) {
        std::vector<std::string> peers, old_peers;
        std::string user_conf;
        for (int i = 0; i < 10; ++i) {
            peers.push_back("peer" + std::to_string(i));
            old_peers.push_back("old_peer" + std::to_string(i));
        }
        user_conf = "user_conf";
        std::vector<EntryMetaUnit> fb_entries;
        for (int i = 0; i < 10; i++) {
            raft::fill_entry_meta<raft::ProtocolFB>(&fb_entries, 10, 1, peers, 100, old_peers, user_conf, &build);
        }
        raft::fill_append_entries_request<raft::ProtocolFB>(&msg, "group_id", "server_id", "peer_id", 1, 10, 20, fb_entries, 30, &build);
    } else {
        std::vector<EntryMetaUnit> fb_entries;
        raft::fill_append_entries_request<raft::ProtocolFB>(&msg, "group_id", "server_id", "peer_id", 1, 10, 20, fb_entries, 30, &build);
    }
    return msg;
}

TEST_F(ProtocolTest, AppendEntriesRequest) {
    std::vector<std::string> peers, old_peers;
    std::string user_conf;
    for (int i = 0; i < 10; ++i) {
        peers.push_back("peer" + std::to_string(i));
        old_peers.push_back("old_peer" + std::to_string(i));
    }
    user_conf = "user_conf";
    ::flatbuffers::brpc::Message* base = init_append_entries_request(true);
    raft::AppendEntriesFBRequest* request = (raft::AppendEntriesFBRequest*)base->GetMutableRoot<raft::fb::AppendEntriesRequest>();
    CHECK_EQ(request->entries_size(), 10);
    for (int i = 0; i < request->entries_size(); ++i) {
        CHECK_EQ(request->entries(i).peers_size(), 10);
        for (int j = 0; j < request->entries(i).peers_size(); ++j) {
            CHECK_EQ(request->entries(i).peers(j), peers[j]);
        }
        CHECK_EQ(request->entries(i).old_peers_size(), 10);
        for (int j = 0; j < request->entries(i).old_peers_size(); ++j) {
            CHECK_EQ(request->entries(i).old_peers(j), old_peers[j]);
        }
        CHECK_EQ(request->entries(i).has_user_conf(), true);
        CHECK_EQ(request->entries(i).user_conf(), user_conf);
    }
    CHECK_EQ(request->GetTypeName(), "raft.fb.AppendEntriesRequest");
    CHECK_EQ(request->group_id(), "group_id");
    CHECK_EQ(request->server_id(), "server_id");
    delete base;

    base = init_append_entries_request(false);
    request = (raft::AppendEntriesFBRequest*)base->GetMutableRoot<raft::fb::AppendEntriesRequest>();
    CHECK_EQ(request->entries_size(), 0);
    CHECK_EQ(request->GetTypeName(), "raft.fb.AppendEntriesRequest");
    CHECK_EQ(request->group_id(), "group_id");
    CHECK_EQ(request->server_id(), "server_id");
    delete base;
}

TEST_F(ProtocolTest, AppendEntriesResponse) {
    ::flatbuffers::brpc::Message* base = new fbrpc::Message();
    {
        fbrpc::MessageBuilder build;
        build.ForceDefaults(true);
        auto msg = raft::fb::CreateAppendEntriesResponse(build);
        build.Finish(msg);
        *base = build.ReleaseMessage();
    }
    raft::fb::AppendEntriesResponse* response = base->GetMutableRoot<raft::fb::AppendEntriesResponse>();
    CHECK_EQ(response->term(), 0);
    CHECK_EQ(response->success(), false);
    CHECK_EQ(response->last_log_index(), 0);
    CHECK_EQ(response->readonly(), false);

    raft::AppendEntriesFBResponse* fb_rsp = (raft::AppendEntriesFBResponse*)response;
    fb_rsp->set_term(1);
    fb_rsp->set_success(true);
    fb_rsp->set_last_log_index(10);
    fb_rsp->set_readonly(true);
    CHECK_EQ(fb_rsp->term(), 1);
    CHECK_EQ(fb_rsp->success(), true);
    CHECK_EQ(fb_rsp->last_log_index(), 10);
    CHECK_EQ(fb_rsp->readonly(), true);
    CHECK_EQ(fb_rsp->has_readonly(), true);
    CHECK_EQ(fb_rsp->GetTypeName(), "raft.fb.AppendEntriesResponse");
    delete base;
}
