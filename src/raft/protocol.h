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

#pragma once

#include <iomanip>
#include "raft/raft.h"
#include "raft/raft.pb.h"
#include "raft/raft_generated.h"
#include "raft/raft.brpc.fb.h"

namespace fbrpc = flatbuffers::brpc;

namespace raft {

DECLARE_bool(raft_enable_fb_verify);
DECLARE_bool(raft_enable_fb_print_hex);

class ProtocolPB {};

class ProtocolFB {};

inline std::ostream& operator<<(std::ostream& os, const ProtocolPB& pb) {
    os << "PB";
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const ProtocolFB& pb) {
    os << "FB";
    return os;
}

class EntryMetaFB : public fb::EntryMeta {
public:
    const std::string GetTypeName() const { return std::string(GetFullyQualifiedName()); }

    inline const std::string peers(int index) const {
        return fb::EntryMeta::peers()->Get(index)->str();
    }
    inline int peers_size() const {
        if (fb::EntryMeta::peers() == nullptr) {
            return 0;
        }
        return fb::EntryMeta::peers()->size();
    }
    inline const std::string old_peers(int index) const {
        return fb::EntryMeta::old_peers()->Get(index)->str();
    }
    inline int old_peers_size() const {
        if (fb::EntryMeta::old_peers() == nullptr) {
            return 0;
        }
        return fb::EntryMeta::old_peers()->size();
    }
    inline bool has_user_conf() const {
        if (fb::EntryMeta::user_conf() == nullptr) {
            return false;
        }
        return fb::EntryMeta::user_conf()->size() > 0;
    }
    inline const std::string user_conf() const {
        return std::string((const char*)fb::EntryMeta::user_conf()->data(),
                            fb::EntryMeta::user_conf()->size());
    }
    inline bool has_data_len() const {
        return data_len() > 0;
    }

    inline void set_term(int64_t value) { mutate_term(value); }
    inline void set_type(int value) { mutate_type((fb::EntryType)value); }
    inline void set_data_len(int64_t value) { mutate_data_len(value); }
};

class AppendEntriesFBRequest : public fb::AppendEntriesRequest {
public:
    const std::string GetTypeName() const { return std::string(GetFullyQualifiedName()); }

    inline int entries_size() const {
        if (fb::AppendEntriesRequest::entries() == nullptr) {
            return 0;
        }
        return fb::AppendEntriesRequest::entries()->size();
    }
    inline const EntryMetaFB& entries(int index) const {
        return (const EntryMetaFB&)(*fb::AppendEntriesRequest::entries()->Get(index));
    }
    inline const std::string server_id() const {
        return fb::AppendEntriesRequest::server_id()->str();
    }
    inline const std::string group_id() const {
        return fb::AppendEntriesRequest::group_id()->str();
    }
    inline void set_term(int64_t value) { mutate_term(value); }
    inline void set_prev_log_term(int64_t value) { mutate_prev_log_term(value); }
    inline void set_prev_log_index(int64_t value) { mutate_prev_log_index(value); }
    inline void set_committed_index(int64_t value) { mutate_committed_index(value); }
    inline void set_crc32_type(uint32_t value) { mutate_crc32_type(value); }
    inline void set_crc32(uint32_t value) { mutate_crc32(value); }
};

class AppendEntriesFBResponse : public fb::AppendEntriesResponse {
public:
    const std::string GetTypeName() const { return std::string(GetFullyQualifiedName()); }

    inline bool has_readonly() { return readonly() == true; }
    inline void set_term(int64_t value) { mutate_term(value); }
    inline void set_success(bool value) { mutate_success(value); }
    inline void set_last_log_index(int64_t value) { mutate_last_log_index(value); }
    inline void set_readonly(bool value) { mutate_readonly(value); }
};

template <typename T>
struct EntryMetaT;
template<>
struct EntryMetaT<ProtocolFB> {
    using type = EntryMetaFB;
};
template<>
struct EntryMetaT<ProtocolPB> {
    using type = EntryMeta;
};
template <typename T>
using EntryMetaType = typename EntryMetaT<T>::type;


template <typename T>
struct EntryMetaUnitType;
template<>
struct EntryMetaUnitType<ProtocolFB> {
    using type = flatbuffers::Offset<fb::EntryMeta>;
};
template<>
struct EntryMetaUnitType<ProtocolPB> {
    using type = EntryMeta;
};
template <typename T>
using EntryMetaUnit = typename EntryMetaUnitType<T>::type;


template <typename T>
struct MsgBuilder;
template<>
struct MsgBuilder<ProtocolFB> {
    using type = fbrpc::MessageBuilder;
};
template<>
struct MsgBuilder<ProtocolPB> {
    using type = int;
};
template <typename T>
using MsgBuilderType = typename MsgBuilder<T>::type;


template <typename T>
struct MessageT;
template<>
struct MessageT<ProtocolFB> {
    using type = fbrpc::Message;
};
template<>
struct MessageT<ProtocolPB> {
    using type = ::google::protobuf::Message;
};
template <typename T>
using MessageType = typename MessageT<T>::type;


template <typename T>
struct AppendEntriesRequestType;
template<>
struct AppendEntriesRequestType<ProtocolFB> {
    using type = AppendEntriesFBRequest;
};
template<>
struct AppendEntriesRequestType<ProtocolPB> {
    using type = AppendEntriesRequest;
};
template <typename T>
using AppendEntriesReq = typename AppendEntriesRequestType<T>::type;


template <typename T>
struct AppendEntriesRequestArgument;
template<>
struct AppendEntriesRequestArgument<ProtocolFB> {
    using type = fbrpc::Message;
};
template<>
struct AppendEntriesRequestArgument<ProtocolPB> {
    using type = AppendEntriesRequest;
};
template <typename T>
using AppendEntriesReqArg = typename AppendEntriesRequestArgument<T>::type;


template <typename T>
struct ProtoAppendEntriesRequestType;
template<>
struct ProtoAppendEntriesRequestType<ProtocolFB> {
    using type = fb::AppendEntriesRequest;
};
template<>
struct ProtoAppendEntriesRequestType<ProtocolPB> {
    using type = AppendEntriesRequest;
};
template <typename T>
using ProtoAppendEntriesReq = typename ProtoAppendEntriesRequestType<T>::type;

template <typename T>
struct AppendEntriesResponseType;
template<>
struct AppendEntriesResponseType<ProtocolFB> {
    using type = AppendEntriesFBResponse;
};
template<>
struct AppendEntriesResponseType<ProtocolPB> {
    using type = AppendEntriesResponse;
};
template <typename T>
using AppendEntriesRsp = typename AppendEntriesResponseType<T>::type;


template <typename T>
struct AppendEntriesResponseArgument;
template<>
struct AppendEntriesResponseArgument<ProtocolFB> {
    using type = fbrpc::Message;
};
template<>
struct AppendEntriesResponseArgument<ProtocolPB> {
    using type = AppendEntriesResponse;
};
template <typename T>
using AppendEntriesRspArg = typename AppendEntriesResponseArgument<T>::type;

template <typename T>
struct ProtoAppendEntriesResponseType;
template<>
struct ProtoAppendEntriesResponseType<ProtocolFB> {
    using type = fb::AppendEntriesResponse;
};
template<>
struct ProtoAppendEntriesResponseType<ProtocolPB> {
    using type = AppendEntriesResponse;
};
template <typename T>
using ProtoAppendEntriesRsp = typename ProtoAppendEntriesResponseType<T>::type;

template <typename T>
struct RaftServiceStubT;
template<>
struct RaftServiceStubT<ProtocolFB> {
    using type = fb::RaftServiceStub;
};
template<>
struct RaftServiceStubT<ProtocolPB> {
    using type = RaftService_Stub;
};
template <typename T>
using RaftServiceStubType = typename RaftServiceStubT<T>::type;

typedef flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>> FbStringVector;
typedef flatbuffers::Offset<flatbuffers::Vector<uint8_t>> FbByteVector;
typedef flatbuffers::Offset<flatbuffers::String> FbString;
typedef flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<fb::EntryMeta>>> FbEntryMetaVector;

[[maybe_unused]]
static FbStringVector string_vector_to_fb(fbrpc::MessageBuilder* build, const std::vector<std::string>& in) {
    if (in.size() == 0) {
        return (FbStringVector)0;
    }
    std::vector<flatbuffers::Offset<flatbuffers::String>> vec;
    for (auto& x : in) {
        auto xx = build->CreateString(x);
        vec.push_back(xx);
    }
    auto ret = build->CreateVector(vec);
    return ret;
}

[[maybe_unused]]
static FbByteVector string_to_fb_bytes(fbrpc::MessageBuilder* build, const std::string& in) {
    if (in.empty()) {
        return (FbByteVector)0;
    }
    return build->CreateVector((const uint8_t*)in.c_str(), in.length());
}

[[maybe_unused]]
static FbString string_to_fb(fbrpc::MessageBuilder* build, const std::string& in) {
    if (in.empty()) {
        return (FbString)0;
    }
    return build->CreateString(in);
}

[[maybe_unused]]
static FbEntryMetaVector entry_meta_vector_to_fb(fbrpc::MessageBuilder* build,
                                                 const std::vector<flatbuffers::Offset<fb::EntryMeta>>& in) {
    if (in.size() == 0) {
        return (FbEntryMetaVector)0;
    }
    return build->CreateVector(in);
}

template <typename T>
void fill_entry_meta(std::vector<EntryMetaUnit<T>>* entry_vec,
                    int64_t term, int type,
                    const std::vector<std::string>& peers, int64_t data_len,
                    const std::vector<std::string>& old_peers, const std::string user_conf,
                    MsgBuilderType<T>* build = nullptr) {
    if constexpr (std::is_same<T, ProtocolFB>::value) {
        CHECK(build);
        auto fb_peers = string_vector_to_fb(build, peers);
        auto fb_old_peers = string_vector_to_fb(build, old_peers);
        auto fb_user_conf = string_to_fb_bytes(build, user_conf);
        auto ptr = fb::CreateEntryMeta(*build, term, (fb::EntryType)type, fb_peers,
                                        data_len, fb_old_peers, fb_user_conf);
        entry_vec->push_back(ptr);
    } else {
        CHECK(build == nullptr);
        entry_vec->push_back(EntryMeta());
        EntryMeta& entry = entry_vec->back();
        entry.set_term(term);
        entry.set_type((raft::EntryType)type);
        for (auto& p : peers) {
            entry.add_peers(p);
        }
        entry.set_data_len(data_len);
        for (auto& p : old_peers) {
            entry.add_old_peers(p);
        }
        if (!user_conf.empty()) {
            entry.set_user_conf(user_conf);
        }
    }
}

template <typename T>
void fill_append_entries_request(MessageType<T>** req, std::string group_id,
                                std::string server_id, std::string peer_id,
                                int64_t term, int64_t prev_log_term, int64_t prev_log_index,
                                std::vector<EntryMetaUnit<T>>& ems, int64_t committed_index,
                                MsgBuilderType<T>* build = nullptr) {
    if constexpr (std::is_same<T, ProtocolFB>::value) {
        CHECK(build);
        auto fb_group_id = string_to_fb(build, group_id);
        auto fb_server_id = string_to_fb(build, server_id);
        auto fb_peer_id = string_to_fb(build, peer_id);
        auto fb_ems = entry_meta_vector_to_fb(build, ems);
        auto ptr = fb::CreateAppendEntriesRequest(*build, fb_group_id, fb_server_id, fb_peer_id,
                        term, prev_log_term, prev_log_index, fb_ems, committed_index);
        build->Finish(ptr);
        fbrpc::Message* msg = new fbrpc::Message();
        *msg = build->ReleaseMessage();
        *req = msg;
    } else {
        CHECK(build == nullptr);
        AppendEntriesRequest* request = new AppendEntriesRequest;
        request->set_term(term);
        request->set_group_id(group_id);
        request->set_server_id(server_id);
        request->set_peer_id(peer_id);
        request->set_prev_log_index(prev_log_index);
        request->set_prev_log_term(prev_log_term);
        request->set_committed_index(committed_index);
        for (auto& e : ems) {
            request->add_entries()->Swap(&e);
        }
        *req = request;
    }
}

[[maybe_unused]]
static const std::string fb_hexdump(const fbrpc::Message* message) {
    if (FLAGS_raft_enable_fb_print_hex == false) {
        return "";
    }
    std::stringstream ss;
    const uint8_t* data = message->data();
    for (size_t i = 0; i < message->size(); ++i) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)(data[i]);
    }
    return ss.str();
}

template <typename T>
AppendEntriesReq<T>* get_request_msg(AppendEntriesReqArg<T>* msg) {
    if constexpr (std::is_same<T, ProtocolFB>::value) {
        return (AppendEntriesFBRequest*)msg->template GetMutableRoot<fb::AppendEntriesRequest>();
    } else {
        return (AppendEntriesRequest*)msg;
    }
}

template <typename T>
AppendEntriesRsp<T>* get_response_msg(AppendEntriesRspArg<T>* msg) {
    if constexpr (std::is_same<T, ProtocolFB>::value) {
        if (FLAGS_raft_enable_fb_verify && !msg->template Verify<fb::AppendEntriesResponse>()) {
            LOG(WARNING) << "unexpected response, hex: " << fb_hexdump(msg);
            return nullptr;
        }
        return (AppendEntriesFBResponse*)msg->template GetMutableRoot<fb::AppendEntriesResponse>();
    } else {
        return (AppendEntriesResponse*)msg;
    }
}

}  // namespace raft
