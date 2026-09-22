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
//          Wang,Yao(wangyao02@baidu.com)

#include <errno.h>
#include <base/string_printf.h>
#include <base/string_splitter.h>
#include <base/logging.h>

#include "raft/storage.h"
#include "raft/log.h"
#include "raft/stable.h"
#include "raft/snapshot.h"

namespace raft {

DEFINE_bool(raft_sync, true, "call fsync when need");
RAFT_VALIDATE_GFLAG(raft_sync, ::rpc::PassValidate);

DEFINE_bool(raft_create_parent_directories, true,
            "Create parent directories of the path in local storage if true");

DEFINE_bool(raft_sync_meta, false, "sync log meta, snapshot meta and stable meta");
RAFT_VALIDATE_GFLAG(raft_sync_meta, ::rpc::PassValidate);

LogStorage* LogStorage::create(const std::string& uri) {
    base::StringPiece copied_uri(uri);
    std::string parameter;
    base::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty()) {
        LOG(ERROR) << "Invalid log storage uri=`" << uri << '\'';
        return NULL;
    }
    const LogStorage* type = log_storage_extension()->Find(
                protocol.as_string().c_str());
    if (type == NULL) {
        LOG(ERROR) << "Fail to find log storage type " << protocol
                   << ", uri=" << uri;
        return NULL;
    }
    return type->new_instance(parameter);
}

base::Status LogStorage::destroy(const std::string& uri) {
    base::Status status;
    base::StringPiece copied_uri(uri);
    std::string parameter;
    base::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty()) {
        LOG(ERROR) << "Invalid log storage uri=`" << uri << '\'';
        status.set_error(EINVAL, "Invalid log storage uri = %s", uri.c_str());
        return status;
    }
    const LogStorage* type = log_storage_extension()->Find(
                protocol.as_string().c_str());
    if (type == NULL) {
        LOG(ERROR) << "Fail to find log storage type " << protocol
                   << ", uri=" << uri;
        status.set_error(EINVAL, "Fail to find log storage type %s uri %s", 
                         protocol.as_string().c_str(), uri.c_str());
        return status;
    }
    return type->gc_instance(parameter);
}

SnapshotStorage* SnapshotStorage::create(const std::string& uri) {
    base::StringPiece copied_uri(uri);
    std::string parameter;
    base::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty()) {
        LOG(ERROR) << "Invalid snapshot storage uri=`" << uri << '\'';
        return NULL;
    }
    const SnapshotStorage* type = snapshot_storage_extension()->Find(
                protocol.as_string().c_str());
    if (type == NULL) {
        LOG(ERROR) << "Fail to find snapshot storage type " << protocol
                   << ", uri=" << uri;
        return NULL;
    }
    return type->new_instance(parameter);
}

base::Status SnapshotStorage::destroy(const std::string& uri) {
    base::Status status;
    base::StringPiece copied_uri(uri);
    std::string parameter;
    base::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty()) {
        LOG(ERROR) << "Invalid snapshot storage uri=`" << uri << '\'';
        status.set_error(EINVAL, "Invalid log storage uri = %s", uri.c_str());
        return status;
    }
    const SnapshotStorage* type = snapshot_storage_extension()->Find(
                protocol.as_string().c_str());
    if (type == NULL) {
        LOG(ERROR) << "Fail to find snapshot storage type " << protocol
                   << ", uri=" << uri;
        status.set_error(EINVAL, "Fail to find snapshot storage type %s uri %s", 
                         protocol.as_string().c_str(), uri.c_str());
        return status;
    }
    return type->gc_instance(parameter);
}

StableStorage* StableStorage::create(const std::string& uri) {
    base::StringPiece copied_uri(uri);
    std::string parameter;
    base::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty()) {
        LOG(ERROR) << "Invalid stable storage uri=`" << uri << '\'';
        return NULL;
    }
    const StableStorage* type = stable_storage_extension()->Find(
                protocol.as_string().c_str());
    if (type == NULL) {
        LOG(ERROR) << "Fail to find stable storage type " << protocol
                   << ", uri=" << uri;
        return NULL;
    }
    return type->new_instance(parameter);
}

base::Status StableStorage::destroy(const std::string& uri,
                                    const VersionedGroupId& vgid) {
    base::Status status;
    base::StringPiece copied_uri(uri);
    std::string parameter;
    base::StringPiece protocol = parse_uri(&copied_uri, &parameter);
    if (protocol.empty()) {
        LOG(ERROR) << "Invalid stable storage uri=`" << uri << '\'';
        status.set_error(EINVAL, "Invalid stable storage uri = %s", uri.c_str());
        return status;
    }
    const StableStorage* type = stable_storage_extension()->Find(
                protocol.as_string().c_str());
    if (type == NULL) {
        LOG(ERROR) << "Fail to find stable storage type " << protocol
                   << ", uri=" << uri;
        status.set_error(EINVAL, "Fail to find stable storage type %s uri %s", 
                         protocol.as_string().c_str(), uri.c_str());
        return status;
    }
    return type->gc_instance(parameter, vgid);
}

}  // namespace raft
