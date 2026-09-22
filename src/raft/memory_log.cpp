// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
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

// Authors: Qin,Duohao(qinduohao@baidu.com)

#include <base/time.h>
#include "raft/log_entry.h"
#include "raft/memory_log.h"
#include "raft/memory_log.h"

namespace raft {

int MemoryLogStorage::init(ConfigurationManager* configuration_manager) {
    _mutex.reset(FLAGS_raft_use_polar, current_polar_idx());
    _first_log_index.store(1);
    _last_log_index.store(0);
    return 0;
}

void MemoryLogStorage::get_entry(const int64_t index, LogEntry** entry, Closure* done) {
    CHECK(entry);
    CHECK(done);
    rpc::ClosureGuard done_guard(done);
    std::unique_lock<Mutex> lck(_mutex);
    if (index < _first_log_index.load(base::memory_order_relaxed)
            || index > _last_log_index.load(base::memory_order_relaxed)) {
        done->status().set_error(ERANGE, "out of range index");
        return;
    }
    LogEntry* temp = _log_entry_data[index - _first_log_index.load(base::memory_order_relaxed)];
    temp->AddRef();
    CHECK(temp->id.index == index) << "get_entry entry index not equal. logentry index:"
            << temp->id.index << " required_index:" << index;
    lck.unlock();
    *entry = temp;
}

void MemoryLogStorage::last_log_id(LogId* log_id, Closure* done) {
    CHECK(log_id);
    CHECK(done);
    rpc::ClosureGuard done_guard(done);
    std::unique_lock<Mutex> lck(_mutex);
    if (_log_entry_data.empty()) {
        done->status().set_error(EINVAL, "empty storage");
        return;
    }
    LogEntry* temp = _log_entry_data.back();
    *log_id = temp->id;
    lck.unlock(); // unlock to avoid dead lock since done->Run() may call last_log_id()
}

int64_t MemoryLogStorage::get_term(const int64_t index) {
    std::unique_lock<Mutex> lck(_mutex);
    if (index < _first_log_index.load(base::memory_order_relaxed)
            || index > _last_log_index.load(base::memory_order_relaxed)) {
        return 0;
    }
    LogEntry* temp = _log_entry_data.at(index - _first_log_index.load(base::memory_order_relaxed));
    CHECK(temp->id.index == index) << "get_term entry index not equal. logentry index:"
            << temp->id.index << " required_index:" << index;
    int64_t ret = temp->id.term;
    lck.unlock();
    return ret;
}

void MemoryLogStorage::append_entry(const LogEntry* input_entry, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    std::unique_lock<Mutex> lck(_mutex);
    if (input_entry->id.index !=
            _last_log_index.load(base::memory_order_relaxed) + 1) {
        CHECK(false) << "input_entry index=" << input_entry->id.index
                  << " _last_log_index=" << _last_log_index
                  << " _first_log_index=" << _first_log_index;
        done->status().set_error(ERANGE, "gap between logs");
    }
    LogEntry* entry = new LogEntry;
    CHECK(entry);
    entry->AddRef();
    // copy entry since input_entry is const
    entry->type = input_entry->type; // log type
    entry->id = input_entry->id;
    entry->data.append(input_entry->data);
    entry->user_conf = input_entry->user_conf;  // used to save user conf
    entry->timestamp_us = input_entry->timestamp_us;
    if (input_entry->peers) {
        base::IOBuf conf;
        base::Status status = serialize_configuration_meta(input_entry, conf);
        CHECK(status.ok());
        status = parse_configuration_meta(conf, entry);
        CHECK(status.ok());
    }
    _log_entry_data.push_back(entry);
    _last_log_index.fetch_add(1, base::memory_order_relaxed);
    lck.unlock();
}

void MemoryLogStorage::append_entries(const std::vector<LogEntry*>& entries, 
                                     IOMetric* metric, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    if (entries.empty()) {
        return;
    }
    ClosureGroup* group = new ClosureGroup(entries.size(),
            dynamic_cast<Closure*>(done_guard.release()));
    for (size_t i = 0; i < entries.size(); i++) {
        LogEntry* entry = entries[i];
        append_entry(entry, group->sub_done(i));
    }
}

void MemoryLogStorage::truncate_prefix(const int64_t first_index_kept, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    std::deque<LogEntry*> popped;
    std::unique_lock<Mutex> lck(_mutex);
    while (!_log_entry_data.empty()) {
        LogEntry* entry = _log_entry_data.front();
        if (entry->id.index < first_index_kept) {
            popped.push_back(entry);
            _log_entry_data.pop_front();
        } else {
            break;
        }
    }
    _first_log_index.store(first_index_kept, base::memory_order_release);
    if (_first_log_index.load(base::memory_order_relaxed)
            > _last_log_index.load(base::memory_order_relaxed)) {
        _last_log_index.store(first_index_kept - 1, base::memory_order_release);
    }
    lck.unlock();

    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->Release();
    }
}

void MemoryLogStorage::truncate_suffix(const int64_t last_index_kept, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    std::deque<LogEntry*> popped;
    std::unique_lock<Mutex> lck(_mutex);
    while (!_log_entry_data.empty()) {
        LogEntry* entry = _log_entry_data.back();
        if (entry->id.index > last_index_kept) {
            popped.push_back(entry);
            _log_entry_data.pop_back();
        } else {
            break;
        }
    }
    _last_log_index.store(last_index_kept, base::memory_order_release);
    if (_first_log_index.load(base::memory_order_relaxed)
            > _last_log_index.load(base::memory_order_relaxed)) {
        _first_log_index.store(last_index_kept + 1, base::memory_order_release);
    }
    lck.unlock();

    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->Release();
    }
}

void MemoryLogStorage::reset(const int64_t next_log_index, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    if (next_log_index <= 0) {
        LOG(ERROR) << "Invalid next_log_index=" << next_log_index;
        done->status().set_error(EINVAL, "invalid next_log_index");
    }
    std::deque<LogEntry*> popped;
    std::unique_lock<Mutex> lck(_mutex);
    while (!_log_entry_data.empty()) {
        LogEntry* entry = _log_entry_data.back();
        popped.push_back(entry);
        _log_entry_data.pop_back();
    }
    _first_log_index.store(next_log_index, base::memory_order_relaxed);
    _last_log_index.store(next_log_index - 1, base::memory_order_relaxed);
    lck.unlock();

    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->Release();
    }
}

LogStorage* MemoryLogStorage::new_instance(const std::string& uri) const {
    return new MemoryLogStorage(uri);
}

base::Status MemoryLogStorage::gc_instance(const std::string& uri) const {
    return base::Status::OK();
}

} // namespace raft
