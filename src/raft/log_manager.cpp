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

#include "raft/log_manager.h"

#include <base/logging.h>                       // LOG
#include <base/object_pool.h>                   // base::get_object
#include <bthread/countdown_event.h>            // bthread::CountdownEvent
#include "raft/storage.h"                       // LogStorage
#include "raft/fsm_caller.h"                    // FSMCaller
#include "raft/rtc_helper.h"                    // RaftExecutionQueue

namespace raft {

DEFINE_int32(raft_leader_batch, 256, "max leader io batch");
RAFT_VALIDATE_GFLAG(raft_leader_batch, ::rpc::PositiveInteger);

DEFINE_int32(raft_node_cached_log_num, 16, "log manager cache entry num");
RAFT_VALIDATE_GFLAG(raft_node_cached_log_num, ::rpc::NonNegativeInteger);
DEFINE_bool(raft_report_error_on_truncate_before_applied_index, false,
            "report_error on truncate log before applied index instead of core dump");
RAFT_VALIDATE_GFLAG(raft_report_error_on_truncate_before_applied_index, ::rpc::PassValidate);

static BvarAdder<int64_t> g_read_entry_from_storage
            ("raft_read_entry_from_storage_count");
static bvar::PerSecond<BvarAdder<int64_t> > g_read_entry_from_storage_second
            ("raft_read_entry_from_storage_second", &g_read_entry_from_storage);

static BvarAdder<int64_t> g_read_term_from_storage
            ("raft_read_term_from_storage_count");
static bvar::PerSecond<BvarAdder<int64_t> > g_read_term_from_storage_second
            ("raft_read_term_from_storage_second", &g_read_term_from_storage);

static BvarLatencyRecorder g_storage_append_entries_latency(
                                        "raft_storage_append_entries");
static BvarLatencyRecorder g_normalized_append_entries_latency(
                                        "raft_storage_append_entries_normalized");

static BvarCounterRecorder g_storage_flush_batch_counter(
                                        "raft_storage_flush_batch_counter");

static BvarAdder<int64_t> g_storage_append_entries_concurrency
            ("raft_storage_append_entries_concurrency");

static BvarAdder<int64_t> g_logs_in_memory_num("raft_logs_in_memory_num");
static BvarAdder<int64_t> g_logs_in_memory_bytes("raft_logs_in_memory_bytes");
static BvarLatencyRecorder g_storage_control_entries_latency(
                                        "raft_storage_contol_entries");
static BvarCounterRecorder g_storage_flush_entries_counter(
                                        "raft_storage_flush_entries_counter");

LogManager::StableClosure::~StableClosure() {
    _timer.stop();
    g_storage_control_entries_latency << _timer.u_elapsed();
}

void LogManager::StableClosure::update_metric(IOMetric* m) {
    metric.open_segment_time_us = m->open_segment_time_us;
    metric.append_entry_time_us = m->append_entry_time_us;
    metric.sync_segment_time_us = m->sync_segment_time_us;
}

LogManagerOptions::LogManagerOptions()
    : log_storage(NULL)
    , configuration_manager(NULL)
    , fsm_caller(NULL)
{}

LogManager::LogManager()
    : _log_storage(NULL)
    , _config_manager(NULL)
    , _stopped(false)
    , _has_error(false)
    , _next_wait_id(0)
    , _first_log_index(0)
    , _last_log_index(0)
{
    CHECK_EQ(0, start_disk_thread());
}

int LogManager::init(const LogManagerOptions &options) {
    _mutex.reset(FLAGS_raft_use_polar, current_polar_idx());
    BAIDU_SCOPED_LOCK(_mutex);
    if (options.log_storage == NULL) {
        return EINVAL;
    }
    if (_wait_map.init(16) != 0) {
        PLOG(ERROR) << "Fail to init _wait_map";
        return ENOMEM;
    }
    _log_storage = options.log_storage;
    _config_manager = options.configuration_manager;
    int ret = _log_storage->init(_config_manager);
    if (ret != 0) {
        return ret;
    }
    _first_log_index = _log_storage->first_log_index();
    _last_log_index = _log_storage->last_log_index();
    _disk_id.index = _last_log_index;
    // Term will be 0 if the node has no logs, and we will correct the value
    // after snapshot load finish.
    _disk_id.term = _log_storage->get_term(_last_log_index);
    _fsm_caller = options.fsm_caller;
    return 0;
}

LogManager::~LogManager() {
    stop_disk_thread();
    int64_t total_bytes = 0;
    for (size_t i = 0; i < _logs_in_memory.size(); ++i) {
        total_bytes += _logs_in_memory[i]->data.size();
        _logs_in_memory[i]->Release();
    }
    g_logs_in_memory_num << -_logs_in_memory.size();
    g_logs_in_memory_bytes << -total_bytes;
    _logs_in_memory.clear();
}

int LogManager::start_disk_thread() {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
    queue_options.max_tasks_size = FLAGS_raft_leader_batch;
    return raft_execution_queue_start(&_disk_queue,
                                   &queue_options,
                                   disk_thread,
                                   this);
}

int LogManager::stop_disk_thread() {
    raft_execution_queue_stop(_disk_queue);
    return raft_execution_queue_join(_disk_queue);
}

void LogManager::clear_memory_logs(const LogId& id) {
    LogEntry* entries_to_clear[256];
    size_t nentries = 0;
    int64_t total_bytes = 0;
    do {
        nentries = 0;
        total_bytes = 0;
        {
            BAIDU_SCOPED_LOCK(_mutex);
            while (!_logs_in_memory.empty()
                    && nentries < ARRAY_SIZE(entries_to_clear)) {
                LogEntry* entry = _logs_in_memory.front();
                if (entry->id > id) {
                    break;
                }
                entries_to_clear[nentries++] = entry;
                total_bytes += entry->data.size();
                _logs_in_memory.pop_front();
            }
        }  // out of _mutex
        for (size_t i = 0; i < nentries; ++i) {
            entries_to_clear[i]->Release();
        }
        g_logs_in_memory_num << -nentries;
        g_logs_in_memory_bytes << -total_bytes;
    } while (nentries == ARRAY_SIZE(entries_to_clear));
}

int64_t LogManager::first_log_index() {
    BAIDU_SCOPED_LOCK(_mutex);
    return _first_log_index;
}

class LastLogIdClosure : public LogManager::StableClosure {
public:
    LastLogIdClosure(LogId* log_id, Closure* done) :
        _last_log_id(log_id), _done(done) {
    }
    LogId* last_log_id() { return _last_log_id; }
    void Run() {
        std::unique_ptr<LastLogIdClosure> guard(this);
        rpc::ClosureGuard done_guard(_done);
    }
    void set_last_log_id(const LogId& log_id) {
        CHECK(log_id.index == 0 || log_id.term != 0) << "Invalid log_id=" << log_id;
        *_last_log_id = log_id;
    }

private:
    LogId* _last_log_id;
    Closure* _done;
};

int64_t LogManager::last_log_index() {
    std::unique_lock<Mutex> lck(_mutex);
    return _last_log_index;
}

LogId LogManager::last_log_id() {
    std::unique_lock<Mutex> lck(_mutex);
    if (_last_log_index >= _first_log_index) {
        return LogId(_last_log_index, unsafe_get_term(_last_log_index));
    }
    return _last_snapshot_id;
}

void LogManager::last_log_id(LogId* log_id, Closure* done) {
    CHECK(log_id);
    CHECK(done);
    rpc::ClosureGuard done_guard(done);
    std::unique_lock<Mutex> lck(_mutex);
    if (_last_log_index == _last_snapshot_id.index) {
        *log_id = _last_snapshot_id;
        return;
    }
    done_guard.release();
    LastLogIdClosure* c = new LastLogIdClosure(log_id, done);
    CHECK(c);
    CHECK_EQ(0, raft_execution_queue_execute(_disk_queue, c));
}

class TruncatePrefixClosure : public LogManager::StableClosure {
public:
    explicit TruncatePrefixClosure(const int64_t first_index_kept, LogManager* log_manager)
        : _first_index_kept(first_index_kept), _log_manager(log_manager)
    {}
    void Run() {
        std::unique_ptr<TruncatePrefixClosure> guard(this);
        if (!status().ok()) {
            _log_manager->report_error(status().error_code(), status().error_cstr());
        }
    }
    int64_t first_index_kept() const { return _first_index_kept; }
private:
    int64_t _first_index_kept;
    LogManager* _log_manager;
};

class TruncateSuffixClosure : public LogManager::StableClosure {
public:
    TruncateSuffixClosure(int64_t last_index_kept, int64_t last_term_kept,
            LogManager* log_manager)
        : _last_index_kept(last_index_kept)
        , _last_term_kept(last_term_kept), _log_manager(log_manager)
    {}
    void Run() {
        std::unique_ptr<TruncateSuffixClosure> guard(this);
        if (!status().ok()) {
            _log_manager->report_error(status().error_code(), status().error_cstr());
            return;
        }
        _log_manager->set_disk_id(LogId(last_index_kept(), last_term_kept()));
    }
    int64_t last_index_kept() const { return _last_index_kept; }
    int64_t last_term_kept() const { return _last_term_kept; }
private:
    int64_t _last_index_kept;
    int64_t _last_term_kept;
    LogManager* _log_manager;
};

class ResetClosure : public LogManager::StableClosure {
public:
    explicit ResetClosure(int64_t next_log_index, LogManager* log_manager)
        : _next_log_index(next_log_index), _log_manager(log_manager) {}
    void Run() {
        std::unique_ptr<ResetClosure> guard(this);
        if (!status().ok()) {
            _log_manager->report_error(status().error_code(), status().error_cstr());
        }
    }
    int64_t next_log_index() const { return _next_log_index; }
private:
    int64_t _next_log_index;
    LogManager* _log_manager;
};

int LogManager::truncate_prefix(const int64_t first_index_kept,
                                std::unique_lock<Mutex>& lck) {
    std::deque<LogEntry*> saved_logs_in_memory;
    // As the duration between two snapshot (which leads to truncate_prefix at
    // last) is likely to be a long period, _logs_in_memory is likely to
    // contain a large amount of logs to release, which holds the mutex so that
    // all the replicator/application are blocked.
    // FIXME(chenzhangyi01): to resolve this issue, we have to build a data
    // structure which is able to pop_front/pop_back N elements into another
    // container in O(1) time, one solution is a segmented double-linked list
    // along with a bounded queue as the indexer, of which the payoff is that
    // _logs_in_memory has to be bounded.
    while (!_logs_in_memory.empty()) {
        LogEntry* entry = _logs_in_memory.front();
        if (entry->id.index < first_index_kept) {
            saved_logs_in_memory.push_back(entry);
            _logs_in_memory.pop_front();
        } else {
            break;
        }
    }
    CHECK_GE(first_index_kept, _first_log_index);
    _first_log_index = first_index_kept;
    if (first_index_kept > _last_log_index) {
        // The entrie log is dropped
        _last_log_index = first_index_kept - 1;
    }
    _config_manager->truncate_prefix(first_index_kept);
    TruncatePrefixClosure* c = new TruncatePrefixClosure(first_index_kept, this);
    const int rc = raft_execution_queue_execute(_disk_queue, c);
    lck.unlock();
    int64_t total_bytes = 0;
    for (size_t i = 0; i < saved_logs_in_memory.size(); ++i) {
        total_bytes += saved_logs_in_memory[i]->data.size();
        saved_logs_in_memory[i]->Release();
    }
    g_logs_in_memory_num << -saved_logs_in_memory.size();
    g_logs_in_memory_bytes << -total_bytes;
    return rc;
}

int LogManager::reset(const int64_t next_log_index,
                      std::unique_lock<Mutex>& lck) {
    CHECK(lck.owns_lock());
    std::deque<LogEntry*> saved_logs_in_memory;
    saved_logs_in_memory.swap(_logs_in_memory);
    _first_log_index = next_log_index;
    _last_log_index = next_log_index - 1;
    _config_manager->truncate_prefix(_first_log_index);
    _config_manager->truncate_suffix(_last_log_index);
    ResetClosure* c = new ResetClosure(next_log_index, this);
    const int ret = raft_execution_queue_execute(_disk_queue, c);
    lck.unlock();
    CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();
    int64_t total_bytes = 0;
    for (size_t i = 0; i < saved_logs_in_memory.size(); ++i) {
        total_bytes += saved_logs_in_memory[i]->data.size();
        saved_logs_in_memory[i]->Release();
    }
    g_logs_in_memory_num << -saved_logs_in_memory.size();
    g_logs_in_memory_bytes << -total_bytes;
    return 0;
}

int LogManager::unsafe_truncate_suffix(const int64_t last_index_kept) {

    if (last_index_kept < _applied_id.index) {
        LOG(WARNING) << "Node " << _fsm_caller->node_id()
                     << " can't truncate logs before _applied_id=" <<_applied_id.index
                     << ", last_log_kept=" << last_index_kept;
        if (FLAGS_raft_report_error_on_truncate_before_applied_index) {
            unsafe_report_error(EIO, "can not truncate before applied index");
            return -1;
        }
        CHECK_GT(last_index_kept, _applied_id.index) << "Node " << _fsm_caller->node_id()
            << " can not truncate before applied index";
        return 0;
    }

    int64_t total_bytes = 0;
    ssize_t count = 0;
    while (!_logs_in_memory.empty()) {
        LogEntry* entry = _logs_in_memory.back();
        if (entry->id.index > last_index_kept) {
            count ++;
            total_bytes += entry->data.size();
            entry->Release();
            _logs_in_memory.pop_back();
        } else {
            break;
        }
    }
    g_logs_in_memory_num << -count;
    g_logs_in_memory_bytes << -total_bytes;
    _last_log_index = last_index_kept;
    const int64_t last_term_kept = unsafe_get_term(last_index_kept);
    CHECK(last_index_kept == 0 || last_term_kept != 0)
        << "Node " << _fsm_caller->node_id()
        << " last_index_kept=" << last_index_kept;
    _config_manager->truncate_suffix(last_index_kept);
    TruncateSuffixClosure* tsc = new
            TruncateSuffixClosure(last_index_kept, last_term_kept, this);
    CHECK_EQ(0, raft_execution_queue_execute(_disk_queue, tsc));
    return 0;
}

int LogManager::check_and_resolve_conflict(
            std::vector<LogEntry*> *entries, StableClosure* done) {
    AsyncClosureGuard done_guard(done);
    if (entries->front()->id.index == 0) {
        // Node is currently the leader and |entries| are from the user who 
        // don't know the correct indexes the logs should assign to. So we have
        // to assign indexes to the appending entries
        for (size_t i = 0; i < entries->size(); ++i) {
            (*entries)[i]->id.index = ++_last_log_index;
        }
        done_guard.release();
        return 0;
    } else {
        // Node is currently a follower and |entries| are from the leader. We 
        // should check and resolve the confliction between the local logs and
        // |entries|
        if (entries->front()->id.index > _last_log_index + 1) {
            done->status().set_error(EINVAL, "There's gap between first_index=%ld "
                                     "and last_log_index=%ld", 
                                     entries->front()->id.index, _last_log_index);
            return -1;
        }
        const int64_t applied_index = _applied_id.index;
        if (entries->back()->id.index <= applied_index) {
            LOG(WARNING) << "Node " << _fsm_caller->node_id()
                << " received entries of which the last_log="
                << entries->back()->id.index
                << " is not greater than _applied_index=" << applied_index
                << ", return immediately with nothing changed";
            return 1;
        }

        if (entries->front()->id.index == _last_log_index + 1) {
            // Fast path
            _last_log_index = entries->back()->id.index;
        } else {
            // Appending entries overlap the local ones. We should find if there
            // is a conflicting index from which we should truncate the local
            // ones.
            size_t conflicting_index = 0;
            for (; conflicting_index < entries->size(); ++conflicting_index) {
                if (unsafe_get_term((*entries)[conflicting_index]->id.index)
                        != (*entries)[conflicting_index]->id.term) {
                    break;
                }
            }
            if (conflicting_index != entries->size()) {
                if ((*entries)[conflicting_index]->id.index <= _last_log_index) {
                    // Truncate all the conflicting entries to make local logs
                    // consensus with the leader.
                    if (0 != unsafe_truncate_suffix((*entries)[conflicting_index]->id.index - 1)) {
                        done->status().set_error(EIO, "unsafe_truncate_suffix failed");
                        return -1;
                    }
                }
                _last_log_index = entries->back()->id.index;
            }  // else this is a duplicated AppendEntriesRequest, we have 
               // nothing to do besides releasing all the entries

            // Release all the entries before the conflicting_index and the rest
            // would be append to _logs_in_memory and _log_storage after this
            // function returns
            for (size_t i = 0; i < conflicting_index; ++i) {
                (*entries)[i]->Release();
            }
            entries->erase(entries->begin(),
                           entries->begin() + conflicting_index);
        }
        done_guard.release();
        return 0;
    }
    CHECK(false) << "Node " << _fsm_caller->node_id() << " can't reach here";
    done->status().set_error(EIO, "Impossible");
    return -1;
}

void LogManager::append_entries(
            std::vector<LogEntry*> *entries, StableClosure* done) {
    CHECK(done);
    if (_has_error.load(base::memory_order_relaxed)) {
        for (size_t i = 0; i < entries->size(); ++i) {
            (*entries)[i]->Release();
        }
        entries->clear();
        done->status().set_error(EIO, "Corrupted LogStorage");
        return raft_run_closure_background(done);
    }
    std::unique_lock<Mutex> lck(_mutex);
    if (!entries->empty() && check_and_resolve_conflict(entries, done) != 0) {
        lck.unlock();
        // release entries
        for (size_t i = 0; i < entries->size(); ++i) {
            (*entries)[i]->Release();
        }
        entries->clear();
        return;
    }

    int64_t total_bytes = 0;
    for (size_t i = 0; i < entries->size(); ++i) {
        // Add ref for disk_thread
        (*entries)[i]->AddRef();
        if ((*entries)[i]->type == ENTRY_TYPE_CONFIGURATION) {
            ConfigurationEntry conf_entry(*((*entries)[i]));
            _config_manager->add(conf_entry);
            RAFT_VLOG << "Node " << _fsm_caller->node_id()
                      << " appends a new conf, log_index=" << conf_entry.id.index
                      << " user_conf: " << conf_entry.user_conf
                      << " conf: " << conf_entry.conf
                      << " old_conf: " << conf_entry.old_conf;
        }
        total_bytes += (*entries)[i]->data.size();
    }

    if (!entries->empty()) {
        done->_first_log_index = entries->front()->id.index;
        _logs_in_memory.insert(_logs_in_memory.end(), entries->begin(), entries->end());
        g_logs_in_memory_num << entries->size();
        g_logs_in_memory_bytes << total_bytes;
    }
    done->_entries.swap(*entries);

    if (FLAGS_raft_use_polar) {
        // rtc dont care about lock
        wakeup_all_waiter(lck);
        int ret = raft_execution_queue_execute(_disk_queue, done);
        CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();
    } else {
        // wakeup_all_waiter will unlock, so persist_log must be done before wakeup
        int ret = raft_execution_queue_execute(_disk_queue, done);
        CHECK_EQ(0, ret) << "execq execute failed, ret: " << ret << " err: " << berror();
        wakeup_all_waiter(lck);
    }
}

void LogManager::append_to_storage(const std::vector<LogEntry*>& to_append,
        IOMetric* metric, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    CHECK(metric);
    CHECK(done);
    if (_has_error.load(base::memory_order_relaxed)) {
        done->status().set_error(EIO, "append entry error");
        return;
    }
    _log_storage->append_entries(to_append, metric,
            dynamic_cast<Closure*>(done_guard.release()));
}

DEFINE_int32(raft_max_append_buffer_size, 256 * 1024, 
             "Flush buffer to LogStorage if the buffer size reaches the limit");

class AppendBatcher : public Closure {
public:
    AppendBatcher(LogManager* lm)
        : _cap(256)
        , _size(0)
        , _buffer_size(0)
        , _lm(lm)
        , _flush_start_us(-1) {
        _to_append.reserve(1024);
    }

    void flush() {
        if (_size > 0) {
            _flush_start_us = base::cpuwide_time_us();
            g_storage_append_entries_concurrency << 1;
            _lm->append_to_storage(_to_append, &_metric, this);
            return;
        }
        Run();
    }

    void Run() override {
        std::unique_ptr<AppendBatcher> guard(this);
        if (!status().ok()) {
            _lm->report_error(status().error_code(), status().error_cstr());
        }
        if (_size <= 0) {
            return;
        }
        auto elapsed_us = base::cpuwide_time_us() - _flush_start_us;
        g_storage_append_entries_concurrency << -1;
        g_storage_append_entries_latency << elapsed_us;
        g_storage_flush_batch_counter << _size;
        g_storage_flush_entries_counter << _to_append.size();
        size_t written_size = 0;
        for (const auto& entry : _to_append) {
            written_size += entry->data.size();
        }
        if (written_size) {
            g_normalized_append_entries_latency << elapsed_us * 1024 / written_size;
        }
        for (size_t i = 0; i < _size; ++i) {
            _storage[i]->_entries.clear();
            if (_lm->_has_error.load(base::memory_order_relaxed)) {
                _storage[i]->status().set_error(EIO, "Corrupted LogStorage");
            }
            if (_lm->_stopped.load(base::memory_order_relaxed)) {
                _storage[i]->status().set_error(EIO, "DiskManager stopped");
            }
            _storage[i]->update_metric(&_metric);
            _storage[i]->Run();
        }
        if (!_lm->_has_error.load(base::memory_order_relaxed) &&
                                !_to_append.empty()) {
            _lm->set_disk_id(_to_append.back()->id);
        }
        for (auto& entry : _to_append) {
            entry->Release();
        }
        _to_append.clear();
    }

    void append(LogManager::StableClosure* done) {
        CHECK_LT(_size, _cap);
        _storage[_size++] = done;
        _to_append.insert(_to_append.end(),
                         done->_entries.begin(), done->_entries.end());
        for (size_t i = 0; i < done->_entries.size(); ++i) {
            _buffer_size += done->_entries[i]->data.length();
        }
    }

    bool full() {
        if (_size == _cap ||
                _buffer_size >= (size_t)FLAGS_raft_max_append_buffer_size) {
            return true;
        }
        return false;
    }

private:
    // FXIME(chenzhangyi01): it's buggy
    LogManager::StableClosure* _storage[256];
    size_t _cap;
    size_t _size;
    size_t _buffer_size;
    std::vector<LogEntry*> _to_append;
    scoped_refptr<LogManager> _lm;
    IOMetric _metric;
    int64_t _flush_start_us;
};

int LogManager::disk_thread(void* meta,
                            bthread::TaskIterator<StableClosure*>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }

    LogManager* log_manager = static_cast<LogManager*>(meta);
    AppendBatcher* ab = new AppendBatcher(log_manager);

    for (; iter; ++iter) {
                // ^^^ Must iterate to the end to release to corresponding
                //     even if some error has ocurred
        StableClosure* done = *iter;
        done->metric.bthread_queue_time_us = base::cpuwide_time_us() - 
                                            done->metric.start_time_us;
        if (!done->_entries.empty()) {
            ab->append(done);
            if (ab->full()) {
                ab->flush();
                ab = new AppendBatcher(log_manager);
            }
        } else {
            // For empty entries, we should wait util all pending io end 
            // Since there is truncate operations

            ab->flush();
            ab = new AppendBatcher(log_manager);
            // TODO: asyc this
            do {
                rpc::ClosureGuard done_guard(done);
                LastLogIdClosure* llic =
                        dynamic_cast<LastLogIdClosure*>(done);
                if (llic) {
                    log_manager->_log_storage->last_log_id(
                            llic->last_log_id(),
                            (Closure*)done_guard.release());
                    break;
                }
                TruncatePrefixClosure* tpc =
                        dynamic_cast<TruncatePrefixClosure*>(done);
                if (tpc) {
                    RAFT_VLOG << "Node " << log_manager->_fsm_caller->node_id()
                        << " truncating storage to first_index_kept="
                        << tpc->first_index_kept();
                    log_manager->_log_storage->truncate_prefix(
                                    tpc->first_index_kept(),
                                    (Closure*)done_guard.release());
                    break;
                }
                TruncateSuffixClosure* tsc =
                        dynamic_cast<TruncateSuffixClosure*>(done);
                if (tsc) {
                    LOG(WARNING) << "Node " << log_manager->_fsm_caller->node_id()
                        << " truncating storage to last_index_kept="
                        << tsc->last_index_kept();
                    log_manager->_log_storage->truncate_suffix(
                                    tsc->last_index_kept(),
                                    (Closure*)done_guard.release());
                    break;
                }
                ResetClosure* rc = dynamic_cast<ResetClosure*>(done);
                if (rc) {
                    LOG(INFO) << "Node " << log_manager->_fsm_caller->node_id()
                        << " resetting storage to next_log_index="
                        << rc->next_log_index();
                    log_manager->_log_storage->reset(rc->next_log_index(),
                                    (Closure*)done_guard.release());
                    break;
                }
            } while (0);
        }
    }
    CHECK(!iter) << "Must iterate to the end";
    CHECK(ab);
    ab->flush();
    return 0;
}

void LogManager::set_snapshot(const SnapshotMeta* meta, const int64_t first_snapshot_index) {
    LOG(INFO) << "Node " << _fsm_caller->node_id()
        << " set snapshot last_included_index="
        << meta->last_included_index()
        << " last_included_term=" <<  meta->last_included_term()
        << " first_snapshot_index=" << first_snapshot_index;
    std::unique_lock<Mutex> lck(_mutex);
    if (meta->last_included_index() <= _last_snapshot_id.index) {
        return;
    }
    Configuration conf;
    for (int i = 0; i < meta->peers_size(); ++i) {
        conf.add_peer(meta->peers(i));
    }
    Configuration old_conf;
    for (int i = 0; i < meta->old_peers_size(); ++i) {
        old_conf.add_peer(meta->old_peers(i));
    }
    ConfigurationEntry entry;
    entry.id = LogId(meta->last_included_index(), meta->last_included_term());
    entry.conf = conf;
    entry.old_conf = old_conf;
    if (meta->has_user_conf()) {
        entry.user_conf = meta->user_conf();
    }
    _config_manager->set_snapshot(entry);
    int64_t term = unsafe_get_term(meta->last_included_index());

    const LogId last_but_one_snapshot_id = _last_snapshot_id;
    _last_snapshot_id.index = meta->last_included_index();
    _last_snapshot_id.term = meta->last_included_term();
    if (_last_snapshot_id > _applied_id) {
        _applied_id = _last_snapshot_id;
    }
    // NOTICE: not to update disk_id here as we are not sure if this node really
    // has these logs on disk storage. Just leave disk_id as it was, which can keep
    // these logs in memory all the time until they are flushed to disk. By this 
    // way we can avoid some corner cases which failed to get logs.

    if (term == 0) {
        // last_included_index is larger than last_index
        // FIXME: what if last_included_index is less than first_index?
        _virtual_first_log_id = _last_snapshot_id;
        truncate_prefix(meta->last_included_index() + 1, lck);
        return;
    } else if (term == meta->last_included_term()) {
        // Truncating log to the index of the last snapshot.
        // We don't truncate log before the latest snapshot immediately since
        // some log around last_snapshot_index is probably needed by some
        // followers
        if (last_but_one_snapshot_id.index > 0) {
            // We have last snapshot index
            _virtual_first_log_id = last_but_one_snapshot_id;
            int64_t truncate_index = std::min(first_snapshot_index + 1, 
                                              last_but_one_snapshot_id.index + 1);
            truncate_prefix(std::max(truncate_index, _first_log_index), lck);
        }
        return;
    } else {
        // TODO: check the result of reset.
        _virtual_first_log_id = _last_snapshot_id;
        reset(meta->last_included_index() + 1, lck);
        return;
    }
    CHECK(false) << "Cannot reach here";
}

void LogManager::clear_bufferred_logs() {
    std::unique_lock<Mutex> lck(_mutex);
    if (_last_snapshot_id.index != 0) {
        _virtual_first_log_id = _last_snapshot_id;
        truncate_prefix(_last_snapshot_id.index + 1, lck);
    }
}

LogEntry* LogManager::get_entry_from_memory(const int64_t index) {
    LogEntry* entry = NULL;
    if (!_logs_in_memory.empty()) {
        int64_t first_index = _logs_in_memory.front()->id.index;
        int64_t last_index = _logs_in_memory.back()->id.index;
        CHECK_EQ(last_index - first_index + 1, static_cast<int64_t>(_logs_in_memory.size()));
        if (index >= first_index && index <= last_index) {
            entry = _logs_in_memory[index - first_index];
            CHECK_EQ(entry->id.index, index);
        }
    }
    return entry;
}

int LogManager::get_entries_from_memory(const int64_t& first_index, const int& num,
                                        std::deque<LogEntry *> *log_entries) {
    CHECK(log_entries);
    if (_logs_in_memory.empty()) {
        return 0;
    }
    int64_t first_mem_index = _logs_in_memory.front()->id.index;
    int64_t last_mem_index = _logs_in_memory.back()->id.index;
    CHECK_EQ(last_mem_index - first_mem_index + 1, static_cast<int64_t>(_logs_in_memory.size()));

    int64_t start_index = std::max(first_mem_index, first_index);
    int64_t end_index = std::min(last_mem_index, first_index + num - 1);
    if (start_index > end_index) {
        return 0;
    }

    int64_t start_pos = start_index - first_mem_index;
    int count = 0;
    for (std::deque<LogEntry*>::iterator it = _logs_in_memory.begin() + start_pos; 
            it != _logs_in_memory.end() && count <= end_index - start_index; ++it) {
        LogEntry* entry = *it;
        entry->AddRef();
        log_entries->push_back(entry);
        ++count;
    }

    return count;
}

int64_t LogManager::unsafe_get_term(const int64_t index) {
    if (index == 0) {
        return 0;
    }
    // check virtual first log
    if (index == _virtual_first_log_id.index) {
        return _virtual_first_log_id.term;
    }
    // check last_snapshot_id
    if (index == _last_snapshot_id.index) {
        return _last_snapshot_id.term;
    }
    // out of range, direct return NULL
    // check this after check last_snapshot_id, because it is likely that
    // last_snapshot_id < first_log_index
    if (index > _last_log_index || index < _first_log_index) {
        return 0;
    }

    LogEntry* entry = get_entry_from_memory(index);
    if (entry) {
        return entry->id.term;
    }
    g_read_term_from_storage << 1;
    return _log_storage->get_term(index);
}

int64_t LogManager::get_term(const int64_t index) {
    if (index == 0) {
        return 0;
    }
    std::unique_lock<Mutex> lck(_mutex);
    // check virtual first log
    if (index == _virtual_first_log_id.index) {
        return _virtual_first_log_id.term;
    }
    // check last_snapshot_id
    if (index == _last_snapshot_id.index) {
        return _last_snapshot_id.term;
    }
    // out of range, direct return NULL
    // check this after check last_snapshot_id, because it is likely that
    // last_snapshot_id < first_log_index
    if (index > _last_log_index || index < _first_log_index) {
        return 0;
    }

    LogEntry* entry = get_entry_from_memory(index);
    if (entry) {
        return entry->id.term;
    }
    lck.unlock();
    g_read_term_from_storage << 1;
    return _log_storage->get_term(index);
}

void LogManager::get_entry(const int64_t index, LogEntry** entry, Closure* done) {
    CHECK(entry);
    CHECK(done);
    rpc::ClosureGuard done_guard(done);
    std::unique_lock<Mutex> lck(_mutex);
    // out of range, direct return NULL
    if (index > _last_log_index || index < _first_log_index) {
        done->status().set_error(ERANGE, "out of range index=%ld", index);
        return;
    }

    LogEntry* temp_entry = get_entry_from_memory(index);
    if (temp_entry) {
        temp_entry->AddRef();
        *entry = temp_entry;
        return;
    }
    lck.unlock();
    g_read_entry_from_storage << 1;
    _log_storage->get_entry(index, entry,
        dynamic_cast<Closure*>(done_guard.release()));
}

int LogManager::get_entries(const int64_t& first_index, const int& num, const size_t& max_size,
                            std::vector<LogEntry* >* entries) {
    std::unique_lock<Mutex> lck(_mutex);

    // out of range, direct return NULL
    if (first_index > _last_log_index || first_index < _first_log_index) {
        RAFT_VLOG << "Node " << _fsm_caller->node_id()
            << " fail to get_entries, index " << first_index
            << " out of range [" << _first_log_index << ", " << _last_log_index << "]";
        return 0;
    }

    int64_t last_index = std::min(first_index + num - 1, _last_log_index);

    std::deque<LogEntry*> mem_entries;
    int ret_size = get_entries_from_memory(first_index, num, &mem_entries);
    lck.unlock();

    int rc = 0;
    size_t entries_length = 0;
    int64_t first_mem_index = ret_size > 0 ? mem_entries[0]->id.index : first_index;
    int64_t left_size = first_mem_index - first_index;
    CHECK(!FLAGS_raft_use_polar);
    SynchronizedClosure done;
    std::vector<LogEntry*> left_entries;
    ClosureGroup* cg = nullptr;
    if (left_size > 0) {
        cg = new ClosureGroup(left_size, &done);
        left_entries.resize(left_size);
        for (int64_t index = first_index; index < first_mem_index; ++index) {
            g_read_entry_from_storage << 1;
            int64_t i = index - first_index;
            _log_storage->get_entry(index, &left_entries[i],cg->sub_done(i));
        }
        done.wait();
        if (!done.status().ok()) {
            rc = -1;
        }
    }
    int64_t index = 0;
    for (; index < left_size; index ++) {
        if (rc != 0 || entries_length >= max_size) {
            break;
        }
        entries->push_back(left_entries[index]);
        entries_length += left_entries[index]->data.length();
    }
    for (; index < left_size; index ++) {
        left_entries[index]->Release();
    }

    while (!mem_entries.empty()) {
        if (rc != 0 || entries_length >= max_size) {
            break;
        }
        LogEntry* entry = mem_entries.front();
        entries->push_back(entry);
        entries_length += entry->data.length();

        mem_entries.pop_front();
    }

    left_size = last_index - (first_mem_index + ret_size) + 1;
    if (left_size > 0) {
        done.reset();
        left_entries.resize(left_size);
        CHECK(!FLAGS_raft_use_polar);
        cg = new ClosureGroup(left_size, &done);
        for (int64_t index = first_mem_index + ret_size; index <= last_index; ++index) {
            int64_t i = index - (first_mem_index + ret_size);
            g_read_entry_from_storage << 1;
            _log_storage->get_entry(index, &left_entries[i], cg->sub_done(i));
        }
        done.wait();
        if (!done.status().ok()) {
            rc = -1;
        }
    }
    index = 0;
    for (; index < left_size; index ++) {
        if (rc != 0 || entries_length >= max_size) {
            break;
        }
        entries->push_back(left_entries[index]);
        entries_length += left_entries[index]->data.length();
    }
    for (; index < left_size; index ++) {
        left_entries[index]->Release();
    }

    while (!mem_entries.empty()) {
        LogEntry* entry = mem_entries.front();
        entry->Release();
        mem_entries.pop_front();
    }
    if (rc != 0) {
        for (size_t i = 0; i < entries->size(); ++i) {
            (*entries)[i]->Release();
        }
        entries->clear();
    } else {
        rc = entries->size();
    }
    RAFT_VLOG << "Node " << _fsm_caller->node_id()
        << " get_entries, index:" << first_index  << ", num:" << num
        << ", range [" << _first_log_index << ", " << _last_log_index << "]";

    return rc;
}

void LogManager::get_configuration(const int64_t index, ConfigurationEntry* conf) {
    BAIDU_SCOPED_LOCK(_mutex);
    return _config_manager->get(index, conf);
}

bool LogManager::check_and_set_configuration(ConfigurationEntry* current) {
    if (current == NULL) {
        CHECK(false) << "current should not be NULL";
        return false;
    }
    BAIDU_SCOPED_LOCK(_mutex);

    const ConfigurationEntry& last_conf = _config_manager->last_configuration();
    if (current->id != last_conf.id) {
        *current = last_conf;
        return true;
    }
    return false;
}

void LogManager::set_disk_id(const LogId& disk_id) {
    std::unique_lock<Mutex> lck(_mutex);  // Race with set_applied_id
    if (disk_id < _disk_id) {
        return;
    }
    _disk_id = disk_id;
    LogId clear_id = std::min(_disk_id, _applied_id);
    int32_t index = _logs_in_memory.size() - FLAGS_raft_node_cached_log_num - 1;
    if (index >= 0) {
        LogId min_cache_id = _logs_in_memory[index]->id;
        if (min_cache_id > _disk_id) {
            clear_id = _disk_id; // not max(_disk_id, _applied_id) since replicator may can not get entry
            // <-|disk_id|min_cache_id|applied_id|-> or <-|disk_id|applied_id|min_cache_id|->
        } else {
            clear_id = std::max(min_cache_id, clear_id); // <-|min_cache_id|disk_id|applied_id|-> or <-|min_cache_id|applied_id|disk_id|->
                        // we must keep |disk_id--->>>| in memory to avoid get failure
        }
    }
    lck.unlock();
    return clear_memory_logs(clear_id);
}

void LogManager::set_applied_id(const LogId& applied_id) {
    std::unique_lock<Mutex> lck(_mutex);  // Race with set_disk_id
    if (applied_id < _applied_id) {
        return;
    }
    _applied_id = applied_id;
    LogId clear_id = std::min(_disk_id, _applied_id);
    lck.unlock();
    return clear_memory_logs(clear_id);
}

void LogManager::reset_applied_id() {
    std::unique_lock<Mutex> lck(_mutex);  // Race with set_disk_id
    LogId clear_id = std::min(_disk_id, _applied_id);
    _applied_id = LogId(0, 0);
    lck.unlock();
    return clear_memory_logs(clear_id);
}

void LogManager::clear_logs(LogId *id) {
    LogId clear_id = std::min(_disk_id, *id);
    clear_memory_logs(clear_id);
}

void LogManager::shutdown() {
    std::unique_lock<Mutex> lck(_mutex);
    _stopped = true;
    wakeup_all_waiter(lck);
}

void* LogManager::run_on_new_log(void *arg) {
    WaitMeta* wm = (WaitMeta*)arg;
    wm->on_new_log(wm->arg, wm->error_code);
    base::return_object(wm);
    return NULL;
}

LogManager::WaitId LogManager::wait(
        int64_t expected_last_log_index, 
        int (*on_new_log)(void *arg, int error_code), void *arg) {
    WaitMeta* wm = base::get_object<WaitMeta>();
    if (BAIDU_UNLIKELY(wm == NULL)) {
        PLOG(FATAL) << "Fail to new WaitMeta";
        abort();
        return -1;
    }
    wm->on_new_log = on_new_log;
    wm->arg = arg;
    wm->error_code = 0;
    return notify_on_new_log(expected_last_log_index, wm);
}

LogManager::WaitId LogManager::notify_on_new_log(
        int64_t expected_last_log_index, WaitMeta* wm) {
    std::unique_lock<Mutex> lck(_mutex);
    if (expected_last_log_index != _last_log_index || _stopped) {
        wm->error_code = _stopped ? ESTOP : 0;
        lck.unlock();
        bthread_t tid;
        if (raft_start_urgent(&tid, NULL, run_on_new_log, wm) != 0) {
            PLOG(ERROR) << "Fail to start bthread";
            run_on_new_log(wm);
        }
        return 0;  // Not pushed into _wait_map
    }
    if (_next_wait_id == 0) {  // skip 0
        ++_next_wait_id;
    }
    const WaitId wait_id = _next_wait_id++;
    _wait_map[wait_id] = wm;
    return wait_id;
}

int LogManager::remove_waiter(WaitId id) {
    WaitMeta* wm = NULL;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        WaitMeta** pwm = _wait_map.seek(id);
        if (pwm) {
            wm = *pwm;
            _wait_map.erase(id);
        }
    }
    if (wm) {
        base::return_object(wm);
    }
    return wm ? 0 : -1;
}

void LogManager::wakeup_all_waiter(std::unique_lock<Mutex>& lck) {
    if (_wait_map.empty()) {
        return;
    }
    WaitMeta* wm[_wait_map.size()];
    size_t nwm = 0;
    for (base::FlatMap<int64_t, WaitMeta*>::const_iterator
            iter = _wait_map.begin(); iter != _wait_map.end(); ++iter) {
        wm[nwm++] = iter->second;
    }
    _wait_map.clear();
    const int error_code = _stopped ? ESTOP : 0;
    lck.unlock();
    for (size_t i = 0; i < nwm; ++i) {
        wm[i]->error_code = error_code;
        bthread_t tid;
        bthread_attr_t attr = BTHREAD_ATTR_NORMAL | BTHREAD_NOSIGNAL;
        if (raft_start_urgent(
                    &tid, &attr,
                    run_on_new_log, wm[i]) != 0) {
            PLOG(ERROR) << "Fail to start bthread";
            run_on_new_log(wm[i]);
        }
    }
    raft_thread_flush();
}

void LogManager::describe(std::ostream& os, bool use_html) {
    const char* newline = use_html ? "<br>" : "\n";
    int64_t first_index = _log_storage->first_log_index();
    int64_t last_index = _log_storage->last_log_index();
    os << "storage: [" << first_index << ", " << last_index << ']' << newline;
    os << "disk_index: " << _disk_id.index << newline;
    os << "known_applied_index: " << _applied_id.index << newline;
    os << "last_log_id: " << last_log_id() << newline;
}

void LogManager::get_status(LogManagerStatus* status) {
    if (!status) {
        return;
    }
    std::unique_lock<Mutex> lck(_mutex);
    status->first_index = _log_storage->first_log_index();
    status->last_index = _log_storage->last_log_index();
    status->disk_index = _disk_id.index;
    status->known_applied_index = _applied_id.index;
}

void LogManager::report_error(int error_code, const char* fmt, ...) {
    _has_error.store(true, base::memory_order_relaxed);
    std::unique_lock<Mutex> lck(_mutex);
    RAFT_RETURN_IF(_stopped);
    unsafe_report_error(error_code, fmt);
}

void LogManager::unsafe_report_error(int error_code, const char* fmt, ...) {
    _has_error.store(true, base::memory_order_relaxed);
    va_list ap;
    va_start(ap, fmt);
    Error e;
    e.set_type(ERROR_TYPE_LOG);
    e.status().set_error(error_code, fmt, ap);
    va_end(ap);
    _fsm_caller->on_error(e);
}

base::Status LogManager::check_consistency() {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK_GT(_first_log_index, 0);
    CHECK_GE(_last_log_index, 0);
    if (_last_snapshot_id == LogId(0, 0)) {
        if (_first_log_index == 1) {
            return base::Status::OK();
        }
        return base::Status(EIO, "Missing logs in (0, %ld)", _first_log_index);
    } else {
        if (_last_snapshot_id.index >= _first_log_index - 1
                && _last_snapshot_id.index <= _last_log_index) {
            return base::Status::OK();
        }
        return base::Status(EIO, "There's a gap between snapshot={%ld, %ld}"
                                 " and log=[%ld, %ld] ",
                            _last_snapshot_id.index, _last_snapshot_id.term,
                            _first_log_index, _last_log_index);
    }
    CHECK(false) << "Can't reach here";
    return base::Status(-1, "Impossible condition");
}

base::Status LogManager::check_snapshot_consistency(const LogId &last_snapshot_id) {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK_GT(_first_log_index, 0);
    CHECK_GE(_last_log_index, 0);
    if (last_snapshot_id == LogId(0, 0)) {
        if (_first_log_index == 1) {
            return base::Status::OK();
        }
        return base::Status(EIO, "Missing logs in (0, %ld)", _first_log_index);
    } else {
        if (last_snapshot_id.index >= _first_log_index - 1
                && last_snapshot_id.index <= _last_log_index) {
            return base::Status::OK();
        }
        return base::Status(EIO, "There's a gap between snapshot={%ld, %ld}"
                                 " and log=[%ld, %ld] ",
                            last_snapshot_id.index, last_snapshot_id.term,
                            _first_log_index, _last_log_index);
    }
    CHECK(false) << "Can't reach here";
    return base::Status(-1, "Impossible condition");
}

}  // namespace raft
