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

#ifndef  PUBLIC_RAFT_LOG_MANAGER_H
#define  PUBLIC_RAFT_LOG_MANAGER_H

#include <base/macros.h>                        // BAIDU_CACHELINE_ALIGNMENT
#include <base/containers/flat_map.h>           // base::FlatMap
#include <deque>                                // std::deque
#include <gflags/gflags.h>
#include <bthread/execution_queue.h>            // bthread::ExecutionQueueId

#include "raft/raft.h"                          // Closure
#include "raft/util.h"                          // Mutex
#include "raft/log_entry.h"                     // LogEntry
#include "raft/configuration_manager.h"         // ConfigurationManager
#include "raft/storage.h"                       // Storage
#include "raft/rtc_helper.h"                    // raft::Mutex

namespace raft {

DECLARE_bool(raft_enable_stat_apply_to_commit);

class LogStorage;
class FSMCaller;

struct LogManagerOptions {
    LogManagerOptions();
    LogStorage* log_storage;
    ConfigurationManager* configuration_manager;
    FSMCaller* fsm_caller;  // To report log error
};

struct LogManagerStatus {
    LogManagerStatus()
        : first_index(1), last_index(0), disk_index(0), known_applied_index(0)
    {}
    int64_t first_index;
    int64_t last_index;
    int64_t disk_index;
    int64_t known_applied_index;
};

class SnapshotMeta;

class BAIDU_CACHELINE_ALIGNMENT LogManager : public RaftRefCounted<LogManager> {
public:
    typedef int64_t WaitId;

    class StableClosure : public Closure {
    public:
        StableClosure() : _first_log_index(0) {
            _timer.start();
        }
        void update_metric(IOMetric* metric);
    protected:
        virtual ~StableClosure();
        int64_t _first_log_index;
        IOMetric metric;
    private:
    friend class LogManager;
    friend class AppendBatcher;
        std::vector<LogEntry*> _entries;
        base::Timer _timer;
    };

    LogManager();
    RAFT_MOCK ~LogManager();
    int init(const LogManagerOptions& options);

    void shutdown();

    // Append log entry vector and wait until it's stable (NOT COMMITTED!)
    // success return 0, fail return errno
    void append_entries(std::vector<LogEntry*> *entries, StableClosure* done);

    // Notify the log manager about the latest snapshot, which indicates the
    // logs which can be safely truncated.
    RAFT_MOCK void set_snapshot(const SnapshotMeta* meta, const int64_t first_snapshot_index);

    // We don't delete all the logs before last snapshot to avoid installing
    // snapshot on slow replica. Call this method to drop all the logs before
    // last snapshot immediately.
    RAFT_MOCK void clear_bufferred_logs();

    // Get the log at |index|
    // Returns:
    //  success return ptr, fail return null
    RAFT_MOCK void get_entry(const int64_t index, LogEntry** entry, Closure* done);

    // Get the |count| logs start with |index|
    // Returns:
    //  success return log entries' num, fail return 0
    int get_entries(const int64_t &fisrt_index, const int& count, const size_t& max_size,
                    std::vector<LogEntry* >* log_entries);

    // Get the log term at |index|
    // Returns:
    //  success return term > 0, fail return 0
    int64_t get_term(const int64_t index);

    // Get the first log index of log
    // Returns:
    //  success return first log index, empty return 0
    int64_t first_log_index();

    // Get the last log index of log
    // Returns:
    //  success return last memory and logstorage index, empty return 0
    int64_t last_log_index();
    
    // Return the id the last log.
    LogId last_log_id();

    RAFT_MOCK void last_log_id(LogId* log_id, Closure* done);

    void get_configuration(int64_t index, ConfigurationEntry* conf);

    // Check if |current| should be updated to the latest configuration
    // Returns true and |current| is assigned to the lastest configuration, returns
    // false otherweise
    bool check_and_set_configuration(ConfigurationEntry* current);

    // Wait until there are more logs since |last_log_index| and |on_new_log| 
    // would be called after there are new logs or error occurs
    WaitId wait(int64_t expected_last_log_index,
                int (*on_new_log)(void *arg, int error_code), void *arg);

    // Remove a waiter
    // Returns:
    //  - 0: success
    //  - -1: id is Invalid
    int remove_waiter(WaitId id);
    
    
    // Set the applied id, indicating that the log before applied_id (inclded)
    // can be droped from memory logs
    void set_applied_id(const LogId& applied_id);

    // Check the consistency between log and snapshot, which must satisfy ANY
    // one of the following condition
    //   - Log starts from 1. OR
    //   - Log starts from a positive position and there must be a snapshot
    //     of which the last_included_id is in the range 
    //     [first_log_index-1, last_log_index]
    // Returns base::Status::OK if valid, a specific error otherwise
    base::Status check_consistency();
    base::Status check_snapshot_consistency(const LogId &last_snapshot_id);

    void describe(std::ostream& os, bool use_html);

    // Get the internal status of LogManager.
    void get_status(LogManagerStatus* status);
    void reset_applied_id();
    void clear_logs(LogId *logid);
    int stop_disk_thread();

private:
friend class AppendBatcher;
friend class TruncatePrefixClosure;
friend class TruncateSuffixClosure;
friend class ResetClosure;
    struct WaitMeta {
        int (*on_new_log)(void *arg, int error_code);
        void* arg;
        int error_code;
    };

    RAFT_MOCK void append_to_storage(const std::vector<LogEntry*>& to_append,
            IOMetric* metric, Closure* done);

    static int disk_thread(void* meta,
                           bthread::TaskIterator<StableClosure*>& iter);
    
    // delete logs from storage's head, [1, first_index_kept) will be discarded
    // Returns:
    //  success return 0, failed return -1
    int truncate_prefix(const int64_t first_index_kept,
                        std::unique_lock<Mutex>& lck);
    
    int reset(const int64_t next_log_index,
              std::unique_lock<Mutex>& lck);

    // Must be called in the disk thread, otherwise the
    // behavior is undefined
    void set_disk_id(const LogId& disk_id);

    LogEntry* get_entry_from_memory(const int64_t index);

    int get_entries_from_memory(const int64_t& first_index, const int& count,
                                std::deque<LogEntry *> *log_entries);

    WaitId notify_on_new_log(int64_t expected_last_log_index, WaitMeta* wm);

    int check_and_resolve_conflict(std::vector<LogEntry*>* entries, 
                                   StableClosure* done);

    int unsafe_truncate_suffix(const int64_t last_index_kept);

    // Clear the logs in memory whose id <= the given |id|
    void clear_memory_logs(const LogId& id);

    int64_t unsafe_get_term(const int64_t index);

    // Start a independent thread to append log to LogStorage
    int start_disk_thread();

    void wakeup_all_waiter(std::unique_lock<Mutex>& lck);
    static void *run_on_new_log(void* arg);

    void report_error(int error_code, const char* fmt, ...);
    void unsafe_report_error(int error_code, const char* fmt, ...);

    // Fast implementation with one lock
    // TODO(chenzhangyi01): reduce the critical section
    LogStorage* _log_storage;

    ConfigurationManager* _config_manager;
    FSMCaller* _fsm_caller;

    Mutex _mutex;
    base::FlatMap<int64_t, WaitMeta*> _wait_map;
    std::atomic<bool> _stopped;
    base::atomic<bool> _has_error;
    WaitId _next_wait_id;

    LogId _disk_id;
    LogId _applied_id;
    // TODO(chenzhangyi01): replace deque with a thread-safe data structure
    std::deque<LogEntry* /*FIXME*/> _logs_in_memory;
    int64_t _first_log_index;
    int64_t _last_log_index;
    // the last snapshot's log_id
    LogId _last_snapshot_id;
    // the virtual first log, for finding next_index of replicator, which 
    // can avoid install_snapshot too often in extreme case where a follower's
    // install_snapshot is slower than leader's save_snapshot
    // [NOTICE] there should not be hole between this log_id and _last_snapshot_id,
    // or may cause some unexpect cases
    LogId _virtual_first_log_id;

    bthread::ExecutionQueueId<StableClosure*> _disk_queue;
};

class ReadEntriesClosure : public ClosureGroup, public Closure {
public:
    ReadEntriesClosure(LogManager* lm,
            int64_t start_index, int32_t entries) :
        ClosureGroup(entries, this), _start_index(start_index), _start_time_us(-1), _lm(lm) {
        CHECK_GT(entries, 0);
        _entries.resize(entries);
        CHECK_LE(_start_index + entries - 1, _lm->last_log_index());
        if (FLAGS_raft_enable_stat_apply_to_commit) {
            _start_time_us = base::monotonic_time_us();
        }
    }
    void read_entries() {
        int32_t size = (int32_t)_entries.size();
        for (int32_t i = 0; i < size; i++) {
            _entries[i] = nullptr;
            _lm->get_entry(_start_index + i, &_entries[i], sub_done(i));
        }
    }

protected:
    int64_t _start_index;
    std::vector<LogEntry*> _entries;
    int64_t _start_time_us;

private:
    LogManager* _lm; // for performance reason, not use scoped_refptr
};

}  // namespace raft

#endif  //PUBLIC_RAFT_LOG_MANAGER_H
