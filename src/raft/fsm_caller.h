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
//          Xiong,Kai(xiongkai@baidu.com)

#ifndef  PUBLIC_RAFT_FSM_CALLER_H
#define  PUBLIC_RAFT_FSM_CALLER_H

#include <base/macros.h>                        // BAIDU_CACHELINE_ALIGNMENT
#include <bthread.h>
#include <bthread/execution_queue.h>
#include "raft/util.h"
#include "raft/ballot_box.h"
#include "raft/closure_queue.h"
#include "raft/macros.h"
#include "raft/log_manager.h"
#include "raft/lease.h"

namespace raft {

class NodeImpl;
class LogManager;
class StateMachine;
class OnErrorClousre;
struct LogEntry;
class LeaderChangeContext;

// Backing implementation of Iterator
class IteratorImpl {
    DISALLOW_COPY_AND_ASSIGN(IteratorImpl);
public:
    // Move to the next
    void next();
    LogEntry* entry() const { return _cur_entry; }
    bool is_good() const { return _cur_index <= _committed_index && !has_error(); }
    Closure* done() const;
    void set_error_and_rollback(size_t ntail, const base::Status* st);
    bool has_error() const { return _error.type() != ERROR_TYPE_NONE; }
    const Error& error() const { return _error; }
    int64_t index() const { return _cur_index; }
    void run_the_rest_closure_with_error();
private:
    IteratorImpl(StateMachine* sm, std::vector<LogEntry*>&& entries,
                 std::vector<Closure*> *closure,
                 int64_t first_closure_index,
                 int64_t last_applied_index,
                 int64_t committed_index,
                 base::atomic<int64_t>* applying_index);
    ~IteratorImpl();
friend class FSMCaller;
    StateMachine* _sm;
    std::vector<LogEntry*> _entries;
    std::vector<Closure*> *_closure;
    int64_t _first_closure_index;
    int64_t _cur_index;
    int64_t _first_index;
    int64_t _committed_index;
    LogEntry* _cur_entry;
    base::atomic<int64_t>* _applying_index;
    Error _error;
};

struct FSMCallerOptions {
    FSMCallerOptions() 
        : log_manager(NULL)
        , fsm(NULL)
        , after_shutdown(NULL)
        , closure_queue(NULL)
        , node(NULL)
        , usercode_in_pthread(false)
        , bootstrap_id()
        , state_machine_running(true)
    {}
    LogManager *log_manager;
    StateMachine *fsm;
    google::protobuf::Closure* after_shutdown;
    ClosureQueue* closure_queue;
    NodeImpl* node;
    bool usercode_in_pthread;
    LogId bootstrap_id;
    bool state_machine_running;
};

class SaveSnapshotClosure : public Closure {
public:
    // TODO: comments
    virtual SnapshotWriter* start(const SnapshotMeta& meta) = 0;
};

class LoadSnapshotClosure : public Closure {
public:
    // TODO: comments
    virtual SnapshotReader* start() = 0;
};

class FetchSnapshotClosure : public LoadSnapshotClosure {
public:
    FetchSnapshotClosure(bool load_snapshot) : _load_snapshot(load_snapshot) {}
    virtual ~FetchSnapshotClosure() {}
    bool load_snapshot() {
        return _load_snapshot;
    }
private:
    bool _load_snapshot;
};

class ReadEntriesAndCommitClosure;

class BAIDU_CACHELINE_ALIGNMENT FSMCaller {
public:
    FSMCaller();
    RAFT_MOCK ~FSMCaller();
    int init(const FSMCallerOptions& options);
    int shutdown();
    RAFT_MOCK int on_committed(int64_t committed_index);
    RAFT_MOCK int on_snapshot_fetched(FetchSnapshotClosure* done);
    RAFT_MOCK int on_snapshot_load(LoadSnapshotClosure* done);
    RAFT_MOCK int on_snapshot_save(SaveSnapshotClosure* done);
    int on_leader_stop(const base::Status& status);
    int on_leader_start(int64_t term, int64_t lease_epoch);
    int on_start_following(const LeaderChangeContext& start_following_context);
    int on_stop_following(const LeaderChangeContext& stop_following_context);
    RAFT_MOCK int on_error(const Error& e);
    int64_t last_applied_index() const {
        return _last_applied_index.load(base::memory_order_relaxed);
    }
    int64_t applying_index() const;
    void describe(std::ostream& os, bool use_html);
    void join();

    RAFT_MOCK int on_reset(Closure* done);
    void do_reset(Closure* done);
    void set_state_machine_running(int64_t commited_index);
    void set_leader_start_notified() {
        _leader_start_notified = true;
    }

    const NodeId& node_id() {
        return _node_id;
    }
private:

friend class IteratorImpl;
friend class SnapshotLoadDone;
friend class ReadEntriesAndCommitClosure;

    enum TaskType {
        IDLE,
        COMMITTED,
        COMMITTED_WITH_LOG,
        SNAPSHOT_SAVE,
        SNAPSHOT_LOAD,
        SNAPSHOT_FETCHED,
        LEADER_STOP,
        LEADER_START,
        START_FOLLOWING,
        STOP_FOLLOWING,
        RESET,
        RESUME,
        ERROR,
    };

    struct LeaderStartContext {
        LeaderStartContext(int64_t term_, int64_t lease_epoch_)
            : term(term_), lease_epoch(lease_epoch_)
        {}

        int64_t term;
        int64_t lease_epoch;
    };

    struct ApplyTask {
        TaskType type;
        union {
            // For applying log entry (including configuartion change)
            int64_t committed_index;
        
            // For on_leader_start
            LeaderStartContext* leader_start_context;
            
            // For on_leader_stop
            base::Status* status;    

            // For on_start_following and on_stop_following
            LeaderChangeContext* leader_change_context;

            // For other operation
            Closure* done;
        };
        std::vector<LogEntry*> entries;
    };

    static int on_commit_with_log(bthread::ExecutionQueueId<ApplyTask> _queue_id,
            std::vector<LogEntry*>& entries);
    static double get_cumulated_cpu_time(void* arg);
    static int run(void* meta, bthread::TaskIterator<ApplyTask>& iter);
    void do_shutdown(); //Closure* done);
    void do_committed(std::vector<LogEntry*>&& entries);
    void do_cleared(int64_t log_index, Closure* done, int error_code);
    void do_snapshot_save(SaveSnapshotClosure* done);
    void do_snapshot_load(LoadSnapshotClosure* done);
    void do_on_snapshot_load_done(SnapshotMeta meta, LoadSnapshotClosure* done,
                                  const base::Status& st);
    void do_snapshot_fetched(LoadSnapshotClosure* done);
    void do_on_error(OnErrorClousre* done);
    void do_leader_stop(const base::Status& status);
    void do_leader_start(const LeaderStartContext& leader_start_context);
    void do_start_following(const LeaderChangeContext& start_following_context);
    void do_stop_following(const LeaderChangeContext& stop_following_context);
    void set_error(const Error& e);
    bool pass_by_status(Closure* done);

    void _read_entries_and_commit();

    bthread::ExecutionQueueId<ApplyTask> _queue_id;
    scoped_refptr<LogManager> _log_manager;
    StateMachine *_fsm;
    ClosureQueue* _closure_queue;
    base::atomic<int64_t> _read_entry_index;
    base::atomic<int64_t> _can_apply_index;
    base::atomic<int64_t> _last_applied_index;
    int64_t _last_applied_term;
    google::protobuf::Closure* _after_shutdown;
    NodeImpl* _node;
    NodeId _node_id;
    TaskType _cur_task;
    base::atomic<int64_t> _applying_index;
    Error _error;
    bool _queue_started;
    bool _state_machine_running;
    bool _leader_start_notified;
};

class ReadEntriesAndCommitClosure : public ReadEntriesClosure {
public:
    ReadEntriesAndCommitClosure(LogManager* lm,
            int64_t start_index, int32_t entries, const NodeId& node_id,
            bthread::ExecutionQueueId<FSMCaller::ApplyTask> queue_id);

protected:
    virtual void Run() override;

private:
    NodeId _node_id;
    bthread::ExecutionQueueId<FSMCaller::ApplyTask> _queue_id;
};

}

#endif  //PUBLIC_RAFT_FSM_CALLER_H
