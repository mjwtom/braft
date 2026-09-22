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

#include <base/logging.h>
#include "raft/raft.h"
#include "raft/log_manager.h"
#include "raft/node.h"
#include "raft/util.h"
#include "raft/raft.pb.h"
#include "raft/log_entry.h"
#include "raft/errno.pb.h"
#include "raft/node.h"
#include "raft/closure_helper.h"

#include "raft/fsm_caller.h"
#include <bthread_unstable.h>

namespace raft {

static BvarCounterRecorder g_commit_tasks_batch_counter(
        "raft_commit_tasks_batch_counter");
DEFINE_int64(raft_commit_get_entry_batch, 64, "Max entries to read for each commit batch");
RAFT_VALIDATE_GFLAG(raft_commit_get_entry_batch, rpc::PositiveInteger);

DEFINE_bool(raft_enable_stat_apply_to_commit, false, "enable stat some latency and count from apply to commit");
HPN_RPC_VALIDATE_GFLAG(raft_enable_stat_apply_to_commit, baidu::hpn::rpc::PassValidate);

static BvarMaxer<uint64_t> g_raft_log_read_for_commit_max_latency;
static bvar::Window<BvarMaxer<uint64_t>> g_raft_log_read_for_commit_max_latency_in_window("raft_log_read_for_commit_max_latency", &g_raft_log_read_for_commit_max_latency, 30);
static BvarMaxer<uint64_t> g_raft_log_commit_max_latency;
static bvar::Window<BvarMaxer<uint64_t>> g_raft_log_commit_max_latency_in_window("raft_log_commit_max_latency", &g_raft_log_commit_max_latency, 30);
static BvarMaxer<uint64_t> g_raft_commit_snapshot_save_max_latency;
static bvar::Window<BvarMaxer<uint64_t>> g_raft_commit_snapshot_save_max_latency_in_window("raft_commit_snapshot_save_max_latency", &g_raft_commit_snapshot_save_max_latency, 30);
static BvarMaxer<uint64_t> g_raft_commit_snapshot_load_max_latency;
static bvar::Window<BvarMaxer<uint64_t>> g_raft_commit_snapshot_load_max_latency_in_window("raft_commit_snapshot_load_max_latency", &g_raft_commit_snapshot_load_max_latency, 30);
static BvarMaxer<uint64_t> g_raft_commit_leader_stop_max_latency;
static bvar::Window<BvarMaxer<uint64_t>> g_raft_commit_leader_stop_max_latency_in_window("raft_commit_leader_stop_max_latency", &g_raft_commit_leader_stop_max_latency, 30);
static BvarMaxer<uint64_t> g_raft_commit_leader_start_max_latency;
static bvar::Window<BvarMaxer<uint64_t>> g_raft_commit_leader_start_max_latency_in_window("raft_commit_leader_start_max_latency", &g_raft_commit_leader_start_max_latency, 30);
static BvarMaxer<uint64_t> g_raft_commit_start_following_max_latency;
static bvar::Window<BvarMaxer<uint64_t>> g_raft_commit_start_following_max_latency_in_window("raft_commit_start_following_max_latency", &g_raft_commit_start_following_max_latency, 30);
static BvarMaxer<uint64_t> g_raft_commit_stop_following_max_latency;
static bvar::Window<BvarMaxer<uint64_t>> g_raft_commit_stop_following_max_latency_in_window("raft_commit_stop_following_max_latency", &g_raft_commit_stop_following_max_latency, 30);
static BvarAdder<uint64_t> g_raft_log_read_for_commit_num;
static bvar::Window<BvarAdder<uint64_t>> g_raft_log_read_for_commit_num_in_window("raft_log_read_for_commit_num", &g_raft_log_read_for_commit_num, 30);
static BvarAdder<uint64_t> g_raft_log_commit_num;
static bvar::Window<BvarAdder<uint64_t>> g_raft_log_commit_num_in_window("raft_log_commit_num", &g_raft_log_commit_num, 30);

FSMCaller::FSMCaller()
    : _log_manager(NULL)
    , _fsm(NULL)
    , _closure_queue(NULL)
    , _read_entry_index(0)
    , _can_apply_index(0)
    , _last_applied_index(0)
    , _last_applied_term(0)
    , _after_shutdown(NULL)
    , _node(NULL)
    , _node_id("", PeerId())
    , _cur_task(IDLE)
    , _applying_index(0)
    , _queue_started(false)
    , _state_machine_running(true)
    , _leader_start_notified(false) {
}

FSMCaller::~FSMCaller() {
    CHECK(_after_shutdown == NULL);
}

ReadEntriesAndCommitClosure::ReadEntriesAndCommitClosure(LogManager* lm,
        int64_t start_index, int32_t entries, const NodeId& node_id,
        bthread::ExecutionQueueId<FSMCaller::ApplyTask> queue_id) :
        ReadEntriesClosure(lm, start_index, entries), _node_id(node_id), _queue_id(queue_id) {}

void ReadEntriesAndCommitClosure::Run() {
    if (FLAGS_raft_enable_stat_apply_to_commit && _start_time_us > 0) {
        g_raft_log_read_for_commit_max_latency << base::monotonic_time_us() - _start_time_us;
        g_raft_log_read_for_commit_num << _entries.size();
    }
    if (!status().ok()) {
        LOG(ERROR) << "Node " << _node_id << " read entries [" << _start_index
            << ", " << _start_index + _entries.size() - 1
            << "] error: " << status();
    }
    FSMCaller::on_commit_with_log(_queue_id, _entries);
}

void FSMCaller::_read_entries_and_commit() {
    _cur_task = IDLE;
    int64_t last_applied_index = _last_applied_index.load(base::memory_order_relaxed);
    int64_t read_entry_index = _read_entry_index.load(base::memory_order_relaxed);
    if (read_entry_index != last_applied_index) {
        LOG(INFO) << node_id() << " NOT read entries because read_entry_index: " << read_entry_index
                  << " != last_applied_index: " << last_applied_index;
        return;
    }
    int64_t max_committed_index = _can_apply_index.load(base::memory_order_relaxed);
    CHECK_LE(max_committed_index, _log_manager->last_log_index());
    int64_t entries_num = std::min(FLAGS_raft_commit_get_entry_batch,
            max_committed_index - last_applied_index);
    if (entries_num == 0) {
        LOG(INFO) << node_id() << " NOT read entries because need read entries_num is 0, max_committed_index: "
                  << max_committed_index << ", last_applied_index: " << last_applied_index;
        return;
    }
    _read_entry_index.store(last_applied_index + entries_num, base::memory_order_relaxed);
    ReadEntriesAndCommitClosure* read_closure = new ReadEntriesAndCommitClosure(_log_manager,
            last_applied_index + 1, entries_num, node_id(), _queue_id);
    RAFT_VLOG << "Node " << node_id()
        << " fsm caller tries to read entries [" << last_applied_index+1
        << "," << last_applied_index+entries_num << "]";
    read_closure->read_entries();
}

int FSMCaller::run(void* meta, bthread::TaskIterator<ApplyTask>& iter) {
    FSMCaller* caller = (FSMCaller*)meta;
    if (iter.is_queue_stopped()) {
        caller->do_shutdown();
        return 0;
    }
    int64_t max_committed_index = -1;
    for (; iter; ++iter) {
        if (iter->type == COMMITTED) {
            if (iter->committed_index > max_committed_index) {
                max_committed_index = iter->committed_index;
            }
        } else {
            int64_t can_apply_index = caller->_can_apply_index.load(base::memory_order_relaxed);
            if (max_committed_index > can_apply_index) {
                caller->_cur_task = COMMITTED;
                caller->_can_apply_index.store(max_committed_index, base::memory_order_relaxed);
                caller->_read_entries_and_commit();
            }
            switch (iter->type) {
            case COMMITTED:
                CHECK(false) << "Impossible: " << iter->type;
                break;
            case COMMITTED_WITH_LOG: {
                caller->_cur_task = COMMITTED_WITH_LOG;
                g_commit_tasks_batch_counter << iter->entries.size();
                int64_t start_time_us = -1;
                int64_t commit_entry_num = 0;
                if (FLAGS_raft_enable_stat_apply_to_commit) {
                    start_time_us = base::monotonic_time_us();
                    commit_entry_num = iter->entries.size();
                }
                caller->do_committed(std::move(iter->entries));
                if (FLAGS_raft_enable_stat_apply_to_commit && start_time_us > 0) {
                    g_raft_log_commit_max_latency << base::monotonic_time_us() - start_time_us;
                    g_raft_log_commit_num << commit_entry_num;
                }
                caller->_read_entries_and_commit();
                break;
            }
            case SNAPSHOT_SAVE:
                caller->_cur_task = SNAPSHOT_SAVE;
                if (caller->pass_by_status(iter->done)) {
                    int64_t start_time_us = base::monotonic_time_us();
                    caller->do_snapshot_save((SaveSnapshotClosure*)iter->done);
                    if (FLAGS_raft_enable_stat_apply_to_commit) {
                        g_raft_commit_snapshot_save_max_latency << base::monotonic_time_us() - start_time_us;
                    }
                }
                break;
            case SNAPSHOT_LOAD:
                caller->_cur_task = SNAPSHOT_LOAD;
                // TODO: do we need to allow the snapshot loading to recover the
                // StateMachine if possible?
                if (caller->pass_by_status(iter->done)) {
                    int64_t start_time_us = base::monotonic_time_us();
                    caller->do_snapshot_load((LoadSnapshotClosure*)iter->done);
                    if (FLAGS_raft_enable_stat_apply_to_commit) {
                        g_raft_commit_snapshot_load_max_latency << base::monotonic_time_us() - start_time_us;
                    }
                }
                break;
            case SNAPSHOT_FETCHED: {
                bool load_snapshot = ((FetchSnapshotClosure*)iter->done)->load_snapshot();
                if (load_snapshot && caller->_state_machine_running) {
                    caller->on_snapshot_load((LoadSnapshotClosure*)iter->done);
                    break;
                }
                caller->_cur_task = SNAPSHOT_FETCHED;
                if (caller->pass_by_status(iter->done)) {
                    caller->do_snapshot_fetched((LoadSnapshotClosure*)iter->done);
                }
                break;
            }
            case LEADER_STOP: {
                caller->_cur_task = LEADER_STOP;
                int64_t start_time_us = base::monotonic_time_us();
                caller->do_leader_stop(*(iter->status));
                if (FLAGS_raft_enable_stat_apply_to_commit) {
                    g_raft_commit_leader_stop_max_latency << base::monotonic_time_us() - start_time_us;
                }
                delete iter->status;
                break;
            }
            case LEADER_START: {
                int64_t start_time_us = base::monotonic_time_us();
                caller->do_leader_start(*(iter->leader_start_context));
                if (FLAGS_raft_enable_stat_apply_to_commit) {
                    g_raft_commit_leader_start_max_latency << base::monotonic_time_us() - start_time_us;
                }
                delete iter->leader_start_context;
                break;
            }
            case START_FOLLOWING: {
                caller->_cur_task = START_FOLLOWING;
                int64_t start_time_us = base::monotonic_time_us();
                caller->do_start_following(*(iter->leader_change_context));
                if (FLAGS_raft_enable_stat_apply_to_commit) {
                    g_raft_commit_start_following_max_latency << base::monotonic_time_us() - start_time_us;
                }
                delete iter->leader_change_context;
                break;
            }
            case STOP_FOLLOWING: {
                caller->_cur_task = STOP_FOLLOWING;
                int64_t start_time_us = base::monotonic_time_us();
                caller->do_stop_following(*(iter->leader_change_context));
                if (FLAGS_raft_enable_stat_apply_to_commit) {
                    g_raft_commit_stop_following_max_latency << base::monotonic_time_us() - start_time_us;
                }
                delete iter->leader_change_context;
                break;
            }
            case RESET:
                caller->_cur_task = RESET;
                caller->do_reset(iter->done);
                break;
            case RESUME:
                caller->_state_machine_running = true;
                caller->on_committed(iter->committed_index);
                break;
            case ERROR:
                caller->_cur_task = ERROR;
                caller->do_on_error((OnErrorClousre*)iter->done);
                break;
            case IDLE:
                CHECK(false) << "Can't reach here";
                break;
            };
        }
    }
    int64_t can_apply_index = caller->_can_apply_index.load(base::memory_order_relaxed);
    if (max_committed_index > can_apply_index) {
        caller->_cur_task = COMMITTED;
        caller->_can_apply_index.store(max_committed_index, base::memory_order_relaxed);
        caller->_read_entries_and_commit();
    }
    caller->_cur_task = IDLE;
    return 0;
}

bool FSMCaller::pass_by_status(Closure* done) {
    rpc::ClosureGuard done_guard(done);
    if (!_error.status().ok()) {
        if (done) {
            done->status().set_error(
                        EINVAL, "FSMCaller is in bad status=`%s'",
                                _error.status().error_cstr());
        }
        return false;
    }
    done_guard.release();
    return true;
}

void FSMCaller::set_state_machine_running(int64_t committed_index) {
    ApplyTask t;
    t.type = RESUME;
    t.committed_index = committed_index;
    raft_execution_queue_execute(_queue_id, t);
}

int FSMCaller::init(const FSMCallerOptions &options) {
    if (options.log_manager == NULL || options.fsm == NULL 
            || options.closure_queue == NULL) {
        return EINVAL;
    }
    _log_manager = options.log_manager;
    _fsm = options.fsm;
    _closure_queue = options.closure_queue;
    _after_shutdown = options.after_shutdown;
    _node = options.node;
    _last_applied_index.store(options.bootstrap_id.index,
                              base::memory_order_relaxed);
    _last_applied_term = options.bootstrap_id.term;
    _read_entry_index.store(_last_applied_index.load(base::memory_order_relaxed),
                              base::memory_order_relaxed);
    _can_apply_index.store(_last_applied_index.load(base::memory_order_relaxed),
                              base::memory_order_relaxed);
    _state_machine_running = options.state_machine_running;
    if (_node) {
        _node->AddRef();
        _node_id = _node->node_id();
    }

    bthread::ExecutionQueueOptions execq_opt;
    execq_opt.urgent = true; // FIXME: for polar mode, we need this to avoid some deadlock
                            // see FirstSnapshotLoadDone used in SnapshotExecutor::init
    execq_opt.bthread_attr = options.usercode_in_pthread 
                             ? BTHREAD_ATTR_PTHREAD
                             : BTHREAD_ATTR_NORMAL;
    if (raft_execution_queue_start(&_queue_id,
                                   &execq_opt,
                                   FSMCaller::run,
                                   this) != 0) {
        LOG(ERROR) << "fsm fail to start execution_queue";
        return -1;
    }
    _queue_started = true;
    return 0;
}

int FSMCaller::shutdown() {
    if (_queue_started) {
        return raft_execution_queue_stop(_queue_id);
    }
    return 0;
}

void FSMCaller::do_shutdown() {
    if (_node) {
        _node->Release();
        _node = NULL;
    }
    _fsm->on_shutdown();
    if (_after_shutdown) {
        google::protobuf::Closure* saved_done = _after_shutdown;
        _after_shutdown = NULL;
        // after this point, |this| is likely to be destroyed, don't touch
        // anything
        saved_done->Run();
    }
}

int FSMCaller::on_committed(int64_t committed_index) {
    ApplyTask t;
    t.type = COMMITTED;
    t.committed_index = committed_index;
    return raft_execution_queue_execute(_queue_id, t);
}

int FSMCaller::on_commit_with_log(bthread::ExecutionQueueId<ApplyTask> queue_id,
        std::vector<LogEntry*>& entries) {
    ApplyTask t;
    t.type = COMMITTED_WITH_LOG;
    t.entries.swap(entries);
    if (raft_execution_queue_execute(queue_id, t) != 0) {
        for (auto& entry : t.entries) {
            if (entry) {
                entry->Release();
            }
        }
        return -1;
    }
    return 0;
}

class OnErrorClousre : public Closure {
public:
    OnErrorClousre(const Error& e) : _e(e) {
    }
    const Error& error() { return _e; }
    void Run() {
        delete this;
    }
private:
    ~OnErrorClousre() {}
    Error _e;
};

int FSMCaller::on_error(const Error& e) {
    OnErrorClousre* c = new OnErrorClousre(e);
    ApplyTask t;
    t.type = ERROR;
    t.done = c;
    if (raft_execution_queue_execute(_queue_id, t,
                &bthread::TASK_OPTIONS_URGENT) != 0) {
        c->Run();
        return -1;
    }
    return 0;
}

void FSMCaller::do_on_error(OnErrorClousre* done) {
    rpc::ClosureGuard done_guard(done);
    set_error(done->error());
}

void FSMCaller::set_error(const Error& e) {
    if (_error.type() != ERROR_TYPE_NONE) {
        // Error has already reported
        return;
    }
    _error = e;
    if (_fsm) {
        _fsm->on_error(_error);
    }
    if (_node) {
        _node->on_error(_error);
    }
}

void FSMCaller::do_committed(std::vector<LogEntry*>&& entries) {
    if (!_error.status().ok()) {
        return;
    }

    if (entries.empty()) {
        return;
    }

    int64_t committed_index = _can_apply_index.load(base::memory_order_relaxed);
    int64_t read_entry_index = _read_entry_index.load(base::memory_order_relaxed);

    if (!_state_machine_running) {
        int64_t committed_term = _log_manager->get_term(read_entry_index);
        LogId committed_id(read_entry_index, committed_term);
        _log_manager->clear_logs(&committed_id);
        return;
    }

    int64_t last_applied_index = _last_applied_index.load(base::memory_order_relaxed);

    // We can tolerate the disorder of committed_index
    if (last_applied_index >= read_entry_index) {
        return;
    }
    CHECK(entries.front() ? entries.front()->id.index == last_applied_index + 1 : true);
    std::vector<Closure*> closure;
    int64_t first_closure_index = 0;
    CHECK_EQ(0, _closure_queue->pop_closure_until(read_entry_index, &closure,
                                                  &first_closure_index));

    IteratorImpl iter_impl(_fsm, std::move(entries), &closure, first_closure_index,
                           last_applied_index, read_entry_index, &_applying_index);
    for (; iter_impl.is_good();) {
        if (iter_impl.entry()->type != ENTRY_TYPE_DATA) {
            if (iter_impl.entry()->type == ENTRY_TYPE_CONFIGURATION) {
                Configuration new_conf(*iter_impl.entry()->peers);
                Configuration old_conf;
                if (iter_impl.entry()->old_peers) {
                    old_conf = *iter_impl.entry()->old_peers;
                }
                _fsm->on_configuration_committed(new_conf, old_conf,
                        iter_impl.entry()->id.index, iter_impl.entry()->user_conf);
            }
            // For other entries, we have nothing to do besides flush the
            // pending tasks and run this closure to notify the caller that the
            // entries before this one were successfully committed and applied.
            if (iter_impl.done()) {
                iter_impl.done()->Run();
            }
            iter_impl.next();
            continue;
        }
        Iterator iter(&iter_impl);
        _fsm->on_apply(iter);
        LOG_IF(ERROR, iter.valid())
                << "Node " << _node_id
                << " Iterator is still valid, did you return before iterator "
                   " reached the end?";
        // Try move to next in case that we pass the same log twice.
        iter.next();
    }
    if (iter_impl.has_error()) {
        set_error(iter_impl.error());
        iter_impl.run_the_rest_closure_with_error();

        std::vector<Closure*> closure;
        CHECK_EQ(0, _closure_queue->pop_closure_until(committed_index, &closure,
                                                      &first_closure_index));
        for (auto done : closure) {
            if (done) {
                done->status() = _error.status();
                raft_run_closure_background(done);
            }
        }
    }
    const int64_t last_index = iter_impl.index() - 1;
    const int64_t last_term = _log_manager->get_term(last_index);
    LogId last_applied_id(last_index, last_term);
    CHECK_GT(read_entry_index,
            _last_applied_index.load(base::memory_order_relaxed)) << "Node " << _node_id;
    _last_applied_index.store(read_entry_index, base::memory_order_release);
    _last_applied_term = last_term;
    _log_manager->set_applied_id(last_applied_id);
}

int FSMCaller::on_snapshot_save(SaveSnapshotClosure* done) {
    ApplyTask task;
    task.type = SNAPSHOT_SAVE;
    task.done = done;
    return raft_execution_queue_execute(_queue_id, task);
}

void FSMCaller::do_snapshot_save(SaveSnapshotClosure* done) {
    CHECK(done);

    if (!_state_machine_running) {
        done->status().set_error(EINVAL, "state machine is not running");
        done->Run();
        return;
    }

    int64_t last_applied_index = _last_applied_index.load(base::memory_order_relaxed);

    SnapshotMeta meta;
    meta.set_last_included_index(last_applied_index);
    meta.set_last_included_term(_last_applied_term);
    ConfigurationEntry conf_entry;
    _log_manager->get_configuration(last_applied_index, &conf_entry);
    for (Configuration::const_iterator
            iter = conf_entry.conf.begin();
            iter != conf_entry.conf.end(); ++iter) { 
        *meta.add_peers() = iter->to_string();
    }
    for (Configuration::const_iterator
            iter = conf_entry.old_conf.begin();
            iter != conf_entry.old_conf.end(); ++iter) { 
        *meta.add_old_peers() = iter->to_string();
    }
    if (!conf_entry.user_conf.empty()) {
        meta.set_user_conf(conf_entry.user_conf);
    }

    SnapshotWriter* writer = done->start(meta);
    if (!writer) {
        done->status().set_error(EINVAL, "snapshot_storage create SnapshotWriter failed");
        done->Run();
        return;
    }

    _fsm->on_snapshot_save(writer, done);
    return;
}

int FSMCaller::on_snapshot_fetched(FetchSnapshotClosure* done) {
    ApplyTask task;
    task.type = SNAPSHOT_FETCHED;
    task.done = done;
    return raft_execution_queue_execute(_queue_id, task);
}

int FSMCaller::on_snapshot_load(LoadSnapshotClosure* done) {
    ApplyTask task;
    task.type = SNAPSHOT_LOAD;
    task.done = done;
    return raft_execution_queue_execute(_queue_id, task);
}

void FSMCaller::do_snapshot_load(LoadSnapshotClosure* done) {
    //TODO done_guard
    SnapshotReader* reader = done->start();
    if (!reader) {
        done->status().set_error(EINVAL, "open SnapshotReader failed");
        done->Run();
        return;
    }

    SnapshotMeta meta;
    int ret = reader->load_meta(&meta);
    if (0 != ret) {
        done->status().set_error(ret, "SnapshotReader load_meta failed.");
        done->Run();
        if (ret == EIO) {
            Error e;
            e.set_type(ERROR_TYPE_SNAPSHOT);
            e.status().set_error(ret, "Fail to load snapshot meta");
            set_error(e);
        }
        return;
    }

    LogId last_applied_id;
    last_applied_id.index = _last_applied_index.load(base::memory_order_relaxed);
    last_applied_id.term = _last_applied_term;
    LogId snapshot_id;
    snapshot_id.index = meta.last_included_index();
    snapshot_id.term = meta.last_included_term();
    if (last_applied_id > snapshot_id) {
        done->status().set_error(ESTALE,"Loading a stale snapshot"
                                 " last_applied_index=%ld last_applied_term=%ld"
                                 " snapshot_index=%ld snapshot_term=%ld",
                                 last_applied_id.index, last_applied_id.term,
                                 snapshot_id.index, snapshot_id.term);
        return done->Run();
    }
    _fsm->on_snapshot_load(reader, NewCallback(this, &FSMCaller::do_on_snapshot_load_done, meta, done));
}

void FSMCaller::do_on_snapshot_load_done(SnapshotMeta meta, LoadSnapshotClosure* done,
                                         const base::Status& st) {
    if (!st.ok()) {
        done->status().set_error(st.error_code(), st.error_cstr());
        done->Run();
        Error e;
        e.set_type(ERROR_TYPE_STATE_MACHINE);
        e.status().set_error(st.error_code(), st.error_cstr());
        set_error(e);
        return;
    }

    Configuration conf;
    for (int i = 0; i < meta.peers_size(); ++i) {
        conf.add_peer(meta.peers(i));
    }
    Configuration old_conf;
    for (int j = 0; j < meta.old_peers_size(); ++j) {
        old_conf.add_peer(meta.old_peers(j));
    }
    std::string user_conf;
    if (meta.has_user_conf()) {
        user_conf = meta.user_conf();
    }
    _fsm->on_configuration_committed(conf, old_conf, meta.last_included_index(), user_conf);

    _last_applied_index.store(meta.last_included_index(),
                              base::memory_order_release);
    _last_applied_term = meta.last_included_term();
    _read_entry_index.store(_last_applied_index.load(base::memory_order_relaxed),
                              base::memory_order_relaxed);
    _can_apply_index.store(_last_applied_index.load(base::memory_order_relaxed),
                              base::memory_order_relaxed);
    done->Run();
}

void FSMCaller::do_snapshot_fetched(LoadSnapshotClosure* done) {
    //TODO done_guard
    SnapshotReader* reader = done->start();
    if (!reader) {
        done->status().set_error(EINVAL, "open SnapshotReader failed");
        done->Run();
        return;
    }

    SnapshotMeta meta;
    int ret = reader->load_meta(&meta);
    if (0 != ret) {
        done->status().set_error(ret, "SnapshotReader load_meta failed.");
        done->Run();
        return;
    }

    ret = _fsm->on_snapshot_fetched(reader);
    if (ret != 0) {
        done->status().set_error(ret, "StateMachine on_snapshot_fetched failed");
        done->Run();
        return;
    }

    done->Run();
}

int FSMCaller::on_leader_stop(const base::Status& status) {
    ApplyTask task;
    task.type = LEADER_STOP;
    base::Status* on_leader_stop_status = new base::Status(status);
    task.status = on_leader_stop_status;
    if (raft_execution_queue_execute(_queue_id, task) != 0) {
        delete on_leader_stop_status;
        return -1;
    }
    return 0;
}

int FSMCaller::on_leader_start(int64_t term, int64_t lease_epoch) {
    ApplyTask task;
    task.type = LEADER_START;
    LeaderStartContext* on_leader_start_context =
        new LeaderStartContext(term, lease_epoch);
    task.leader_start_context = on_leader_start_context;
    if (raft_execution_queue_execute(_queue_id, task) != 0) {
        delete on_leader_start_context;
        return -1;
    }
    return 0;
}

void FSMCaller::do_leader_stop(const base::Status& status) {
    // When a raft node step down from STATE_LEADER, an leader stop notification
    // is triggered. But it's has very little chance that the configuration log
    // is not committed yet. In this case, the leader start notification will be
    // canceled by BallotBox::clear_pending_task.
    if (_leader_start_notified) {
        _fsm->on_leader_stop(status);
        _leader_start_notified = false;
    }
}

void FSMCaller::do_leader_start(const LeaderStartContext& leader_start_context) {
    _node->leader_lease_start(leader_start_context.lease_epoch);
    _fsm->on_leader_start(leader_start_context.term);
    _leader_start_notified = true;
}

int FSMCaller::on_start_following(const LeaderChangeContext& start_following_context) {
    ApplyTask task;
    task.type = START_FOLLOWING;
    LeaderChangeContext* context  = new LeaderChangeContext(start_following_context.leader_id(), 
            start_following_context.term(), start_following_context.status());
    task.leader_change_context = context;
    if (raft_execution_queue_execute(_queue_id, task) != 0) {
        delete context;
        return -1;
    }
    return 0;
}

int FSMCaller::on_stop_following(const LeaderChangeContext& stop_following_context) {
    ApplyTask task;
    task.type = STOP_FOLLOWING;
    LeaderChangeContext* context = new LeaderChangeContext(stop_following_context.leader_id(), 
            stop_following_context.term(), stop_following_context.status());
    task.leader_change_context = context;
    if (raft_execution_queue_execute(_queue_id, task) != 0) {
        delete context;
        return -1;
    }
    return 0;
}

void FSMCaller::do_start_following(const LeaderChangeContext& start_following_context) {
    _fsm->on_start_following(start_following_context);
}

void FSMCaller::do_stop_following(const LeaderChangeContext& stop_following_context) {
    _fsm->on_stop_following(stop_following_context);
}

void FSMCaller::describe(std::ostream &os, bool use_html) {
    const char* newline = (use_html) ? "<br>" : "\n";
    TaskType cur_task = _cur_task;
    const int64_t applying_index = _applying_index.load(
                                    base::memory_order_relaxed);
    const int64_t can_apply_index = _can_apply_index.load(
                                    base::memory_order_relaxed);
    os << "state_machine: ";
    switch (cur_task) {
    case IDLE:
        os << "Idle";
        break;
    case COMMITTED:
        os << "Known committed=" << can_apply_index;
        break;
    case COMMITTED_WITH_LOG:
        os << "Applying log_index=" << applying_index;
        break;
    case SNAPSHOT_SAVE:
        os << "Saving snapshot";
        break;
    case SNAPSHOT_LOAD:
        os << "Loading snapshot";
        break;
    case SNAPSHOT_FETCHED:
        os << "Fetching snapshot";
        break;
    case ERROR:
        os << "Notifying error";
        break;
    case LEADER_STOP:
        os << "Notifying leader stop";
        break;
    case LEADER_START:
        os << "Notifying leader start";
        break;
    case START_FOLLOWING:
        os << "Notifying start following";
        break;
    case STOP_FOLLOWING:
        os << "Notifying stop following";
        break;
    case RESET:
        os << "snapshot reset";
        break;
    case RESUME:
        os << "snapshot resume";
        break;
    }
    os << newline;
}

int64_t FSMCaller::applying_index() const {
    TaskType cur_task = _cur_task;
    if (cur_task != COMMITTED_WITH_LOG) {
        return 0;
    } else {
        return _applying_index.load(base::memory_order_relaxed);
    }
}

void FSMCaller::join() {
    if (_queue_started) {
        raft_execution_queue_join(_queue_id);
        _queue_started = false;
    }
}

int FSMCaller::on_reset(Closure* done) {
    ApplyTask t;
    t.type = RESET;
    t.done = done;
    if (raft_execution_queue_execute(_queue_id, t) != 0) {
        return -1;
    }
    return 0;
}

void FSMCaller::do_reset(Closure* done) {
    if (!_state_machine_running) {
        if (done) {
            raft_run_closure_background(done);
        }
        return;
    }
    if (!_fsm) {
        if (done) {
            done->status().set_error(EINVAL, "fms is NULL");
            raft_run_closure_background(done);
        }
        return;
    }
    _fsm->on_reset();
    _last_applied_index = 0;
    _applying_index = 0;
    _last_applied_term = 0;
    _state_machine_running = false;
    _log_manager->reset_applied_id();
    if (done) {
        raft_run_closure_background(done);
    }
}

IteratorImpl::IteratorImpl(StateMachine* sm, std::vector<LogEntry*>&& entries,
                          std::vector<Closure*> *closure,
                          int64_t first_closure_index,
                          int64_t last_applied_index,
                          int64_t committed_index,
                          base::atomic<int64_t>* applying_index)
        : _sm(sm)
        , _entries(entries)
        , _closure(closure)
        , _first_closure_index(first_closure_index)
        , _cur_index(last_applied_index)
        , _first_index(_cur_index + 1)
        , _committed_index(committed_index)
        , _cur_entry(NULL)
        , _applying_index(applying_index)
{ next(); }

IteratorImpl::~IteratorImpl() {
    for (auto& entry : _entries) {
        if (entry) {
            entry->Release();
        }
    }
    _entries.clear();
}

void IteratorImpl::next() {
    if (_cur_entry) {
        _cur_entry = nullptr;
    }
    if (_cur_index > _committed_index) {
        return;
    }
    ++_cur_index;
    if (_cur_index > _committed_index) {
        return;
    }
    _cur_entry = _entries[_cur_index-_first_index];
    if (_cur_entry == NULL) {
         _error.set_type(ERROR_TYPE_LOG);
         _error.status().set_error(-1,
                                   "Fail to get entry at index=%ld "
                                   "while committed_index=%ld",
                                   _cur_index, _committed_index);
    } else {
        CHECK_EQ(_cur_index, _cur_entry->id.index) << "entries should continuous";
        _applying_index->store(_cur_index, base::memory_order_relaxed);
    }
}

Closure* IteratorImpl::done() const {
    if (_cur_index < _first_closure_index) {
        return nullptr;
    }
    return (*_closure)[_cur_index - _first_closure_index];
}

void IteratorImpl::set_error_and_rollback(
            size_t ntail, const base::Status* st) {
    if (ntail == 0) {
        CHECK(false) << "Invalid ntail=" << ntail;
        return;
    }
    if (_cur_entry == NULL || _cur_entry->type != ENTRY_TYPE_DATA) {
        _cur_index -= ntail;
    } else {
        _cur_index -= (ntail - 1);
    }
    if (_cur_entry) {
        _cur_entry = nullptr;
    }
    _error.set_type(ERROR_TYPE_STATE_MACHINE);
    _error.status().set_error(ESTATEMACHINE,
            "StateMachine meet critical error when applying one "
            " or more tasks since index=%ld, %s", _cur_index,
            (st ? st->error_cstr() : "none"));
}

void IteratorImpl::run_the_rest_closure_with_error() {
    for (int64_t i = std::max(_cur_index, _first_closure_index);
            i <= _committed_index; ++i) {
        Closure* done = (*_closure)[i - _first_closure_index];
        if (done) {
            done->status() = _error.status();
            raft_run_closure_background(done);
        }
    }
}

}  // namespace raft
