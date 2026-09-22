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

#ifndef  PUBLIC_RAFT_SNAPSHOT_EXECUTOR_H
#define  PUBLIC_RAFT_SNAPSHOT_EXECUTOR_H


#include "raft/raft.h"
#include "raft/util.h"
#include "raft/snapshot.h"
#include "raft/storage.h"
#include "raft/raft.pb.h"
#include "raft/fsm_caller.h"
#include "raft/log_manager.h"
#include "raft/snapshot_service.pb.h"

namespace raft {

class NodeImpl;
class SnapshotHook;
class FileSystemAdaptor;

struct SnapshotExecutorOptions {
    SnapshotExecutorOptions();
    // URI of SnapshotStorage
    std::string uri;
   
    FSMCaller* fsm_caller;
    NodeImpl* node;
    LogManager* log_manager;
    int64_t init_term;
    base::EndPoint addr;
    bool filter_before_copy_remote;
    bool usercode_in_pthread;
    scoped_refptr<FileSystemAdaptor> file_system_adaptor;
    scoped_refptr<SnapshotThrottle> snapshot_throttle;
    int32_t polar_worker_index;

    bool learner_auto_load_applied_logs;
};

class SnapshotExecutor;
struct SnapshotFetcherCtx {
    bthread_t tid;
    bthread_id_t resource_id;
    int64_t max_index;
    GroupId group_id;
    PeerId peer_id;
    SnapshotExecutor* se;
    SnapshotWriter* writer;
    Closure* done;

    SnapshotFetcherCtx()
        : tid(INVALID_BTHREAD)
        , resource_id(INVALID_BTHREAD_ID)
        , max_index(INT64_MAX)
        , se(NULL)
        , writer(NULL)
        , done(NULL) {}
};

class SaveSnapshotDone : public SaveSnapshotClosure {
public:
    SaveSnapshotDone(SnapshotExecutor* node, SnapshotWriter* writer, Closure* done);
    virtual ~SaveSnapshotDone();

    SnapshotWriter* start(const SnapshotMeta& meta);
    virtual void Run();
    void set_meta(const SnapshotMeta& meta);
 
    inline const SnapshotMeta& meta() const { return _meta; }

private:
    static void* continue_run(void* arg);

    SnapshotExecutor* _se;
    SnapshotWriter* _writer;
    Closure* _done; // user done
    SnapshotMeta _meta;
};

// Executing Snapshot related stuff
class BAIDU_CACHELINE_ALIGNMENT SnapshotExecutor {
    DISALLOW_COPY_AND_ASSIGN(SnapshotExecutor);
public:
    SnapshotExecutor();
    ~SnapshotExecutor();

    int init(const SnapshotExecutorOptions& options);

    // Return the owner NodeImpl
    NodeImpl* node() const { return _node; }
    int polar_worker_index() { return _polar_worker_index; }

    // Start to snapshot StateMachine, and |done| is called after the execution
    // finishes or fails.
    void do_snapshot(Closure* done);

    // Install snapshot according to the very RPC from leader
    // After the installing succeeds (StateMachine is reset with the snapshot)
    // or fails, done will be called to respond
    // 
    // Errors:
    //  - Term dismatches: which happens interrupt_downloading_snapshot was 
    //    called before install_snapshot, indicating that this RPC was issued by
    //    the old leader.
    //  - Interrupted: happens when interrupt_downloading_snapshot is called or
    //    a new RPC with the same or newer snapshot arrives
    //  - Busy: the state machine is saving or loading snapshot
    void install_snapshot(rpc::Controller* controller,
                          const InstallSnapshotRequest* request,
                          InstallSnapshotResponse* response,
                          google::protobuf::Closure* done);

    // Fetch snapshot from remote node and restore.
    // Return 0 if fetch and restore successful.
    int fetch_snapshot(const GroupId& group_id, const PeerId& peer_id, 
                       const int64_t max_snapshot_index);
    void fetch_snapshot(const GroupId& group_id, const PeerId& peer_id,
                        const int64_t max_snapshot_index,
                        SnapshotWriter* writer, Closure* done);
    void cancel_fetch_snapshot(const bool is_wait);

    void handle_tail_snapshot(rpc::Controller* cntl,
                              const TailSnapshotRequest* request,
                              TailSnapshotResponse* response);

    // Interrupt the downloading if possible.
    // This is called when the term of node increased to |new_term|, which
    // happens when receiving RPC from new peer. In this case, it's hard to
    // determine whether to keep downloading snapshot as the new leader
    // possibly contains the missing logs and is going to send AppendEntries. To
    // make things simplicity and leader changing during snapshot installing is 
    // very rare. So we interrupt snapshot downloading when leader changes, and
    // let the new leader decide whether to install a new snapshot or continue 
    // appending log entries.
    //
    // NOTE: we can't interrupt the snapshot insalling which has finsihed
    // downloading and is reseting the State Machine.
    void interrupt_downloading_snapshot(int64_t new_term);

    // Return true if this is currently installing a snapshot, either
    // downloading or loading.
    bool is_installing_snapshot() const { 
        // 1: acquire fence makes this thread sees the latest change when seeing
        //    the lastest _loading_snapshot
        // _downloading_snapshot is NULL when then downloading was successfully 
        // interrupted or installing has finished
        return _downloading_snapshot.load(base::memory_order_acquire/*1*/);
    }

    // Return the backing snapshot storage
    SnapshotStorage* snapshot_storage() { return _snapshot_storage; }

    int64_t last_snapshot_index() {
        std::unique_lock<Mutex> lck(_mutex);
        return _last_snapshot_index; 
    }

    void describe(std::ostream& os, bool use_html);

    // Shutdown the SnapshotExecutor and all the following jobs would be refused
    void shutdown();

    // Join, run done if all the running job finishes
    void join(::google::protobuf::Closure* done);

    int load_snapshot(int64_t commited_index, Closure* done);
    void check_consistency();
    base::Status check_snapshot_consistency();

private:
friend class SaveSnapshotDone;
friend class FirstSnapshotLoadDone;
friend class SnapshotLoadDone;
friend class InstallSnapshotDone;
friend class FetchSnapshotDone;
friend class InstallSnapshotStepClosure;

    static void unlock_snapshot_fetcher_id(bthread_id_t context_id);
    int fetch_snapshot(const GroupId& group_id, const PeerId& peer_id, 
                       const int64_t snapshot_index, 
                       const bthread_id_t& context_id,
                       bool load_snapshot);

    void on_snapshot_load_done(const base::Status& st);
    int on_snapshot_save_done(const base::Status& st,
                              const SnapshotMeta& meta, 
                              SnapshotWriter* writer);
    void on_load_fetched_snapshot_done(const base::Status& st);

    struct DownloadingSnapshot {
        const InstallSnapshotRequest* request;
        InstallSnapshotResponse* response;
        rpc::Controller* cntl;
        google::protobuf::Closure* done;
    };

    int register_downloading_snapshot(DownloadingSnapshot* ds, Closure* step = nullptr);
    int parse_install_snapshot_request(
            const InstallSnapshotRequest* request,
            SnapshotMeta* meta);
    void on_snapshot_copier_done(DownloadingSnapshot* ds, const SnapshotMeta& meta);
    void load_downloading_snapshot(DownloadingSnapshot* ds,
                                  const SnapshotMeta& meta);
    int load_fetched_snapshot(const std::string& uri, bool load_snapshot);
    int start_reader_expire_timer(const std::unique_lock<Mutex>& lck);
    int reset_reader_expire_timer(const std::unique_lock<Mutex>& lck);
    void destroy_fetch_snapshot_reader();
    void unsafe_destroy_fetch_snapshot_reader(const std::unique_lock<Mutex>& lck);
    static void* run_snapshot_fetcher(void* arg);
    static void on_snapshot_reader_expired(void* arg);
    void report_error(int error_code, const char* fmt, ...);
    static bool destroy_snapshot_fetcher_id(const bthread_id_t& id);
    bool reset_snapshot_fetcher_id(const bthread_id_t& id);
    void add_one_running_job(const std::unique_lock<Mutex>& lck);
    void finish_one_running_job(const std::unique_lock<Mutex>& lck);

    Mutex _mutex;
    int64_t _last_snapshot_term;
    int64_t _last_snapshot_index;
    int64_t _term;
    bool _saving_snapshot;
    bool _loading_snapshot;
    bool _stopped;
    bool _usercode_in_pthread;
    SnapshotStorage* _snapshot_storage;
    SnapshotCopier* _cur_copier;
    FSMCaller* _fsm_caller;
    NodeImpl* _node;
    scoped_refptr<LogManager> _log_manager;
    // The ownership of _downloading_snapshot is a little messy:
    // - Before we start to replace the FSM with the downloaded one. The
    //   ownership belongs with the downloding thread
    // - After we push the load task to FSMCaller, the ownership belongs to the
    //   closure which is called after the Snapshot replaces FSM
    base::atomic<DownloadingSnapshot*> _downloading_snapshot;
    SnapshotMeta _loading_snapshot_meta;
    ssize_t _running_jobs;
    std::vector<google::protobuf::Closure*> _join_closures;
    scoped_refptr<SnapshotThrottle> _snapshot_throttle;
    SnapshotReader* _fetch_snapshot_reader;
    bthread_timer_t _reader_expire_timer;
    bthread_id_t _snapshot_fetcher_id;
    int _polar_worker_index;
};

inline SnapshotExecutorOptions::SnapshotExecutorOptions() 
    : fsm_caller(NULL)
    , node(NULL)
    , log_manager(NULL)
    , init_term(0)
    , filter_before_copy_remote(false)
    , usercode_in_pthread(false)
    , polar_worker_index(-1)
    , learner_auto_load_applied_logs(true)
{}

}  // namespace raft

#endif  // PUBLIC_RAFT_SNAPSHOT_EXECUTOR_H
