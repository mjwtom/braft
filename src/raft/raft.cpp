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

// Authors: Wang,Yao(wangyao02@baidu.com)
//          Zhangyi Chen(chenzhangyi01@baidu.com)

#include <pthread.h>
#include <unistd.h>
#include <base/string_printf.h>
#include <base/class_name.h>
#include "raft/io_uring.h"
#include "raft/raft.h"
#include "raft/node.h"
#include "raft/learner.h"
#include "raft/storage.h"
#include "raft/node_manager.h"
#include "raft/log.h"
#include "raft/io_uring.h"
#include "raft/memory_log.h"
#include "raft/stable.h"
#include "raft/snapshot.h"
#include "raft/fsm_caller.h"            // IteratorImpl

namespace raft {

DEFINE_bool(raft_use_rdma, false, "use rdma for raft");
DEFINE_bool(raft_use_polar, false, "use polar user space tcp for raft");

DEFINE_int32(crc32_type, 1, "set which crc32_func to call, value: 0 base, 1 iscsi, 2 ieee, 3 gzip");
DEFINE_bool(enable_crc_send_log, false, "whether enable crc in send log entries");
RAFT_VALIDATE_GFLAG(enable_crc_send_log, rpc::PassValidate);
DEFINE_bool(enable_crc_install_snapshot, false, "whether enable crc in install snapshot");
RAFT_VALIDATE_GFLAG(enable_crc_install_snapshot, rpc::PassValidate);
DEFINE_int64(crc_error_retry_interval_ms, 10, "retry after received ECRCERROR");
RAFT_VALIDATE_GFLAG(crc_error_retry_interval_ms, rpc::PositiveInteger);
DEFINE_int32(raft_join_timeout_ms, 1, "the timeout(ms) of raft join");
DECLARE_bool(raft_log_with_aio);

static void print_revision(std::ostream& os, void*) {
#if defined(RAFT_REVISION)
        os << RAFT_REVISION;
#else
        os << "undefined";
#endif
}

static const char* built_datetime() {
#if defined( __DATE__ ) && defined ( __TIME__ )
    return __DATE__ " " __TIME__;
#else
    return "unknown";
#endif
}

static void print_version(std::ostream& os, void*) {
#ifdef NDEBUG
#define RAFT_DEBUG_VERSION ""
#else
#define RAFT_DEBUG_VERSION " (debug)"
#endif
#if defined(RAFT_VERSION)
        os << RAFT_VERSION << std::string(RAFT_DEBUG_VERSION)
           << ", Build: " << built_datetime();
#else
        os << "undefined";
#endif
}

static bvar::PassiveStatus<std::string> s_raft_revision(
        "raft_revision", print_revision, NULL);

static bvar::PassiveStatus<std::string> s_raft_version(
        "raft_version", print_version, NULL);


static pthread_once_t global_init_once = PTHREAD_ONCE_INIT;

struct GlobalExtension {
    SegmentLogStorage local_log;
    MemoryLogStorage memory_log;
    
    // manage only one raft instance
    FileBasedSingleStableStorage single_stable;
    // manage a batch of raft instances
    KVBasedMergedStableStorage merged_stable;
    // mix two types for double write when upgrade and downgrade  
    MixedStableStorage mixed_stable;

    LocalSnapshotStorage local_snapshot;
};

static void global_init_or_die_impl() {
    static GlobalExtension s_ext;

    log_storage_extension()->RegisterOrDie("local", &s_ext.local_log);
    log_storage_extension()->RegisterOrDie("memory", &s_ext.memory_log);
  
    // uri = local://{single_path}
    // |single_path| usually ends with `/stable'
    // NOTICE: not change "local" to "local-single" because of compatibility
    stable_storage_extension()->RegisterOrDie("local", &s_ext.single_stable);
    // uri = local-merged://{merged_path}
    // |merged_path| usually ends with `/merged_stable'
    stable_storage_extension()->RegisterOrDie("local-merged", &s_ext.merged_stable);
    // uri = local-mixed://merged_path={merged_path}&&single_path={single_path}
    stable_storage_extension()->RegisterOrDie("local-mixed", &s_ext.mixed_stable);

    snapshot_storage_extension()->RegisterOrDie("local", &s_ext.local_snapshot);

    if (g_raft_io_man->init() != 0) {
        PLOG(FATAL) << "Fail to init io worker";
        exit(-1);
    }
}

// Non-static for unit test
void global_init_once_or_die() {
    if (pthread_once(&global_init_once, global_init_or_die_impl) != 0) {
        PLOG(FATAL) << "Fail to pthread_once";
        exit(-1);
    }
}

int add_service(rpc::Server* server, const base::EndPoint& listen_addr) {
    global_init_once_or_die();
    return global_node_manager->add_service(server, listen_addr);
}

int add_service(rpc::Server* server, int port) {
    base::EndPoint addr(base::IP_ANY, port);
    return add_service(server, addr);
}

int add_service(rpc::Server* server, const char* listen_ip_and_port) {
    base::EndPoint addr;
    if (base::str2endpoint(listen_ip_and_port, &addr) != 0) {
        LOG(ERROR) << "Fail to parse `" << listen_ip_and_port << "'";
        return -1;
    }
    return add_service(server, addr);
}

// GC 
int gc_raft_data(const GCOptions& gc_options) {
    const VersionedGroupId vgid = gc_options.vgid;
    const std::string log_uri = gc_options.log_uri;
    const std::string stable_uri = gc_options.stable_uri;
    const std::string snapshot_uri = gc_options.snapshot_uri;
    bool is_success = true;

    base::Status status = LogStorage::destroy(log_uri);
    if (!status.ok()) {
        is_success = false;
        LOG(WARNING) << "Group " << vgid << " failed to gc raft log, uri " << log_uri; 
    }
    // TODO encode vgid into stable_uri ?
    status = StableStorage::destroy(stable_uri, vgid);
    if (!status.ok()) {
        is_success = false;
        LOG(WARNING) << "Group " << vgid << " failed to gc raft stable, uri " << stable_uri; 
    }
    status = SnapshotStorage::destroy(snapshot_uri);
    if (!status.ok()) {
        is_success = false;
        LOG(WARNING) << "Group " << vgid << " failed to gc raft snapshot, uri " << snapshot_uri; 
    }
    return is_success ? 0 : -1; 
}

// ------------- Node
Node::Node(const GroupId& group_id, const PeerId& peer_id) {
    _impl = new NodeImpl(group_id, peer_id);
}

Node::~Node() {
    if (_impl) {
        _impl->shutdown(NULL);
        google::protobuf::Closure* done = rpc::NewCallback(
                _impl, &NodeImpl::Release);
        _impl->join(done);
        _impl = NULL;
    }
}

NodeId Node::node_id() {
    return _impl->node_id();
}

PeerId Node::leader_id() {
    return _impl->leader_id();
}

bool Node::is_leader() {
    return _impl->is_leader();
}

bool Node::is_leader_lease_valid() {
    return _impl->is_leader_lease_valid();
}

void Node::get_leader_lease_status(LeaderLeaseStatus* status) {
    return _impl->get_leader_lease_status(status);
}

int Node::init(const NodeOptions& options) {
    return _impl->init(options);
}

void Node::shutdown(Closure* done) {
    _impl->shutdown(done);
}

void Node::join(::google::protobuf::Closure* done) {
    _impl->join(done);
}

void Node::apply(const Task& task) {
    _impl->apply(task);
}

base::Status Node::list_peers(std::vector<PeerId>* peers, bool check_leader) {
    return _impl->list_peers(peers, check_leader);
}

base::Status Node::get_configuration(std::vector<PeerId>* old_peers,
                     std::vector<PeerId>* new_peers, std::string* user_conf) {
    return _impl->get_configuration(old_peers, new_peers, user_conf);
}

void Node::add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done) {
    _impl->add_peer(old_peers, peer, done);
}

void Node::add_peer(const PeerId& peer, Closure* done) {
    _impl->add_peer(peer, done);
}

void Node::add_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                    const std::string& user_conf, Closure* done) {
    _impl->add_peer(old_peers, peer, user_conf, done);
}

void Node::remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer, Closure* done) {
    _impl->remove_peer(old_peers, peer, done);
}

void Node::remove_peer(const PeerId& peer, Closure* done) {
    _impl->remove_peer(peer, done);
}

void Node::remove_peer(const std::vector<PeerId>& old_peers, const PeerId& peer,
                       const std::string& user_conf, Closure* done) {
    _impl->remove_peer(old_peers, peer, user_conf, done);
}

void Node::change_peers(const Configuration& new_peers, Closure* done) {
    _impl->change_peers(new_peers, done);
}

int Node::set_peer(const std::vector<PeerId>& old_peers, const std::vector<PeerId>& new_peers) {
    return _impl->set_peer(old_peers, new_peers);
}

int Node::set_peer(const std::vector<PeerId>& new_peers) {
    return _impl->set_peer(std::vector<PeerId>(), new_peers);
}

base::Status Node::reset_peers(const Configuration& new_peers) {
    return _impl->reset_peers(new_peers);
}

void Node::snapshot(Closure* done) {
    _impl->snapshot(done);
}

base::Status Node::vote(int election_timeout) {
    return _impl->vote(election_timeout);
}

base::Status Node::reset_election_timeout_ms(int election_timeout_ms) {
    return _impl->reset_election_timeout_ms(election_timeout_ms);
}

void Node::reset_election_timeout_ms(int election_timeout_ms, int max_clock_drift_ms) {
    _impl->reset_election_timeout_ms(election_timeout_ms, max_clock_drift_ms);
}

int Node::transfer_leadership_to(const PeerId& peer) {
    return _impl->transfer_leadership_to(peer);
}

base::Status Node::read_committed_user_log(const int64_t index, UserLog* user_log) {
    return _impl->read_committed_user_log(index, user_log);
}

void Node::get_status(NodeStatus* status) {
    return _impl->get_status(status);
}

void Node::enter_readonly_mode() {
    return _impl->enter_readonly_mode();
}

void Node::leave_readonly_mode() {
    return _impl->leave_readonly_mode();
}

bool Node::readonly() {
    return _impl->readonly();
}

int Node::polar_worker_index() const {
    return _impl->polar_worker_index();
}

// ------------- Learner
Learner::Learner(const GroupId& group_id, const PeerId& peer_id) {
    _impl = new LearnerImpl(group_id, peer_id);
}

Learner::~Learner() {
    if (_impl) {
        _impl->shutdown(NULL);
        google::protobuf::Closure* done = rpc::NewCallback(
                _impl, &LearnerImpl::Release);
        _impl->join(done);
        _impl = NULL;
    }
}

NodeId Learner::node_id() {
    return _impl->node_id();
}

int Learner::init(const NodeOptions& options) {
    return _impl->init(options);
}

void Learner::shutdown(Closure* done) {
    _impl->shutdown(done);
}

void Learner::join(::google::protobuf::Closure* done) {
    _impl->join(done);
}

void Learner::snapshot(Closure* done) {
    _impl->snapshot(done);
}

base::Status Learner::set_read_barrier(const int64_t index) {
    return _impl->set_read_barrier(index);
}

void Learner::get_status(NodeStatus* status) {
    return _impl->get_status(status);
}

void Learner::resume(Closure* done) {
    _impl->resume(done);
}

void Learner::reset(Closure* done) {
    _impl->reset(done);
}
// ------------- Iterator
void Iterator::next() {
    if (valid()) {
        _impl->next();
    }
}

bool Iterator::valid() const {
    return _impl->is_good() && _impl->entry()->type == ENTRY_TYPE_DATA;
}

int64_t Iterator::index() const { return _impl->index(); }

int64_t Iterator::term() const { return _impl->entry()->id.term; }

const base::IOBuf& Iterator::data() const {
    return _impl->entry()->data;
}

Closure* Iterator::done() const {
    return _impl->done();
}

void Iterator::set_error_and_rollback(size_t ntail, const base::Status* st) {
    return _impl->set_error_and_rollback(ntail, st);
}

// ------------- Default Implementation of StateMachine
StateMachine::~StateMachine() {}
void StateMachine::on_shutdown() {}

void StateMachine::on_snapshot_save(SnapshotWriter* writer, Closure* done) {
    (void)writer;
    CHECK(done);
    LOG(ERROR) << base::class_name_str(*this)
               << " didn't implement on_snapshot_save";
    done->status().set_error(-1, "%s didn't implement on_snapshot_save",
                                 base::class_name_str(*this).c_str());
    done->Run();
}

void StateMachine::on_snapshot_load(SnapshotReader* reader, Closure* done) {
    (void)reader;
    LOG(ERROR) << base::class_name_str(*this)
               << " didn't implement on_snapshot_load"
               << " while a snapshot is saved in " << reader->get_path();
    done->status().set_error(-1, "%s didn't implement on_snapshot_load",
                                 base::class_name_str(*this).c_str());
    done->Run();
}

int StateMachine::on_snapshot_fetched(SnapshotReader* reader) {
    (void)reader;
    LOG(ERROR) << base::class_name_str(*this)
               << " didn't implement on_snapshot_fetched"
               << " while a snapshot is saved in " << reader->get_path();
    return -1;
}

void StateMachine::on_leader_start() {}
void StateMachine::on_leader_start(int64_t) { return on_leader_start(); }
void StateMachine::on_leader_stop() {}
void StateMachine::on_leader_stop(const base::Status&) { return on_leader_stop(); }
void StateMachine::on_error(const Error& e) {
    LOG(ERROR) << "An error=" << e << " raised on StateMachine "
               << base::class_name_str(*this)
               << ", it's highly recommended to implement this interface"
                  " as raft stops working since some error ocurrs,"
                  " you should figure out the cause and repair or remove this node";
}

void StateMachine::on_configuration_committed(const Configuration& conf) {
    (void)conf;
    return;
}

void StateMachine::on_configuration_committed(const Configuration& conf, int64_t index) {
    (void)index;
    return on_configuration_committed(conf);
}

void StateMachine::on_configuration_committed(const Configuration& conf,
                                              const Configuration& old_conf,
                                              int64_t index, const std::string& user_conf) {
    (void)(user_conf);
    if (old_conf.size() == 0) {
        // Joint stage is not supposed to be noticeable by end users.
        on_configuration_committed(conf, index);
    }
}

void StateMachine::on_stop_following(const LeaderChangeContext&) {}
void StateMachine::on_start_following(const LeaderChangeContext&) {}
void StateMachine::on_reset() {}

void StateMachine::notify_configuration_change_stage1(std::string* user_conf,
                                              ::raft::Closure* done) {
    (void)user_conf;
    if (done) {
        done->Run();
    }
}

BootstrapOptions::BootstrapOptions()
    : last_log_index(0)
    , fsm(NULL)
    , node_owns_fsm(false)
    , usercode_in_pthread(false)
{}

int bootstrap(const BootstrapOptions& options) {
    global_init_once_or_die();
    NodeImpl* node = new NodeImpl();
    const int rc = node->bootstrap(options);
    node->shutdown(NULL);
    google::protobuf::Closure* done = rpc::NewCallback(
            node, &NodeImpl::Release);
    node->join(done);
    return rc;
}

}
