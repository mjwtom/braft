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
//          Xiong,Kai(xiongkai@baidu.com)

#include <errno.h>
#include <base/time.h>
#include <base/logging.h>
#include <base/file_util.h>                         // base::CreateDirectory
#include <gflags/gflags.h>
#include "raft/util.h"
#include "raft/protobuf_file.h"
#include "raft/local_storage.pb.h"
#include "raft/stable.h"

namespace raft {

DEFINE_int32(raft_stable_meta_write_batch, 128, 
             "Max number of tasks that can be written into db in a single batch");
RAFT_VALIDATE_GFLAG(raft_stable_meta_write_batch, rpc::PositiveInteger);

static BvarLatencyRecorder g_load_pb_stable_meta("raft_load_pb_stable_meta");
static BvarLatencyRecorder g_save_pb_stable_meta("raft_save_pb_stable_meta");
static BvarLatencyRecorder g_load_kv_stable_meta("raft_load_kv_stable_meta");
static BvarLatencyRecorder g_save_kv_stable_meta("raft_save_kv_stable_meta");
static BvarLatencyRecorder g_delete_kv_stable_meta("raft_delete_kv_stable_meta");

static BvarCounterRecorder g_save_kv_stable_meta_batch_counter(
                                    "raft_save_kv_stable_meta_batch_counter");

const char* FileBasedSingleStableStorage::_s_stable_meta = "stable_meta";

// StableStorageManager
//
// To manage all KVBasedMergedStableStorageImpl of all the raft instances.
// Typically nodes on the same disk will share a KVBasedMergedStableStorageImpl, 
// so we use disk_path as the KEY to manage all the instances.
class StableStorageManager {
public:
    static StableStorageManager* GetInstance() {
        return Singleton<StableStorageManager>::get();
    }

    scoped_refptr<KVBasedMergedStableStorageImpl> 
    register_stable_storage(const std::string& path) {
        scoped_refptr<KVBasedMergedStableStorageImpl> mss = get_stable_storage(path);
        if (mss != NULL) {
            return mss;
        }
        
        mss = new KVBasedMergedStableStorageImpl(path);
        {
            _ss_map.Modify(_add, path, mss);
        }
        return get_stable_storage(path); 
    }
    
    scoped_refptr<KVBasedMergedStableStorageImpl> 
    get_stable_storage(const std::string& path) {
        DoublyBufferedStableStorageMap::ScopedPtr ptr;
        CHECK_EQ(0, _ss_map.Read(&ptr));
        StableStorageMap::const_iterator it = ptr->find(path);
        if (it != ptr->end()) {
            return it->second;
        }
        return NULL;
    }
    
    // GC an invalid item in KVBasedMergedStableStorageImpl when destroying 
    // an raft instance on the disk for some reason, such as IO error.
    int remove_instance_from_stable_storage(const std::string& path, 
                                            const VersionedGroupId& v_group_id) {
        scoped_refptr<KVBasedMergedStableStorageImpl> mss = 
                                                get_stable_storage(path);
        if (mss == NULL) {
            return 0;
        }
        base::Status status = mss->delete_meta(v_group_id);
        if (!status.ok()) {
            return -1;
        }
        return 0;
    }

private:
    StableStorageManager() {};
    ~StableStorageManager() {};
    DISALLOW_COPY_AND_ASSIGN(StableStorageManager);
    friend struct DefaultSingletonTraits<StableStorageManager>;
    
    typedef std::map<std::string, scoped_refptr<KVBasedMergedStableStorageImpl> > 
                                                                StableStorageMap;
    typedef base::DoublyBufferedData<StableStorageMap> DoublyBufferedStableStorageMap;
    
    static size_t _add(StableStorageMap& m, const std::string& path, 
                       const scoped_refptr<KVBasedMergedStableStorageImpl>& mss) {
        std::pair<StableStorageMap::const_iterator, bool> iter = 
                                        m.insert(std::make_pair(path, mss));
        if (iter.second) {
            return 1lu;
        }
        return 0lu;
    }

    static size_t _remove(StableStorageMap& m, const std::string& path) {
        return m.erase(path);
    }

    DoublyBufferedStableStorageMap _ss_map;
};

#define global_mss_manager StableStorageManager::GetInstance()

// MixedStableStorage
//
// Uri of Multi-raft using mixed stable storage is: 
//     local-mixed://merged_path={merged_path}&&single_path={single_path}
int MixedStableStorage::parse_mixed_path(const std::string& uri, 
                                         std::string& merged_path, 
                                         std::string& single_path) {
    // here uri has removed protocol already, check just for safety
    base::StringPiece copied_uri(uri);
    size_t pos = copied_uri.find("://");
    if (pos != base::StringPiece::npos) {
        copied_uri.remove_prefix(pos + 3/* length of '://' */);
    }
    
    pos = copied_uri.find("merged_path=");
    if (pos == base::StringPiece::npos) {
        return -1;
    }
    copied_uri.remove_prefix(pos + 12/* length of 'merged_path=' */);
    
    pos = copied_uri.find("&&single_path=");
    if (pos == base::StringPiece::npos) {
        return -1;
    }
    merged_path = copied_uri.substr(0, pos).as_string();
    copied_uri.remove_prefix(pos + 14/* length of '&&single_path=' */);
    single_path = copied_uri.as_string();
    
    return 0;
}

MixedStableStorage::MixedStableStorage(const std::string& uri) {
    _is_inited = false;
    _is_bad = false;

    std::string merged_path;
    std::string single_path;

    int ret = parse_mixed_path(uri, merged_path, single_path);
    if (ret != 0) {
        LOG(ERROR) << "node parse mixed path failed, uri " << uri;
        _is_bad = true; 
    } else {
        // Use single_path as the path of MixedStableStorage as it usually 
        // contains group_id
        _path = single_path;

        _single_impl = new FileBasedSingleStableStorage(single_path);
        _merged_impl = global_mss_manager->register_stable_storage(merged_path);

        if (!_single_impl || !_merged_impl) {
            // Both _single_impl and _merged_impl are needed in MixedStableStorage
            LOG(ERROR) << "MixedStableStorage failed to create both"
                            " sub stable storage, uri " << uri;
            _is_bad = true;
        }
    }
}

MixedStableStorage::~MixedStableStorage() {
    if (_single_impl) {
        delete _single_impl;
        _single_impl = NULL;
    } 
    if (_merged_impl) {
        _merged_impl = NULL;
    }
}

base::Status MixedStableStorage::init() {
    base::Status status;
    if (_is_inited) {
        return status;
    }
    // check bad
    if (_is_bad) {
        status.set_error(EINVAL, "MixedStableStorage is bad, path %s", 
                         _path.c_str());
        return status;
    }
    
    // both _single_impl and _merged_impl are valid since _is_bad is false
    status = _single_impl->init();
    if (!status.ok()) {
        LOG(ERROR) << "Init Mixed stable storage failed because init Single"
                      " stable storage failed, path " << _path;
        return status;
    }

    status = _merged_impl->init();
    if (!status.ok()) {
        LOG(ERROR) << "Init Mixed stable storage failed because init merged"
                      " stable storage failed, path " << _path;
        return status;
    }

    _is_inited = true;
    LOG(NOTICE) << "Succeed to init MixedStableStorage, path: " << _path;
    return status;
}

class StableMetaClosure : public Closure {
public:
    StableMetaClosure(const int64_t term, const PeerId& votedfor, 
                      const VersionedGroupId& vgid, const std::string& path) 
        : _term(term)
        , _votedfor(votedfor)
        , _vgid(vgid)
        , _path(path)
        , _start_time_us(base::cpuwide_time_us()) 
    {}

    ~StableMetaClosure() {}

    void Run() {
        if (!status().ok()) {
            LOG(ERROR) << "Failed to write stable meta into db, group " << _vgid
                       << " term " << _term << " vote for " << _votedfor
                       << ", path: " << _path << ", error: " << status();
        } else {
            int64_t u_elapsed = base::cpuwide_time_us() - _start_time_us;
            g_save_kv_stable_meta << u_elapsed;
            LOG(INFO) << "Saved merged stable meta, path " << _path
                      << " group " << _vgid
                      << " term " << _term
                      << " votedfor " << _votedfor
                      << " time: " << u_elapsed;  
        }
        
        _sync.Run();
    }

    void wait() { _sync.wait(); }

private:
    int64_t _term;
    PeerId _votedfor;
    VersionedGroupId _vgid;
    std::string _path;
    int64_t _start_time_us;
    SynchronizedClosure _sync;
};

base::Status MixedStableStorage::set_term_and_votedfor(const int64_t term, 
                        const PeerId& peer_id, const VersionedGroupId& group) {
    base::Status status;
    if (!_is_inited) {
        LOG(WARNING) << "MixedStableStorage not init, path: " << _path;
        status.set_error(EINVAL, "MixedStableStorage of group %s not init, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }

    status = _single_impl->set_term_and_votedfor(term, peer_id, group);
    if (!status.ok()) {
        LOG(WARNING) << "node " << group 
            << " single stable storage failed to set_term_and_votedfor, path: "
            << _path;
        return status;
    }

    StableMetaClosure done(term, peer_id, group, _path);
    _merged_impl->set_term_and_votedfor(term, peer_id, group, &done);
    done.wait();
    return done.status();
}

// [NOTICE] Conflict cases may occur in this mode, it's important to ensure consistency
// 1. Single is newer than Merged:
//      case 1: upgrade storage from Single to Mixed, data in Merged is stale
//      case 2: last set_term_and_votedfor succeeded in Single but failed in Merged
// 2. Merged is newer than Single:
//      case: downgrade storage from Merged to Mixed, data in Single is stale
base::Status MixedStableStorage::get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                                       const VersionedGroupId& group) {
    base::Status status;
    if (!_is_inited) {
        LOG(WARNING) << "MixedStableStorage not init, path: " << _path;
        status.set_error(EINVAL, "MixedStableStorage of group %s not init, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }
    
    // If data from single stable storage is newer than that from merged stable storage,
    // merged stable storage should catch up the newer data to ensure safety; Vice versa.
    bool single_newer_than_merged = false;
    
    int64_t term_1;
    PeerId peer_id_1;
    status = _single_impl->get_term_and_votedfor(&term_1, &peer_id_1, group);
    if (!status.ok()) {
        LOG(WARNING) << "node " << group 
            << " single stable storage failed to get_term_and_votedfor, path: "
            << _path << ", error: " << status.error_cstr();
        return status;
    }
    
    int64_t term_2;
    PeerId peer_id_2;
    status = _merged_impl->get_term_and_votedfor(&term_2, 
                                                 &peer_id_2, group);
    if (!status.ok()) {
        LOG(WARNING) << "node " << group
            << " merged stable storage failed to get_term_and_votdfor,"
            << " path: " << _path << ", error: " << status.error_cstr();
        return status;
    // check consistency of two stable storage
    } else if (term_1 == term_2 && peer_id_1 == peer_id_2) {
        // if two results are consistent, just return success
        *term = term_1;
        *peer_id = peer_id_1;
        return status;
    }

    // this case is theoretically impossible, pay much attention to it if happens
    if (term_1 == term_2 && peer_id_1 != ANY_PEER 
                         && peer_id_2 != ANY_PEER) {
        CHECK(false) << "Unexpected conflict when mixed stable storage of " 
            << group << " get_term_and_votedfor, the same term " << term_1
            << ", but different non-empty votdfor(" << peer_id_1 
            << " from single stable storage and " << peer_id_2
            << " from merged stable storage), path: " << _path;
        status.set_error(EINVAL, "Unexpected conflict");
        return status;
    }

    // if two results are not consistent, check out which is newer and catch up
    // data for the stale one 
    single_newer_than_merged = term_1 > term_2 || 
        (term_1 == term_2 && peer_id_1 != ANY_PEER && peer_id_2 == ANY_PEER);
    
    if (single_newer_than_merged) {
        *term = term_1;
        *peer_id = peer_id_1;
        StableMetaClosure done(*term, *peer_id, group, _path);
        _merged_impl->set_term_and_votedfor(*term, *peer_id, group, &done);
        done.wait();
        status = done.status();
        if (!status.ok()) {
            LOG(WARNING) << "node " << group 
                << " merged stable storage failed to set term " << *term
                << " and vote for peer " << *peer_id
                << " when catch up data, path " << _path
                << ", error: " << status.error_cstr();
            return status;
        }
        LOG(NOTICE) << "node " << group 
            << " merged stable storage succeed to set term " << *term
            << " and vote for peer " << *peer_id
            << " when catch up data, path " << _path;
    } else {
        *term = term_2;
        *peer_id = peer_id_2;
        status = _single_impl->set_term_and_votedfor(*term, *peer_id, group);
        if (!status.ok()) {
            LOG(WARNING) << "node " << group 
                << " single stable storage failed to set term " << *term
                << " and vote for peer " << *peer_id
                << " when catch up data, path " << _path
                << ", error: " << status.error_cstr();
            return status;
        } 
        LOG(NOTICE) << "node " << group 
            << " single stable storage succeed to set term " << *term
            << " and vote for peer " << *peer_id
            << " when catch up data, path " << _path;
    }  

    return status;
}

StableStorage* MixedStableStorage::new_instance(const std::string& uri) const {
    return new MixedStableStorage(uri);
}

base::Status MixedStableStorage::gc_instance(const std::string& uri, 
                                             const VersionedGroupId& vgid) const {
    base::Status status;
    std::string merged_path;
    std::string single_path;

    int ret = parse_mixed_path(uri, merged_path, single_path);
    if (ret != 0) {
        LOG(WARNING) << "node parse mixed path failed, uri " << uri;
        status.set_error(EINVAL, "Group %s failed to parse mixed path, uri %s", 
                         vgid.c_str(), uri.c_str());
        return status;
    }
    if (0 != gc_dir(single_path)) {
        LOG(WARNING) << "Group " << vgid << " failed to gc path " << single_path;
        status.set_error(EIO, "Group %s failed to gc path %s", 
                         vgid.c_str(), single_path.c_str());
        return status;
    }
    if (0 != global_mss_manager->
                    remove_instance_from_stable_storage(merged_path, vgid)) {
        LOG(ERROR) << "Group " << vgid << " failed to gc kv from path: " 
                   << merged_path;
        status.set_error(EIO, "Group %s failed to gc kv from path %s", 
                         vgid.c_str(), merged_path.c_str());
        return status;
    }
    LOG(INFO) << "Group " << vgid << " succeed to gc from single path: " 
              << single_path << " and merged path: " << merged_path;
    return status; 
}

// FileBasedSingleStableStorage
base::Status FileBasedSingleStableStorage::init() {
    base::Status status;
    if (_is_inited) {
        return status;
    }

    base::FilePath dir_path(_path);
    base::File::Error e;
    if (!base::CreateDirectoryAndGetError(
                dir_path, &e, FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        status.set_error(e, "Fail to create dir when init SingleStableStorage, "
                         "path: %s", _path.c_str());
        return status;
    }

    int ret = load();
    if (ret != 0) {
        LOG(ERROR) << "Fail to load pb meta when init single stable storage"
                      ", path: " << _path;
        status.set_error(EIO, "Fail to load pb meta when init stabel storage"
                         ", path: %s", _path.c_str());
        return status;
    }

    _is_inited = true;
    return status;
}

base::Status FileBasedSingleStableStorage::set_term_and_votedfor(const int64_t term, 
            const PeerId& peer_id, const VersionedGroupId&) {
    base::Status status;
    if (!_is_inited) {
        status.set_error(EINVAL, "SingleStableStorage not init, path: %s", 
                         _path.c_str());
        return status;
    }   
    _term = term;
    _votedfor = peer_id;
    if (save() != 0) {
        status.set_error(EIO, "SingleStableStorage failed to save pb meta, path: %s", 
                         _path.c_str());
        return status;
    }
    return status;
}

base::Status FileBasedSingleStableStorage::get_term_and_votedfor(int64_t* term, 
                                PeerId* peer_id, const VersionedGroupId& group) {
    base::Status status;
    if (!_is_inited) {
        status.set_error(EINVAL, "SingleStableStorage not init, path: %s", 
                         _path.c_str());
        return status;
    }   
    *term = _term;
    *peer_id = _votedfor;
    return status;
}

int FileBasedSingleStableStorage::load() {
    base::Timer timer;
    timer.start();

    std::string path(_path);
    path.append("/");
    path.append(_s_stable_meta);

    ProtoBufFile pb_file(path);

    StablePBMeta meta;
    int ret = pb_file.load(&meta);
    if (ret == 0) {
        _term = meta.term();
        ret = _votedfor.parse(meta.votedfor());
    } else if (errno == ENOENT) {
        ret = 0;
    } else {
        PLOG(ERROR) << "Fail to load meta from " << path;
    }
    
    timer.stop();
    // Only reload process will load stable meta of raft instances,
    // reading just go through memory
    g_load_pb_stable_meta << timer.u_elapsed();
    LOG(INFO) << "Loaded single stable meta, path " << _path
              << " term " << _term 
              << " votedfor " << _votedfor.to_string() 
              << " time: " << timer.u_elapsed();
    return ret;
}

int FileBasedSingleStableStorage::save() {
    base::Timer timer;
    timer.start();

    StablePBMeta meta;
    meta.set_term(_term);
    meta.set_votedfor(_votedfor.to_string());

    std::string path(_path);
    path.append("/");
    path.append(_s_stable_meta);

    ProtoBufFile pb_file(path);
    int ret = pb_file.save(&meta, raft_sync_meta());
    PLOG_IF(ERROR, ret != 0) << "Fail to save meta to " << path;

    timer.stop();
    g_save_pb_stable_meta << timer.u_elapsed();
    LOG(INFO) << "Saved single stable meta, path " << _path
              << " term " << _term 
              << " voted for " << _votedfor.to_string() 
              << " time: " << timer.u_elapsed();
    return ret;
}

StableStorage* FileBasedSingleStableStorage::new_instance(
                                        const std::string& uri) const {
    return new FileBasedSingleStableStorage(uri);
}

base::Status FileBasedSingleStableStorage::gc_instance(const std::string& uri, 
                                        const VersionedGroupId& vgid) const {
    base::Status status;
    if (0 != gc_dir(uri)) {
        LOG(WARNING) << "Group " << vgid << " failed to gc single stable storage"
                        ", path: " << uri;
        status.set_error(EIO, "Group %s failed to gc single stable storage"
                         ", path: %s", vgid.c_str(), uri.c_str());
        return status;
    }
    LOG(INFO) << "Group " << vgid << " succeed to gc single stable storage"
                 ", path: " << uri;
    return status;
}

// KVBasedMergedStableStorage
KVBasedMergedStableStorage::KVBasedMergedStableStorage(const std::string& path) {
    _merged_impl = global_mss_manager->register_stable_storage(path);
}

KVBasedMergedStableStorage::~KVBasedMergedStableStorage() {
    if (_merged_impl) {
        _merged_impl = NULL;
    }
}

base::Status KVBasedMergedStableStorage::init() {
    return _merged_impl->init();
};

base::Status KVBasedMergedStableStorage::set_term_and_votedfor(const int64_t term, 
            const PeerId& peer_id, const VersionedGroupId& group) {
    StableMetaClosure done(term, peer_id, group, "");
    _merged_impl->set_term_and_votedfor(term, peer_id, group, &done);
    done.wait();
    
    return done.status();
};

base::Status KVBasedMergedStableStorage::get_term_and_votedfor(int64_t* term, 
            PeerId* peer_id, const VersionedGroupId& group) {
    return _merged_impl->get_term_and_votedfor(term, peer_id, group);
};

StableStorage* KVBasedMergedStableStorage::new_instance(
                                    const std::string& uri) const {
    return new KVBasedMergedStableStorage(uri);
}

base::Status KVBasedMergedStableStorage::gc_instance(const std::string& uri,
            const VersionedGroupId& vgid) const {
    base::Status status;
    if (0 != global_mss_manager->
                remove_instance_from_stable_storage(uri, vgid)) {
        LOG(WARNING) << "Group " << vgid << " failed to gc kv, path: " << uri;
        status.set_error(EIO, "Group %s failed to gc kv in path: %s", 
                         vgid.c_str(), uri.c_str());
        return status;
    }
    LOG(INFO) << "Group " << vgid << " succeed to gc kv, path: " << uri;
    return status;
};

base::Status KVBasedMergedStableStorage::delete_meta(
                                    const VersionedGroupId& group) {
    return _merged_impl->delete_meta(group);
};

// KVBasedMergedStableStorageImpl
base::Status KVBasedMergedStableStorageImpl::init() {
    _mutex.reset(FLAGS_raft_use_polar, current_polar_idx());
    std::unique_lock<Mutex> lck(_mutex); 

    base::Status status;
    if (_is_inited) {
        return status;
    }
    
    base::FilePath dir_path(_path);
    base::File::Error e;
    if (!base::CreateDirectoryAndGetError(
                dir_path, &e, FLAGS_raft_create_parent_directories)) {
        lck.unlock();
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        status.set_error(e, "Fail to create dir when init MergedStableStorage, "
                         "path: %s", _path.c_str());
        return status;
    }

    leveldb::Options options;
    options.create_if_missing = true;
    //options.error_if_exists = true;   

    leveldb::Status st;
    st = leveldb::DB::Open(options, _path.c_str(), &_db);
    if (!st.ok()) {
        lck.unlock();
        LOG(ERROR) << "Fail to open db: " << st.ToString() << " path: " << _path;
        status.set_error(EIO, "Fail to open db, path: %s, error: %s", 
                         _path.c_str(), st.ToString().c_str());
        return status;
    }   
    
    // start execution_queue
    bthread::ExecutionQueueOptions execq_opt;
    execq_opt.bthread_attr = BTHREAD_ATTR_NORMAL;
    //execq_opt.max_tasks_size = 256;
    if (raft_execution_queue_start(&_queue_id,
                                       &execq_opt,
                                       KVBasedMergedStableStorageImpl::run,
                                       this) != 0) {
        status.set_error(EINVAL, "Fail to start execution_queue, path: %s", 
                         _path.c_str());
        return status;
    }

    _is_inited = true;
    return status;
}

void KVBasedMergedStableStorageImpl::run_tasks(leveldb::WriteBatch& updates, 
                                               Closure* dones[], size_t size) {
    g_save_kv_stable_meta_batch_counter << size; 
    
    leveldb::WriteOptions options;
    options.sync = raft_sync_meta(); 
    leveldb::Status st = _db->Write(options, &updates);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to write batch into db, path: " << _path
                   << ", error: " << st.ToString();
        base::Status status;
        status.set_error(EIO, "MergedStableStorage failed to write batch"
                              ", path: %s, error: %s", 
                              _path.c_str(), st.ToString().c_str());
        for (size_t i = 0; i < size; ++i) {
            dones[i]->status() = status; 
            raft_run_closure_background_nosig(dones[i]);
        }
    } else {
        for (size_t i = 0; i < size; ++i) {
            raft_run_closure_background_nosig(dones[i]);
        } 
    }
    raft_thread_flush();
}

int KVBasedMergedStableStorageImpl::run(void* meta, 
                                bthread::TaskIterator<WriteTask>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }

    KVBasedMergedStableStorageImpl* mss = (KVBasedMergedStableStorageImpl*)meta;
    const size_t batch_size = FLAGS_raft_stable_meta_write_batch;
    size_t cur_size = 0;
    leveldb::WriteBatch updates;
    DEFINE_SMALL_ARRAY(Closure*, dones, batch_size, 256);

    for (; iter; ++iter) {
        if (cur_size == batch_size) {
            mss->run_tasks(updates, dones, cur_size); 
            updates.Clear();
            cur_size = 0;
        }

        const int64_t term = iter->term;
        const PeerId votedfor = iter->votedfor;
        const VersionedGroupId vgid = iter->vgid;
        Closure* done = iter->done; 
        // get key and value 
        leveldb::Slice key(vgid.data(), vgid.size());  
        StablePBMeta meta;
        meta.set_term(term);
        meta.set_votedfor(votedfor.to_string());
        std::string meta_string;
        meta.SerializeToString(&meta_string);
        leveldb::Slice value(meta_string.data(), meta_string.size());

        updates.Put(key, value);
        dones[cur_size++] = done;
    }
    if (cur_size > 0) {
        mss->run_tasks(updates, dones, cur_size);
        updates.Clear();
        cur_size = 0;
    }
    return 0;
}

void KVBasedMergedStableStorageImpl::set_term_and_votedfor(
                                const int64_t term, const PeerId& peer_id, 
                                const VersionedGroupId& group, Closure* done) {
    if (!_is_inited) {
        done->status().set_error(EINVAL, "MergedStableStorage of group %s not"
                                 " init, path: %s", group.c_str(), _path.c_str());
        return raft_run_closure_background(done);
    }
    
    WriteTask task;
    task.term = term;
    task.votedfor = peer_id;
    task.vgid = group;
    task.done = done;
    if (raft_execution_queue_execute(_queue_id, task) != 0) {
        task.done->status().set_error(EIO, "Failed to put task into queue");
        return raft_run_closure_background(task.done);
    }
}

base::Status KVBasedMergedStableStorageImpl::get_term_and_votedfor(int64_t* term, 
                                PeerId* peer_id, const VersionedGroupId& group) {
    base::Status status;
    if (!_is_inited) {
        status.set_error(EINVAL, "MergedStableStorage of group %s not init, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }
    
    base::Timer timer;
    timer.start();
    leveldb::Slice key(group.data(), group.size());
    std::string value;
    leveldb::Status st = _db->Get(leveldb::ReadOptions(), key, &value);
    if (st.IsNotFound()) {
        // Not exist in db, set initial term 1 and votedfor 0.0.0.0:0:0
        *term = 1;
        *peer_id = ANY_PEER;
        StableMetaClosure done(*term, *peer_id, group, _path);
        set_term_and_votedfor(*term, *peer_id, group, &done);
        done.wait();
        status = done.status();
        if (!status.ok()) {
            LOG(ERROR) << "node " << group
                     << " failed to set initial term and votedfor when first time init"
                     << ", path " << _path
                     << ", error " << status.error_cstr();
            return status;
        }
        LOG(NOTICE) << "node " << group
                 << " succeed to set initial term and votedfor when first time init"
                 << ", path " << _path;
        return status;
    } else if (!st.ok()) {
        LOG(ERROR) << "node " << group
                << " failed to get value from db, path " << _path
                << ", error " << st.ToString().c_str();
        status.set_error(EIO, "MergedStableStorage of group %s failed to"
                         "get value from db, path: %s, error: %s", 
                         group.c_str(), _path.c_str(), st.ToString().c_str());
        return status;
    }
   
    // TODO replace pb
    StablePBMeta meta;
    meta.ParseFromString(value);
    *term = meta.term();
    if (0 != peer_id->parse(meta.votedfor())) {
        LOG(ERROR) << "node " << group 
                   << " failed to parse votedfor when loading meta from db, path " 
                   << _path;
        status.set_error(EINVAL, "MergedStableStorage of group %s failed to"
                         " parse votedfor when loading meta from db, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }

    timer.stop(); 
    g_load_kv_stable_meta << timer.u_elapsed(); 
    LOG(INFO) << "Loaded merged stable meta, path " << _path
              << " group " << group
              << " term " << *term
              << " votedfor " << *peer_id
              << " time: " << timer.u_elapsed();
    return status;
}

base::Status KVBasedMergedStableStorageImpl::delete_meta(
                                        const VersionedGroupId& group) { 
    base::Status status;
    if (!_is_inited) {
        status.set_error(EINVAL, "MergedStableStorage of group %s not init, path: %s", 
                         group.c_str(), _path.c_str());
        return status;
    }
    
    base::Timer timer;
    timer.start();
    leveldb::WriteOptions options;
    options.sync = raft_sync_meta();
    
    leveldb::Slice key(group.data(), group.size()); 
    leveldb::Status st = _db->Delete(options, key);
    if (!st.ok()) {
        LOG(ERROR) << "Fail to delete meta info from db, group " << group;
        status.set_error(EIO, "MergedStableStorage failed to delete group %s"
                         ", path: %s, error: %s",
                         group.c_str(), _path.c_str(), st.ToString().c_str());
        return status;
    }

    timer.stop();
    g_delete_kv_stable_meta << timer.u_elapsed(); 
    LOG(INFO) << "Deleted merged stable meta, path " << _path
              << " group " << group
              << " time: " << timer.u_elapsed();
    return status;
}

} // namespace raft
