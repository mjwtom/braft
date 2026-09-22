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

#ifndef PUBLIC_RAFT_STABLE_H
#define PUBLIC_RAFT_STABLE_H

#include <base/memory/ref_counted.h>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <bthread/execution_queue.h>
#include "raft/rtc_helper.h"                       // raft::Mutex
#include "raft/storage.h"

namespace raft {

class FileBasedSingleStableStorage;
class KVBasedMergedStableStorageImpl;

class MixedStableStorage : public StableStorage { 

public:
    explicit MixedStableStorage(const std::string& path);
    MixedStableStorage() {}
    virtual ~MixedStableStorage();

    // init stable storage
    virtual base::Status init();
    
    // set term and votedfor information
    virtual base::Status set_term_and_votedfor(const int64_t term, 
                const PeerId& peer_id, const VersionedGroupId& group);

    // get term and votedfor information
    virtual base::Status get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                               const VersionedGroupId& group);

    StableStorage* new_instance(const std::string& uri) const;
    
    base::Status gc_instance(const std::string& uri, 
                             const VersionedGroupId& vgid) const;
    
    bool is_bad() { return _is_bad; }

private:

    static int parse_mixed_path(const std::string& uri, std::string& merged_path, 
                                std::string& single_path); 

    bool _is_inited;
    bool _is_bad;
    std::string _path;
    // Origin stable storage for each raft node 
    FileBasedSingleStableStorage* _single_impl;
    // Merged stable storage for raft nodes on the same disk
    scoped_refptr<KVBasedMergedStableStorageImpl> _merged_impl;
};

// Manage meta info of ONLY ONE raft instance
class FileBasedSingleStableStorage : public StableStorage { 

public:
    explicit FileBasedSingleStableStorage(const std::string& path)
        : _is_inited(false), _path(path), _term(1) {}
    FileBasedSingleStableStorage() {}
    virtual ~FileBasedSingleStableStorage() {}

    // init stable storage
    virtual base::Status init();
    
    // set term and votedfor information
    virtual base::Status set_term_and_votedfor(const int64_t term, const PeerId& peer_id, 
                                       const VersionedGroupId& group);
    
    // get term and votedfor information
    virtual base::Status get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                               const VersionedGroupId& group);

    StableStorage* new_instance(const std::string& uri) const;
    
    base::Status gc_instance(const std::string& uri,
                             const VersionedGroupId& vgid) const;
    
private:
    
    static const char* _s_stable_meta;
    int load();
    int save();

    bool _is_inited;
    std::string _path;
    int64_t _term;
    PeerId _votedfor;
};

// Manage meta info of A BATCH of raft instances who share the same disk_path prefix 
class KVBasedMergedStableStorage : public StableStorage { 

public:
    explicit KVBasedMergedStableStorage(const std::string& path);
    KVBasedMergedStableStorage() {}

    virtual ~KVBasedMergedStableStorage();

    // init stable storage
    virtual base::Status init();
    
    // set term and votedfor information
    virtual base::Status set_term_and_votedfor(const int64_t term, 
                                               const PeerId& peer_id, 
                                               const VersionedGroupId& group);

    // get term and votedfor information
    virtual base::Status get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                               const VersionedGroupId& group);

    StableStorage* new_instance(const std::string& uri) const;
    
    base::Status gc_instance(const std::string& uri,
                             const VersionedGroupId& vgid) const;
    
    // GC meta info of a raft instance indicated by |group|
    virtual base::Status delete_meta(const VersionedGroupId& group);

private:

    scoped_refptr<KVBasedMergedStableStorageImpl> _merged_impl;
};

// Inner class of KVBasedMergedStableStorage
class KVBasedMergedStableStorageImpl : 
            public RaftRefCounted<KVBasedMergedStableStorageImpl> {
friend class scoped_refptr<KVBasedMergedStableStorageImpl>;

public:
    explicit KVBasedMergedStableStorageImpl(const std::string& path)
        : RaftRefCounted<KVBasedMergedStableStorageImpl>(false)
        , _is_inited(false)
        , _path(path) 
        , _db(nullptr)
    {}
    KVBasedMergedStableStorageImpl() : RaftRefCounted<KVBasedMergedStableStorageImpl>(false)
    {}
    virtual ~KVBasedMergedStableStorageImpl() {
        if (_db) {
            delete _db;
            _db = nullptr;
        }
    }

    struct WriteTask {
        //TaskType type;
        int64_t term;
        PeerId votedfor;
        VersionedGroupId vgid;
        Closure* done;
    };

    // init stable storage
    virtual base::Status init();

    // set term and votedfor information
    virtual void set_term_and_votedfor(const int64_t term, const PeerId& peer_id, 
                                const VersionedGroupId& group, Closure* done);

    // get term and votedfor information
    // [NOTICE] If some new instance init stable storage for the first time,
    // no record would be found from db, in which case initial term and votedfor
    // will be set.
    // Initial term: 1   Initial votedfor: ANY_PEER
    virtual base::Status get_term_and_votedfor(int64_t* term, PeerId* peer_id, 
                                               const VersionedGroupId& group);

    // GC meta info of a raft instance indicated by |group|
    virtual base::Status delete_meta(const VersionedGroupId& group);

private:
    friend class RaftRefCounted<KVBasedMergedStableStorageImpl>;
   
    static int run(void* meta, bthread::TaskIterator<WriteTask>& iter);

    void run_tasks(leveldb::WriteBatch& updates, Closure* dones[], size_t size);

    bthread::ExecutionQueueId<WriteTask> _queue_id;

    Mutex _mutex;
    bool _is_inited;
    std::string _path;
    leveldb::DB* _db;
};

}

#endif //~PUBLIC_RAFT_STABLE_H
