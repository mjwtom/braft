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
//          Xiong,Kai(xiongkai@baidu.com)

#ifndef PUBLIC_RAFT_LOG_H
#define PUBLIC_RAFT_LOG_H

#include <vector>
#include <map>
#include <base/memory/ref_counted.h>
#include <base/atomicops.h>
#include <base/iobuf.h>
#include <base/logging.h>
#include "raft/log_entry.h"
#include "raft/storage.h"
#include "raft/util.h"
#include "raft/rtc_helper.h"

namespace raft {

class BAIDU_CACHELINE_ALIGNMENT Segment 
        : public RaftRefCounted<Segment> {
public:
    Segment(const std::string& path, const int64_t first_index, int checksum_type)
        : _path(path), _bytes(0),
        _mutex(FLAGS_raft_use_polar),
        _fd(-1), _is_open(true),
        _first_index(first_index), _last_index(first_index - 1),
        _checksum_type(checksum_type)
    {}
    Segment(const std::string& path, const int64_t first_index, const int64_t last_index,
            int checksum_type)
        : _path(path), _bytes(0),
        _mutex(FLAGS_raft_use_polar),
        _fd(-1), _is_open(false),
        _first_index(first_index), _last_index(last_index),
        _checksum_type(checksum_type)
    {}

    struct EntryHeader {
        int64_t term;
        int type;
        int checksum_type;
        uint32_t data_len;
        uint32_t data_checksum;
    };

    // create open segment
    int create();

    // load open or closed segment
    // open fd, load index, truncate uncompleted entry
    int load(ConfigurationManager* configuration_manager);

    // serialize entry, and append to open segment
    void append(const LogEntry* entry, IOMetric* metric, Closure* done);

    // get last_log_id after data flushed
    void last_log_id(LogId* log_id, Closure* done);

    // get entry by index
    void get(const int64_t index, LogEntry** entry, Closure* done);

    // get log offset on file
    int64_t get_offset(const int64_t index) const;

    // get entry's term by index
    int64_t get_term(const int64_t index) const;

    // close open segment
    int close(bool will_sync = true);

    // sync open segment
    int sync(bool will_sync);

    // force sync a segment
    int force_sync();

    // unlink segment
    int unlink();

    // truncate segment to last_index_kept
    void truncate(const int64_t last_index_kept, Closure* done);

    bool is_open() const {
        return _is_open;
    }

    int64_t bytes() const {
        return _bytes;
    }

    int64_t first_index() const {
        return _first_index;
    }

    int64_t last_index() const {
        return _last_index.load(base::memory_order_consume);
    }

    std::string file_name();
private:
friend class RaftRefCounted<Segment>;
    ~Segment() {
        if (_fd >= 0) {
            LOG(INFO) << "Close segment " << _path << " with fd=" << _fd;
            ::close(_fd);
            _fd = -1;
        }
    }

    class InFlightIoClosure : public Closure {
    public:
        InFlightIoClosure(off_t offset, const base::IOBuf& data, Closure* done) :
            _offset(offset), _data(data), _done(done) {
            status().set_error(EINPROGRESS, "io in process");
        }
        void Run() override {
            std::unique_ptr<InFlightIoClosure> guard(this);
            if (_done) {
                _done->status().swap(status());
                _done->Run();
            }
        }
        const base::IOBuf& data() { return _data; }
        off_t offset() { return _offset; }
    private:
        off_t _offset;
        base::IOBuf _data;
        Closure* _done;
    };
    class PendingIoQueue {
    public:
        PendingIoQueue(Segment* segment) : _segment(segment),
            _truncate_size(-1), _done(nullptr) {}
        InFlightIoClosure* get_pending(off_t offset) const {
            CHECK_EQ(_pending.size(), _map.size());
            auto iter = _map.find(offset);
            if (iter == _map.end()) {
                return nullptr;
            }
            return iter->second;
        }
        bool empty() const {
            CHECK_EQ(_pending.size(), _map.size());
            return _pending.empty();
        }
        InFlightIoClosure* front() const {
            CHECK(!empty());
            return _pending.front();
        }
        void pop_front() {
            CHECK(!empty());
            InFlightIoClosure* io = _pending.front();
            CHECK_EQ(static_cast<int>(_map.erase(io->offset())), 1) << io->offset() << " must exist";
            _pending.pop_front();
            CHECK_EQ(_pending.size(), _map.size());
        }
        void pend(InFlightIoClosure* io) {
            _pending.push_back(io);
            CHECK(_map.insert(std::make_pair(io->offset(), io)).second);
            CHECK_EQ(_pending.size(), _map.size());
        }
        std::deque<InFlightIoClosure*>& pending() {
            return _pending;
        }
        void set_operation(int64_t truncate_size, Closure* done) {
            CHECK(!_done);
            CHECK_LT(_truncate_size, 0);
            _truncate_size = truncate_size;
            _done = done;
        }
        bool is_truncate() {
            return _truncate_size >= 0;
        }
        bool has_operation() {
            return _done != nullptr;
        }
        int64_t truncate_size() { return _truncate_size; }
        Closure* release_closure() {
            Closure* saved = _done;
            _done = nullptr;
            _truncate_size = -1;
            return saved;
        }
    private:
        scoped_refptr<Segment> _segment; // protect segment from deleting
        std::deque<InFlightIoClosure*> _pending;
        std::unordered_map<off_t, InFlightIoClosure*> _map;
        int64_t _truncate_size;
        Closure* _done;
    };
    void append_done(scoped_refptr<Segment> deref, InFlightIoClosure* done, const base::Status& st);
    void truncate_file(const int64_t truncate_size, Closure* done);

    struct LogMeta {
        off_t offset;
        size_t length;
        int64_t term;
    };

    int _load_entry(off_t offset, EntryHeader *head, base::IOBuf *body,
                    size_t size_hint) const;
    void _read_data_done(scoped_refptr<Segment> deref, base::IOPortal* data,
            int64_t index, int64_t term,
            LogEntry** entry, Closure* done, const base::Status& st);

    int _get_meta(int64_t index, LogMeta* meta) const;

    int _truncate_meta_and_get_last(int64_t last);

    std::string _path;
    int64_t _bytes;
    mutable Mutex _mutex;
    int _fd;
    bool _is_open;
    const int64_t _first_index;
    base::atomic<int64_t> _last_index; // index stored
    int _checksum_type;
    std::vector<std::pair<int64_t/*offset*/, int64_t/*term*/> > _offset_and_term;
    std::deque<PendingIoQueue> _pending; // we need deque since there are possibility truncate follow io follow truncate
};

// LogStorage use segmented append-only file, all data in disk, all index in memory.
// append one log entry, only cause one disk write, every disk write will call fsync().
//
// SegmentLog layout:
//      log_meta: record start_log
//      log_000001-0001000: closed segment
//      log_inprogress_0001001: open segment
class SegmentLogStorage : public LogStorage {
public:
    typedef std::map<int64_t, scoped_refptr<Segment> > SegmentMap;

    explicit SegmentLogStorage(const std::string& path, bool enable_sync = true)
        : _path(path)
        , _first_log_index(1)
        , _last_log_index(0)
        , _checksum_type(0)
        , _enable_sync(enable_sync)
    {} 

    // used for GlobalExtension
    SegmentLogStorage()
        : _first_log_index(1)
        , _last_log_index(0)
        , _checksum_type(0)
        , _enable_sync(true)
    {}

    virtual ~SegmentLogStorage() {}

    // init logstorage, check consistency and integrity
    virtual int init(ConfigurationManager* configuration_manager);

    // first log index in log
    virtual int64_t first_log_index() {
        return _first_log_index.load(base::memory_order_acquire);
    }

    // last log index in log
    virtual int64_t last_log_index();

    // get logentry by index
    virtual void get_entry(const int64_t index, LogEntry** entry, Closure* done) override;

    // get logentry by index
    virtual void last_log_id(LogId* log_id, Closure* done) override;

    // get logentry's term by index
    virtual int64_t get_term(const int64_t index);

    // append entry to log
    virtual void append_entry(const LogEntry* entry, Closure* done) override;

    // append entries to log and update IOMetric, return success append number
    virtual void append_entries(const std::vector<LogEntry*>& entries,
            IOMetric* metric, Closure* done) override;

    // delete logs from storage's head, [1, first_index_kept) will be discarded
    virtual void truncate_prefix(const int64_t first_index_kept, Closure* done) override;

    // delete uncommitted logs from storage's tail, (last_index_kept, infinity) will be discarded
    virtual void truncate_suffix(const int64_t last_index_kept, Closure* done) override;

    virtual void reset(const int64_t next_log_index, Closure* done) override;

    LogStorage* new_instance(const std::string& uri) const;
    
    base::Status gc_instance(const std::string& uri) const;

    SegmentMap& segments() {
        return _segments;
    }

    int get_segment(int64_t log_index, scoped_refptr<Segment>* ptr);

    void list_files(std::vector<std::string>* seg_files);

    // force sync all segments
    void sync();
private:
    scoped_refptr<Segment> open_segment();
    int save_meta(const int64_t log_index);
    int load_meta();
    int list_segments(bool is_empty);
    int load_segments(ConfigurationManager* configuration_manager);
    void pop_segments(
            int64_t first_index_kept, 
            std::vector<scoped_refptr<Segment> >* poped);
    void pop_segments_from_back(
            const int64_t first_index_kept,
            std::vector<scoped_refptr<Segment> >* popped,
            scoped_refptr<Segment>* last_segment);

    std::string _path;
    base::atomic<int64_t> _first_log_index;
    base::atomic<int64_t> _last_log_index;
    Mutex _mutex;
    SegmentMap _segments;
    scoped_refptr<Segment> _open_segment;
    int _checksum_type;
    bool _enable_sync;
};

bool verify_checksum(int checksum_type,
                     const char* data, size_t len, uint32_t value);

bool verify_checksum(int checksum_type, 
                     const base::IOBuf& data, uint32_t value);

std::ostream& operator<<(std::ostream& os, const Segment::EntryHeader& h);

}  // namespace raft

#endif //~PUBLIC_RAFT_LOG_H
