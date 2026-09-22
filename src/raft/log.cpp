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

#include "raft/log.h"

#include <gflags/gflags.h>
#include <base/files/dir_reader_posix.h>            // base::DirReaderPosix
#include <base/file_util.h>                         // base::CreateDirectory
#include <base/string_printf.h>                     // base::string_appendf
#include <base/time.h>
#include <base/raw_pack.h>                          // base::RawPacker
#include <base/fd_utility.h>                        // base::make_close_on_exec
#include "raft/local_storage.pb.h"
#include "raft/log_entry.h"
#include "raft/protobuf_file.h"
#include "raft/util.h"
#include "raft/io_uring.h"
#include "raft/closure_helper.h"                    // raft::NewCallback

#define RAFT_SEGMENT_OPEN_PATTERN "log_inprogress_%020ld"
#define RAFT_SEGMENT_CLOSED_PATTERN "log_%020ld_%020ld"
#define RAFT_SEGMENT_META_FILE  "log_meta"

namespace raft {

using ::base::RawPacker;
using ::base::RawUnpacker;

DECLARE_bool(raft_trace_append_entry_latency);
DEFINE_bool(raft_segment_done_without_persist, false, "io return success without persist");
DEFINE_bool(raft_log_with_aio, true, "use aio to do raft log");

DEFINE_int32(raft_max_segment_size, 8 * 1024 * 1024 /*8M*/, 
             "Max size of one segment file");
RAFT_VALIDATE_GFLAG(raft_max_segment_size, rpc::PositiveInteger);

DEFINE_bool(raft_sync_segments, false, "call fsync when a segment is closed");
RAFT_VALIDATE_GFLAG(raft_sync_segments, ::rpc::PassValidate);

static BvarLatencyRecorder g_open_segment_latency("raft_open_segment");
static BvarLatencyRecorder g_segment_append_entry_latency("raft_segment_append_entry");

#ifndef UNIT_TEST
static BvarLatencyRecorder g_sync_segment_latency("raft_sync_segment");
#else
// Unit tests should check this value
BvarLatencyRecorder g_sync_segment_latency("raft_sync_segment");
#endif

int ftruncate_uninterrupted(int fd, off_t length) {
    int rc = 0;
    do {
        rc = ftruncate(fd, length);
    } while (rc == -1 && errno == EINTR);
    return rc;
}

enum CheckSumType {
    CHECKSUM_MURMURHASH32 = 0,
    CHECKSUM_CRC32 = 1,   
};

// Format of Header, all fields are in network order
// | -------------------- term (64bits) -------------------------  |
// | entry-type (8bits) | checksum_type (8bits) | reserved(16bits) |
// | ------------------ data len (32bits) -----------------------  |
// | data_checksum (32bits) | header checksum (32bits)             |

const static size_t ENTRY_HEADER_SIZE = 24;

std::ostream& operator<<(std::ostream& os, const Segment::EntryHeader& h) {
    os << "{term=" << h.term << ", type=" << h.type << ", data_len="
       << h.data_len << ", checksum_type=" << h.checksum_type
       << ", data_checksum=" << h.data_checksum << '}';
    return os;
}

static inline bool io_without_persist() {
    return FLAGS_raft_log_with_aio && FLAGS_raft_segment_done_without_persist;
}

int Segment::create() {
    if (!_is_open) {
        CHECK(false) << "Create on a closed segment at first_index=" 
                     << _first_index << " in " << _path;
        return -1;
    }

    std::string path(_path);
    base::string_appendf(&path, "/" RAFT_SEGMENT_OPEN_PATTERN, _first_index);
    int mode = O_RDWR | O_CREAT | O_TRUNC;
    _fd = ::open(path.c_str(), mode, 0644);
    if (_fd >= 0) {
        base::make_close_on_exec(_fd);
    }
    LOG_IF(INFO, _fd >= 0) << "Created new segment `" << path
                           << "' with fd=" << _fd;
    return _fd >= 0 ? 0 : -1;
}

bool verify_checksum(int checksum_type,
                     const char* data, size_t len, uint32_t value) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return (value == murmurhash32(data, len));
    case CHECKSUM_CRC32:
        return (value == crc32(data, len));
    default:
        LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
        return false;
    }
}

bool verify_checksum(int checksum_type, 
                     const base::IOBuf& data, uint32_t value) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return (value == murmurhash32(data));
    case CHECKSUM_CRC32:
        return (value == crc32(data));
    default:
        LOG(ERROR) << "Unknown checksum_type=" << checksum_type;
        return false;
    }
}

inline uint32_t get_checksum(int checksum_type, const char* data, size_t len) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return murmurhash32(data, len);
    case CHECKSUM_CRC32:
        return crc32(data, len);
    default:
        CHECK(false) << "Unknown checksum_type=" << checksum_type;
        abort();
        return 0;
    }
}

inline uint32_t get_checksum(int checksum_type, const base::IOBuf& data) {
    switch (checksum_type) {
    case CHECKSUM_MURMURHASH32:
        return murmurhash32(data);
    case CHECKSUM_CRC32:
        return crc32(data);
    default:
        CHECK(false) << "Unknown checksum_type=" << checksum_type;
        abort();
        return 0;
    }
}

int Segment::_load_entry(off_t offset, EntryHeader* head, base::IOBuf* data,
                         size_t size_hint) const {
    base::IOPortal buf;
    size_t to_read = std::max(size_hint, ENTRY_HEADER_SIZE);
    const ssize_t n = file_pread(&buf, _fd, offset, to_read);
    if (n != (ssize_t)to_read) {
        return n < 0 ? -1 : 1;
    }
    char header_buf[ENTRY_HEADER_SIZE];
    const char *p = (const char *)buf.fetch(header_buf, ENTRY_HEADER_SIZE);
    int64_t term = 0;
    uint32_t meta_field;
    uint32_t data_len = 0;
    uint32_t data_checksum = 0;
    uint32_t header_checksum = 0;
    RawUnpacker(p).unpack64((uint64_t&)term)
                  .unpack32(meta_field)
                  .unpack32(data_len)
                  .unpack32(data_checksum)
                  .unpack32(header_checksum);
    EntryHeader tmp;
    tmp.term = term;
    tmp.type = meta_field >> 24;
    tmp.checksum_type = (meta_field << 8) >> 24;
    tmp.data_len = data_len;
    tmp.data_checksum = data_checksum;
    if (!verify_checksum(tmp.checksum_type,
                        p, ENTRY_HEADER_SIZE - 4, header_checksum)) {
        LOG(ERROR) << "Found corrupted header at offset=" << offset
                   << ", header=" << tmp << ", path: " << _path
                   << ", first_index: " <<  _first_index;
        return -1;
    }
    if (head != NULL) {
        *head = tmp;
    }
    if (data != NULL) {
        if (buf.length() < ENTRY_HEADER_SIZE + data_len) {
            const size_t to_read = ENTRY_HEADER_SIZE + data_len - buf.length();
            const ssize_t n = file_pread(&buf, _fd, offset + buf.length(), to_read);
            if (n != (ssize_t)to_read) {
                return n < 0 ? -1 : 1;
            }
        } else if (buf.length() > ENTRY_HEADER_SIZE + data_len) {
            buf.pop_back(buf.length() - ENTRY_HEADER_SIZE - data_len);
        }
        CHECK_EQ(buf.length(), ENTRY_HEADER_SIZE + data_len);
        buf.pop_front(ENTRY_HEADER_SIZE);
        if (!verify_checksum(tmp.checksum_type, buf, tmp.data_checksum)) {
            LOG(ERROR) << "Found corrupted data at offset=" 
                       << offset + ENTRY_HEADER_SIZE
                       << " header=" << tmp
                       << " path: " << _path
                       << " first_index: " <<  _first_index;
            // TODO: abort()?
            return -1;
        }
        data->swap(buf);
    }
    return 0;
}

int Segment::_get_meta(int64_t index, LogMeta* meta) const {
    BAIDU_SCOPED_LOCK(_mutex);
    if (index > _last_index.load(base::memory_order_relaxed)
                    || index < _first_index) {
        // out of range
        RAFT_VLOG << "_last_index=" << _last_index.load(base::memory_order_relaxed)
                  << " _first_index=" << _first_index
                  << " required index=" << index;
        return -1;
    } else if (_last_index.load(std::memory_order_relaxed) ==
            _first_index - 1) {
        RAFT_VLOG << "_last_index=" << _last_index.load(base::memory_order_relaxed)
                  << " _first_index=" << _first_index << " required index=" << index;
        // empty
        return -1;
    }
    CHECK_EQ(_last_index.load(base::memory_order_relaxed) - _first_index + 1,
                _offset_and_term.size());
    int64_t meta_index = index - _first_index;
    int64_t entry_cursor = _offset_and_term[meta_index].first;
    int64_t next_cursor = (index < _last_index.load(base::memory_order_relaxed))
                          ? _offset_and_term[meta_index + 1].first : _bytes;
    DCHECK_LT(entry_cursor, next_cursor);
    meta->offset = entry_cursor;
    meta->term = _offset_and_term[meta_index].second;
    meta->length = next_cursor - entry_cursor;
    return 0;
}

int Segment::load(ConfigurationManager* configuration_manager) {
    int ret = 0;

    std::string path(_path);
    // create fd
    if (_is_open) {
        base::string_appendf(&path, "/" RAFT_SEGMENT_OPEN_PATTERN, _first_index);
    } else {
        base::string_appendf(&path, "/" RAFT_SEGMENT_CLOSED_PATTERN, 
                             _first_index, _last_index.load());
    }
    _fd = ::open(path.c_str(), O_RDWR);
    if (_fd < 0) {
        LOG(ERROR) << "Fail to open " << path << ", " << berror();
        return -1;
    }
    base::make_close_on_exec(_fd);
    LOG_IF(INFO, _fd >= 0) << "Loaded segment `" << path
                           << "' with fd=" << _fd;
    // get file size
    struct stat st_buf;
    if (fstat(_fd, &st_buf) != 0) {
        LOG(ERROR) << "Fail to get the stat of " << path << ", " << berror()
            << " close segment `" << path << "' with fd=" << _fd;
        ::close(_fd);
        _fd = -1;
        return -1;
    }

    // load entry index
    int64_t file_size = st_buf.st_size;
    int64_t entry_off = 0;
    int64_t actual_last_index = _first_index - 1;
    for (int64_t i = _first_index; entry_off < file_size; i++) {
        EntryHeader header;
        const int rc = _load_entry(entry_off, &header, NULL, ENTRY_HEADER_SIZE);
        LOG(DEBUG) << "Path: " << _path << ", log=" << LogId(i, header.term)
            << ", crc_type: " << header.checksum_type
            << ", crc_value: " << header.data_checksum;
        if (rc > 0) {
            // The last log was not completely written, which should be truncated
            break;
        }
        if (rc < 0) {
            if (!_is_open) {
                // it is ok to truncate last segment
                ret = rc;
            }
            break;
        }
        // rc == 0
        const int64_t skip_len = ENTRY_HEADER_SIZE + header.data_len;
        if (entry_off + skip_len > file_size) {
            // The last log was not completely written and it should be
            // truncated
            break;
        }
        if (header.type == ENTRY_TYPE_CONFIGURATION) {
            base::IOBuf data;
            // Header will be parsed again but it's fine as configuration
            // changing is rare
            if (_load_entry(entry_off, NULL, &data, skip_len) != 0) {
                break;
            }
            scoped_refptr<LogEntry> entry = new LogEntry();
            entry->id.index = i;
            entry->id.term = header.term;
            base::Status status = parse_configuration_meta(data, entry);
            if (status.ok()) {
                ConfigurationEntry conf_entry(*entry);
                configuration_manager->add(conf_entry); 
            } else {
                LOG(ERROR) << "fail to parse configuration meta, path: " << _path
                    << " entry_off " << entry_off;
                ret = -1;
                break;
            }
        }
        _offset_and_term.push_back(std::make_pair(entry_off, header.term));
        ++actual_last_index;
        entry_off += skip_len;
    }

    const int64_t last_index = _last_index.load(base::memory_order_relaxed);
    if (ret == 0 && !_is_open) {
        if (actual_last_index < last_index) {
            LOG(ERROR) << "data lost in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        } else if (actual_last_index > last_index) {
            // FIXME(zhengpengfei): should we ignore garbage entries silently
            LOG(ERROR) << "found garbage in a full segment, path: " << _path
                << " first_index: " << _first_index << " expect_last_index: "
                << last_index << " actual_last_index: " << actual_last_index;
            ret = -1;
        }
    }

    if (ret != 0) {
        return ret;
    }

    if (_is_open) {
        _last_index = actual_last_index;
    }

    // truncate last uncompleted entry
    if (entry_off != file_size) {
        LOG(NOTICE) << "truncate last uncompleted write entry, path: " << _path
            << " first_index: " << _first_index
            << " last_index: " << _last_index
            << " old_size: " << file_size << " new_size: " << entry_off;
        ret = ftruncate_uninterrupted(_fd, entry_off);
    }

    // seek to end, for opening segment
    ::lseek(_fd, entry_off, SEEK_SET);

    _bytes = entry_off;
    return ret;
}

void Segment::append_done(scoped_refptr<Segment> deref, InFlightIoClosure* done, const base::Status& st) {
    CHECK_EQ(deref, this);
    CHECK(done);
    std::vector<Closure*> closures;
    std::unique_lock<Mutex> lck(_mutex);
    done->status().set_error(st.error_code(), st.error_str());
                  // ^^ need lock to protect since 
                  // other thread may access it in the following pop progress
    CHECK(!_pending.empty());
    PendingIoQueue& queue = _pending.front();
    CHECK(!queue.has_operation());
    CHECK(!queue.empty());
    if (done != queue.front()) {
        return;
    }
    // pop finished io in sequence
    bool continuous_success = true;
    while (!queue.empty()) {
        done = queue.front();
        if (done->status().error_code() == EINPROGRESS) {
            break;
        }
        queue.pop_front();
        if (done->status().ok() && !continuous_success) {
            // if previous log fail, make succeed log fail
            done->status().set_error(EIO, "previous log error");
        }
        if (!done->status().ok()) {
            continuous_success = false;
        }
        closures.push_back(done);
    }
    do {
        if (!queue.empty()) {
            // ^^ truncate should wait io finished
            break;
        }
        std::deque<PendingIoQueue> pending; // just protect
        pending.swap(_pending); // just avoid core when destroy segment
        pending.pop_front();
        while (!pending.empty()) {
            // there is possibility for multiply truncate
            PendingIoQueue& io_queue = pending.front();
            std::deque<InFlightIoClosure*>& pending_io = io_queue.pending();
            if (io_queue.is_truncate()) {
                int64_t truncate_size = io_queue.truncate_size();
                Closure* truncate_done = io_queue.release_closure();
                CHECK_GE(truncate_size, 0);
                CHECK(truncate_done);
                truncate_file(truncate_size, truncate_done);
                // ^^ need holding lock
            } else {
                Closure* action_done = io_queue.release_closure();
                CHECK(action_done) << _path
                    << " must has action following io when no truncate";
                if (!continuous_success || !st.ok()) {
                    action_done->status().set_error(EIO, "previous log error");
                }
                closures.push_back(action_done);
            }
            if (pending_io.empty()) {
                pending.pop_front();
                continue;
            }
            for (auto& io_done : pending_io) {
                // submit pending request
                g_raft_io_man->write(_fd, io_done->offset(), io_done->data(),
                        NewCallback(this, &Segment::append_done, scoped_refptr<Segment>(this), io_done));
            }
            break;
        }
        if (!pending.empty()) {
            pending.swap(_pending);
        }
    } while (false);
    lck.unlock(); // unlock to avoid deadlock logmanager->segment
    for (auto& done : closures) {
        done->Run();
    }
}

void Segment::last_log_id(LogId* log_id, Closure* done) {
    AsyncClosureGuard done_guard(done);
    int64_t index = _last_index.load(std::memory_order_relaxed);
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        done->status().set_error(EINVAL, "bad segment");
        return;
    }
    log_id->index = index;
    log_id->term = meta.term;
    std::unique_lock<Mutex> lck(_mutex);
    if (_pending.empty()) {
        lck.unlock();
        return;
    }
    // just wait in flight io
    PendingIoQueue tc(this);
    tc.set_operation(-1, (Closure*)done_guard.release());
    _pending.emplace_back(std::move(tc));
}

void Segment::append(const LogEntry* entry, 
        IOMetric* metric, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    if (BAIDU_UNLIKELY(!entry || !_is_open)) {
        done->status().set_error(EINVAL, "segment not open");
        return;
    } else if (entry->id.index !=
                    _last_index.load(base::memory_order_consume) + 1) {
        CHECK(false) << "entry->index=" << entry->id.index
                  << " _last_index=" << _last_index
                  << " _first_index=" << _first_index;
        done->status().set_error(ERANGE, "range not match, exptect: %ld, given: %ld",
                _last_index + 1, entry->id.index);
        return;
    }

    base::IOBuf data;
    switch (entry->type) {
    case ENTRY_TYPE_DATA:
        data.append(entry->data);
        break;
    case ENTRY_TYPE_NO_OP:
        break;
    case ENTRY_TYPE_CONFIGURATION: 
        {
            base::Status status = serialize_configuration_meta(entry, data);
            if (!status.ok()) {
                LOG(ERROR) << "Fail to serialize ConfigurationPBMeta, path: "
                    << _path;
                done->status().set_error(EINVAL, "error configuration meta");
                return;
            }
        }
        break;
    default:
        LOG(FATAL) << "unknow entry type: " << entry->type 
            << ", path: " << _path;
        done->status().set_error(EINVAL, "unknown entry type");
        return;
    }
    CHECK_LE(data.length(), 1ul << 56ul);
    const uint32_t meta_field = (entry->type << 24 ) | (_checksum_type << 16);
    size_t to_write = ENTRY_HEADER_SIZE + data.size();
    char header_buf[ENTRY_HEADER_SIZE];
    RawPacker packer(header_buf);
    const uint32_t data_checksum = get_checksum(_checksum_type, data);
    packer.pack64(entry->id.term)
          .pack32(meta_field)
          .pack32((uint32_t)data.length())
          .pack32(data_checksum);
    const uint32_t meta_checksum = get_checksum(
                  _checksum_type, header_buf, ENTRY_HEADER_SIZE - 4);
    packer.pack32(meta_checksum);
    base::IOBuf write_data;
    write_data.append(header_buf, ENTRY_HEADER_SIZE);
    write_data.append(data);

    uint32_t crc_code = 0;
    LOG(DEBUG) << "Path: " << _path << ", log=" << entry->id
        << ", crc_type: " << _checksum_type
        << ", data_crc_value: " << data_checksum
        << ", meta_crc_value: " << meta_checksum
        << ", write_data crc: " << (get_crc32((CrcType)FLAGS_crc32_type, write_data, &crc_code) ?
            crc_code : 0);

    std::unique_lock<Mutex> lck(_mutex);

    _offset_and_term.push_back(std::make_pair(_bytes, entry->id.term));
    _last_index.fetch_add(1, base::memory_order_relaxed);
    off_t write_offset = _bytes;
    _bytes += to_write;
    CHECK_EQ(_last_index.load(base::memory_order_relaxed) - _first_index + 1,
        _offset_and_term.size());
    if (FLAGS_raft_log_with_aio) {
        InFlightIoClosure* io_done = new InFlightIoClosure(write_offset, write_data,
                io_without_persist() ? nullptr : dynamic_cast<Closure*>(done_guard.release()));
        CHECK(io_done);
        if (_pending.empty()) {
            _pending.emplace_back(PendingIoQueue(this));
        }
        _pending.back().pend(io_done);
        if (!_pending.back().has_operation()) {
                            // if truncate happens before, just pending not submit
            g_raft_io_man->write(_fd, write_offset, io_done->data(),
                    NewCallback(this, &Segment::append_done, scoped_refptr<Segment>(this), io_done));
        }
    } else {
        uint64_t start = base::cpuwide_time_us();
        if (file_pwrite(write_data, _fd, write_offset) != to_write) {
            PLOG(ERROR) << "write " << _fd << ", size: " << to_write << ", error";
            done->status().set_error(EIO, "write log error");
        }
        if (FLAGS_raft_trace_append_entry_latency && metric) {
            uint64_t delta_time_us = base::cpuwide_time_us() - start;
            metric->append_entry_time_us += delta_time_us;
            g_segment_append_entry_latency << delta_time_us;
        }
    }
}

int Segment::sync(bool will_sync) {
    if (_last_index <= _first_index) {
        return 0;
    }
    CHECK(_is_open);
    if (!FLAGS_raft_sync) {
        return 0;
    }
    if (!will_sync) {
        return 0;
    }
    int64_t now = base::cpuwide_time_us();
    int ret = raft_fsync(_fd);
    g_sync_segment_latency << base::cpuwide_time_us() - now;
    return ret;
}

int Segment::force_sync() {
    if (_last_index > _first_index) {
        return raft_fsync(_fd);
    }
    return 0;
}

void Segment::get(const int64_t index, LogEntry** entry, Closure* done) {
    CHECK(entry);
    CHECK(done);
    rpc::ClosureGuard done_guard(done);
    if (index > _last_index.load(base::memory_order_relaxed)
                    || index < _first_index) {
        done->status().set_error(ERANGE, "out of range index=%ld", index);
        // out of range
        RAFT_VLOG << "_last_index=" << _last_index.load(base::memory_order_relaxed)
                  << " _first_index=" << _first_index
                  << " required index=" << index;
        return;
    } else if (_last_index.load(std::memory_order_relaxed) ==
            _first_index - 1) {
        done->status().set_error(ERANGE, "out of range index=%ld", index);
        RAFT_VLOG << "_last_index=" << _last_index.load(base::memory_order_relaxed)
                  << " _first_index=" << _first_index << " required index=" << index;
        // empty
        return;
    }

    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        done->status().set_error(ENOENT, "get meta error index=%ld", index);
        LOG(WARNING) << "get meta error index: " << index;
        return;
    }

    base::IOPortal* data = new base::IOPortal;
    CHECK(data);
    Closure* read_closure = NewCallback(this, &Segment::_read_data_done,
            scoped_refptr<Segment>(this), data,
            index, meta.term, entry, dynamic_cast<Closure*>(done_guard.release()));
    rpc::ClosureGuard read_done_guard(read_closure);
    {
        // try to find from pending
        BAIDU_SCOPED_LOCK(_mutex); 
        InFlightIoClosure* io = nullptr;
        for (auto iter = _pending.rbegin(); iter != _pending.rend(); ++iter) {
            io = iter->get_pending(meta.offset);
            if (io) {
                data->append(io->data());
                CHECK_EQ(meta.length, data->size());
                return;
            }
        }
    }
    if (FLAGS_raft_log_with_aio) {
        g_raft_io_man->read(_fd, meta.offset, data, meta.length,
                dynamic_cast<Closure*>(read_done_guard.release()));
        return;
    }
    const ssize_t n = file_pread(data, _fd, meta.offset, meta.length);
    if (n != static_cast<ssize_t>(meta.length)) {
        read_closure->status().set_error(EIO, "read log error");
    }
}

void Segment::_read_data_done(scoped_refptr<Segment> deref, base::IOPortal* buf, int64_t index,
        int64_t term, LogEntry** result, Closure* done, const base::Status& st) {
    std::unique_ptr<base::IOPortal> ptr(buf);
    CHECK_EQ(this, deref);
    CHECK(result);
    CHECK(done);
    rpc::ClosureGuard done_guard(done);
    if (!st.ok()) {
        done->status().set_error(st.error_code(), st.error_str());
        return;
    }
    char header_buf[ENTRY_HEADER_SIZE];
    const char *p = (const char *)buf->fetch(header_buf, ENTRY_HEADER_SIZE);
    int64_t tmp_term = 0;
    uint32_t meta_field;
    uint32_t data_len = 0;
    uint32_t data_checksum = 0;
    uint32_t header_checksum = 0;
    RawUnpacker(p).unpack64((uint64_t&)tmp_term)
                  .unpack32(meta_field)
                  .unpack32(data_len)
                  .unpack32(data_checksum)
                  .unpack32(header_checksum);
    EntryHeader header;
    header.term = tmp_term;
    header.type = meta_field >> 24;
    header.checksum_type = (meta_field << 8) >> 24;
    header.data_len = data_len;
    header.data_checksum = data_checksum;
    uint32_t crc_code = 0;
    LOG(DEBUG) << "Path: " << _path << ", log=" << LogId(index, header.term)
        << ", crc_type: " << header.checksum_type
        << ", data_crc_value: " << data_checksum
        << ", meta_crc_value: " << header_checksum
        << ", read_data crc: " << (get_crc32((CrcType)FLAGS_crc32_type, *buf, &crc_code) ?
                crc_code : 0);;
    if (!verify_checksum(header.checksum_type,
                        p, ENTRY_HEADER_SIZE - 4, header_checksum)) {
        done->status().set_error(EBADMSG, "header error");
        LOG(ERROR) << "Found corrupted header=" << header << ", path: " << _path;
        return;
    }
    CHECK_EQ(buf->length(), ENTRY_HEADER_SIZE + data_len);
    buf->pop_front(ENTRY_HEADER_SIZE);
    if (!verify_checksum(header.checksum_type, *buf, header.data_checksum)) {
        done->status().set_error(EBADMSG, "header error");
        LOG(ERROR) << "Found corrupted data header=" << header
                   << " path: " << _path;
        return;
    }
    CHECK_EQ(term, header.term) << "path: " << _path
        << ", index: " << index << ", _fd: " << _fd;
    bool ok = true;
    LogEntry* entry = nullptr;
    do {
        ConfigurationPBMeta configuration_meta;
        entry = new LogEntry();
        entry->AddRef();
        switch (header.type) {
        case ENTRY_TYPE_DATA:
            entry->data.swap(*buf);
            break;
        case ENTRY_TYPE_NO_OP:
            CHECK(buf->empty()) << "Data of NO_OP must be empty";
            break;
        case ENTRY_TYPE_CONFIGURATION:
            {
                base::Status status = parse_configuration_meta(*buf, entry); 
                if (!status.ok()) {
                    LOG(WARNING) << "Fail to parse ConfigurationPBMeta, path: "
                                 << _path;
                    ok = false;
                    break;
                }
            }
            break;
        default:
            CHECK(false) << "Unknown entry type, path: " << _path;
            break;
        }

        if (!ok) { 
            break;
        }
        entry->id.index = index;
        entry->id.term = header.term;
        entry->type = (EntryType)header.type;
    } while (0);

    if (!ok && entry != NULL) {
        entry->Release();
        done->status().set_error(EBADMSG, "load error");
        return;
    }
    *result = entry;
}

int64_t Segment::get_offset(const int64_t index) const {
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return -1;
    }
    return (int64_t)meta.offset;
}

int64_t Segment::get_term(const int64_t index) const {
    LogMeta meta;
    if (_get_meta(index, &meta) != 0) {
        return 0;
    }
    return meta.term;
}

int Segment::close(bool will_sync) {
    CHECK(_is_open);

    std::string old_path(_path);
    base::string_appendf(&old_path, "/" RAFT_SEGMENT_OPEN_PATTERN,
                         _first_index);
    std::string new_path(_path);
    base::string_appendf(&new_path, "/" RAFT_SEGMENT_CLOSED_PATTERN, 
                         _first_index, _last_index.load());

    // TODO: optimize index memory usage by reconstruct vector
    LOG(INFO) << "Close a full segment. Current first_index: " << _first_index 
              << " last_index: " << _last_index 
              << " raft_sync_segments: " << FLAGS_raft_sync_segments 
              << " will_sync: " << will_sync 
              << " path: " << new_path;
    _is_open = false;
    const int rc = ::rename(old_path.c_str(), new_path.c_str());
    LOG_IF(INFO, rc == 0) << "Renamed `" << old_path
                          << "' to `" << new_path <<'\'';
    LOG_IF(ERROR, rc != 0) << "Fail to rename `" << old_path
                           << "' to `" << new_path <<"\', "
                           << berror();
    if (_last_index > _first_index
            && (FLAGS_raft_sync_segments || FLAGS_raft_sync)
            && will_sync) {
        int64_t now = base::cpuwide_time_us();
        auto release_segment = [this]() {
            scoped_refptr<Segment> guard(this);
        };
        google::protobuf::Closure* release_closure = nullptr;
        int64_t polar_worker_index = current_polar_idx();
        if (polar_worker_index >= 0) {
            release_closure = new SamePollerClosure(create_pb_closure(std::move(release_segment)));
        } else {
            release_closure = create_pb_closure(std::move(release_segment));
        }
        auto l = [now, fd = this->_fd, release_closure](const base::Status& st) {
            rpc::ClosureGuard done_guard(release_closure);
            if ((!st.ok()) && (raft_fsync(fd) != 0)) {
                LOG(WARNING) << "Failed to sync: " << fd;
            }
            g_sync_segment_latency << base::cpuwide_time_us() - now;
        };
        g_raft_io_man->sync(_fd, create_raft_closure(std::move(l)));
    }
    return rc;
}

std::string Segment::file_name() {
    if (!_is_open) {
        return base::string_printf(RAFT_SEGMENT_CLOSED_PATTERN, _first_index, _last_index.load());
    } else {
        return base::string_printf(RAFT_SEGMENT_OPEN_PATTERN, _first_index);
    }
}

static void* run_unlink(void* arg) {
    std::string* file_path = (std::string*) arg;
    base::Timer timer;
    timer.start();
    int ret = ::unlink(file_path->c_str());
    timer.stop();
    RAFT_VLOG << "unlink " << *file_path << " ret " << ret << " time: " << timer.u_elapsed();
    delete file_path;

    return NULL;
}

int Segment::unlink() {
    int ret = 0;
    do {
        std::string path(_path);
        if (_is_open) {
            base::string_appendf(&path, "/" RAFT_SEGMENT_OPEN_PATTERN,
                                 _first_index);
        } else {
            base::string_appendf(&path, "/" RAFT_SEGMENT_CLOSED_PATTERN,
                                _first_index, _last_index.load());
        }

        std::string tmp_path(path);
        tmp_path.append(".tmp");
        ret = ::rename(path.c_str(), tmp_path.c_str());
        if (ret != 0) {
            PLOG(ERROR) << "Fail to rename " << path << " to " << tmp_path;
            break;
        }

        // start bthread to unlink
        // TODO unlink follow control
        std::string* file_path = new std::string(tmp_path);
        bthread_t tid;
        if (raft_start_background(&tid, &BTHREAD_ATTR_NORMAL, run_unlink, file_path) != 0) {
            run_unlink(file_path);
        }

        LOG(INFO) << "Unlinked segment `" << path << '\'';
    } while (0);

    return ret;
}

void Segment::truncate(const int64_t last_index_kept, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    int64_t truncate_size = 0;
    int64_t first_truncate_in_offset = 0;
    std::unique_lock<Mutex> lck(_mutex);
    if (last_index_kept >= _last_index) {
        return;
    }

    first_truncate_in_offset = last_index_kept + 1 - _first_index;
    truncate_size = _offset_and_term[first_truncate_in_offset].first;
    RAFT_VLOG << "Truncating " << _path << " first_index: " << _first_index
              << " last_index from " << _last_index << " to " << last_index_kept
              << " truncate size to " << truncate_size;
    lck.unlock();

    // Truncate on a full segment need to rename back to inprogess segment again,
    // because the node may crash before truncate.
    if (!_is_open) {
        std::string old_path(_path);
        base::string_appendf(&old_path, "/" RAFT_SEGMENT_CLOSED_PATTERN,
                             _first_index, _last_index.load());

        std::string new_path(_path);
        base::string_appendf(&new_path, "/" RAFT_SEGMENT_OPEN_PATTERN,
                             _first_index);
        int ret = ::rename(old_path.c_str(), new_path.c_str());
        LOG_IF(INFO, ret == 0) << "Renamed `" << old_path << "' to `"
                               << new_path << '\'';
        LOG_IF(ERROR, ret != 0) << "Fail to rename `" << old_path << "' to `"
                                << new_path << "', " << berror();
        if (ret != 0) {
            done->status().set_error(EIO, "Truncate rename fail");
            return;
        }
        _is_open = true;
    }

    lck.lock();
    // update memory var
    _offset_and_term.resize(first_truncate_in_offset);
    _last_index.store(last_index_kept, base::memory_order_relaxed);
    _bytes = truncate_size;
    if (_pending.empty()) {
        lck.unlock();
        // just do truncate
        return truncate_file(truncate_size, (Closure*)done_guard.release());
    }
    PendingIoQueue tc(this);
    tc.set_operation(truncate_size, (Closure*)done_guard.release());
    _pending.emplace_back(std::move(tc));
}

void Segment::truncate_file(const off_t truncate_size, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    // truncate fd
    int ret = ftruncate_uninterrupted(_fd, truncate_size);
    //        ^^ TODO: do this in iouring after kernel updated, if do this in bthread, we still need lock
    if (ret < 0) {
        done->status().set_error(EIO, "Truncate fail fd=%d", _fd);
        return;
    }

    // seek fd
    off_t ret_off = ::lseek(_fd, truncate_size, SEEK_SET);
    if (ret_off < 0) {
        PLOG(ERROR) << "Fail to lseek fd=" << _fd << " to size=" << truncate_size
                    << " path: " << _path;
        done->status().set_error(EIO, "Truncate seek fail");
        return;
    }
}

int SegmentLogStorage::init(ConfigurationManager* configuration_manager) {
    _mutex.reset(FLAGS_raft_use_polar, current_polar_idx());
    if (FLAGS_raft_max_segment_size < 0) {
        LOG(FATAL) << "FLAGS_raft_max_segment_size ["
                   << FLAGS_raft_max_segment_size
                   << "] must be greater than or equal to 0 ";
        return -1;
    }
    base::FilePath dir_path(_path);
    base::File::Error e;
    if (!base::CreateDirectoryAndGetError(
                dir_path, &e, FLAGS_raft_create_parent_directories)) {
        LOG(ERROR) << "Fail to create " << dir_path.value() << " : " << e;
        return -1;
    }

    if (base::crc32c::IsFastCrc32Supported()) {
        _checksum_type = CHECKSUM_CRC32;
        LOG_ONCE(INFO) << "Use crc32c as the checksum type of appending entries";
    } else {
        _checksum_type = CHECKSUM_MURMURHASH32;
        LOG_ONCE(INFO) << "Use murmurhash32 as the checksum type of appending entries";
    }

    int ret = 0;
    bool is_empty = false;
    do {
        ret = load_meta();
        if (ret != 0 && errno == ENOENT) {
            LOG(WARNING) << _path << " is empty";
            is_empty = true;
        } else if (ret != 0) {
            break;
        }

        ret = list_segments(is_empty);
        if (ret != 0) {
            break;
        }

        ret = load_segments(configuration_manager);
        if (ret != 0) {
            break;
        }
    } while (0);

    if (is_empty) {
        _first_log_index.store(1);
        _last_log_index.store(0);
        ret = save_meta(1);
    }
    return ret;
}

int64_t SegmentLogStorage::last_log_index() {
    return _last_log_index.load(base::memory_order_acquire);
}

class AppendClosureGroup : public ClosureGroup {
public:
    AppendClosureGroup(IOMetric* metric, int32_t sub_done_count,
             raft::Closure *done) : ClosureGroup(sub_done_count, done),
    _metric(metric), _start_us(base::cpuwide_time_us()) {}
protected:
    virtual void before_run_done() override {
        if (FLAGS_raft_trace_append_entry_latency && _metric) {
            _metric->append_entry_time_us = base::cpuwide_time_us() - _start_us;
        }
    }
private:
    IOMetric* _metric;
    uint64_t _start_us;
};

void SegmentLogStorage::append_entries(const std::vector<LogEntry*>& entries,
        IOMetric* metric, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    if (entries.empty()) {
        return;
    }
    if (_last_log_index.load(base::memory_order_relaxed) + 1
            != entries.front()->id.index) {
        LOG(FATAL) << "There's gap between appending entries and _last_log_index"
                   << " path: " << _path 
                   << " last_log_index: " << _last_log_index.load(base::memory_order_relaxed)
                   << " entry_index: " << entries.front()->id.index;
        done->status().set_error(ERANGE, "there is gap between entries");
        return;
    }
    scoped_refptr<Segment> last_segment = NULL;
    bool enable_sync = _enable_sync; // in case use after free, see INF-EBS-6897 for detail
    size_t entry_num = entries.size(); // protect entries accessed in for loop
    ClosureGroup* group = new AppendClosureGroup(metric, entry_num, (Closure*)done_guard.release());
    int ret = 0;
    for (size_t i = 0; i < entry_num; i++) {
        rpc::ClosureGuard sub_guard(group->sub_done(i));
        if (ret != 0) {
            group->sub_done(i)->status().set_error(EIO, "the previous segment fail");
            continue;
        }
        LogEntry* entry = entries[i];
        int64_t now = base::cpuwide_time_us();
        scoped_refptr<Segment> segment = open_segment();
        int64_t delta_time_us = base::cpuwide_time_us() - now;
        if (FLAGS_raft_trace_append_entry_latency && metric) {
            metric->open_segment_time_us += delta_time_us;
            g_open_segment_latency << delta_time_us;
        }
        if (FLAGS_raft_trace_append_entry_latency && metric) {
            metric->sync_segment_time_us += delta_time_us;
            // TODO: add sync metric, how to do this for aio?
        }
        if (NULL == segment) {
            ret = -1;
            group->sub_done(i)->status().set_error(EIO, "open segment fail");
            continue;
        }
        _last_log_index.fetch_add(1, std::memory_order_relaxed);
        last_segment = segment;
        segment->append(entry, metric, (Closure*)sub_guard.release());
    }
    if (last_segment && last_segment->is_open()) {
        auto start_us = base::cpuwide_time_us();
        last_segment->sync(enable_sync);
        if (FLAGS_raft_trace_append_entry_latency && metric) {
            metric->sync_segment_time_us += base::cpuwide_time_us() - start_us;
        }
    }
}

void SegmentLogStorage::append_entry(const LogEntry* entry, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    scoped_refptr<Segment> segment = open_segment();
    if (NULL == segment) {
        done->status().set_error(EIO, "open segment error");
        return;
    }
    _last_log_index.fetch_add(1, std::memory_order_relaxed);
    segment->append(entry, nullptr, (Closure*)done_guard.release());
    segment->sync(_enable_sync);
}

void SegmentLogStorage::get_entry(const int64_t index, LogEntry** entry, Closure* done) {
    CHECK(entry);
    CHECK(done);
    rpc::ClosureGuard done_guard(done);
    scoped_refptr<Segment> ptr;
    if (get_segment(index, &ptr) != 0) {
        done->status().set_error(ENOENT, "not find index=%ld", index);
        return;
    }
    ptr->get(index, entry, dynamic_cast<Closure*>(done_guard.release()));
}

void SegmentLogStorage::last_log_id(LogId* log_id, Closure* done) {
    CHECK(log_id);
    CHECK(done);
    AsyncClosureGuard done_guard(done);
    scoped_refptr<Segment> segment;
    std::unique_lock lck(_mutex);
    if (_open_segment) {
        segment = _open_segment;
    } else if (!_segments.empty()) {
        segment = _segments.rbegin()->second;
    }
    if (!segment) {
        done->status().set_error(EINVAL, "empty storage");
        return;
    }
    lck.unlock(); // unlock to avoid dead lock since done->Run() may call last_log_id()
    segment->last_log_id(log_id, dynamic_cast<Closure*>(done_guard.release()));
}

int64_t SegmentLogStorage::get_term(const int64_t index) {
    scoped_refptr<Segment> ptr;
    if (get_segment(index, &ptr) != 0) {
        return 0;
    }
    return ptr->get_term(index);
}

void SegmentLogStorage::pop_segments(
        const int64_t first_index_kept,
        std::vector<scoped_refptr<Segment> >* popped) {
    popped->clear();
    popped->reserve(32);
    BAIDU_SCOPED_LOCK(_mutex);
    _first_log_index.store(first_index_kept, base::memory_order_release);
    for (SegmentMap::iterator it = _segments.begin(); it != _segments.end();) {
        scoped_refptr<Segment>& segment = it->second;
        if (segment->last_index() < first_index_kept) {
            popped->push_back(segment);
            _segments.erase(it++);
        } else {
            return;
        }
    }
    if (_open_segment) {
        if (_open_segment->last_index() < first_index_kept) {
            popped->push_back(_open_segment);
            _open_segment = NULL;
            // _log_storage is empty
            _last_log_index.store(first_index_kept - 1);
        } else {
            CHECK(_open_segment->first_index() <= first_index_kept);
        }
    } else {
        // _log_storage is empty
        _last_log_index.store(first_index_kept - 1);
    }
}

void SegmentLogStorage::truncate_prefix(const int64_t first_index_kept, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    // segment files
    if (_first_log_index.load(base::memory_order_acquire) >= first_index_kept) {
      RAFT_VLOG << "Nothing is going to happen since _first_log_index=" 
                     << _first_log_index.load(base::memory_order_relaxed)
                     << " >= first_index_kept="
                     << first_index_kept;
    }
    // NOTE: truncate_prefix is not important, as it has nothing to do with 
    // consensus. We try to save meta on the disk first to make sure even if
    // the deleting fails or the process crashes (which is unlikely to happen).
    // The new process would see the latest `first_log_index'
    if (save_meta(first_index_kept) != 0) { // NOTE
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        done->status().set_error(EIO, "Fail to save meta");
        return;
    }
    std::vector<scoped_refptr<Segment> > popped;
    pop_segments(first_index_kept, &popped);
    for (auto& pop: popped) {
        pop->unlink();
        pop = NULL;
    }
}

void SegmentLogStorage::pop_segments_from_back(
        const int64_t last_index_kept,
        std::vector<scoped_refptr<Segment> >* popped,
        scoped_refptr<Segment>* last_segment) {
    popped->clear();
    popped->reserve(32);
    *last_segment = NULL;
    BAIDU_SCOPED_LOCK(_mutex);
    _last_log_index.store(last_index_kept, base::memory_order_release);
    if (_open_segment) {
        if (_open_segment->first_index() <= last_index_kept) {
            *last_segment = _open_segment;
            return;
        }
        popped->push_back(_open_segment);
        _open_segment = NULL;
    }
    for (SegmentMap::reverse_iterator 
            it = _segments.rbegin(); it != _segments.rend(); ++it) {
        if (it->second->first_index() <= last_index_kept) {
            // Not return as we need to maintain _segments at the end of this
            // routine
            break;
        }
        popped->push_back(it->second);
        //XXX: C++03 not support erase reverse_iterator
    }
    for (size_t i = 0; i < popped->size(); i++) {
        _segments.erase((*popped)[i]->first_index());
    }
    if (_segments.rbegin() != _segments.rend()) {
        *last_segment = _segments.rbegin()->second;
    } else {
        // all the logs have been cleared, the we move _first_log_index to the
        // next index
        _first_log_index.store(last_index_kept + 1, base::memory_order_release);
    }
}

void SegmentLogStorage::truncate_suffix(const int64_t last_index_kept, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    // segment files
    std::vector<scoped_refptr<Segment> > popped;
    scoped_refptr<Segment> last_segment;
    pop_segments_from_back(last_index_kept, &popped, &last_segment);
    bool truncate_last_segment = false;
    int ret = -1;

    if (last_segment) {
        if (_first_log_index.load(base::memory_order_relaxed) <=
            _last_log_index.load(base::memory_order_relaxed)) {
            truncate_last_segment = true;
        } else {
            // trucate_prefix() and truncate_suffix() to discard entire logs
            BAIDU_SCOPED_LOCK(_mutex);
            popped.push_back(last_segment);
            _segments.erase(last_segment->first_index());
            if (_open_segment) {
                CHECK_EQ(_open_segment.get(), last_segment.get());
                _open_segment = NULL;
            }
        }
    }

    // The truncate suffix order is crucial to satisfy log matching property of raft
    // log must be truncated from back to front.
    for (size_t i = 0; i < popped.size(); ++i) {
        ret = popped[i]->unlink();
        if (ret != 0) {
            done->status().set_error(EIO, "unlink error");
            return;
        }
        popped[i] = NULL;
    }
    if (truncate_last_segment) {
        bool closed = !last_segment->is_open();
        last_segment->truncate(last_index_kept, (Closure*)done_guard.release());
        if (closed && last_segment->is_open()) {
            BAIDU_SCOPED_LOCK(_mutex);
            CHECK(!_open_segment);
            CHECK_EQ(last_segment, _segments.rbegin()->second);
            _segments.erase(last_segment->first_index());
            _open_segment.swap(last_segment);
        }
    }
}

void SegmentLogStorage::reset(const int64_t next_log_index, Closure* done) {
    rpc::ClosureGuard done_guard(done);
    if (next_log_index <= 0) {
        LOG(ERROR) << "Invalid next_log_index=" << next_log_index
                   << " path: " << _path;
        done->status().set_error(EINVAL, "Invalid next_log_index=%ld, path: %s",
                next_log_index, _path.c_str());
        return;
    }
    std::vector<scoped_refptr<Segment> > popped;
    std::unique_lock<Mutex> lck(_mutex);
    popped.reserve(_segments.size());
    for (SegmentMap::const_iterator
            it = _segments.begin(); it != _segments.end(); ++it) {
        popped.push_back(it->second);
    }
    _segments.clear();
    if (_open_segment) {
        popped.push_back(_open_segment);
        _open_segment = NULL;
    }
    _first_log_index.store(next_log_index, base::memory_order_relaxed);
    _last_log_index.store(next_log_index - 1, base::memory_order_relaxed);
    lck.unlock();
    // NOTE: see the comments in truncate_prefix
    if (save_meta(next_log_index) != 0) {
        PLOG(ERROR) << "Fail to save meta, path: " << _path;
        done->status().set_error(EIO, "save meta error");
        return;
    }
    for (size_t i = 0; i < popped.size(); ++i) {
        popped[i]->unlink();
        popped[i] = NULL;
    }
}

int SegmentLogStorage::list_segments(bool is_empty) {
    base::DirReaderPosix dir_reader(_path.c_str());
    if (!dir_reader.IsValid()) {
        LOG(WARNING) << "directory reader failed, maybe NOEXIST or PERMISSION."
                     << " path: " << _path;
        return -1;
    }

    // restore segment meta
    while (dir_reader.Next()) {
        // unlink unneed segments and unfinished unlinked segments
        if ((is_empty && 0 == strncmp(dir_reader.name(), "log_", strlen("log_"))) ||
            (0 == strncmp(dir_reader.name() + (strlen(dir_reader.name()) - strlen(".tmp")),
                          ".tmp", strlen(".tmp")))) {
            std::string segment_path(_path);
            segment_path.append("/");
            segment_path.append(dir_reader.name());
            ::unlink(segment_path.c_str());

            LOG(WARNING) << "unlink unused segment, path: " << segment_path;

            continue;
        }

        int match = 0;
        int64_t first_index = 0;
        int64_t last_index = 0;
        match = sscanf(dir_reader.name(), RAFT_SEGMENT_CLOSED_PATTERN, 
                       &first_index, &last_index);
        if (match == 2) {
            LOG(INFO) << "restore closed segment, path: " << _path
                      << " first_index: " << first_index
                      << " last_index: " << last_index;
            Segment* segment = new Segment(_path, first_index, last_index, _checksum_type);
            _segments[first_index] = segment;
            continue;
        }

        match = sscanf(dir_reader.name(), RAFT_SEGMENT_OPEN_PATTERN, 
                       &first_index);
        if (match == 1) {
            RAFT_VLOG << "restore open segment, path: " << _path
                << " first_index: " << first_index;
            if (!_open_segment) {
                _open_segment = new Segment(_path, first_index, _checksum_type);
                continue;
            } else {
                LOG(WARNING) << "open segment conflict, path: " << _path
                    << " first_index: " << first_index;
                return -1;
            }
        }
    }

    // check segment
    int64_t last_log_index = -1;
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end(); ) {
        Segment* segment = it->second.get();
        if (segment->first_index() > segment->last_index()) {
            LOG(WARNING) << "closed segment is bad, path: " << _path
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index != -1 &&
                   segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "closed segment not in order, path: " << _path
                << " first_index: " << segment->first_index()
                << " last_log_index: " << last_log_index;
            return -1;
        } else if (last_log_index == -1 &&
                    _first_log_index.load(base::memory_order_acquire) 
                    < segment->first_index()) {
            LOG(WARNING) << "closed segment has hole, path: " << _path
                << " first_log_index: " << _first_log_index.load(base::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            return -1;
        } else if (last_log_index == -1 &&
                   _first_log_index > segment->last_index()) {
            LOG(WARNING) << "closed segment need discard, path: " << _path
                << " first_log_index: " << _first_log_index.load(base::memory_order_relaxed)
                << " first_index: " << segment->first_index()
                << " last_index: " << segment->last_index();
            segment->unlink();
            _segments.erase(it++);
            continue;
        }

        last_log_index = segment->last_index();
        ++it;
    }
    if (_open_segment) {
        if (last_log_index == -1 &&
                _first_log_index.load(base::memory_order_relaxed) < _open_segment->first_index()) {
        LOG(WARNING) << "open segment has hole, path: " << _path
            << " first_log_index: " << _first_log_index.load(base::memory_order_relaxed)
            << " first_index: " << _open_segment->first_index();
        } else if (last_log_index != -1 && _open_segment->first_index() != last_log_index + 1) {
            LOG(WARNING) << "open segment has hole, path: " << _path
                << " first_log_index: " << _first_log_index.load(base::memory_order_relaxed)
                << " first_index: " << _open_segment->first_index();
        }
        CHECK_LE(last_log_index, _open_segment->last_index());
    }

    return 0;
}

int SegmentLogStorage::load_segments(ConfigurationManager* configuration_manager) {
    int ret = 0;

    // closed segments
    SegmentMap::iterator it;
    for (it = _segments.begin(); it != _segments.end(); ++it) {
        Segment* segment = it->second.get();
        LOG(INFO) << "load closed segment, path: " << _path
            << " first_index: " << segment->first_index()
            << " last_index: " << segment->last_index();
        ret = segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        }
        _last_log_index.store(segment->last_index(), base::memory_order_release);
    }

    // open segment
    if (_open_segment) {
        LOG(INFO) << "load open segment, path: " << _path
            << " first_index: " << _open_segment->first_index();
        ret = _open_segment->load(configuration_manager);
        if (ret != 0) {
            return ret;
        }
        if (_first_log_index.load() > _open_segment->last_index()) {
            LOG(WARNING) << "open segment need discard, path: " << _path
                << " first_log_index: " << _first_log_index.load()
                << " first_index: " << _open_segment->first_index()
                << " last_index: " << _open_segment->last_index();
            _open_segment->unlink();
            _open_segment = NULL;
        } else {
            _last_log_index.store(_open_segment->last_index(), 
                                 base::memory_order_release);
        }
    }
    if (_last_log_index == 0) {
        _last_log_index = _first_log_index - 1;
    }
    return 0;
}

int SegmentLogStorage::save_meta(const int64_t log_index) {
    base::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" RAFT_SEGMENT_META_FILE);

    LogPBMeta meta;
    meta.set_first_log_index(log_index);
    ProtoBufFile pb_file(meta_path);
    int ret = pb_file.save(&meta, raft_sync_meta());

    timer.stop();
    PLOG_IF(ERROR, ret != 0) << "Fail to save meta to " << meta_path;
    LOG(INFO) << "log save_meta " << meta_path << " first_log_index: " << log_index
              << " time: " << timer.u_elapsed();
    return ret;
}

int SegmentLogStorage::load_meta() {
    base::Timer timer;
    timer.start();

    std::string meta_path(_path);
    meta_path.append("/" RAFT_SEGMENT_META_FILE);

    ProtoBufFile pb_file(meta_path);
    LogPBMeta meta;
    if (0 != pb_file.load(&meta)) {
        PLOG_IF(ERROR, errno != ENOENT) << "Fail to load meta from " << meta_path;
        return -1;
    }

    _first_log_index.store(meta.first_log_index());

    timer.stop();
    LOG(INFO) << "log load_meta " << meta_path << " first_log_index: " << meta.first_log_index()
              << " time: " << timer.u_elapsed();
    return 0;
}

scoped_refptr<Segment> SegmentLogStorage::open_segment() {
    scoped_refptr<Segment> prev_open_segment;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        if (!_open_segment) {
            _open_segment = new Segment(_path, _last_log_index + 1, _checksum_type);
            if (_open_segment->create() != 0) {
                _open_segment = NULL;
                return NULL;
            }
        }
        if (_open_segment->bytes() > FLAGS_raft_max_segment_size) {
            _segments[_open_segment->first_index()] = _open_segment;
            prev_open_segment.swap(_open_segment);
        }
    }
    do {
        if (prev_open_segment) {
            if (prev_open_segment->close(_enable_sync) == 0) {
                BAIDU_SCOPED_LOCK(_mutex);
                _open_segment = new Segment(_path, _last_log_index + 1, _checksum_type);
                if (_open_segment->create() == 0) {
                    // success
                    break;
                }
            }
            PLOG(ERROR) << "Fail to close old open_segment or create new open_segment"
                        << " path: " << _path;
            // Failed, revert former changes
            BAIDU_SCOPED_LOCK(_mutex);
            _segments.erase(prev_open_segment->first_index());
            _open_segment.swap(prev_open_segment);
            return NULL;
        }
    } while (0);
    return _open_segment;
}

int SegmentLogStorage::get_segment(int64_t index, scoped_refptr<Segment>* ptr) {
    BAIDU_SCOPED_LOCK(_mutex);
    int64_t first_index = first_log_index();
    int64_t last_index = last_log_index();
    if (first_index == last_index + 1) {
        return -1;
    }
    if (index < first_index || index > last_index + 1) {
        LOG_IF(WARNING, index > last_index) << "Attempted to access entry " << index
            << " outside of log, first_log_index: " << first_index
            << " last_log_index: " << last_index;
        return -1;
    } else if (index == last_index + 1) {
        return -1;
    }

    if (_open_segment && index >= _open_segment->first_index()) {
        *ptr = _open_segment;
        CHECK(ptr->get() != NULL);
    } else {
        CHECK(!_segments.empty());
        SegmentMap::iterator it = _segments.upper_bound(index);
        SegmentMap::iterator saved_it = it;
        --it;
        CHECK(it != saved_it);
        *ptr = it->second;
    }
    return 0;
}

void SegmentLogStorage::list_files(std::vector<std::string>* seg_files) {
    BAIDU_SCOPED_LOCK(_mutex);
    seg_files->push_back(RAFT_SEGMENT_META_FILE);
    for (SegmentMap::iterator it = _segments.begin(); it != _segments.end(); ++it) {
        scoped_refptr<Segment>& segment = it->second;
        seg_files->push_back(segment->file_name());
    }
    if (_open_segment) {
        seg_files->push_back(_open_segment->file_name());
    }
}

void SegmentLogStorage::sync() {
    std::vector<scoped_refptr<Segment> > segments;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        for (SegmentMap::iterator it = _segments.begin(); it != _segments.end(); ++it) {
            segments.push_back(it->second);
        }
        if (_open_segment) {
            segments.push_back(_open_segment);
        }
    }

    for (size_t i = 0; i < segments.size(); i++) {
        segments[i]->force_sync();
    }
}

LogStorage* SegmentLogStorage::new_instance(const std::string& uri) const {
    return new SegmentLogStorage(uri);
}

base::Status SegmentLogStorage::gc_instance(const std::string& uri) const {
    base::Status status;
    if (gc_dir(uri) != 0) {
        LOG(WARNING) << "Failed to gc log storage from path " << _path;
        status.set_error(EINVAL, "Failed to gc log storage from path %s", 
                         uri.c_str());
        return status;
    }
    LOG(INFO) << "Succeed to gc log storage from path " << uri;
    return status;
}

}
