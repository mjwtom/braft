// Copyright (c) 2016 Baidu.com, Inc. All Rights Reserved
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
//          Zheng,Pengfei(zhengpengfei@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)
//          Yang,Guodong(yangguodong01@baidu.com)

#include "raft/file_reader.h"
#include "raft/util.h"
#include "raft/closure_helper.h"

namespace raft {

LocalDirReader::~LocalDirReader() {
    if (_current_file) {
        _current_file->close();
        delete _current_file;
        _current_file = NULL;
    }
    _fs->close_snapshot(_path);
}

bool LocalDirReader::open() {
    return _fs->open_snapshot(_path);
}

void LocalDirReader::read_file(base::IOPortal* portal,
                               const std::string &filename,
                               off_t offset,
                               size_t max_count,
                               bool read_partly,
                               size_t* read_count,
                               bool* is_eof,
                               Closure* done) {
    read_file_with_meta(portal, filename, NULL, offset, max_count, read_count, is_eof, done);
}

void LocalDirReader::read_file_with_meta(base::IOPortal* portal,
                                         const std::string &filename,
                                         google::protobuf::Message* file_meta,
                                         off_t offset,
                                         size_t max_count,
                                         size_t* read_count,
                                         bool* is_eof,
                                         Closure* done) {
    rpc::ClosureGuard done_guard(done);
    int ret = open_file_with_meta(filename, file_meta, offset);
    if (ret != 0) {
        done->status().set_error(ret, "open file failed, %s", filename.c_str());
        return;
    }

    done = NewCallback(this, &LocalDirReader::read_file_with_meta_done, offset, max_count,
                       const_cast<const size_t*>(read_count), is_eof,
                       dynamic_cast<Closure*>(done_guard.release()));

    _current_file->read(portal, offset, max_count, read_count, done);
}


int LocalDirReader::open_file_with_meta(const std::string &filename,
                                        google::protobuf::Message* file_meta,
                                        off_t offset) const {
    std::unique_lock<Mutex> lck(_mutex);
    if (_is_reading) {
        // Just let follower retry, if there already a reading request in process.
        lck.unlock();
        RAFT_VLOG << "A courrent read file is in process, path: " << _path;
        return EAGAIN;
    }
    if (filename != _current_filename) {
        if (!_eof_reached || offset != 0) {
            lck.unlock();
            RAFT_VLOG << "Out of order read request, path: " << _path
                      << " filename: " << filename << " offset: " << offset;
            return EINVAL;
        }
        if (_current_file) {
            _current_file->close();
            delete _current_file;
            _current_file = NULL;
            _current_filename.clear();
        }
        std::string file_path(_path + "/" + filename);
        base::File::Error e;
        FileAdaptor* file = _fs->open(file_path, O_RDONLY | O_CLOEXEC, file_meta, &e);
        if (!file) {
            return file_error_to_os_error(e);
        }
        _current_filename = filename;
        _current_file = file;
        _eof_reached = false;
    }
    _is_reading = true;
    return 0;
}

void LocalDirReader::read_file_with_meta_done(off_t offset, size_t max_count,
        const size_t* read_count, bool* is_eof, Closure* done, const base::Status& status) {
    rpc::ClosureGuard done_guard(done);
    
    do {
        if (!status.ok()) {
            done->status() = status;
            break;
        }
        *is_eof = false;
        ssize_t size = _current_file->size();
        if (size < 0) {
            done->status().set_error(EIO, berror(EIO));
            return;
        }
        if (size == ssize_t(offset + *read_count)) {
            *is_eof = true;
        }
        if (*read_count < max_count) {
            CHECK_EQ(offset+*read_count, size)
                << "offset: " << offset << ", required: " << max_count
                << ", actual_read: " << *read_count
                << ", _current_file: " << _current_file
                << ", size: " << size;
            CHECK(*is_eof)
                << "offset: " << offset << ", required: " << max_count
                << ", actual_read: " << *read_count
                << ", _current_file: " << _current_file
                << ", size: " << size;
        } else {
            CHECK_EQ(*read_count, max_count)
                << "offset: " << offset << ", required: " << max_count
                << ", actual_read: " << *read_count
                << ", _current_file: " << _current_file
                << ", size: " << size;
            CHECK_LE(offset+*read_count, size)
                << "offset: " << offset << ", required: " << max_count
                << ", actual_read: " << *read_count
                << ", _current_file: " << _current_file
                << ", size: " << size;
        }
    } while (false);

    std::unique_lock<Mutex> lck(_mutex);
    _is_reading = false;
    if (done->status().ok()) {
        _eof_reached = *is_eof;
    }
    lck.unlock(); // avoid dead lock when done_guard call done->Run()
}

}  // namespace raft
