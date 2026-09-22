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

#ifndef  PUBLIC_RAFT_FILE_READER_H
#define  PUBLIC_RAFT_FILE_READER_H

#include <set>                              // std::set
#include <base/memory/ref_counted.h>        // base::RefCountedThreadsafe
#include <base/iobuf.h>                     // base::IOBuf
#include "raft/macros.h"
#include "raft/file_system_adaptor.h"
#include "raft/rtc_helper.h"                       // raft::Mutex

namespace raft {

// Abstract class to read data from a file
// All the const method should be thread safe
class FileReader : public RaftRefCounted<FileReader> {
friend class RaftRefCounted<FileReader>;
public:
    // Read data from filename at |offset| (from the start of the file) for at
    // most |max_count| bytes to |out|. Reading part of a file is allowed if 
    // |read_partly| is TRUE. If successfully read the file, |read_count|
    // is the actual read count, it's maybe smaller than |max_count| if the
    // request is throttled or reach the end of the file. In the case of
    // reaching the end of the file, |is_eof| is also set to true.
    // Returns 0 on success, the error otherwise
    virtual void read_file(base::IOPortal* portal,
                           const std::string &filename,
                           off_t offset,
                           size_t max_count,
                           bool read_partly,
                           size_t* read_count,
                           bool* is_eof,
                           Closure* done) = 0;
    // Get the path of this reader
    virtual const std::string& path() const = 0;

    int32_t polar_worker_index() { return _polar_worker_index; }

protected:
    FileReader() : RaftRefCounted<FileReader>(false), // shared in file_service
        _polar_worker_index(-1) {}
    explicit FileReader(int32_t polar_worker_index) : RaftRefCounted<FileReader>(false), // shared in file_service
        _polar_worker_index(polar_worker_index) {}
    virtual ~FileReader() {}

private:
    int32_t _polar_worker_index;
};

// Read files within a local directory
class LocalDirReader : public FileReader {
public:
    LocalDirReader(int32_t polar_worker_index, FileSystemAdaptor* fs, const std::string& path) 
        : FileReader(polar_worker_index)
        , _mutex(FLAGS_raft_use_polar)
        , _path(path)
        , _fs(fs)
        , _current_file(NULL)
        , _is_reading(false)
        , _eof_reached(true)
    {}
    virtual ~LocalDirReader();

    // Open a snapshot for read
    virtual bool open();
    // Read data from filename at |offset| (from the start of the file) for at
    // most |max_count| bytes to |out|. Reading part of a file is allowed if 
    // |read_partly| is TRUE. If successfully read the file, |read_count|
    // is the actual read count, it's maybe smaller than |max_count| if the
    // request is throttled or reach the end of the file. In the case of
    // reaching the end of the file, |is_eof| is also set to true.
    // Returns 0 on success, the error otherwise
    virtual void read_file(base::IOPortal* portal,
                           const std::string &filename,
                           off_t offset,
                           size_t max_count,
                           bool read_partly,
                           size_t* read_count,
                           bool* is_eof,
                           Closure* done);
    virtual const std::string& path() const { return _path; }

protected:
    void read_file_with_meta(base::IOPortal* portal,
                             const std::string &filename,
                             google::protobuf::Message* file_meta,
                             off_t offset,
                             size_t max_count,
                             size_t* read_count,
                             bool* is_eof,
                             Closure* done);
    const scoped_refptr<FileSystemAdaptor>& file_system() const { return _fs; }

private:
    int open_file_with_meta(const std::string &filename, google::protobuf::Message* file_meta,
                            off_t offset) const;
    void read_file_with_meta_done(off_t offset, size_t max_count,
            const size_t* read_count, bool* is_eof, Closure* done,
            const base::Status& status);

private:
    mutable Mutex _mutex;
    std::string _path;
    scoped_refptr<FileSystemAdaptor> _fs;
    mutable FileAdaptor* _current_file;
    mutable std::string _current_filename;
    mutable bool _is_reading;
    mutable bool _eof_reached;
};

}  // namespace raft

#endif  //PUBLIC_RAFT_FILE_READER_H
