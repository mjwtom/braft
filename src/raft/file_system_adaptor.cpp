// Copyright (c) 2017 Baidu.com, Inc. All Rights Reserved
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

// Authors: Zheng,PengFei(zhengpengfei@baidu.com)

#include <base/fd_utility.h>                        // base::make_close_on_exec
#include <base/memory/singleton_on_pthread_once.h>  // base::get_leaky_singleton
#include "raft/io_uring.h"
#include "raft/file_system_adaptor.h"
#include "raft/rtc_helper.h"

namespace raft {

DECLARE_bool(raft_use_iouring);

bool PosixDirReader::is_valid() const {
    return _dir_reader.IsValid();
}

bool PosixDirReader::next() {
    bool rc = _dir_reader.Next();
    while (rc && (strcmp(name(), ".") == 0 || strcmp(name(), "..") == 0)) {
        rc = _dir_reader.Next();
    }
    return rc;
}

const char* PosixDirReader::name() const {
    return _dir_reader.name();
}

void PosixFileAdaptor::write(const base::IOBuf& data, off_t offset, Closure* done) {
    g_raft_io_man->write(_fd->fd(), offset, data, done);
}

void PosixFileAdaptor::read(base::IOPortal* portal, off_t offset, size_t size, 
                            size_t *nread, Closure* done) {
    CHECK(nread) << _path;
    auto l = [=] (const base::Status& st) {
        rpc::ClosureGuard done_guard(done);
        if (!st.ok()) {
            LOG(WARNING) << "read: " << _path << ", error: " << st;
            done->status() = st;
            *nread = 0;
            return;
        }
        *nread = portal->size();
    };
    g_raft_io_man->read(_fd->fd(), offset, portal, size,
            create_raft_closure(std::move(l)));
}

PosixFileAdaptor::~PosixFileAdaptor() {
}

ssize_t PosixFileAdaptor::write(const base::IOBuf& data, off_t offset) {
    RAFT_VLOG << "Write to fd=" << _fd->fd() << ", path=" << _path << ", offset "
        << offset << ", data_size " << data.size();
    return raft::file_pwrite(data, fd(), offset);
}

ssize_t PosixFileAdaptor::read(base::IOPortal* portal, off_t offset, size_t size) {
    RAFT_VLOG << "Read from fd=" << _fd->fd() << ", path=" << _path << ", offset "
        << offset << ", data_size " << size;
    return raft::file_pread(portal, fd(), offset, size);
}
// ===TODO: remove above code===

ssize_t PosixFileAdaptor::size() {
    off_t sz = lseek(fd(), 0, SEEK_END);
    return ssize_t(sz);
}

bool PosixFileAdaptor::sync() {
    auto holder = _fd; // protect from release
    auto sync_fd = fd();
    auto l = [=] (const base::Status& st) {
        if ((!st.ok()) && (raft_fsync(sync_fd) != 0)) {
            LOG(WARNING) << "Fail to sync " << sync_fd << ": " << st
                << ", holder: " << holder;
        }
        LOG(INFO) << "Finish sync: " << sync_fd << ", holder: " << holder;
    };
    g_raft_io_man->sync(sync_fd, create_raft_closure(std::move(l)));
    return true;
}

FdGuard::FdGuard(int fd) : _fd(fd) {}

FdGuard::~FdGuard() {
    if (_fd >= 0) {
        LOG(INFO) << "System close fd=" << _fd;
        ::close(_fd);
    }
    _fd = -1;
}

int FdGuard::fd() {
    return _fd;
}

bool PosixFileAdaptor::close() {
    LOG(INFO) << "Prepare close fd=" << fd() << " path=" << _path;
    _fd = nullptr;
    _path.clear();
    return true;
}

void BufferedSequentialReadFileAdaptor::read(
            base::IOPortal* portal, off_t offset, size_t size, 
            size_t *nread, Closure* done) {
    CHECK(done);
    CHECK(nread);
    AsyncClosureGuard done_guard(done);
    if (_error) {
        done->status().set_error(_error, "io error");
        *nread = 0;
        errno = _error;
        return;
    }
    
    RAFT_VLOG << "begin read offset " << offset << " count " << size
              << ", buffer_offset " << _buffer_offset
              << " buffer_size " << _buffer_size;
    if (offset < _buffer_offset || offset > off_t(_buffer_offset + _buffer_size)) {
        LOG(WARNING) << "Fail to read from buffered file adaptor with invalid range"
            << ", buffer_offset: " << _buffer_offset
            << ", buffer_size: " << _buffer_size
            << ", read offset: " << offset
            << ", read size: " << size;
        errno = EINVAL;
        *nread = 0;
        done->status().set_error(EINVAL, "io error");
        return;
    }
    if (offset > _buffer_offset) {
        _buffer.pop_front(std::min(size_t(offset - _buffer_offset), _buffer.size()));
        _buffer_size -= (offset - _buffer_offset);
        _buffer_offset = offset;
    }
    off_t end_offset = offset + size;
    if (!_reach_file_eof && end_offset > off_t(_buffer_offset + _buffer_size)) {
        base::IOPortal tmp_portal;
        size_t need_count = end_offset - _buffer_offset - _buffer_size;
        size_t read_count = 0;
        int rc = do_read(&tmp_portal, need_count, &read_count);
        if (rc != 0) {
            _error = ((errno != 0) ? errno : EIO);
            errno = _error;
            *nread = 0;
            done->status().set_error(_error, "io error");
            return;
        }
        _reach_file_eof = (read_count < need_count);
        if (!tmp_portal.empty()) {
            _buffer.resize(_buffer_size);
            _buffer.append(tmp_portal);
        }
        _buffer_size += read_count;
    }
    *nread = std::min(_buffer_size, size);
    if (!_buffer.empty()) {
        _buffer.append_to(portal, std::min(_buffer.size(), *nread));
    }
}

void BufferedSequentialWriteFileAdaptor::write(const base::IOBuf& data,
        off_t offset, Closure* done) {
    CHECK(done);
    AsyncClosureGuard done_guard(done);
    if (_error) {
        done->status().set_error(_error, "io error");
        errno = _error;
        return;
    }
    
    RAFT_VLOG << "begin write offset " << offset << ", data_size " << data.size()
              << ", buffer_offset " << _buffer_offset 
              << ", buffer_size " << _buffer_size;
    if (offset < _buffer_offset + static_cast<off_t>(_buffer_size)) {
        LOG(WARNING) << "Fail to write into buffered file adaptor with invalid range"
                     << ", offset: " << offset
                     << ", data_size: " << data.size()
                     << ", buffer_offset: " << _buffer_offset
                     << ", buffer_size: " << _buffer_size;
        errno = EINVAL;
        done->status().set_error(EINVAL, "io error");
        return;
    }
    if (offset > _buffer_offset + static_cast<off_t>(_buffer_size)) {
        // passby hole
        CHECK(_buffer_size == 0);
        RAFT_VLOG << "seek to new offset " << offset << " as there is hole";
        seek(offset);
    }
    _buffer.append(data);
    _buffer_size = _buffer.size();
    if (_buffer.size() > 0) {
        size_t write_count = 0;
        int rc = do_write(_buffer, &write_count);
        if (rc < 0) {
            _error = ((errno != 0) ? errno : EIO);
            done->status().set_error(_error, "io error");
            errno = _error;
            return;
        }
        _buffer_offset += write_count; 
        _buffer_size -= write_count;
        _buffer.pop_front(write_count);
        CHECK_EQ(_buffer_size, _buffer.size());
    }
}

static pthread_once_t s_check_cloexec_once = PTHREAD_ONCE_INIT;
static bool s_support_cloexec_on_open = false;

static void check_cloexec(void) {
    int fd = ::open("/dev/zero", O_RDONLY | O_CLOEXEC, 0644);
    s_support_cloexec_on_open = (fd != -1);
    if (fd != -1) {
        ::close(fd);
    }
}

FileAdaptor* PosixFileSystemAdaptor::open(const std::string& path, int oflag, 
                                     const ::google::protobuf::Message* file_meta,
                                     base::File::Error* e) {
    (void) file_meta;
    pthread_once(&s_check_cloexec_once, check_cloexec);
    bool cloexec = (oflag & O_CLOEXEC);
    if (cloexec && !s_support_cloexec_on_open) {
        oflag &= (~O_CLOEXEC);
    }
    int fd = ::open(path.c_str(), oflag, 0644);
    if (e) {
        *e = (fd == -1) ? base::File::OSErrorToFileError(errno) : base::File::FILE_OK;
    }
    if (fd == -1) {
        return NULL;
    }
    if (cloexec && !s_support_cloexec_on_open) {
        base::make_close_on_exec(fd);
    }
    LOG(INFO) << "Open " << path << " with fd=" << fd;
    return new PosixFileAdaptor(fd, path);
}

bool PosixFileSystemAdaptor::delete_file(const std::string& path, bool recursive) {
    base::FilePath file_path(path);
    return base::DeleteFile(file_path, recursive);
}

bool PosixFileSystemAdaptor::rename(const std::string& old_path, const std::string& new_path) {
    return ::rename(old_path.c_str(), new_path.c_str()) == 0;
}

bool PosixFileSystemAdaptor::link(const std::string& old_path, const std::string& new_path) {
    return ::link(old_path.c_str(), new_path.c_str()) == 0;
}

bool PosixFileSystemAdaptor::create_directory(const std::string& path, 
                                         base::File::Error* error,
                                         bool create_parent_directories) {
    base::FilePath dir(path);
    return base::CreateDirectoryAndGetError(dir, error, create_parent_directories);
}

bool PosixFileSystemAdaptor::path_exists(const std::string& path) {
    base::FilePath file_path(path);
    return base::PathExists(file_path);
}

bool PosixFileSystemAdaptor::directory_exists(const std::string& path) {
    base::FilePath file_path(path);
    return base::DirectoryExists(file_path);
}

DirReader* PosixFileSystemAdaptor::directory_reader(const std::string& path) {
    return new PosixDirReader(path.c_str());
}

FileSystemAdaptor* default_file_system() {
    static scoped_refptr<PosixFileSystemAdaptor> fs = 
        base::get_leaky_singleton<PosixFileSystemAdaptor>();
    return fs.get();
}

int file_error_to_os_error(base::File::Error e) {
    switch (e) {
        case base::File::FILE_OK: 
            return 0;
        case base::File::FILE_ERROR_IN_USE:
            return EAGAIN;
        case base::File::FILE_ERROR_ACCESS_DENIED:
            return EACCES;
        case base::File::FILE_ERROR_EXISTS:
            return EEXIST;
        case base::File::FILE_ERROR_NOT_FOUND:
            return ENOENT;
        case base::File::FILE_ERROR_TOO_MANY_OPENED:
            return EMFILE;
        case base::File::FILE_ERROR_NO_MEMORY:
            return ENOMEM;
        case base::File::FILE_ERROR_NO_SPACE:
            return ENOSPC;
        case base::File::FILE_ERROR_NOT_A_DIRECTORY:
            return ENOTDIR;
        case base::File::FILE_ERROR_IO:
            return EIO;
        default:
            return EINVAL;
    };
}

bool create_sub_directory(const std::string& parent_path,
                          const std::string& sub_path,
                          FileSystemAdaptor* fs,
                          base::File::Error* error) {
    if (!fs) {
        fs = default_file_system();
    }
    if (!fs->directory_exists(parent_path)) {
        if (error) {
            *error = base::File::FILE_ERROR_NOT_FOUND;
        }
        return false;
    }
    base::FilePath sub_dir_path(sub_path);
    if (sub_dir_path.ReferencesParent()) {
        if (error) {
            *error = base::File::FILE_ERROR_INVALID_URL;
        }
        return false;
    }
    std::vector<base::FilePath> subpaths;

    // Collect a list of all parent directories.
    base::FilePath last_path = sub_dir_path;
    subpaths.push_back(sub_dir_path.BaseName());
    for (base::FilePath path = last_path.DirName();
            path.value() != last_path.value(); path = path.DirName()) {
        subpaths.push_back(path.BaseName());
        last_path = path;
    }
    base::FilePath full_path(parent_path);
    for (std::vector<base::FilePath>::reverse_iterator i = subpaths.rbegin();
            i != subpaths.rend(); ++i) {
        if (i->value() == "/") {
            continue;
        }
        if (i->value() == ".") {
            continue;
        }
        full_path = full_path.Append(*i);
        DLOG(INFO) << "Creating " << full_path.value();
        if (!fs->create_directory(full_path.value(), error, false)) {
            return false;
        }
    }
    return true;
}


} // namespace raft
