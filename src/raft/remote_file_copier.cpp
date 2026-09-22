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
//          Zheng,Pengfei(zhengpengfei@baidu.com)
//          Xiong,Kai(xiongkai@baidu.com)

#include <gflags/gflags.h>
#include <base/strings/string_piece.h>
#include <base/strings/string_number_conversions.h>
#include <base/files/file_path.h>
#include <base/file_util.h>
#include <bthread.h>
#include "raft/util.h"
#include "raft/snapshot.h"
#include "raft/closure_helper.h"
#include "raft/remote_file_copier.h"

namespace raft {

DEFINE_int32(raft_max_byte_count_per_rpc, 1024 * 128 /*128K*/,
             "Maximum of block size per RPC");
RAFT_VALIDATE_GFLAG(raft_max_byte_count_per_rpc, rpc::PositiveInteger);

DEFINE_bool(raft_allow_read_partly_when_install_snapshot, true,
            "Whether allowing read snapshot data partly");
RAFT_VALIDATE_GFLAG(raft_allow_read_partly_when_install_snapshot, 
            ::rpc::PassValidate);

DEFINE_bool(raft_enable_throttle_when_install_snapshot, true, 
            "enable throttle when install snapshot, for both leader and follower");
RAFT_VALIDATE_GFLAG(raft_enable_throttle_when_install_snapshot, 
            ::rpc::PassValidate);

DEFINE_int32(raft_copy_remote_file_timeout_ms, 10 * 1000 /*10s*/,
             "timeout of copying remote file when install_snapshot");
RAFT_VALIDATE_GFLAG(raft_copy_remote_file_timeout_ms, rpc::PositiveInteger);

#ifndef UNIT_TEST
static BvarAdder<uint64_t> g_remote_copier_crc_error("raft_remote_copier_crc_error");
#else
// Unit tests should check this value
BvarAdder<uint64_t> g_remote_copier_crc_error("raft_remote_copier_crc_error");
#endif

RemoteFileCopier::RemoteFileCopier()
    : _reader_id(0)
    , _throttle(NULL)
    , _polar_idx(current_polar_idx())
{}

int RemoteFileCopier::init(const std::string& uri, FileSystemAdaptor* fs, 
        SnapshotThrottle* throttle) {
    // Parse uri format: remote://ip:port/reader_id
    static const size_t prefix_size = strlen("remote://");
    base::StringPiece uri_str(uri);
    if (!uri_str.starts_with("remote://")) {
        LOG(ERROR) << "Invalid uri=" << uri;
        return -1;
    }
    uri_str.remove_prefix(prefix_size);
    size_t slash_pos = uri_str.find('/');
    base::StringPiece ip_and_port = uri_str.substr(0, slash_pos);
    uri_str.remove_prefix(slash_pos + 1);
    if (!base::StringToInt64(uri_str, &_reader_id)) {
        LOG(ERROR) << "Invalid reader_id_format=" << uri_str
                   << " in " << uri;
        return -1;
    }
    rpc::ChannelOptions channel_options;
    fill_rpc_channel_option(&channel_options);
    if (_channel.Init(ip_and_port.as_string().c_str(), &channel_options) != 0) {
        LOG(ERROR) << "Fail to init Channel to " << ip_and_port;
        return -1;
    }
    _fs = fs;
    _throttle = throttle;
    return 0;
}

int RemoteFileCopier::read_piece_of_file(
            base::IOBuf* buf,
            const std::string& source,
            off_t offset,
            size_t max_count,
            long timeout_ms,
            bool* is_eof) {
    rpc::Controller cntl;
    GetFileRequest request;
    request.set_reader_id(_reader_id);
    request.set_filename(source);
    request.set_count(max_count);
    request.set_offset(offset);
    GetFileResponse response;
    FileService_Stub stub(&_channel);
    cntl.set_timeout_ms(timeout_ms);
    stub.get_file(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << "Fail to issue RPC, " << cntl.ErrorText();
        return cntl.ErrorCode();
    }
    *is_eof = response.eof();
    buf->swap(cntl.response_attachment());
    return 0;
}

int RemoteFileCopier::copy_to_file(const std::string& source,
                                   const std::string& dest_path,
                                   const CopyOptions* options) {
    scoped_refptr<Session> session = start_to_copy_to_file(
            source, dest_path, options);
    if (session == NULL) {
        return -1;
    }
    session->join();
    return session->status().error_code();
}

int RemoteFileCopier::copy_to_iobuf(const std::string& source,
                                    base::IOBuf* dest_buf, 
                                    const CopyOptions* options) {
    scoped_refptr<Session> session = start_to_copy_to_iobuf(
                                        source, dest_buf, options);
    if (session == NULL) {
        return -1;
    }
    session->join();
    return session->status().error_code();
}

scoped_refptr<RemoteFileCopier::Session> 
RemoteFileCopier::start_to_copy_to_file(
                      const std::string& source,
                      const std::string& dest_path,
                      const CopyOptions* options) {
    base::File::Error e;
    FileAdaptor* file = _fs->open(dest_path, O_TRUNC | O_WRONLY | O_CREAT | O_CLOEXEC, NULL, &e);
    
    if (!file) {
        LOG(ERROR) << "Fail to open " << dest_path 
                   << ", " << base::File::ErrorToString(e);
        return NULL;
    }

    scoped_refptr<Session> session(new Session());
    session->_dest_path = dest_path;
    session->_file = file;
    session->_request.set_filename(source);
    session->_request.set_reader_id(_reader_id);
    session->_channel = &_channel;
    session->_polar_idx = _polar_idx;
    if (options) {
        session->_options = *options;
    }
    // pass throttle to Session
    if (_throttle) {
        session->_throttle = _throttle;
    }
    CHECK(!running_in_polar()) << "should not run in polar because of join";
    google::protobuf::Closure* done = google::protobuf::NewCallback(
            Session::on_timer, reinterpret_cast<void*>(session.get()));
                    // TODO: avoid background twice
    CHECK(done);
    session->AddRef(); // protect from release by cancel
    raft_run_closure_in_worker(done, _polar_idx);
    return session;
}

scoped_refptr<RemoteFileCopier::Session> 
RemoteFileCopier::start_to_copy_to_iobuf(
                      const std::string& source,
                      base::IOBuf* dest_buf,
                      const CopyOptions* options) {
    dest_buf->clear();
    scoped_refptr<Session> session(new Session());
    session->_file = NULL;
    session->_buf = dest_buf;
    session->_request.set_filename(source);
    session->_request.set_reader_id(_reader_id);
    session->_channel = &_channel;
    session->_polar_idx = _polar_idx;
    if (options) {
        session->_options = *options;
    }
    CHECK(!running_in_polar()) << "should not run in polar because of join";
    google::protobuf::Closure* done = google::protobuf::NewCallback(
            Session::on_timer, reinterpret_cast<void*>(session.get()));
                    // TODO: avoid background twice
    CHECK(done);
    session->AddRef(); // protect from release by cancel
    raft_run_closure_in_worker(done, _polar_idx);
    return session;
}

RemoteFileCopier::Session::Session() 
    : RaftRefCounted<Session>(false), _channel(NULL)
                            // ^^ run in bthread
    , _file(NULL)
    , _retry_times(0)
    , _finished(false)
    , _in_flight_io(false)
    , _buf(NULL)
    , _timer()
    , _throttle(NULL)
    , _throttle_token_acquire_time_us(0)
    , _polar_idx(-1)
{
    _done.owner = this;
}

RemoteFileCopier::Session::~Session() {
    if (_file) {
        _file->close();
        delete _file;
        _file = NULL;
    }
}

void RemoteFileCopier::Session::send_next_rpc() {
    if (FLAGS_raft_use_polar) {
        CHECK_GE(_polar_idx, 0);
        CHECK_EQ(_polar_idx, current_polar_idx());
    }
    _cntl.Reset();
    _cntl.set_log_id(base::fast_rand());
    _response.Clear();
    // Not clear request as we need some fields of the previous RPC
    off_t offset = _request.offset() + _request.count();
    const size_t max_count = 
            (!_buf) ? FLAGS_raft_max_byte_count_per_rpc : UINT_MAX;
    _cntl.set_timeout_ms(_options.timeout_ms);
    _request.set_offset(offset);
    // Read partly when throttled
    _request.set_read_partly(FLAGS_raft_allow_read_partly_when_install_snapshot);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_finished) {
        return;
    }
    // throttle
    size_t new_max_count = max_count;
    if (_throttle && FLAGS_raft_enable_throttle_when_install_snapshot) {
        _throttle_token_acquire_time_us = base::cpuwide_time_us();
        new_max_count = _throttle->throttled_by_throughput(max_count);
        if (new_max_count == 0) {
            RAFT_VLOG << "Copy file throttled, path: " << _dest_path;
            // Reset count to make next rpc retry the previous one
            _request.set_count(0);
            AddRef();
            int64_t retry_interval_ms_when_throttled = 
                                    _throttle->get_retry_interval_ms();
            if (raft_timer_add(
                    &_timer, 
                    base::milliseconds_from_now(retry_interval_ms_when_throttled),
                    on_timer, this) != 0) {
                lck.unlock();
                LOG(ERROR) << "Fail to add timer, file: " << _file
                    << ", path: " << _dest_path;
                return on_timer(this);
            }
            return;
        }
    }
    _request.set_count(new_max_count);
    _rpc_call = _cntl.call_id();
    FileService_Stub stub(_channel);
    AddRef();  // Release in on_rpc_returned
    return stub.get_file(&_cntl, &_request, &_response, &_done);
}

void RemoteFileCopier::Session::on_rpc_returned() {
    if (FLAGS_raft_use_polar) {
        CHECK_GE(_polar_idx, 0);
        CHECK_EQ(_polar_idx, current_polar_idx());
    }
    LOG(INFO) << "Path: " << _dest_path << ", copier rpc "
        << (_cntl.Failed() ? "failed, error_code: " + std::to_string(_cntl.ErrorCode()) + _cntl.ErrorText() :
                "success, attachment: " + std::to_string(_cntl.response_attachment().length()))
        << ", call_id: " << _cntl.call_id() << ", log_id: " << _cntl.log_id();
    scoped_refptr<Session> ref_guard;
    Session* this_ref = this;
    ref_guard.swap(&this_ref); // pair with send_next_rpc
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_finished) {
        return;
    }
    if (_cntl.Failed()) {
        LOG(WARNING) << "Copy to file: " << _dest_path
            << " failed: " << _cntl.ErrorText() << " code: " << _cntl.ErrorCode()
            << " call_id: " << _cntl.call_id() << " log_id: " << _cntl.log_id();
        // Reset count to make next rpc retry the previous one
        int64_t request_count = _request.count();
        _request.set_count(0);
        if (_cntl.ErrorCode() == ECANCELED) {
            if (_st.ok()) {
                _st.set_error(_cntl.ErrorCode(), _cntl.ErrorText());
                return on_finished();
            }
        }
        // Throttled reading failure does not increase _retry_times
        if (_cntl.ErrorCode() != EAGAIN && _retry_times++ >= _options.max_retry) {
            if (_st.ok()) {
                _st.set_error(_cntl.ErrorCode(), _cntl.ErrorText());
                return on_finished();
            }
        }
        // set retry time interval
        int64_t retry_interval_ms = _options.retry_interval_ms; 
        if (_cntl.ErrorCode() == EAGAIN && _throttle) {
            retry_interval_ms = _throttle->get_retry_interval_ms();
            // No token consumed, just return back, other nodes maybe able to use them
            if (FLAGS_raft_enable_throttle_when_install_snapshot) {
                _throttle->return_unused_throughput(
                        request_count, 0,
                        base::cpuwide_time_us() - _throttle_token_acquire_time_us);
            }
        }
        AddRef();
        if (raft_timer_add(
                    &_timer, 
                    base::milliseconds_from_now(retry_interval_ms),
                    on_timer, this) != 0) {
            lck.unlock();
            LOG(ERROR) << "Fail to add timer";
            return on_timer(this);
        }
        return;
    }
    if (_response.has_crc32_type() && _response.has_crc32()) {
        uint32_t crc_code = 0;
        bool ok = get_crc32((CrcType)_response.crc32_type(),
                            _cntl.response_attachment(),
                            &crc_code);
        if (!ok || crc_code != _response.crc32()) {
            g_remote_copier_crc_error << 1;
            LOG(WARNING) << "install_snapshot crc error, "
                         <<  _cntl.remote_side() << " --> " << _cntl.local_side()
                         << " " << (CrcType)_response.crc32_type()
                         << " (remote:" << _response.crc32() << ") (local:" << crc_code
                         << ") filename: " << _request.filename()
                         << " call_id: " << _cntl.call_id() << ", log_id: " << _cntl.log_id();
            // Reset count to make next rpc retry the previous one
            int64_t request_count = _request.count();
            _request.set_count(0);
            // set retry time interval
            int64_t retry_interval_ms = _options.retry_interval_ms; 
            if (_throttle) {
                retry_interval_ms = _throttle->get_retry_interval_ms();
                // No token consumed, just return back, other nodes maybe able to use them
                if (FLAGS_raft_enable_throttle_when_install_snapshot) {
                    _throttle->return_unused_throughput(
                            request_count, 0,
                            base::cpuwide_time_us() - _throttle_token_acquire_time_us);
                }
            }
            AddRef();
            if (raft_timer_add(
                        &_timer, 
                        base::milliseconds_from_now(retry_interval_ms),
                        on_timer, this) != 0) {
                lck.unlock();
                LOG(ERROR) << "Fail to add timer";
                return on_timer(this);
            }
            return;
        }
        LOG(INFO) << "Install_snapshot crc, "
                     <<  _cntl.remote_side() << " --> " << _cntl.local_side()
                     << " " << (CrcType)_response.crc32_type()
                     << " (remote:" << _response.crc32() << ") (local:" << crc_code
                     << ") filename: " << _request.filename()
                     << " call_id: " << _cntl.call_id() << ", log_id: " << _cntl.log_id();
    }
    if (_throttle && FLAGS_raft_enable_throttle_when_install_snapshot &&
        _request.count() > (int64_t)_cntl.response_attachment().size()) {
        _throttle->return_unused_throughput(
                _request.count(), _cntl.response_attachment().size(),
                base::cpuwide_time_us() - _throttle_token_acquire_time_us);
    }
    _retry_times = 0;
    // Reset count to |real_read_size| to make next rpc get the right offset
    if (_response.has_read_size() && (_response.read_size() != 0)
            && FLAGS_raft_allow_read_partly_when_install_snapshot) {
        _request.set_count(_response.read_size());
    }
    _in_flight_io = true;
    if (_cntl.response_attachment().length() == 0) {
        lck.unlock();
        AddRef();
        continue_send(base::Status());
        return;
    }
    if (_file) {
        FileSegData data(_cntl.response_attachment());
        uint64_t seg_offset = 0;
        base::IOBuf seg_data;
        std::vector<std::pair<base::IOBuf, uint64_t>> w_para_vec;
        while (0 != data.next(&seg_offset, &seg_data)) {
            w_para_vec.push_back(std::make_pair(seg_data, seg_offset));
            seg_data.clear();
        }
        ClosureGroup* group_done = new ClosureGroup(w_para_vec.size(),
                NewCallback(this, &RemoteFileCopier::Session::continue_send));
        AddRef();
        lck.unlock();
        for (uint64_t i = 0; i < w_para_vec.size(); ++i) {
            _file->write(w_para_vec[i].first, w_para_vec[i].second, group_done->sub_done(i));
        }
        return;
    } else {
        FileSegData data(_cntl.response_attachment());
        uint64_t seg_offset = 0;
        base::IOBuf seg_data;
        while (0 != data.next(&seg_offset, &seg_data)) {
            CHECK_GE((size_t)seg_offset, _buf->length());
            _buf->resize(seg_offset);
            _buf->append(seg_data);
        }
        lck.unlock();
        AddRef();
        continue_send(base::Status());
        return;
    }
}

void RemoteFileCopier::Session::continue_send(const base::Status& st) {
    if (FLAGS_raft_use_polar) {
        CHECK_GE(_polar_idx, 0);
        CHECK_EQ(_polar_idx, current_polar_idx());
    }
    scoped_refptr<Session> ref_gurad;
    Session* this_ref = this;
    ref_gurad.swap(&this_ref); // pair with on_rpc_returned
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _in_flight_io = false;
    if (_finished && _file) {
        LOG(NOTICE) << "Sync and close file: " << _dest_path
            << " since copy canceled";
        // continue process of cancel which can not do while there are in flight io
        if (!_file->sync() || !_file->close()) {
            _st.set_error(EIO, "%s", berror(EIO));
        }
        delete _file;
        _file = NULL;
        return;
    }
    if (!st.ok()) {
        LOG(WARNING) << "Fail to write into file: " << _dest_path;
        _st.set_error(EIO, "%s", berror(EIO));
        return on_finished();
    }
    if (_response.eof()) {
        on_finished();
        return;
    }
    lck.unlock();
    return send_next_rpc();
}

void* RemoteFileCopier::Session::send_next_rpc_on_timedout(void* arg) {
    Session* m = (Session*)arg;
    m->send_next_rpc();
    m->Release();
    return NULL;
}

void RemoteFileCopier::Session::on_timer(void* arg) {
    bthread_t tid;
    if (raft_start_background(
                &tid, NULL, send_next_rpc_on_timedout, arg) != 0) {
        PLOG(ERROR) << "Fail to start background thread";
        send_next_rpc_on_timedout(arg);
    }
}

void RemoteFileCopier::Session::on_finished() {
    if (!_finished) {
        if (_file && !_in_flight_io) {
            if (!_file->sync() || !_file->close()) {
                _st.set_error(EIO, "%s", berror(EIO));
            }
            delete _file;
            _file = NULL;
        }
        _finished = true;
        _finish_event.signal();
    }
}

void RemoteFileCopier::Session::cancel() {
    BAIDU_SCOPED_LOCK(_mutex);
    if (_finished) {
        return;
    }
    LOG(NOTICE) << "Path: " << _dest_path << " cancel rpc: " << _rpc_call
        << " delete timer: " << _timer;
    rpc::StartCancel(_rpc_call);
    if (raft_timer_del(_timer) == 0) {
        // Release reference of the timer task
        Release();
    }
    if (_st.ok()) {
        _st.set_error(ECANCELED, "%s", berror(ECANCELED));
    }
    on_finished();
}

void RemoteFileCopier::Session::join() {
    _finish_event.wait();
}

} // namespace raft
