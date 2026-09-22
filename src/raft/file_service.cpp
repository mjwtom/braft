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

#include "raft/file_service.h"

#include <stack>
#include <base/file_util.h>
#include <base/files/file_path.h>
#include <base/files/file_enumerator.h>
#include "raft/util.h"
#include "raft/closure_helper.h"
#include "raft/rtc_helper.h"

namespace raft {

DEFINE_bool(raft_file_check_hole, false, "file service check hole switch, default disable");
DECLARE_int32(crc32_type);
DECLARE_bool(enable_crc_install_snapshot);
DECLARE_bool(raft_enable_crc_test);
DECLARE_int32(raft_crc_wrong_checksum_rate);
DECLARE_int32(raft_crc_wrong_checksum_flip_interval_s);

void FileServiceImpl::get_file(::google::protobuf::RpcController* controller,
                               const ::raft::GetFileRequest* request,
                               ::raft::GetFileResponse* response,
                               ::google::protobuf::Closure* done) {
    rpc::ClosureGuard done_guard(done);
    rpc::Controller* cntl = (rpc::Controller*)controller;

    if (request->count() <= 0 || request->offset() < 0) {
        cntl->SetFailed(rpc::EREQUEST, "Invalid request=%s",
                        request->ShortDebugString().c_str());
        return;
    }

    scoped_refptr<FileReader> reader = get_reader(request->reader_id());
    if (nullptr == reader.get()) {
        cntl->SetFailed(ENOENT, "Fail to find reader=%ld", request->reader_id());
        return;
    }

    RAFT_DISPATCH_SELF(get_file, controller, request, response, done_guard, reader->polar_worker_index());

    RAFT_VLOG << "Get file for " << cntl->remote_side() << " reader_id=" << request->reader_id()
        << " path=" << reader->path()
        << " filename=" << request->filename()
        << " offset=" << request->offset() << " count=" << request->count()
        << " polar_worker_index=" << current_polar_idx()
        << " call_id=" << cntl->call_id() << " log_id=" << cntl->log_id();

    GetFileContext* ctx = new GetFileContext;
    ctx->reader = reader;
    Closure* closure = NewCallback(this, &FileServiceImpl::get_file_done,
                                cntl, request, response, done_guard.release(), ctx);
    reader->read_file(&ctx->portal, request->filename(), request->offset(), request->count(), 
                    request->read_partly(), &ctx->read_count, &ctx->is_eof, closure);
}

void FileServiceImpl::get_file_done(rpc::Controller* cntl,
                                    const ::raft::GetFileRequest* request,
                                    ::raft::GetFileResponse* response,
                                    ::google::protobuf::Closure* done,
                                    GetFileContext* ctx,
                                    const base::Status& status) {
    rpc::ClosureGuard done_gurad(done);
    std::unique_ptr<GetFileContext> ctx_guard(ctx); // auto delete
    if (!status.ok()) {
        cntl->SetFailed(status.error_code(), "Fail to read from path=%s filename=%s : %s",
                        ctx->reader->path().c_str(), request->filename().c_str(),
                        status.error_cstr());
        return;
    }

    response->set_eof(ctx->is_eof);
    response->set_read_size(ctx->read_count);
    // skip empty data
    if (ctx->portal.size() == 0) {
        return;
    }

    FileSegData seg_data;
    if (!FLAGS_raft_file_check_hole) {
        seg_data.append(ctx->portal, request->offset());
    } else {
        off_t buf_off = request->offset();
        while (!ctx->portal.empty()) {
            base::StringPiece p = ctx->portal.backing_block(0);
            if (!is_zero(p.data(), p.size())) {
                base::IOBuf piece_buf;
                ctx->portal.cutn(&piece_buf, p.size());
                seg_data.append(piece_buf, buf_off);
            } else {
                // skip zero IOBuf block
                ctx->portal.pop_front(p.size());
            }
            buf_off += p.size();
        }
    }
    cntl->response_attachment().swap(seg_data.data());

    if (FLAGS_enable_crc_install_snapshot) {
        uint32_t crc_code = 0;
        if (get_crc32((CrcType)FLAGS_crc32_type, cntl->response_attachment(), &crc_code)) {
            response->set_crc32_type(FLAGS_crc32_type);
            response->set_crc32(crc_code);
            if (FLAGS_raft_enable_crc_test) {
                static std::atomic<int64_t> last_crc_flip_time(base::monotonic_time_us());
                static std::atomic<int32_t> whether_gen_wrong_crc(-1);
                uint32_t wrong_crc_code = crc_code;
                bool is_wrong = check_wrong_checksum_flipping_and_gen_wrong_crc(
                        FLAGS_raft_crc_wrong_checksum_flip_interval_s, FLAGS_raft_crc_wrong_checksum_rate,
                        last_crc_flip_time, whether_gen_wrong_crc, cntl->response_attachment(),
                        wrong_crc_code, "install_snapshot");
                if (is_wrong) {
                    response->set_crc32(wrong_crc_code);
                }
            }
        } else {
            LOG(WARNING) << "get_crc32 failed, crc32_type: " << (CrcType)FLAGS_crc32_type;
        }
        LOG(INFO) << "Set crc32 type: " << static_cast<CrcType>(response->crc32_type())
            << ", crc32 value: " << response->crc32() << ", call_id: " << cntl->call_id()
            << ", log_id: " << cntl->log_id()
            << ", attachment: " << cntl->response_attachment().size();
    }
}

FileServiceImpl::FileServiceImpl() {
    _next_id = ((int64_t)getpid() << 45) | (base::gettimeofday_us() << 17 >> 17);
}

int FileServiceImpl::add_reader(FileReader* reader, int64_t* reader_id) {
    BAIDU_SCOPED_LOCK(_mutex);
    *reader_id = _next_id++;
    _reader_map[*reader_id] = reader;
    return 0;
}

int FileServiceImpl::remove_reader(int64_t reader_id) {
    BAIDU_SCOPED_LOCK(_mutex);
    return _reader_map.erase(reader_id) == 1 ? 0 : -1;
}

scoped_refptr<FileReader> FileServiceImpl::get_reader(int64_t reader_id) {
    BAIDU_SCOPED_LOCK(_mutex);
    auto iter = _reader_map.find(reader_id);
    if (iter != _reader_map.end()) {
        return iter->second;
    }
    return nullptr;
}

}  // namespace raft
