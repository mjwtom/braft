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
//          Wang,Yao(wangyao02@baidu.com)

#include "raft/util.h"
#include <gflags/gflags.h>
#include <stdlib.h>
#include <base/macros.h>
#include <base/raw_pack.h>                     // base::RawPacker
#include <base/file_util.h>
#include "raft/raft.h"
#include "raft/rtc_helper.h"
#include "raft/stack.h"

namespace raft {

DEFINE_bool(raft_enable_crc_test, false, "enable crc check rd test or qa test");
DEFINE_int32(raft_crc_wrong_checksum_flip_interval_s, 1800,
        "-1 for always wrong, 0 for always correct, positive number for flip interval");
RAFT_VALIDATE_GFLAG(raft_crc_wrong_checksum_flip_interval_s, rpc::PassValidate);
DEFINE_int32(raft_crc_wrong_checksum_rate, 50,
        "control the proportion of wrong crc requests during crc test \
        0 for always correct, 50 for half correct half wrong, \
        100 for always wrong");
RAFT_VALIDATE_GFLAG(raft_crc_wrong_checksum_rate, ::rpc::NonNegativeInteger);

DEFINE_bool(enable_stat_bthread_execute_latency, false, "enable stat bthread execute latency by using bthread_start_background_ex");
HPN_RPC_VALIDATE_GFLAG(enable_stat_bthread_execute_latency, baidu::hpn::rpc::PassValidate);
DEFINE_int64(bthread_execute_high_latency_us, 20000, "bthread func execute high latency us");
HPN_RPC_VALIDATE_GFLAG(bthread_execute_high_latency_us, baidu::hpn::rpc::PositiveInteger);
DEFINE_int32(bthread_execute_stack_skip_nframes, 1, "bthread execute stack skip nframes");
HPN_RPC_VALIDATE_GFLAG(bthread_execute_stack_skip_nframes, baidu::hpn::rpc::NonNegativeInteger);
DEFINE_int32(bthread_execute_stack_max_depth, 2, "bhread execute stack max depth");
HPN_RPC_VALIDATE_GFLAG(bthread_execute_stack_max_depth, baidu::hpn::rpc::PositiveInteger);

bvar::MultiDimension<bvar::WindowEx<BvarMaxer<uint64_t>, 30>> g_bthread_execute_max_latency_by_stack("bthread_execute_max_latency_by_stack", {"stack"});


// Reloading following gflags does not change names of the corresponding bvars.
// Avoid reloading in practice.
DEFINE_int32(bvar_counter_p1, 80, "First counter percentile");
DEFINE_int32(bvar_counter_p2, 90, "Second counter percentile");
DEFINE_int32(bvar_counter_p3, 99, "Third counter percentile");

static bool valid_percentile(const char*, int32_t v) {
    return v > 0 && v < 100;
}

const bool ALLOW_UNUSED dummy_bvar_counter_p1 = ::google::RegisterFlagValidator(
    &FLAGS_bvar_counter_p1, valid_percentile);
const bool ALLOW_UNUSED dummy_bvar_counter_p2 = ::google::RegisterFlagValidator(
    &FLAGS_bvar_counter_p2, valid_percentile);
const bool ALLOW_UNUSED dummy_bvar_counter_p3 = ::google::RegisterFlagValidator(
    &FLAGS_bvar_counter_p3, valid_percentile);

typedef bvar::detail::PercentileSamples<1022> CombinedPercentileSamples;

static int64_t get_window_recorder_qps(void* arg) {
    bvar::detail::Sample<bvar::Stat> s;
    static_cast<RecorderWindow*>(arg)->get_span(1, &s);
    // Use floating point to avoid overflow.
    if (s.time_us <= 0) {
        return 0;
    }
    return static_cast<int64_t>(round(s.data.num * 1000000.0 / s.time_us));
}

static int64_t get_recorder_count(void* arg) {
    return static_cast<BvarIntRecorder*>(arg)->get_value().num;
}

// Caller is responsible for deleting the return value.
static CombinedPercentileSamples* combine(PercentileWindow* w) {
    CombinedPercentileSamples* cb = new CombinedPercentileSamples;
    std::vector<bvar::detail::GlobalPercentileSamples> buckets;
    w->get_samples(&buckets);
    cb->combine_of(buckets.begin(), buckets.end());
    return cb;
}

template <int64_t numerator, int64_t denominator>
static int64_t get_counter_percetile(void* arg) {
    return ((BvarCounterRecorder*)arg)->counter_percentile(
            (double)numerator / double(denominator));
}

static int64_t get_p1_counter(void* arg) {
    BvarCounterRecorder* cr = static_cast<BvarCounterRecorder*>(arg);
    return cr->counter_percentile(FLAGS_bvar_counter_p1 / 100.0);
}
static int64_t get_p2_counter(void* arg) {
    BvarCounterRecorder* cr = static_cast<BvarCounterRecorder*>(arg);
    return cr->counter_percentile(FLAGS_bvar_counter_p2 / 100.0);
}
static int64_t get_p3_counter(void* arg) {
    BvarCounterRecorder* cr = static_cast<BvarCounterRecorder*>(arg);
    return cr->counter_percentile(FLAGS_bvar_counter_p3 / 100.0);
}

static bvar::Vector<int64_t, 4> get_counters(void *arg) {
    std::unique_ptr<CombinedPercentileSamples> cb(
        combine((PercentileWindow*)arg));
    // NOTE: We don't show 99.99% since it's often significantly larger than
    // other values and make other curves on the plotted graph small and
    // hard to read.ggggnnn
    bvar::Vector<int64_t, 4> result;
    result[0] = cb->get_number(FLAGS_bvar_counter_p1 / 100.0);
    result[1] = cb->get_number(FLAGS_bvar_counter_p2 / 100.0);
    result[2] = cb->get_number(FLAGS_bvar_counter_p3 / 100.0);
    result[3] = cb->get_number(0.999);
    return result;
}

BvarCounterRecorderBase::BvarCounterRecorderBase(time_t window_size)
    : _max_counter()
    , _avg_counter_window(&_avg_counter, window_size)
    , _max_counter_window(&_max_counter, window_size)
    , _counter_percentile_window(&_counter_percentile, window_size)
    , _total_times(get_recorder_count, &_avg_counter)
    , _qps(get_window_recorder_qps, &_avg_counter_window)
    , _counter_p1(get_p1_counter, this)
    , _counter_p2(get_p2_counter, this)
    , _counter_p3(get_p3_counter, this)
    , _counter_999(get_counter_percetile<999, 1000>, this)
    , _counter_9999(get_counter_percetile<9999, 10000>, this)
    , _counter_cdf(&_counter_percentile_window)
    , _counter_percentiles(get_counters, &_counter_percentile_window)
{}

// BvarCounterRecorder
bvar::Vector<int64_t, 4> BvarCounterRecorder::counter_percentiles() const {
    // const_cast here is just to adapt parameter type and safe.
    return get_counters(
        const_cast<PercentileWindow*>(&_counter_percentile_window));
}

int64_t BvarCounterRecorder::qps(time_t window_size) const {
    bvar::detail::Sample<bvar::Stat> s;
    _avg_counter_window.get_span(window_size, &s);
    // Use floating point to avoid overflow.
    if (s.time_us <= 0) {
        return 0;
    }
    return static_cast<int64_t>(round(s.data.num * 1000000.0 / s.time_us));
}

int BvarCounterRecorder::expose(const base::StringPiece& prefix1,
                            const base::StringPiece& prefix2) {
    if (prefix2.empty()) {
        LOG(ERROR) << "Parameter[prefix2] is empty";
        return -1;
    }
    base::StringPiece prefix = prefix2;
    // User may add "_counter" as the suffix, remove it.
    if (prefix.ends_with("counter") || prefix.ends_with("Counter")) {
        prefix.remove_suffix(7);
        if (prefix.empty()) {
            LOG(ERROR) << "Invalid prefix2=" << prefix2;
            return -1;
        }
    }
    std::string tmp;
    if (!prefix1.empty()) {
        tmp.reserve(prefix1.size() + prefix.size() + 1);
        tmp.append(prefix1.data(), prefix1.size());
        tmp.push_back('_'); // prefix1 ending with _ is good.
        tmp.append(prefix.data(), prefix.size());
        prefix = tmp;
    }

    // set debug names for printing helpful error log.
    //_avg_counter.set_debug_name(prefix);
    _counter_percentile.set_debug_name(prefix);

    if (_avg_counter_window.expose_as(prefix, "avg_counter") != 0) {
        return -1;
    }
    if (_max_counter_window.expose_as(prefix, "max_counter") != 0) {
        return -1;
    }
    if (_total_times.expose_as(prefix, "total_times") != 0) {
        return -1;
    }
    if (_qps.expose_as(prefix, "qps") != 0) {
        return -1;
    }
    char namebuf[32];
    snprintf(namebuf, sizeof(namebuf), "counter_%d", (int)FLAGS_bvar_counter_p1);
    if (_counter_p1.expose_as(prefix, namebuf, bvar::DISPLAY_ON_PLAIN_TEXT) != 0) {
        return -1;
    }
    snprintf(namebuf, sizeof(namebuf), "counter_%d", (int)FLAGS_bvar_counter_p2);
    if (_counter_p2.expose_as(prefix, namebuf, bvar::DISPLAY_ON_PLAIN_TEXT) != 0) {
        return -1;
    }
    snprintf(namebuf, sizeof(namebuf), "counter_%u", (int)FLAGS_bvar_counter_p3);
    if (_counter_p3.expose_as(prefix, namebuf, bvar::DISPLAY_ON_PLAIN_TEXT) != 0) {
        return -1;
    }
    if (_counter_999.expose_as(prefix, "counter_999", bvar::DISPLAY_ON_PLAIN_TEXT) != 0) {
        return -1;
    }
    if (_counter_9999.expose_as(prefix, "counter_9999") != 0) {
        return -1;
    }
    if (_counter_cdf.expose_as(prefix, "counter_cdf", bvar::DISPLAY_ON_HTML) != 0) {
        return -1;
    }
    if (_counter_percentiles.expose_as(prefix, "counter_percentiles", bvar::DISPLAY_ON_HTML) != 0) {
        return -1;
    }
    snprintf(namebuf, sizeof(namebuf), "%d%%,%d%%,%d%%,99.9%%",
             (int)FLAGS_bvar_counter_p1, (int)FLAGS_bvar_counter_p2,
             (int)FLAGS_bvar_counter_p3);
    CHECK_EQ(0, _counter_percentiles.set_vector_names(namebuf));
    return 0;
}

int64_t BvarCounterRecorder::counter_percentile(double ratio) const {
    std::unique_ptr<CombinedPercentileSamples> cb(
        combine((PercentileWindow*)&_counter_percentile_window));
    return cb->get_number(ratio);
}

void BvarCounterRecorder::hide() {
    _avg_counter_window.hide();
    _max_counter_window.hide();
    _total_times.hide();
    _qps.hide();
    _counter_p1.hide();
    _counter_p2.hide();
    _counter_p3.hide();
    _counter_999.hide();
    _counter_9999.hide();
    _counter_cdf.hide();
    _counter_percentiles.hide();
}

BvarCounterRecorder& BvarCounterRecorder::operator<<(int64_t count_num) {
    _avg_counter << count_num;
    _max_counter << count_num;
    _counter_percentile << count_num;
    return *this;
}

std::ostream& operator<<(std::ostream& os, const BvarCounterRecorder& rec) {
    return os << "{avg=" << rec.avg_counter()
              << " max" << rec.window_size() << '=' << rec.max_counter()
              << " qps=" << rec.qps()
              << " count=" << rec.total_times() << '}';
}


struct BthreadEventContext {
    std::string stack;
    int64_t swap_in_time_us = -1;
    uint64_t latency = 0;
};

static void bthread_event_handler(void* arg, bthread_schedule_event_t event) {
    CHECK(arg);
    BthreadEventContext* ctx = static_cast<BthreadEventContext*>(arg);
    if (event == BTHREAD_SCHEDULE_EVENT_START) {
        //do nothing
    } else if (event == BTHREAD_SCHEDULE_EVENT_SWAP_IN) {
        ctx->swap_in_time_us = base::monotonic_time_us();
    } else if (event == BTHREAD_SCHEDULE_EVENT_SWAP_OUT) {
        CHECK_GT(ctx->swap_in_time_us, 0);
        int64_t cur_time_us = base::monotonic_time_us();
        uint64_t latency = (cur_time_us > ctx->swap_in_time_us) ? (cur_time_us - ctx->swap_in_time_us) : 1;
        ctx->latency += latency;
    } else {
        CHECK_EQ(event, BTHREAD_SCHEDULE_EVENT_END);
        if (ctx->latency >= FLAGS_bthread_execute_high_latency_us) {
            RECORD_MBVAR(g_bthread_execute_max_latency_by_stack, {ctx->stack}, ctx->latency);
        }
        delete ctx;
    }
}

static void* run_closure(void* arg) {
    ::google::protobuf::Closure *c = (google::protobuf::Closure*)arg;
    if (c) {
        c->Run();
    }
    return NULL;
}

void run_closure_in_bthread(google::protobuf::Closure* closure,
                            bool in_pthread) {
    DCHECK(closure);
    bthread_t tid;
    int ret = 0;

    if (FLAGS_enable_stat_bthread_execute_latency) {
        bthread_stacktype_t type = (in_pthread)
                                   ? BTHREAD_STACKTYPE_PTHREAD : BTHREAD_STACKTYPE_NORMAL;
        BthreadEventContext* ctx = new BthreadEventContext;
        ctx->stack = stack_string(FLAGS_bthread_execute_stack_skip_nframes,
                                  FLAGS_bthread_execute_stack_max_depth);
        bthread_attr_ex_t attr_ex = {type, 0, NULL, bthread_event_handler, ctx};
        ret = bthread_start_background_ex(&tid, &attr_ex, sizeof(bthread_attr_ex_t), run_closure, closure);
    } else {
        bthread_attr_t attr = (in_pthread) ? BTHREAD_ATTR_PTHREAD : BTHREAD_ATTR_NORMAL;
        ret = bthread_start_background(&tid, &attr, run_closure, closure);
    }

    if (0 != ret) {
        PLOG(ERROR) << "Fail to start bthread";
        return closure->Run();
    }
}

void run_closure_in_bthread_nosig(google::protobuf::Closure* closure,
                                  bool in_pthread) {
    DCHECK(closure);
    bthread_t tid;
    int ret = 0;

    if (FLAGS_enable_stat_bthread_execute_latency) {
        bthread_stacktype_t type = (in_pthread)
                                   ? BTHREAD_STACKTYPE_PTHREAD : BTHREAD_STACKTYPE_NORMAL;
        BthreadEventContext* ctx = new BthreadEventContext;
        ctx->stack = stack_string(FLAGS_bthread_execute_stack_skip_nframes,
                                  FLAGS_bthread_execute_stack_max_depth);
        bthread_attr_ex_t attr_ex = {type, 0, NULL, bthread_event_handler, ctx};
        attr_ex.flags |= BTHREAD_NOSIGNAL;
        ret = bthread_start_background_ex(&tid, &attr_ex, sizeof(bthread_attr_ex_t), run_closure, closure);
    } else {
        bthread_attr_t attr = (in_pthread) ? BTHREAD_ATTR_PTHREAD : BTHREAD_ATTR_NORMAL;
        attr =  attr | BTHREAD_NOSIGNAL;
        ret = bthread_start_background(&tid, &attr, run_closure, closure);
    }

    if (0 != ret) {
        PLOG(ERROR) << "Fail to start bthread";
        return closure->Run();
    }
}

ssize_t file_pread(base::IOPortal* portal, int fd, off_t offset, size_t size) {
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) {
        ssize_t read_len = portal->pappend_from_file_descriptor(
                fd, offset, static_cast<size_t>(left));
        if (read_len > 0) {
            left -= read_len;
            offset += read_len;
        } else if (read_len == 0) {
            break;
        } else if (errno == EINTR) {
            continue;
        } else {
            LOG(WARNING) << "read failed, err: " << berror()
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size;
            return -1;
        }
    }

    return size - left;
}

ssize_t file_pwrite(const base::IOBuf& data, int fd, off_t offset) {
    size_t size = data.size();
    base::IOBuf piece_data(data);
    off_t orig_offset = offset;
    ssize_t left = size;
    while (left > 0) {
        ssize_t written = piece_data.pcut_into_file_descriptor(fd, offset, left);
        if (written >= 0) {
            offset += written;
            left -= written;
        } else if (errno == EINTR) {
            continue;
        } else {
            LOG(WARNING) << "write falied, err: " << berror()
                << " fd: " << fd << " offset: " << orig_offset << " size: " << size;
            return -1;
        }
    }

    return size - left;
}

void FileSegData::append(const base::IOBuf& data, uint64_t offset) {
    uint32_t len = data.size();
    if (0 != _seg_offset && offset == (_seg_offset + _seg_len)) {
        // append to last segment
        _seg_len += len;
        _data.append(data);
    } else {
        // close last segment
        char seg_header[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
        if (_seg_len > 0) {
            ::base::RawPacker(seg_header).pack64(_seg_offset).pack32(_seg_len);
            CHECK_EQ(0, _data.unsafe_assign(_seg_header, seg_header));
        }

        // start new segment
        _seg_offset = offset;
        _seg_len = len;
        _seg_header = _data.reserve(sizeof(seg_header));
        CHECK(_seg_header != base::IOBuf::INVALID_AREA);
        _data.append(data);
    }
}

void FileSegData::append(void* data, uint64_t offset, uint32_t len) {
    if (0 != _seg_offset && offset == (_seg_offset + _seg_len)) {
        // append to last segment
        _seg_len += len;
        _data.append(data, len);
    } else {
        // close last segment
        char seg_header[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
        if (_seg_len > 0) {
            ::base::RawPacker(seg_header).pack64(_seg_offset).pack32(_seg_len);
            CHECK_EQ(0, _data.unsafe_assign(_seg_header, seg_header));
        }

        // start new segment
        _seg_offset = offset;
        _seg_len = len;
        _seg_header = _data.reserve(sizeof(seg_header));
        CHECK(_seg_header != base::IOBuf::INVALID_AREA);
        _data.append(data, len);
    }
}

void FileSegData::close() {
    char seg_header[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
    if (_seg_len > 0) {
        ::base::RawPacker(seg_header).pack64(_seg_offset).pack32(_seg_len);
        CHECK_EQ(0, _data.unsafe_assign(_seg_header, seg_header));
    }

    _seg_offset = 0;
    _seg_len = 0;
}

size_t FileSegData::next(uint64_t* offset, base::IOBuf* data) {
    data->clear();
    if (_data.length() == 0) {
        return 0;
    }

    char header_buf[sizeof(uint64_t) + sizeof(uint32_t)] = {0};
    size_t header_len = _data.cutn(header_buf, sizeof(header_buf));
    CHECK_EQ(header_len, sizeof(header_buf)) << "header_len: " << header_len;

    uint64_t seg_offset = 0;
    uint32_t seg_len = 0;
    ::base::RawUnpacker(header_buf).unpack64(seg_offset).unpack32(seg_len);

    *offset = seg_offset;
    size_t body_len = _data.cutn(data, seg_len);
    CHECK_EQ(body_len, seg_len) << "seg_len: " << seg_len << " body_len: " << body_len;
    return seg_len;
}

void fill_rpc_channel_option(rpc::ChannelOptions* opt) {
    // polar only support CONNECTION_TYPE_SINGLE
    opt->connection_type = rpc::CONNECTION_TYPE_SINGLE;
    opt->use_rdma = FLAGS_raft_use_rdma;
    opt->use_polar = FLAGS_raft_use_polar;
    if (opt->use_polar) {
        opt->polar_thread_index = current_polar_idx();
    }
}

ClosureGroup::SubDone::SubDone(ClosureGroup* g) : _g(g) {}

void ClosureGroup::SubDone::Run() {
    _g->on_sub_done();
}

ClosureGroup::SubDoneMemory::SubDoneMemory(size_t sub_done_count, ClosureGroup* g)
            : _count(sub_done_count)
            , _extra_mem(NULL) {
    if (sub_done_count > SMALL_COUNT) {
        const size_t extra_count = sub_done_count - SMALL_COUNT;
        _extra_mem = (char*)malloc(sizeof(SubDone) * extra_count);
        if (BAIDU_UNLIKELY(_extra_mem == NULL)) {
            PLOG(ERROR) << "Fail to malloc _extra_mem";
            abort();
        }
    }
    for (size_t i = 0; i < sub_done_count; ++i) {
        new (sub_done_mem(i)) SubDone(g);
    }
}

ClosureGroup::SubDoneMemory::~SubDoneMemory() {
    for (size_t i = 0; i < sub_done_count(); ++i) {
        ((SubDone*)sub_done_mem(i))->~SubDone();
    }
    if (_extra_mem) {
        free(_extra_mem);
        _extra_mem = NULL;
    }
    _count = 0;
}

size_t ClosureGroup::SubDoneMemory::sub_done_count() const { return _count; }

void* ClosureGroup::SubDoneMemory::sub_done_mem(size_t i) {
    if (i < SMALL_COUNT) {
        return _static_mem + sizeof(SubDone) * i;
    }
    return _extra_mem + (i - SMALL_COUNT) * sizeof(SubDone);
}

ClosureGroup::ClosureGroup(int32_t sub_done_count,
             raft::Closure *done)
    : _done(done)
    , _remain(sub_done_count)
    , _memory(sub_done_count, this) {
    CHECK_GT(sub_done_count, 0) << sub_done_count;
}

ClosureGroup::~ClosureGroup() {}

ClosureGroup::SubDone* ClosureGroup::sub_done(size_t i) {
    CHECK_LT(i, _memory.sub_done_count());
    return (SubDone*)_memory.sub_done_mem(i);
}

void ClosureGroup::on_sub_done() {
    if (_remain.fetch_sub(1, std::memory_order_release) != 1) {
        return;
    }
    std::unique_ptr<ClosureGroup> guard(this);
    std::atomic_thread_fence(std::memory_order_acquire);
    for (size_t i = 0; i < _memory.sub_done_count(); ++i) {
        if (!sub_done(i)->status().ok()) {
            if (_done) {
                _done->status() = sub_done(i)->status();
            }
            break;
        }
    }
    before_run_done();
    if (_done) {
        _done->Run();
    }
}

void ClosureGroup::before_run_done() {}

}  // namespace raft
