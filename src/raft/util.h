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

#ifndef PUBLIC_RAFT_RAFT_UTIL_H
#define PUBLIC_RAFT_RAFT_UTIL_H

#include <zlib.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <limits.h>
#include <stdlib.h>
#include <string>
#include <set>
#include <base/third_party/murmurhash3/murmurhash3.h>
#include <base/endpoint.h>
#include <base/scoped_lock.h>
#include <base/fast_rand.h>
#include <base/time.h>
#include <base/logging.h>
#include <base/iobuf.h>
#include <base/unique_ptr.h>
#include <base/memory/singleton.h>
#include <base/containers/doubly_buffered_data.h>
#include <base/crc32c.h>
#include <base/file_util.h>
#include <bthread.h>
#include <bthread_unstable.h>
#include <bthread/countdown_event.h>
#include <bvar/bvar.h>
#include <baidu/feed/mlarch/babylon/bvar/recorder.h>
#include "raft/macros.h"
#include "raft/raft.h"

namespace raft {
#ifdef CRC_TEST
DECLARE_int32(crc32_type);
DECLARE_bool(enable_crc_test);
DECLARE_int32(crc_wrong_checksum_rate);
DECLARE_int32(crc_wrong_checksum_flip_interval_s);
#endif
}

namespace base {

inline ip_t get_host_ip_by_interface(const char* interface) {
    int sockfd = -1;
    struct ::ifreq req;
    ip_t ip = IP_ANY;
    if ((sockfd = socket(PF_INET, SOCK_DGRAM, 0)) < 0) {
        return ip;
    }

    memset(&req, 0, sizeof(struct ::ifreq));
    snprintf(req.ifr_name, sizeof(req.ifr_name), "%s", interface);

    if (!ioctl(sockfd, SIOCGIFADDR, (char*)&req)) {
        struct in_addr ip_addr;
        ip_addr = ((struct sockaddr_in*)&req.ifr_addr)->sin_addr;
        //ip_addr.s_addr = *((int*) &req.ifr_addr.sa_data[2]);
        ip.s_addr = ip_addr.s_addr;
    }
    LOG(INFO) << "Close socket with fd=" << sockfd;
    close(sockfd);
    return ip;
}

inline ip_t get_host_ip() {
    const char* interfaces[] = { "xgbe0", "xgbe1", "eth1", "eth0", "bond0", "br-ex" };
    ip_t ip = IP_ANY;

    for (size_t i = 0; i < 6; ++i) {
        ip = get_host_ip_by_interface(interfaces[i]);
        if (INADDR_ANY != ip.s_addr) {
            break;
        }
    }

    if (INADDR_ANY == ip.s_addr) {
        LOG(FATAL) << "can not get a valid ip";
    }

    return ip;
}

}  // namespace base

namespace raft {

template <typename T>
using BvarAdder = ::baidu::feed::mlarch::babylon::BvarAdder<T>;

template <typename T>
using BvarMaxer = ::baidu::feed::mlarch::babylon::BvarMaxer<T>;

using BvarIntRecorder = ::baidu::feed::mlarch::babylon::BvarIntRecorder;
using BvarLatencyRecorder = ::baidu::feed::mlarch::babylon::BvarLatencyRecorder;
using Percentile = bvar::detail::Percentile;    // 此处不使用BvarPercentile，是因BvarCounterRecorderBase需要CDF，而Bvar尚未实现CDF


typedef bvar::Window<BvarIntRecorder, bvar::SERIES_IN_SECOND> RecorderWindow;
typedef bvar::Window<BvarMaxer<uint64_t>, bvar::SERIES_IN_SECOND> MaxUint64Window;
typedef bvar::Window<Percentile, bvar::SERIES_IN_SECOND> PercentileWindow;

// For mimic constructor inheritance.
class BvarCounterRecorderBase {
public:
    explicit BvarCounterRecorderBase(time_t window_size);
    time_t window_size() const { return _avg_counter_window.window_size(); }
protected:
    BvarIntRecorder _avg_counter;
    BvarMaxer<uint64_t> _max_counter;
    Percentile _counter_percentile;
    RecorderWindow _avg_counter_window;
    MaxUint64Window _max_counter_window;
    PercentileWindow _counter_percentile_window;

    bvar::PassiveStatus<int64_t> _total_times;
    bvar::PassiveStatus<int64_t> _qps;
    bvar::PassiveStatus<int64_t> _counter_p1;
    bvar::PassiveStatus<int64_t> _counter_p2;
    bvar::PassiveStatus<int64_t> _counter_p3;
    bvar::PassiveStatus<int64_t> _counter_999;  // 99.9%
    bvar::PassiveStatus<int64_t> _counter_9999; // 99.99%
    bvar::detail::CDF _counter_cdf;
    bvar::PassiveStatus<bvar::Vector<int64_t, 4> > _counter_percentiles;
};

// Specialized structure to record counter.
// It's not a Variable, but it contains multiple bvar inside.
class BvarCounterRecorder : public raft::BvarCounterRecorderBase {
    typedef BvarCounterRecorderBase Base;
public:
    BvarCounterRecorder() : Base(-1) {}
    explicit BvarCounterRecorder(time_t window_size) : Base(window_size) {}
    explicit BvarCounterRecorder(const base::StringPiece& prefix) : Base(-1) {
        expose(prefix);
    }
    BvarCounterRecorder(const base::StringPiece& prefix,
                    time_t window_size) : Base(window_size) {
        expose(prefix);
    }
    BvarCounterRecorder(const base::StringPiece& prefix1,
                    const base::StringPiece& prefix2) : Base(-1) {
        expose(prefix1, prefix2);
    }
    BvarCounterRecorder(const base::StringPiece& prefix1,
                    const base::StringPiece& prefix2,
                    time_t window_size) : Base(window_size) {
        expose(prefix1, prefix2);
    }

    ~BvarCounterRecorder() { hide(); }

    // Record the counter num.
    BvarCounterRecorder& operator<<(int64_t count_num);
        
    // Expose all internal variables using `prefix' as prefix.
    // Returns 0 on success, -1 otherwise.
    // Example:
    //   BvarCounterRecorder rec;
    //   rec.expose("foo_bar_add");     // foo_bar_add_avg_counter
    //                                    // foo_bar_add_max_counter
    //                                    // foo_bar_add_total_times
    //                                    // foo_bar_add_qps
    //   rec.expose("foo_bar", "apply");   // foo_bar_apply_avg_counter
    //                                    // foo_bar_apply_max_counter
    //                                    // foo_bar_apply_total_times
    //                                    // foo_bar_apply_qps
    int expose(const base::StringPiece& prefix) {
        return expose(base::StringPiece(), prefix);
    }
    int expose(const base::StringPiece& prefix1,
               const base::StringPiece& prefix2);
    
    // Hide all internal variables, called in dtor as well.
    void hide();

    // Get the average counter num in recent |window_size| seconds
    // If |window_size| is absent, use the window_size to ctor.
    int64_t avg_counter(time_t window_size) const
    { return _avg_counter_window.get_value(window_size).get_average_int(); }
    int64_t avg_counter() const
    { return _avg_counter_window.get_value().get_average_int(); }

    // Get p1/p2/p3/99.9-ile counter num in recent window_size-to-ctor seconds.
    bvar::Vector<int64_t, 4> counter_percentiles() const;

    // Get the max counter numer in recent window_size-to-ctor seconds.
    int64_t max_counter() const { return _max_counter_window.get_value(); }

    // Get the total number of recorded counter nums
    int64_t total_times() const { return _avg_counter.get_value().num; }

    // Get qps in recent |window_size| seconds. The `q' means counter nums.
    // recorded by operator<<().
    // If |window_size| is absent, use the window_size to ctor.
    int64_t qps(time_t window_size) const;
    int64_t qps() const { return _qps.get_value(); }

    // Get |ratio|-ile counter num in recent |window_size| seconds
    // E.g. 0.99 means 99%-ile
    int64_t counter_percentile(double ratio) const;

    // Get name of a sub-bvar.
    const std::string& avg_counter_name() const { return _avg_counter_window.name(); }
    const std::string& counter_percentiles_name() const
    { return _counter_percentiles.name(); }
    const std::string& counter_cdf_name() const { return _counter_cdf.name(); }
    const std::string& max_counter_name() const
    { return _max_counter_window.name(); }
    const std::string& total_times_name() const { return _total_times.name(); }
    const std::string& qps_name() const { return _qps.name(); }
};

std::ostream& operator<<(std::ostream& os, const BvarCounterRecorder&);

}  // namespace raft

namespace raft {
class Closure;

// http://stackoverflow.com/questions/1493936/faster-approach-to-checking-for-an-all-zero-buffer-in-c
inline bool is_zero(const char* buff, const size_t size) {
    if (size >= sizeof(uint64_t)) {
        return (0 == *(uint64_t*)buff) &&
            (0 == memcmp(buff, buff + sizeof(uint64_t), size - sizeof(uint64_t)));
    } else if (size > 0) {
        return (0 == *(uint8_t*)buff) &&
            (0 == memcmp(buff, buff + sizeof(uint8_t), size - sizeof(uint8_t)));
    } else {
        return 0;
    }
}

inline uint32_t murmurhash32(const void *key, int len) {
    uint32_t hash = 0;
    base::MurmurHash3_x86_32(key, len, 0, &hash);
    return hash;
}

inline uint32_t murmurhash32(const base::IOBuf& buf) {
    base::MurmurHash3_x86_32_Context ctx;
    base::MurmurHash3_x86_32_Init(&ctx, 0);
    const size_t block_num = buf.backing_block_num();
    for (size_t i = 0; i < block_num; ++i) {
        base::StringPiece sp = buf.backing_block(i);
        if (!sp.empty()) {
            base::MurmurHash3_x86_32_Update(&ctx, sp.data(), sp.size());
        }
    }
    uint32_t hash = 0;
    base::MurmurHash3_x86_32_Final(&hash, &ctx);
    return hash;
}

inline uint32_t crc32(const void* key, int len) {
    return base::crc32c::Value((const char*)key, len);
}

inline uint32_t crc32(const base::IOBuf& buf) {
    uint32_t hash = 0;
    const size_t block_num = buf.backing_block_num();
    for (size_t i = 0; i < block_num; ++i) {
        base::StringPiece sp = buf.backing_block(i);
        if (!sp.empty()) {
            hash = base::crc32c::Extend(hash, sp.data(), sp.size());
        }
    }
    return hash;
}

// Start a bthread to run closure
void run_closure_in_bthread(::google::protobuf::Closure* closure,
                           bool in_pthread = false);

// Start a bthread to run closure without signal other worker thread to steal
// it. You should call bthread_flush() at last.
void run_closure_in_bthread_nosig(::google::protobuf::Closure* closure,
                                  bool in_pthread = false);

struct RunClosureInBthreadNoSig {
    void operator()(google::protobuf::Closure* done) {
        return run_closure_in_bthread_nosig(done);
    }
};

ssize_t file_pread(base::IOPortal* portal, int fd, off_t offset, size_t size);

ssize_t file_pwrite(const base::IOBuf& data, int fd, off_t offset);

// unsequence file data, reduce the overhead of copy some files have hole.
class FileSegData {
public:
    // for reader
    FileSegData(const base::IOBuf& data)
        : _data(data), _seg_header(base::IOBuf::INVALID_AREA), _seg_offset(0), _seg_len(0) {}
    // for writer
    FileSegData() : _seg_header(base::IOBuf::INVALID_AREA), _seg_offset(0), _seg_len(0) {}
    ~FileSegData() {
        close();
    }

    // writer append
    void append(const base::IOBuf& data, uint64_t offset);
    void append(void* data, uint64_t offset, uint32_t len);

    // writer get
    base::IOBuf& data() {
        close();
        return _data;
    }

    // read next, NEED clear data when call next in loop
    size_t next(uint64_t* offset, base::IOBuf* data);

private:
    void close();

    base::IOBuf _data;
    base::IOBuf::Area _seg_header;
    uint64_t _seg_offset;
    uint32_t _seg_len;
};

// A special Closure which provides synchronization primitives
class SynchronizedClosure : public Closure {
public:
    SynchronizedClosure() : _event(1) {}

    SynchronizedClosure(int num_signal) : _event(num_signal) {}
    // Implements raft::Closure
    void Run() {
        _event.signal();
    }
    // Block the thread until Run() has been called
    void wait() { _event.wait(); }
    // Reset the event
    void reset() {
        status().reset();
        _event.reset();
    }
private:
    bthread::CountdownEvent _event;
};

class BAIDU_CACHELINE_ALIGNMENT ClosureGroup {
public:
    class SubDone : public raft::Closure {
    public:
        SubDone(ClosureGroup* g);
        void Run() override;

    private:
        ClosureGroup* _g;
    };


    class SubDoneMemory {
    public:
        // Most of read/write requests would not cross over 2 slices
        static constexpr size_t SMALL_COUNT = 2;

        SubDoneMemory(size_t sub_done_count, ClosureGroup* g);

        ~SubDoneMemory();

        size_t sub_done_count() const;
        void* sub_done_mem(size_t i);

    private:
        uint32_t _count;
        char _static_mem[SMALL_COUNT * sizeof(SubDone)];
        char *_extra_mem;
    };

    ClosureGroup(int32_t sub_done_count,
                 raft::Closure *done);

    virtual ~ClosureGroup();

    SubDone* sub_done(size_t i);

    void on_sub_done();

protected:
    virtual void before_run_done();
    raft::Closure *_done;

private:
    std::atomic<int32_t> _remain;
    ClosureGroup::SubDoneMemory _memory;
};

inline uint32_t generate_wrong_crc32(base::IOBuf data) {
    std::string data_str_before_modify = data.to_string();
    std::string data_str_after_modify;

    for (char c : data_str_before_modify) {
        unsigned char uc = static_cast<unsigned char>(c);
        uc = ~uc;
        data_str_after_modify += static_cast<char>(uc);
    }

    base::IOBuf data_after_modify;
    data_after_modify.append(data_str_after_modify);
    uint32_t crc_code = 0;
    CHECK(raft::get_crc32((raft::CrcType)raft::FLAGS_crc32_type, data_after_modify, &crc_code));
    return crc_code;
}

inline bool check_wrong_checksum_flipping_and_gen_wrong_crc(
        int32_t crc_wrong_checksum_flip_interval_s,
        int32_t crc_wrong_checksum_rate,
        std::atomic<int64_t>& last_crc_flip_time,
        std::atomic<int32_t>& whether_gen_wrong_crc,
        const base::IOBuf& data, uint32_t& wrong_crc_code,
        const std::string& name) {
    bool is_wrong = false;
    uint32_t saved_crc_code = wrong_crc_code;
    if (crc_wrong_checksum_flip_interval_s != 0) {
        if (crc_wrong_checksum_flip_interval_s != -1) {
            int64_t cur_time = base::monotonic_time_us();
            if (cur_time - last_crc_flip_time.load()
                    > crc_wrong_checksum_flip_interval_s * 1000 * 1000) {
                last_crc_flip_time.store(cur_time);
                whether_gen_wrong_crc.store(-1 * whether_gen_wrong_crc.load());
                LOG(NOTICE) << name << " correct or wrong crc flipped at time: "
                    << cur_time << " the state after flip is: "
                    << (whether_gen_wrong_crc.load() == -1 ? "wrong" : "correct");
            }
        }

        if (whether_gen_wrong_crc.load() < 0
                && static_cast<int>(base::fast_rand() % 100) < crc_wrong_checksum_rate) {
            wrong_crc_code = generate_wrong_crc32(data);
            is_wrong = true;
            LOG(INFO) << name << " set wrong crc32: " << wrong_crc_code;
            LOG_IF(WARNING, wrong_crc_code == saved_crc_code)
                << name << " generate wrong_crc_code failed, wrong_crc_code == correct_crc_code: "
                << saved_crc_code; 
        } else {
            LOG(INFO) << name << " set correct crc32: " << saved_crc_code;
        }
    } else {
        LOG(INFO) << name << " always generate correct crc";
    }
    return is_wrong;
}

void fill_rpc_channel_option(rpc::ChannelOptions* opt);

}  // namespace raft

#endif // PUBLIC_RAFT_RAFT_UTIL_H
