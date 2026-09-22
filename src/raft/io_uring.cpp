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

// Authors: Jingwei Ma (majingwei@baidu.com)

#include <sys/eventfd.h>
#include <baidu/hpn/polar/poll_thread.h>                        // running_in_poll_thread
#include "raft/util.h"
#include "raft/io_uring.h"
#include "raft/fsync.h"

/*
static char s_binary_char_map[] = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'A', 'B', 'C', 'D', 'E', 'F'
};*/

inline std::ostream& operator<<(std::ostream& os, const io_uring_sqe& sqe) {
    uint32_t crc_code = 0;
    base::IOBuf crc_buf;
    crc_buf.appendv((iovec*)sqe.addr, sqe.len);
    CHECK(raft::get_crc32((raft::CrcType)raft::FLAGS_crc32_type, crc_buf, &crc_code));
    os << "op: " << (sqe.opcode == 1 ? "read" : "write") << ", fd: " << sqe.fd
        << ", off: " << sqe.off << ", iov_addr: " << (void*)sqe.addr << ", len: " << sqe.len
        << ", crc: " << crc_code << ", user_data_ptr: " << (void*)sqe.user_data;
    /* << ", content: [";
    iovec* iov = (iovec*)sqe.addr;
    const int count = sqe.len;
    for (int i = 0; i < count; i++) {
        auto io = iov[i];
        os << "iov: " << i << ", ptr: " << io.iov_base << ", iov_len: " << io.iov_len << ", (";
        for (size_t j = 0; j < io.iov_len; j++) {
            uint8_t c = ((uint8_t*)io.iov_base)[j];
            os << s_binary_char_map[c >> 4] << s_binary_char_map[c & 0xF];
        }
        os << std::dec << ")";
    }
    os << "]";*/
    return os;
}

inline std::ostream& operator<<(std::ostream& os, const io_uring_cqe& cqe) {
    os << "res: " << cqe.res << ", user_data_ptr: " << (void*)cqe.user_data;
    return os;
}

namespace bthread {
DECLARE_int32(bthread_concurrency);
}

namespace raft {

DEFINE_int32(uring_max_event, 16384, "max event each uring");
DECLARE_bool(raft_use_polar);
DEFINE_bool(raft_use_iouring, false, "use iouring to execute io");
DEFINE_int32(raft_sync_threads, 8, "raft thread num to do sync");
DEFINE_int32(raft_io_threads, -1, "io worker num for raft, if -1, following bthread_concurrency");

static bool raft_iouring_available(const char*, bool value) {
    if (!value) {
        return true;
    }
    io_uring ring;
    int ret = io_uring_queue_init(FLAGS_uring_max_event, &ring, 0);
    if (ret != 0) {
        LOG(WARNING) << "io_uring not supported";
        return false;
    }
    io_uring_queue_exit(&ring);
    return true;
}
DEFINE_validator(raft_use_iouring, &raft_iouring_available);

static BvarAdder<int64_t> g_uring_waiting_submit("raft_io_uring_waiting_submit");
static BvarAdder<int64_t> g_uring_waiting_queue("raft_io_uring_waiting_queue");
static BvarAdder<int64_t> g_uring_inflight_io("raft_io_uring_inflight");
static BvarLatencyRecorder g_uring_us("raft_io_uring_us");

static BvarLatencyRecorder g_sync_queue_us("raft_sync_queue_us");
static BvarAdder<int64_t> g_sync_queue_entries("raft_sync_queue_entries");
static BvarLatencyRecorder g_sync_process_us("raft_sync_process_us");

inline std::ostream& operator<<(std::ostream& os, const UringWorker::ReaderWriter& rw) {
    uint32_t crc_code = 0;
    base::IOBuf crc_buf;
    crc_buf.appendv(rw.io_vec(), rw.iov_cnt());
    CHECK(raft::get_crc32((raft::CrcType)raft::FLAGS_crc32_type, crc_buf, &crc_code));
    os << "fd: " << rw.fd() << ", offset: " << rw.offset() << ", size: " << rw.size()
        << " type: " << (rw.type() == IOWorker::IOType::READ ? "read" : "write")
        << ", crc: " << crc_code << " iov: " << rw.io_vec() << " iov_cnt: " << rw.iov_cnt();
    /* << ", [";
    iovec* iov = rw.io_vec();
    const int count = rw.iov_cnt();
    for (int i = 0; i < count; i++) {
        auto io = iov[i];
        os << "iov: " << i << ", ptr: " << io.iov_base << ", iov_len: " << io.iov_len << ", content: (";
        for (size_t j = 0; j < io.iov_len; j++) {
            uint8_t c = ((uint8_t*)io.iov_base)[j];
            os << s_binary_char_map[c >> 4] << s_binary_char_map[c & 0xF];
        }
        os << std::dec << ")";
    }
    os << "]";*/
    return os;
}

int PthreadWorker::init() {
    if (FLAGS_raft_use_polar) {
        baidu::hpn::polar::Poller p;
        p.fn = polar_polling;
        p.ctx = this;
        p.name = name();
        LOG(NOTICE) << "add poller: " << name();
        CHECK_EQ(0, baidu::hpn::polar::poll_thread_add_poller(p, id()));
        return 0;
    }
    _stopped = false;
    _pthread = std::thread(&PthreadWorker::io_run, this);
    return 0;
}

void PthreadWorker::stop() {
    if (FLAGS_raft_use_polar) {
        LOG(NOTICE) << "remove poller: " << name();
        CHECK_EQ(0, baidu::hpn::polar::poll_thread_remove_poller(name(), id()));
        return;
    }
    {
        std::unique_lock<std::mutex> lck(_mutex);
        _stopped = true;
        if (_queue.empty()) {
            _cv.notify_one();
        }
    }
    _pthread.join();
}

bool PthreadWorker::pop_task(IOTask* task) {
    CHECK(task);
    // make auto unlock
    if (_queue.empty()) {
        return false;
    }
    *task = _queue.front();
    _queue.pop_front();
    return true;
}

void PthreadWorker::io_run() {
    IOTask task;
    std::unique_lock<std::mutex> lck(_mutex);
    while (!_stopped) {
        bool ret = pop_task(&task);
        if (ret == false) {
            _cv.wait(lck);
            continue;
        }
        lck.unlock();
        do_io(task);
        lck.lock();
    }
}

int PthreadWorker::polar_polling(void* arg) {
    PthreadWorker* worker = reinterpret_cast<PthreadWorker*>(arg);
    IOTask task;
    size_t round = 0;
    int ret = 0;
    while (worker->pop_task(&task)) {
        worker->do_io(task);
        round += 1;
    }
    ret = round == 0 ? POLAR_IDLE : POLAR_OK;
    return ret;
}

void PthreadWorker::do_io(const IOTask& task) {
    ssize_t ret = -1;
    if (task.type == PthreadWorker::WRITE) {
        ret = file_pwrite(task.write_buf, task.fd, task.offset);
    } else {
        ret = file_pread(task.read_buf_ptr, task.fd, task.offset, task.size);
    }
    if (ret < 0) {
        task.done->status().set_error(EIO, berror(errno));
    } else if (static_cast<size_t>(ret) < task.size) {
        PLOG(WARNING) << "Partial done, fd: " << task.fd << " required_size: " << task.size
            << " returned_size: " << ret << " type: " << task.type;
    }
    task.done->Run();
}

void PthreadWorker::read(int fd, off_t offset, base::IOPortal* portal, size_t size, Closure* done) {
    IOTask task;
    task.fd = fd;
    task.offset = offset;
    task.size = size;
    task.read_buf_ptr = portal;
    task.type = READ;
    task.done = done;
    std::lock_guard<std::mutex> guard(_mutex);
    bool need_notify = _queue.empty();
    _queue.push_back(std::move(task));
    if (need_notify) {
        _cv.notify_one();
    }
}

void PthreadWorker::write(int fd, off_t offset, const base::IOBuf& data, Closure* done) {
    IOTask task;
    task.fd = fd;
    task.offset = offset;
    task.write_buf = data;
    task.size = data.size();
    task.type = WRITE;
    task.done = done;
    std::lock_guard<std::mutex> guard(_mutex);
    bool need_notify = ((!FLAGS_raft_use_polar) && _queue.empty());
    _queue.push_back(std::move(task));
    if (need_notify) {
        _cv.notify_one();
    }
}

UringWorker::UringWorker(int32_t id) : IOWorker(id)
                                     , _mutex(FLAGS_raft_use_polar)
                                     , _waiting_submit(0)
                                     , _in_flight(0)
                                     , _event_fd(-1) {
    if (FLAGS_raft_use_polar) {
        int32_t polar_worker_index;
        CHECK(baidu::hpn::polar::running_in_poll_thread(&polar_worker_index));
        CHECK_EQ(polar_worker_index, id);
    }
}

void UringWorker::BeforeRecycle(rpc::Socket*) {
    // event fd will be auto closed by baidu-rpc
    if (_event_fd >= 0) {
        io_uring_unregister_eventfd(&_ring);
    }
    io_uring_queue_exit(&_ring);
    _event_fd = -1;
    Release(); // pair with init and auto delete
}

int UringWorker::init() {
    int ret = io_uring_queue_init(FLAGS_uring_max_event, &_ring, 0);
    if (ret != 0) {
        PLOG(ERROR) << "Fail to init uring, error: " << ret
            << ", " << berror(-ret);
        return ret;
    }
    if (FLAGS_raft_use_polar) {
        AddRef(); // pair with polar_destroy
        baidu::hpn::polar::Poller p;
        p.fn = polar_polling;
        p.destroy = polar_destroy;
        p.ctx = this;
        p.name = name();
        LOG(NOTICE) << "add poller: " << name() << ", ptr: " << this << ", id: " << id();
        CHECK_EQ(0, baidu::hpn::polar::poll_thread_add_poller(p, id()));
        AddRef(); // prevent fro delete before recycle
        return 0;
    }
    /* Create an eventfd instance for epoll*/
    _event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK); // make it unblock
    if (_event_fd < 0) {
        io_uring_queue_exit(&_ring);
        LOG(ERROR) << "Fail to create eventfd";
        return _event_fd;
    }
    ret = io_uring_register_eventfd(&_ring, _event_fd);
    if (ret != 0) {
        LOG(ERROR) << "Fail to register iouring event, close event fd=" << _event_fd;
        io_uring_queue_exit(&_ring);
        close(_event_fd);
        _event_fd = -1;
        return ret;
    }
    rpc::SocketOptions options;
    options.fd = _event_fd;
    options.user = this;
    options.health_check_interval_s = -1;
    options.on_edge_triggered_events = edge_event;
    ret = rpc::Socket::Create(options, &_socket_id);
    if (ret != 0) {
        LOG(ERROR) << "Fail to create Socket, close event fd=" << _event_fd;
        io_uring_unregister_eventfd(&_ring);
        io_uring_queue_exit(&_ring);
        close(_event_fd);
        _event_fd = -1;
        return -1;
    }
    AddRef(); // prevent fro delete before recycle
    return ret;
}

void UringWorker::stop() {
    if (FLAGS_raft_use_polar) {
        LOG(NOTICE) << "remove poller: " << name() << ", ptr: " << this << ", id: " << id();
        CHECK_EQ(0, baidu::hpn::polar::poll_thread_remove_poller(name(), id()));
        BeforeRecycle(nullptr);
        return;
    }
    rpc::Socket::SetFailed(_socket_id);
}

void UringWorker::edge_event(rpc::Socket* s) {
    UringWorker* w = reinterpret_cast<UringWorker*>(s->user());
    int progress = rpc::Socket::PROGRESS_INIT;

    do {
        w->on_event();
        if (s->Failed()) {
            return;
        }
    } while (s->MoreReadEvents(&progress));
}

void UringWorker::on_event() {
    // https://man7.org/linux/man-pages/man3/io_uring_register_eventfd.3.html
    // NOTES: An eventfd notification must thus only be treated as a hint to check the CQ ring for completions.
    std::unique_lock<Mutex> lck(_mutex); // uring is not thread safe
    eventfd_t v;
    int ret = -1;
    do {
        ret = eventfd_read(_event_fd, &v); // should be clean
        if (ret == 0) {
            // LOG(NOTICE) << "value: " << v;
            break;
        }
        if (errno == EAGAIN) {
            return;
        }
        LOG(WARNING) << "read event_fd error: " << errno
            << ", msg: " << berror(errno);
        return;
    } while (0);
    CHECK_GE(_in_flight, 0);
    if (_in_flight == 0) {
        return;
    }
    std::vector<ReaderWriter*> closures;
    unsafe_poll_event(&closures);
    lck.unlock(); // safe to run closure outside of lock
    for (auto& done: closures) {
        done->Run();
    }
}

int UringWorker::polar_polling(void* arg) {
    UringWorker* worker = reinterpret_cast<UringWorker*>(arg);
    CHECK(worker);
    std::vector<ReaderWriter*> closures;
    worker->unsafe_poll_event(&closures);
    for (auto& done: closures) {
        done->Run();
    }
    int ret = worker->submit_uring() == 0 && closures.empty() ? POLAR_IDLE : POLAR_OK;
    return ret;
}

void UringWorker::polar_destroy(void* arg) {
    UringWorker* worker = reinterpret_cast<UringWorker*>(arg);
    CHECK(worker);
    LOG(NOTICE) << "destroy poller: " << worker->name()
        << ", ptr: " << worker << ", id: " << worker->id();
    worker->Release();
}

void UringWorker::unsafe_poll_event(std::vector<ReaderWriter*>* closures) {
    RAFT_RETURN_IF(_in_flight <= 0);
    CHECK(closures);
    while (_in_flight > 0) {
        struct io_uring_cqe *cqe = nullptr;
        int ret = io_uring_peek_cqe(&_ring, &cqe);
        if (ret == -EAGAIN) {
            return;
        }
        if (ret != 0) {
            LOG(ERROR) << "wait cqe error: " << errno;
            return;
        }
        ReaderWriter* done = (ReaderWriter*)io_uring_cqe_get_data(cqe);
        CHECK(done);
        do {
            if (done->size() == cqe->res) {
                LOG(INFO) << "complete: " << *done << " on: " << *cqe;
                closures->push_back(done);
                break;
            }
            if (cqe->res < 0) {
                LOG(ERROR) << "uring error, errno: " << cqe->res
                    << " error message: " << berror(-cqe->res) 
                    << " rw: " << *done
                    << " system max iov_cnt: " << IOV_MAX
                    << " expected size: " << done->size()
                    << " iov: " << done->io_vec() << " iov_cnt: " << done->iov_cnt();
                    done->status().set_error(EIO, berror(-cqe->res));
                closures->push_back(done);
                break;
            }
            LOG(INFO) << "complete size: " << cqe->res << " expected size: " << done->size()
                << " rw: " << *done << " cqe: " << *cqe;
            if (cqe->res == 0) {
                // if cqe->res > 0 and not equal to required size, we chose a queue to retry, is this needed?
                done->pop_back_buffer();
                closures->push_back(done);
                break;
            }
            CHECK_EQ(cqe->res, done->pop_front(cqe->res));
            queue_request(done);
        } while (0);
        io_uring_cqe_seen(&_ring, cqe);
        _in_flight --;
        g_uring_inflight_io << -1;
    }
}

UringWorker::ReaderWriter::ReaderWriter(int fd, off_t offset, IOType type,
        base::IOBuf* buffer, Closure* done) :
    _io_vec(&_io), _iov_cnt(0), _fd(fd), _offset(offset), _type(type),
    _size(0), _buffer(buffer), _done(done), _timestamp_us(base::monotonic_time_us()) {}

UringWorker::ReaderWriter::~ReaderWriter() {
    if (_io_vec != &_io) {
        // extra iovec allocated
        delete [] _io_vec;
        _io_vec = &_io;
    }
}

ssize_t UringWorker::ReaderWriter::ReadV(const iovec* iov, int iovcnt) {
    return append_iov(iov, iovcnt);
}

ssize_t UringWorker::ReaderWriter::WriteV(const iovec* iov, int iovcnt) {
    return append_iov(iov, iovcnt);
}

ssize_t UringWorker::ReaderWriter::append_iov(const iovec* iov, int iovcnt) {
    CHECK(iov);
    ssize_t size = 0;
    if (iovcnt <= 1 && _iov_cnt <= 1) {
        // ^^ most case
        _io = *iov;
        size = _io.iov_len;
        _iov_cnt = 1;
        _io_vec = &_io;
    } else {
        iovec* old_iov = _io_vec;
        CHECK(old_iov);
        _io_vec = new iovec[iovcnt + _iov_cnt];
        CHECK(_io_vec);
        for (int i = 0; i < _iov_cnt; i ++) {
            _io_vec[i] = old_iov[i];
        }
        for (int i = 0; i < iovcnt; i ++) {
            _io_vec[i + _iov_cnt] = iov[i];
            size += iov[i].iov_len;
        }
        if (old_iov != &_io) {
            delete [] old_iov;
        }
        _iov_cnt += iovcnt;
    }
    _size += size;
    return size;
}

ssize_t UringWorker::ReaderWriter::pop_front(size_t size) {
   ssize_t pop_size = 0;
    if (_iov_cnt <= 1) {
        ssize_t pop = std::min(size, _io.iov_len);
        _io.iov_len -= pop;
        _io.iov_base = (uint8_t*)_io.iov_base + pop;;
        pop_size += pop;
    } else {
        CHECK_NE(_io_vec, &_io);
        int index = 0;
        while (index < _iov_cnt && size > 0) {
            iovec& vec = _io_vec[index];
            if (vec.iov_len <= size) {
                index ++;
                size -= vec.iov_len;
                pop_size += vec.iov_len;
                continue;
            }
            vec.iov_len -= size;
            vec.iov_base = (uint8_t*)vec.iov_base + size;
            pop_size += size;
            size = 0;
        }
        iovec* old_iovc = _io_vec;
        if (_iov_cnt - index <= 1) {
            _io = _io_vec[index];
            _io_vec = &_io;
        } else {
            int left = _iov_cnt - index;
            _io_vec = new iovec[left];
            for (int i = 0; i < left; i++) {
                _io_vec[i] = old_iovc[index + i];
            }
        }
        _iov_cnt = _iov_cnt - index;
        delete [] old_iovc;
    }
    _size -= pop_size;
    _offset += pop_size;
    return pop_size;
}

void UringWorker::ReaderWriter::pop_back_buffer() {
    if (_buffer) {
        CHECK_GE(_buffer->size(), _size);
        _buffer->pop_back(_size);
    }
}

void UringWorker::ReaderWriter::Run() {
    std::unique_ptr<UringWorker::ReaderWriter> ptr(this); // auto delete
    g_uring_us << base::monotonic_time_us() - _timestamp_us;
    rpc::ClosureGuard done_guard(_done);
    _done->status().swap(status());
}

void UringWorker::read(int fd, off_t offset, base::IOPortal* portal, size_t size, Closure* done) {
    ReaderWriter* reader = new ReaderWriter(fd, offset, IOType::READ, portal, done);
    while (size > 0) {
        size -= portal->append_from_reader(reader, size);
    }
    queue_request(reader);
}

void UringWorker::write(int fd, off_t offset, const base::IOBuf& data, Closure* done) {
    ssize_t size = data.size();
    base::IOBuf write_data(data);
    ReaderWriter* writer = new ReaderWriter(fd, offset, IOType::WRITE, nullptr, done);
    while (size > 0) {
        size -= write_data.cut_into_writer(writer);
    }
    queue_request(writer);
}

/* no need to support
void UringWorker::write(int fd, off_t offset, void* data, size_t size, Closure* done) {
    ReaderWriter* writer = new ReaderWriter(fd, offset, IOType::WRITE, done, nullptr);
    iovec iov;
    iov.iov_base = data;
    iov.iov_len = size;
    CHECK_EQ(size, writer->append_iov(&iov, 1));
    queue_request(writer);
}*/

void UringWorker::queue_request(ReaderWriter* done) {
    std::unique_lock<Mutex> lck(_mutex); // uring is not thread safe
    CHECK(done);
    rpc::ClosureGuard done_guard(done);
    io_uring_sqe *sqe = io_uring_get_sqe(&_ring);
    if (sqe) {
        _waiting_submit ++;
        g_uring_waiting_submit << 1;
    } else {
        LOG(DEBUG) << "can not get sqe on " << name()
            << ", put into pending queue, uring_io:" << *done;
        _pending_queue.push_back(io_uring_sqe());
        sqe = &_pending_queue.back();
        g_uring_waiting_queue << 1;
    }
    CHECK(sqe);
    const int iov_cnt = std::min(IOV_MAX, done->iov_cnt());
    if (done->type() == READ) {
        io_uring_prep_readv(sqe, done->fd(),
                done->io_vec(), iov_cnt, done->offset());
    } else {
        io_uring_prep_writev(sqe, done->fd(),
                done->io_vec(), iov_cnt, done->offset());
    }
    sqe->user_data = (unsigned long) done;
    done_guard.release();
    LOG(DEBUG) << "put " << *done << " to " << *sqe
        << ", sqe_ptr: " << sqe << ", uring_ptr: " << &_ring;
    if (FLAGS_raft_use_polar) {
        return; // polar will submit each round
    }
    submit_uring();
}

int UringWorker::submit_uring() {
    CHECK_GE(_waiting_submit, 0);
    while BAIDU_UNLIKELY(!_pending_queue.empty()) {
        // submit elements in pending queue
        io_uring_sqe *sqe = io_uring_get_sqe(&_ring);
        if (sqe == nullptr) {
            // uring queue full
            break;
        }
        *sqe = _pending_queue.front();
        _waiting_submit ++;
        g_uring_waiting_submit << 1;
        g_uring_waiting_queue << -1;
        _pending_queue.pop_front();
        ReaderWriter* done = (ReaderWriter*)sqe->user_data;
        CHECK(done);
        LOG(DEBUG) << "put " << *done << " to " << *sqe
            << ", sqe_ptr: " << sqe;
    }
    RAFT_RETURN_IF(_waiting_submit == 0, 0);
    int ret = io_uring_submit(&_ring);
    if (ret >= 0) {
        g_uring_waiting_submit << -ret;
        _waiting_submit -= ret;
        _in_flight += ret;
        g_uring_inflight_io << ret;
        LOG(DEBUG) << "submitted: " << ret
            << ", queued: " << _pending_queue.size()
            << ", in_flight: " << _in_flight << ", uring: " << &_ring;
        return ret;
    }
    PLOG(WARNING) << "io uring submit error, ret: " << ret << ", msg: " << berror(-ret)
        << ", uring: " << &_ring;
    return ret;
}

int IOManager::init() {
    CHECK_GT(FLAGS_raft_io_threads, 0);
    SynchronizedClosure done;
    ClosureGroup* cg = new ClosureGroup(FLAGS_raft_io_threads, &done);
    CHECK(cg);
    for (int32_t i = 0; i < FLAGS_raft_io_threads; i++) {
        if (!FLAGS_raft_use_polar) {
            rpc::ClosureGuard done_guard(cg->sub_done(i));
            add_new_worker(i);
            if (!get_worker(i)) {
                cg->sub_done(i)->status().set_error(EIO, "can not create worker");
            }
            continue;
        }
        auto l = [&, i] {
            rpc::ClosureGuard done_guard(cg->sub_done(i));
            scoped_refptr<IOWorker> worker = get_or_create_worker();
            if (!worker) {
                cg->sub_done(i)->status().set_error(EIO, "can not create worker");
            }
        };
        run_closure_in_polar(create_pb_closure(std::move(l)), i);
    }
    done.wait();
    std::unique_lock<std::mutex> lck(_mutex);
    _stopped = false;
    for (int i = 0; i < FLAGS_raft_sync_threads; i++) {
        _sync_threads.emplace_back(std::thread(&IOManager::sync_run, this));
    }
    return done.status().error_code();
}

void IOManager::add_new_worker(int32_t index) {
    scoped_refptr<IOWorker> worker;
    if (FLAGS_raft_use_iouring) {
        worker = new UringWorker(index);
    } else {
        worker = new PthreadWorker(index);
    }
    if (worker->init() != 0) {
        return;
    }
    auto add = [&] (WorkerMap& m) {
        if (m.find(index) != m.end()) {
            return 0lu; // already exist
        }
        CHECK(m.insert(std::make_pair(index, worker)).second);
        return 1lu;
    };
    if (_workers.Modify(add) == 0) {
        // duplicate, just stop it and rpc to recycle
        worker->stop();
    }
    LOG(NOTICE) << "Raft add io worker: " << worker->name() << ", ptr: " << worker
        << ", id: " << worker->id();
}

IOManager::IOManager() : _stopped(true) {
    if (FLAGS_raft_use_polar) {
        FLAGS_raft_io_threads = baidu::hpn::polar::poll_thread_num();
        return;
    }
    if (FLAGS_raft_io_threads <= 0) {
        FLAGS_raft_io_threads = bthread::FLAGS_bthread_concurrency;
    }
}

scoped_refptr<IOWorker> IOManager::get_worker(int32_t index) {
    // auto release ptr
    base::DoublyBufferedData<WorkerMap>::ScopedPtr ptr;
    if (_workers.Read(&ptr) != 0) {
        return nullptr;
    }
    auto iter = ptr->find(index);
    if BAIDU_LIKELY(iter != ptr->end()) {
        return iter->second;
    }
    return nullptr; 
}

scoped_refptr<IOWorker> IOManager::get_or_create_worker() {
    int pick_worker_index = -1;
    if (FLAGS_raft_use_polar) {
        CHECK(baidu::hpn::polar::running_in_poll_thread(&pick_worker_index));
    } else {
        pick_worker_index = base::fast_rand_less_than(static_cast<uint32_t>(FLAGS_raft_io_threads));
    }
    scoped_refptr<IOWorker> worker = get_worker(pick_worker_index);
    if BAIDU_LIKELY(worker) {
        return worker;
    }
    add_new_worker(pick_worker_index);
    return get_worker(pick_worker_index);
}

IOManager::~IOManager() {
    stop();
}

void IOManager::stop() {
    WorkerMap map;
    { // auto unlock when ptr destroyed
        base::DoublyBufferedData<WorkerMap>::ScopedPtr ptr;
        if (_workers.Read(&ptr) != 0) {
            return;
        }
        map = *ptr;
    }
    for (auto& iter : map) {
        iter.second->stop();
    }
    auto l = [&] (WorkerMap& m) {
        m.clear();
        return 1lu;
    };
    _workers.Modify(l);

    // stop sync threads
    std::vector<std::thread> sync_threads;
    std::unique_lock<std::mutex> lck(_mutex);
    _stopped = true;
    _sync_cv.notify_all();
    _sync_threads.swap(sync_threads);
    lck.unlock();
    for (auto& sync_thread : sync_threads) {
        sync_thread.join();
    }
}

void IOManager::read(int fd, off_t offset, base::IOPortal* portal, size_t size, Closure* done) {
    CHECK(done);
    rpc::ClosureGuard done_guard(done);
    scoped_refptr<IOWorker> worker = get_or_create_worker();
    if (!worker) {
        done->status().set_error(EIO, "can not get uring worker");
        return;
    }
    worker->read(fd, offset, portal, size, done);
    done_guard.release();
}

void IOManager::write(int fd, off_t offset, const base::IOBuf& data, Closure* done) {
    CHECK(done);
    scoped_refptr<IOWorker> worker = get_or_create_worker();
    if (!worker) {
        LOG(WARNING) << "can not get uring worker";
        done->status().set_error(EIO, "can not get uring worker");
        raft_run_closure_background(done); // avoid deadlock
        return;
    }
    worker->write(fd, offset, data, done);
}

void IOManager::sync(int fd, Closure* done) {
    std::unique_lock<std::mutex> lck(_mutex);
    if (_stopped && done) {
        done->status().set_error(EAGAIN, "sync thread not running");
        raft_run_closure_background(done);
        return;
    }
    bool need_notify = _sync_queue.empty();
    SyncTask task;
    task.fd = fd;
    task.done = done;
    task.start_us = base::monotonic_time_us();
    _sync_queue.emplace_back(std::move(task));
    g_sync_queue_entries << 1;
    if (need_notify) {
        _sync_cv.notify_one();
    }
}

void IOManager::sync_run() {
    std::unique_lock<std::mutex> lck(_mutex);
    while (!_stopped) {
        if (_sync_queue.empty()) {
            _sync_cv.wait(lck);
            continue;
        }
        auto task = _sync_queue.front();
        _sync_queue.pop_front();
        lck.unlock();
        g_sync_queue_entries << -1;
        g_sync_queue_us << base::monotonic_time_us() - task.start_us;
        base::Timer timer;
        timer.start();
        if (0 != raft_fsync(task.fd) && task.done) {
            task.done->status().set_error(EIO, "sync %d error", task.fd);
        }
        timer.stop();
        g_sync_process_us << timer.u_elapsed();
        {
            // run closure
            rpc::ClosureGuard done_guard(task.done);
        }
        lck.lock();
    }
}

}  // namespace raft
