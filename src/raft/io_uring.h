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

#pragma once
#include <thread>
#include <mutex>
#include <condition_variable>
#include <base/memory/singleton.h>
#include <base/iobuf.h>
#include <base/reader_writer.h>
#include "raft/rpc.h"
#include "raft/raft.h"
#include "raft/rtc_helper.h"
#include <liburing.h>      // high performance io

namespace raft {

class BAIDU_CACHELINE_ALIGNMENT IOWorker : 
    public RaftRefCounted<IOWorker> {
DISALLOW_COPY_AND_ASSIGN(IOWorker);
public:
    enum IOType {
        READ = 0,
        WRITE = 1,
    };
    IOWorker(int32_t id) : RaftRefCounted<IOWorker>(false) // in bthread
        , _id(id)
        , _name("raft_io_polling_worker_" + std::to_string(id))
        {};
    virtual ~IOWorker() {
        LOG(NOTICE) << "Raft io woker: " << id() << " destroyed, ptr: " << this;
    }
    int32_t id() { return _id; }
    virtual int init() = 0;
    virtual void stop() = 0;
    virtual void read(int fd, off_t offset, base::IOPortal* portal,
            size_t size, Closure* done) = 0;
    virtual void write(int fd, off_t offset, const base::IOBuf& data,
            Closure* done) = 0;
    // virtual void write(int fd, off_t offset, void* data, size_t size,
    //      Closure* done) = 0;
    std::string name() { return _name; }
private:
    int32_t _id;
    std::string _name;
};

class BAIDU_CACHELINE_ALIGNMENT PthreadWorker : public IOWorker {
public:
    PthreadWorker(int32_t id) : IOWorker(id), _stopped(true) {}
    virtual int init() override;
    virtual void stop() override;
    virtual void read(int fd, off_t offset, base::IOPortal* portal, 
            size_t size, Closure* done) override;
    virtual void write(int fd, off_t offset, const base::IOBuf& data, 
            Closure* done) override;
    // virtual void write(int fd, off_t offset, void* data, size_t size,
    //        Closure* done) override;
    void io_run();
    static int polar_polling(void* arg);

private:
    struct IOTask {
        IOTask() : fd(-1), read_buf_ptr(nullptr), offset(0),
            size(0), type(READ), done(nullptr) {}
        int fd;
        base::IOBuf write_buf;
        base::IOPortal* read_buf_ptr;
        off_t offset;
        size_t size;
        IOType type;
        Closure* done;
    };
    bool pop_task(IOTask* task);
    void do_io(const IOTask& task);
    std::mutex _mutex;
    std::condition_variable _cv;
    std::deque<IOTask> _queue;
    bool _stopped;
    std::thread _pthread;
};

class BAIDU_CACHELINE_ALIGNMENT UringWorker : public rpc::SocketUser, public IOWorker {
public:
    UringWorker(int32_t id);
    virtual void BeforeRecycle(rpc::Socket*) override;
    virtual int init() override;
    virtual void stop() override;
    ~UringWorker() override {}
    static void edge_event(rpc::Socket* s);
    static int polar_polling(void* arg);
    static void polar_destroy(void* arg);
    virtual void read(int fd, off_t offset, base::IOPortal* portal, 
            size_t size, Closure* done) override;
    virtual void write(int fd, off_t offset, const base::IOBuf& data, 
            Closure* done) override;
    // virtual void write(int fd, off_t offset, void* data, size_t size,
    //        Closure* done) override;
    
private:
    class ReaderWriter : public base::IReader, public base::IWriter, public Closure {
    public:
        ReaderWriter(int fd, off_t offset, IOType type,
                base::IOBuf* buffer, Closure* done);
                            // ^^ pop back if partial read
        virtual ~ReaderWriter() override;
        virtual ssize_t ReadV(const iovec* iov, int iovcnt) override;
        virtual ssize_t WriteV(const iovec* iov, int iovcnt) override;
        virtual void Run() override;
        ssize_t append_iov(const iovec* iov, int iovcnt);
        ssize_t pop_front(size_t size);
        IOType type() const { return _type; }
        int fd() const { return _fd; }
        off_t offset() const { return _offset; }
        ssize_t size() const { return _size; }
        iovec* io_vec() const { return _io_vec; }
        int iov_cnt() const { return _iov_cnt; }
        void pop_back_buffer();

    private:
        iovec _io; // one io supported
        iovec* _io_vec; // vector suppored
        int _iov_cnt;
        int _fd;
        off_t _offset;
        IOType _type;
        ssize_t _size;
        base::IOBuf* _buffer; // if partial read, need pop back data
        Closure* _done;
        int64_t _timestamp_us;
    };

    friend std::ostream& operator<<(std::ostream& os, const ReaderWriter& rw);
    RAFT_MOCK void queue_request(ReaderWriter* done);
    int submit_uring();

    void on_event();

    void poll_event(); // do not use in CQ Poll Mode, see: https://github.com/axboe/liburing/issues/385 
    void unsafe_poll_event(std::vector<ReaderWriter*>* closures);

private:
    Mutex _mutex;
    std::deque<io_uring_sqe> _pending_queue;
    int64_t _waiting_submit;
    int64_t _in_flight;
    int _event_fd;
    io_uring _ring;
    rpc::SocketId _socket_id;
};

class IOManager {
DISALLOW_COPY_AND_ASSIGN(IOManager);
public:
    static IOManager* GetInstance() {
        return Singleton<IOManager>::get();
    }
    IOManager();
    RAFT_MOCK ~IOManager();

    int init(); // for uring mode, there is possibility uring initialize failed when touch io, so this function creates workers before io and improve the success rate of io
    void stop();

    void read(int fd, off_t offset, base::IOPortal* portal, size_t size, Closure* done);
    void write(int fd, off_t offset, const base::IOBuf& data, Closure* done);
    void sync(int fd, Closure* done);
    // void write(int fd, off_t offset, void* data, size_t size, Closure* done);
private:
    RAFT_MOCK scoped_refptr<IOWorker> get_or_create_worker();
    scoped_refptr<IOWorker> get_worker(int32_t index);
    void add_new_worker(int32_t index);
    void sync_run();
    typedef std::unordered_map<int, scoped_refptr<IOWorker>> WorkerMap;
    base::DoublyBufferedData<WorkerMap> _workers;

    // used for sync
    struct SyncTask {
        int fd;
        Closure* done;
        int64_t start_us;
        SyncTask() : fd(0), done(nullptr), start_us(0) {}
    };
    std::mutex _mutex;
    bool _stopped;
    std::condition_variable _sync_cv;
    std::deque<SyncTask> _sync_queue;
    std::vector<std::thread> _sync_threads;
};

#define g_raft_io_man IOManager::GetInstance()
}  // namespace raft

