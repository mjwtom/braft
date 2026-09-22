// Copyright (c) 2023 Baidu.com, Inc. All Rights Reserved
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
#include <memory>                       // unique_ptr
// #include <source_location>              // std::source_location
#include <type_traits>                  // std::conditional
#include <bthread.h>                    // bthread_id
#include <bthread/countdown_event.h>    // bthread::CountdownEvent
#include <bthread/execution_queue.h>
#include <base/object_pool.h>           // base::get_object
#include <base/resource_pool.h>         // base::get_resource
#include <bthread_unstable.h>         // bthread_timer_add
// TODO(wangyuanbo): need remove
#undef GCC_VERSION
#include <baidu/hpn/polar/pdl_utils.h>
#include <baidu/hpn/polar/poll_thread_polar_timer.h>
#include <baidu/hpn/polar/poll_thread.h>                        // running_in_poll_thread
#include "baidu/hpn/polar/poll_thread_dynamic_callback.h"       // dynamic_callback
#include "raft/macros.h"               // raft_mutex_t
#include "raft/util.h"                 // raft_async_run_closure_nosig
#include <google/protobuf/stubs/common.h> // pb::Closure
#if GOOGLE_PROTOBUF_VERSION >= 3000000
// After protobuf 3.X, callback.h is removed from common.h, we need to explicitly
// include this file.
#include <google/protobuf/stubs/callback.h> // pb::Closure
#endif

namespace raft {
DECLARE_bool(raft_use_polar);
DECLARE_int64(add_poll_retry_interval_us);

template <typename L>
class PbClosureWithLambda : public ::google::protobuf::Closure {
public:
    PbClosureWithLambda(L&& l) : _l(l) {};
    void Run() override {
        _l();
        delete this;
    };
private:
    L _l;
};

template <typename L>
class RaftClosureWithLambda : public ::raft::Closure {
public:
    RaftClosureWithLambda(L&& l) : _l(l) {};
    void Run() override {
        _l(status());
        delete this;
    };
private:
    L _l;
};

template <typename L>
::google::protobuf::Closure* create_pb_closure(L&& l) {
    return new PbClosureWithLambda<L>(std::move(l));
}

template <typename L>
::raft::Closure* create_raft_closure(L&& l) {
    return new RaftClosureWithLambda<L>(std::move(l));
}

template <typename T>
inline std::ostream& operator<<(std::ostream& os,
        const bthread::ExecutionQueueId<T>& id) {
    os << id.value;
    return os;
}

static int polar_run_closure(void* arg) {
    ::google::protobuf::Closure *c = static_cast<google::protobuf::Closure*>(arg);
    if (c) {
        c->Run();
    }
    return 0;
}

class SamePollerClosure : public google::protobuf::Closure {
public:
    SamePollerClosure(google::protobuf::Closure* done, int polar_worker_index) : _done(done), _polar_worker_index(polar_worker_index) {
        LOG(TRACE) << "create SamePollerClosure " << this << " in thread: " << _polar_worker_index;
        CHECK(_done);
    }
    SamePollerClosure(google::protobuf::Closure* done) : _done(done), _polar_worker_index(-1) {
        CHECK(baidu::hpn::polar::running_in_poll_thread(&_polar_worker_index)) << " need call in polar thread";
        LOG(TRACE) << "create SamePollerClosure " << this << " in thread: " << _polar_worker_index;
        CHECK(_done);
    }
    void Run() override;
private:
    google::protobuf::Closure* _done;
    int32_t _polar_worker_index;
};

inline void try_run_closure_in_polar(google::protobuf::Closure* closure,
    int32_t polar_worker_index) {
    baidu::hpn::polar::CallBack callback;
    callback.fn = polar_run_closure;
    callback.ctx = (void*)closure;
    if (baidu::hpn::polar::poll_thread_add_dynamic_callback(callback, -1, &polar_worker_index) == 0) {
        return;
    }
    PLOG(ERROR) << "Fail to add to poll thread: " << polar_worker_index;
    closure->Run();
}

inline void run_polar_from_bthread(void* args) {
    SamePollerClosure* closure = static_cast<SamePollerClosure*>(args);
    closure->Run();
}

inline void run_closure_in_polar(google::protobuf::Closure* closure,
                                 int32_t polar_worker_index,
                                 bool poll_closure = false) {
    baidu::hpn::polar::CallBack callback;
    callback.fn = polar_run_closure;
    callback.ctx = (void*)closure;
    if (baidu::hpn::polar::poll_thread_add_dynamic_callback(callback, -1, &polar_worker_index) != 0) {
        // polar thread may be busy so we put it into bthread instead of polar thread.
        PLOG(ERROR) << "Fail to add to poll thread: " << polar_worker_index << ", retry run in polar from bthread later";
        bthread_t timer;
        // if add_callback failed alaway, SamePollerClosure will new more and more
        bthread_timer_add(&timer, base::microseconds_from_now(FLAGS_add_poll_retry_interval_us),
                          run_polar_from_bthread,
                          poll_closure ? closure : (void*)(new SamePollerClosure(closure, polar_worker_index)));
    }
}

template <typename T>
class RaftEqTaskIterator : public bthread::TaskIteratorBase {
public:
    RaftEqTaskIterator(bthread::TaskNode* head, bthread::ExecutionQueueBase* queue,
            bool is_stopped, bool high_priority) : 
        bthread::TaskIteratorBase(head, queue, is_stopped, high_priority) {}
};

inline bool running_in_polar() {
    if (FLAGS_raft_use_polar) {
        return baidu::hpn::polar::running_in_poll_thread();
    }
    return false;
}

inline int current_polar_idx() {
    int idx = -1;
    if (FLAGS_raft_use_polar) {
        baidu::hpn::polar::running_in_poll_thread(&idx);
    }
    return idx;
}

#define RAFT_CHECK_POLAR_THREAD(polar_worker_index) do { \
    if (raft::FLAGS_raft_use_polar) { \
        CHECK_EQ(polar_worker_index, raft::current_polar_idx()); \
    } \
} while (0)

inline uint64_t make_id(const uint32_t slot_value, const uint32_t version) {
        return ((uint64_t)slot_value << 32 | (uint64_t)version);
}

inline uint32_t get_slot(uint64_t id_value) {
    return id_value >> 32;
}

inline uint32_t get_version(uint64_t id_value) {
    return (uint32_t)(id_value & 0xFFFFFFFFul);
}

template <typename T>
class RaftExecutionQueue {
public:
    RaftExecutionQueue() : _head(nullptr), _tail(nullptr), _meta(nullptr),
        _version(1), _polar_worker_index(-1) {
            _id.value = 0;
        }
    ~RaftExecutionQueue() { clean_queue(); }
                        // id inited as version zero, to make valid, we start from version 1
    // User cannot create ExecutionQueue from construct
    typedef base::ResourceId<RaftExecutionQueue<T>> raft_eq_slot_id_t;
    static int create(typename bthread::ExecutionQueueId<T>* id, const bthread::ExecutionQueueOptions* options,
            typename bthread::ExecutionQueue<T>::execute_func_t execute_func, void* meta);
    int execute(typename base::add_const_reference<T>::type task,
                const bthread::TaskOptions* options, bthread::TaskHandle* handle, bool stop = false);
    int stop();
    static RaftExecutionQueue<T>* address(bthread::ExecutionQueueId<T> id);

protected:
    typedef bthread::TaskAllocator<T> allocator;
    static void clear_task_mem(bthread::TaskNode* node) {
        T* const task = (T*)allocator::get_allocated_mem(node);
        task->~T();
        allocator::deallocate(node);
    }
    void run();
    void clean_queue();

private:
    // for rtc
    bthread::TaskNode* _head;
    bthread::TaskNode* _tail;
    void* _meta;
    uint32_t _version;
    int _polar_worker_index;
    bthread::ExecutionQueueId<T> _id;
    typename bthread::ExecutionQueue<T>::execute_func_t _execute_func;
    bthread::ExecutionQueueOptions _options;
};

template <typename T>
int RaftExecutionQueue<T>::create(bthread::ExecutionQueueId<T>* id,
        const bthread::ExecutionQueueOptions* options,
        typename bthread::ExecutionQueue<T>::execute_func_t execute_func, void* meta) {
    if (execute_func == NULL) {
        return EINVAL;
    }
    raft_eq_slot_id_t slot;
    RaftExecutionQueue<T>* const m = base::get_resource(&slot);
    if (BAIDU_UNLIKELY(m == NULL)) {
        return ENOMEM;
    }
    m->_id.value = make_id(slot.value, m->_version);
    m->_polar_worker_index = current_polar_idx();
    m->_execute_func = execute_func;
    m->_meta = meta;
    bthread::ExecutionQueueOptions opt;
    if (options != NULL) {
        opt = *options;
    }
    m->_options = opt;
    *id = m->_id;
    LOG(INFO) << "create raft execution queue: " << *id
        << " on polar: " << m->_polar_worker_index;
    return 0;
}

template <typename T>
RaftExecutionQueue<T>* RaftExecutionQueue<T>::address(bthread::ExecutionQueueId<T> id) {
    RaftExecutionQueue<T>::raft_eq_slot_id_t slot;
    slot.value = get_slot(id.value);
    RaftExecutionQueue<T>* const m = base::address_resource(slot);
    if BAIDU_UNLIKELY((m == nullptr) || (m->_version != get_version(id.value))) {
        return nullptr;
    }
    CHECK_EQ(m->_polar_worker_index, current_polar_idx()) << id;
    return m;
}

template <typename T>
int RaftExecutionQueue<T>::execute(typename base::add_const_reference<T>::type task,
                const bthread::TaskOptions* options, bthread::TaskHandle* handle, bool stop) {
    bthread::TaskNode* node = base::get_object<bthread::TaskNode>();
    CHECK(node);
    void* const mem = allocator::allocate(node);
    if (BAIDU_UNLIKELY(!mem)) {
        base::return_object<bthread::TaskNode>(node);
        return ENOMEM;
    }
    new (mem) T(task);
    node->status = bthread::TaskStatus::UNEXECUTED;
    node->next = nullptr;
    node->stop_task = stop;
    node->high_priority = true;
    node->iterated = false;
    if (_tail == nullptr) {
        CHECK_EQ(_head, nullptr) << "raft execution queue should be empty";
        _head = node;
        _tail = node;
        // empty, start to run
        if (_options.urgent) {
            LOG(INFO) << "execution queue: " << _id << " start front";
            run();
            return 0;
        }
        LOG(INFO) << "execution queue: " << _id << " start background";
        run_closure_in_polar(google::protobuf::NewCallback(this, &RaftExecutionQueue<T>::run),
                _polar_worker_index);
        return 0;
    }
    _tail->next = node;
    _tail = node;
    return 0;
}

template <typename T>
void RaftExecutionQueue<T>::run() {
    CHECK_NE(_head, nullptr);
    bthread::TaskNode* head = _head;
    while (head) {
        bthread::TaskNode* known_tail = _tail;
        CHECK(known_tail);
        if (head->stop_task) {
            CHECK_EQ(head, _tail) << "no more task should be added to " << _id;
            break;
        }
        RaftEqTaskIterator<T> iter(_head, nullptr, false,
                true /*high_priority to make ++ work without crash*/);
        _execute_func(_meta, static_cast<bthread::TaskIterator<T>&>(
                    static_cast<bthread::TaskIteratorBase&>(iter)));
                    // they share the same memory structure
        head = known_tail->next; // new item added out of Iterator
    }
    // for task added during iterator execution, we still can run since it must be added to the tail
    if (_tail->stop_task) {
        CHECK_EQ(get_version(_id.value) + 1, _version) << _id;
        RaftEqTaskIterator<T> stop_iter(_tail, nullptr, true,
                true /*high_priority to make ++ work without crash*/);
        _execute_func(_meta, static_cast<bthread::TaskIterator<T>&>(
                    static_cast<bthread::TaskIteratorBase&>(stop_iter)));
        clean_queue();
        typename RaftExecutionQueue<T>::raft_eq_slot_id_t slot;
        slot.value = get_slot(_id.value);
        _meta = nullptr;
        LOG(INFO) << "destroy raft execution queue: " << _id
            << " on polar: " << _polar_worker_index;
        base::return_resource(slot);
        return;
    }
    clean_queue();
}

template <typename T>
void RaftExecutionQueue<T>::clean_queue() {
    bthread::TaskNode* cur = _head;
    while (cur) {
        bthread::TaskNode* next = cur->next;
        clear_task_mem(cur);
        base::return_object<bthread::TaskNode>(cur);
        cur = next;
    }
    _head = nullptr;
    _tail = nullptr;
}

template <typename T>
int RaftExecutionQueue<T>::stop() {
    _version ++; // make id invalid first
    execute(T(), nullptr, nullptr, true);
    return 0;
}

template <typename T>
int raft_execution_queue_start(
        bthread::ExecutionQueueId<T>* id, 
        const bthread::ExecutionQueueOptions* options,
        int (*execute)(void* meta, bthread::TaskIterator<T>& iter),
        void* meta) {
    if (FLAGS_raft_use_polar) {
        return RaftExecutionQueue<T>::create(id, options, execute, meta);
    }
    return execution_queue_start(id, options, execute, meta);
}

template <typename T>
int raft_execution_queue_stop(bthread::ExecutionQueueId<T> id) {
    if (FLAGS_raft_use_polar) {
        RaftExecutionQueue<T>* ptr = RaftExecutionQueue<T>::address(id);
        if (ptr == NULL) {
            return EINVAL;
        }
        int ret = ptr->stop();
        return ret;
    }
    return execution_queue_stop(id);
}

template <typename T>
int raft_execution_queue_join(bthread::ExecutionQueueId<T> id) {
    if (FLAGS_raft_use_polar) {
        return 0;
    }
    return execution_queue_join(id);
}

template <typename T>
int raft_execution_queue_execute(bthread::ExecutionQueueId<T> id,
                            typename base::add_const_reference<T>::type task,
                            const bthread::TaskOptions* options = nullptr, 
                            bthread::TaskHandle* handle = nullptr) {
    if (FLAGS_raft_use_polar) {
        RaftExecutionQueue<T>* ptr = RaftExecutionQueue<T>::address(id);
        if (ptr == NULL) {
            return EINVAL;
        }
        return ptr->execute(task, options, handle);
    }
    return execution_queue_execute(id, task, options, handle);
}

class Mutex {
public:
    Mutex() : _poll_mode(false), _thread_index(-1) {}
    explicit Mutex(bool mode) : _poll_mode(mode), _thread_index(-1) {
        if (_poll_mode) {
            _thread_index = current_polar_idx();
            CHECK_GE(_thread_index, 0);
        }
    }
    ~Mutex() {}
    void lock() {
        if (_poll_mode) {
            CHECK_EQ(current_polar_idx(), _thread_index);
            return;
        }
        _mutex.lock();
    }
    void unlock() {
        if (_poll_mode) {
            CHECK_EQ(current_polar_idx(), _thread_index);
            return;
        }
        _mutex.unlock();
    }
    bool try_lock() {
        if (_poll_mode) {
            CHECK_EQ(current_polar_idx(), _thread_index);
            return true;
        }
        return _mutex.try_lock();
    }
    // TODO: remove reset
    void reset(bool mode, int tidx) {
        _poll_mode = mode;
        _thread_index = tidx;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(Mutex);
    raft_mutex_t _mutex;
    bool _poll_mode;
    int _thread_index;
};

struct RaftBthreadCallback {
    std::function<void*(void*)> fn;
    void* args;
};

static int call_void_fn(void* args) {
    RaftBthreadCallback* callback = static_cast<RaftBthreadCallback*>(args);
    CHECK(callback);
    CHECK(callback->fn);
    // CHECK(callback->args);
            // ^^^^ some function need arg as id (e.g. replicator pass bthread_id value)
    callback->fn(callback->args);
    delete callback;
    return 0;
}

inline void raft_run_closure_background(google::protobuf::Closure* done,
                           bool in_pthread = false) {
    if (FLAGS_raft_use_polar) {
        int cur_thread_index = -1;
        CHECK(baidu::hpn::polar::running_in_poll_thread(&cur_thread_index));
        run_closure_in_polar(done, cur_thread_index);
        return;
    }
    run_closure_in_bthread(done, in_pthread);
}

inline void raft_run_closure_in_worker(google::protobuf::Closure* done,
        int32_t thread_index) {
    if (thread_index >= 0) {
        CHECK_LT(static_cast<unsigned int>(thread_index), baidu::hpn::polar::poll_thread_num());
        run_closure_in_polar(done, thread_index);
        return;
    }
    run_closure_in_bthread(done);
}

inline void raft_run_closure_background_nosig(google::protobuf::Closure* done,
        bool in_pthread = false) {
    if (FLAGS_raft_use_polar) {
        raft_run_closure_background(done);
        return;
    }
    run_closure_in_bthread_nosig(done, in_pthread);
}

inline void raft_thread_flush() {
    if (FLAGS_raft_use_polar) {
        return;
    }
    bthread_flush();
}

typedef void* (*background_fn)(void*);

inline int raft_start_background(bthread_t* __restrict tid,
        const bthread_attr_t* __restrict attr,
        void * (*fn)(void*), void* __restrict args) __THROW {
    int cur_thread_index = -1;
    if (FLAGS_raft_use_polar && baidu::hpn::polar::running_in_poll_thread(&cur_thread_index)) {
                                // ^^ in case some task not running in polar
        RaftBthreadCallback* raft_callback = new RaftBthreadCallback;
        raft_callback->fn = fn;
        raft_callback->args = args;
        baidu::hpn::polar::CallBack callback;
        callback.fn = call_void_fn;
        callback.ctx = raft_callback;
        if (baidu::hpn::polar::poll_thread_add_dynamic_callback(callback, -1, &cur_thread_index) == 0) {
            return 0;
        }
        PLOG(ERROR) << "Fail to add to poll thread";
        fn(args);
        return 0;
    }
    return bthread_start_background(tid, attr, fn, args);
}

inline int raft_start_urgent(bthread_t* __restrict tid,
        const bthread_attr_t* __restrict attr,
        void * (*fn)(void*), void* __restrict args) __THROW {
    if (FLAGS_raft_use_polar) {
        int cur_thread_index = -1;
        CHECK(baidu::hpn::polar::running_in_poll_thread(&cur_thread_index));
        fn(args);
        return 0;
    }
    return bthread_start_urgent(tid, attr, fn, args);
}

typedef void (*on_timer_fn)(void*);

inline int raft_timer_add(bthread_timer_t* id, timespec abstime,
        void (*on_timer)(void*), void* arg) __THROW {
    if (FLAGS_raft_use_polar) {
        timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        int64_t time_diff_ms = base::timespec_to_milliseconds(abstime) - base::timespec_to_milliseconds(now);
        if (time_diff_ms < 0) {
            LOG(WARNING) << "Negative timer diff added, diff: " << time_diff_ms;
            time_diff_ms = 0;
        }
        int cur_thread_index = -1;
        if (baidu::hpn::polar::running_in_poll_thread(&cur_thread_index)) {
            return baidu::hpn::polar::poll_thread_add_timer(id,
                    time_diff_ms,
                    on_timer, arg, -1, &cur_thread_index);
        }
        // FIXME: now user can use bthread_timer if no polar thread
    }
    return bthread_timer_add(id, abstime, on_timer, arg);
}

inline int raft_timer_del(bthread_timer_t id) __THROW {
    int cur_thread_index = -1;
    if (FLAGS_raft_use_polar && 
            baidu::hpn::polar::running_in_poll_thread(&cur_thread_index)) {
        return baidu::hpn::polar::poll_thread_del_timer(id, -1, &cur_thread_index);
    }
    return bthread_timer_del(id);
}

typedef int (*raft_on_error)(bthread_id_t, void*, int);

struct BAIDU_CACHELINE_ALIGNMENT RaftBthreadId {
    enum IdState {
        RAFT_BTHREAD_ID_STATE_NORMAL = 0,
        RAFT_BTHREAD_ID_STATE_ERROR = 1,
    };

    void* data;
    raft_on_error on_error;
    int error_code;
    uint32_t version;
    int32_t lock_count;
    IdState state;
    int32_t polar_worker_index;
    RaftBthreadId() : data(nullptr), on_error(nullptr), error_code(0),
        version(1), lock_count(0), state(RAFT_BTHREAD_ID_STATE_NORMAL),
            // ^^^ there are many bthread_id inited at 0, so make 0 invalid
        polar_worker_index(-1) {}
};

typedef base::ResourceId<RaftBthreadId> RaftBthreadResourceId;

inline int raft_bthread_id_create(bthread_id_t* id, void* data,
            int (*on_error)(bthread_id_t, void*, int)) {
    CHECK(id);
    if (FLAGS_raft_use_polar) {
        RaftBthreadResourceId slot;
        RaftBthreadId* const raft_id = base::get_resource(&slot);
        CHECK(raft_id);
        raft_id->data = data;
        raft_id->on_error = on_error;
        CHECK_EQ(raft_id->error_code, 0); // id returned with error_code zero
        CHECK_EQ(raft_id->lock_count, 0); // id returned with lock_count zero
        CHECK_EQ(raft_id->state, RaftBthreadId::RAFT_BTHREAD_ID_STATE_NORMAL); // id returned with normal state
        raft_id->polar_worker_index = current_polar_idx();
        id->value = make_id(slot.value, raft_id->version);
        LOG(INFO) << "created raft_id " << *id << " on polar worker: "
            << current_polar_idx();
        return 0;
    }
    return bthread_id_create(id, data, on_error);
}

inline int raft_bthread_id_unlock_and_destroy(bthread_id_t id) {
    if (FLAGS_raft_use_polar) {
        RaftBthreadResourceId slot;
        slot.value = get_slot(id.value);
        RaftBthreadId* const raft_id = base::address_resource(slot);
        if (!raft_id || raft_id->version != get_version(id.value)) {
            return EINVAL;
        }
        CHECK_EQ(raft_id->polar_worker_index, current_polar_idx()) << id;
        raft_id->lock_count --;
        CHECK_EQ(raft_id->lock_count, 0) << "no one should hold: " << id; // no one hold lock
        CHECK_EQ(raft_id->state, RaftBthreadId::RAFT_BTHREAD_ID_STATE_NORMAL); // on_error executed
        raft_id->version ++;
        LOG(INFO) << "unlock and destroy raft_id " << id << " ok, lock_count: " << raft_id->lock_count;
        base::return_resource(slot);
        return 0;
    }
    return bthread_id_unlock_and_destroy(id);
}

inline int raft_bthread_id_cancel(bthread_id_t id) {
    LOG(INFO) << "cancel: " << id;
    if (FLAGS_raft_use_polar) {
        RaftBthreadResourceId slot;
        slot.value = get_slot(id.value);
        RaftBthreadId* const raft_id = base::address_resource(slot);
        if (!raft_id || raft_id->version != get_version(id.value)) {
            LOG(INFO) << "cancel: " << id << " invalid";
            return EINVAL;
        }
        CHECK_EQ(raft_id->polar_worker_index, current_polar_idx()) << id;
        int32_t saved_version = raft_id->version;
        new (raft_id) RaftBthreadId(); // make id inited
        raft_id->version = saved_version + 1;
        LOG(INFO) << "cancel raft_id " << id << " ok, lock_count: " << raft_id->lock_count;
        base::return_resource(slot);
        return 0;
    }
    return bthread_id_cancel(id);
}

inline int raft_bthread_id_lock(bthread_id_t id, void** pdata) {
    if (FLAGS_raft_use_polar) {
        RaftBthreadResourceId slot;
        slot.value = get_slot(id.value);
        RaftBthreadId* const raft_id = base::address_resource(slot);
        if (!raft_id || raft_id->version != get_version(id.value)) {
            LOG(INFO) << "lock: " << id << " invalid";
            return EINVAL;
        }
        CHECK_EQ(raft_id->polar_worker_index, current_polar_idx()) << id;
        raft_id->lock_count ++;
        CHECK_GT(raft_id->lock_count, 0) << id;
        if (pdata) {
            *pdata = raft_id->data;
        }
        return 0;
    }
    return bthread_id_lock(id, pdata);
}

inline int raft_bthread_id_unlock(bthread_id_t id) {
    if (FLAGS_raft_use_polar) {
        RaftBthreadResourceId slot;
        slot.value = get_slot(id.value);
        RaftBthreadId* const raft_id = base::address_resource(slot);
        if (!raft_id || raft_id->version != get_version(id.value)) {
            LOG(INFO) << "unlock: " << id << " invalid";
            return EINVAL;
        }
        CHECK_EQ(raft_id->polar_worker_index, current_polar_idx()) << id;
        CHECK_GT(raft_id->lock_count, 0) << id;
        raft_id->lock_count --;
        if BAIDU_UNLIKELY(raft_id->state == RaftBthreadId::RAFT_BTHREAD_ID_STATE_ERROR &&
                raft_id->lock_count == 1) {
            int saved_error_code = raft_id->error_code;
            raft_id->state = RaftBthreadId::RAFT_BTHREAD_ID_STATE_NORMAL;
            raft_id->error_code = 0;
            raft_id->on_error(id, raft_id->data, saved_error_code);
        }
        return 0;
    }
    return bthread_id_unlock(id);
}

inline int raft_bthread_id_error(bthread_id_t id, int error_code) {
    if (FLAGS_raft_use_polar) {
        RaftBthreadResourceId slot;
        slot.value = get_slot(id.value);
        RaftBthreadId* const raft_id = base::address_resource(slot);
        if (!raft_id || raft_id->version != get_version(id.value)) {
            return EINVAL;
        }
        CHECK_EQ(raft_id->polar_worker_index, current_polar_idx()) << id;
        CHECK_EQ(raft_id->state, RaftBthreadId::RAFT_BTHREAD_ID_STATE_NORMAL) << id
            << " only support one id_error called";
        raft_id->lock_count ++;
        CHECK_GT(raft_id->lock_count, 0) << id;
        if (raft_id->lock_count == 1) {
            // acquired lock
            return raft_id->on_error(id, raft_id->data, error_code);
        }
        raft_id->state = RaftBthreadId::RAFT_BTHREAD_ID_STATE_ERROR;
        raft_id->error_code = error_code;
        return 0;
    }
    return bthread_id_error(id, error_code);
}

inline int raft_bthread_id_join(bthread_id_t id) {
    if (FLAGS_raft_use_polar) {
        return 0;
    }
    return bthread_id_join(id);
}

#define RAFT_SWITCH_POLAR(ptr, method, cntl, request, response, done_guard, target_tidx)   \
    do {                                                                                \
        if (FLAGS_raft_use_polar) {                                                     \
            int current_tidx = current_polar_idx();                                     \
            CHECK_GE(target_tidx, 0);                                                   \
            LOG(INFO) << "run " #method " current_polar_idx: " << current_tidx              \
                      << ", target_polar_idx:" << target_tidx;                          \
            if (current_tidx != target_tidx) {                                          \
                google::protobuf::Closure* same_done =                                  \
                        new raft::SamePollerClosure(done_guard.release());              \
                auto l = [ptr, cntl, request, response, same_done] {                    \
                    ptr->method(cntl, request, response, same_done);                    \
                };                                                                      \
                raft::run_closure_in_polar(create_pb_closure(std::move(l)), target_tidx);\
                return;                                                                 \
            }                                                                           \
        }                                                                               \
    } while (0)

struct RaftBthreadRunOnPolarCallback {
    std::function<void(void*)> fn;
    void* args;
    bthread::CountdownEvent event;
    bool sync;
};

int run_on_polar_fn(void* args);

template<typename T, typename R = typename std::conditional<std::is_void_v<T>, int, T>::type>
void bthread_run_on_polar(std::function<T()> f, const int tid, const bool sync, R& r) {
    R return_value;
    RaftBthreadRunOnPolarCallback* sync_cb = new RaftBthreadRunOnPolarCallback;
    sync_cb->fn = [f](void* ret) { *((R*)ret) = f(); };
    sync_cb->args = (void*)&return_value;
    sync_cb->sync = sync;
    baidu::hpn::polar::CallBack cb;
    cb.fn = run_on_polar_fn;
    cb.ctx = (void*)sync_cb;
    int polar_idx = tid;
    baidu::hpn::polar::poll_thread_add_dynamic_callback(cb, -1, &polar_idx);
    if (sync) {
        sync_cb->event.wait();
        delete sync_cb;
    }
    r = return_value;
}

template <>
void bthread_run_on_polar(std::function<void()> f, const int tid, const bool sync, int& r);

template<typename T, typename R = typename std::conditional<std::is_void_v<T>, int, T>::type>
void bthread_run_self(std::function<T()> f, R& r) {
    r = f();
}

template <>
void bthread_run_self(std::function<void()> f, int& r);

struct RunClosureBackGround {
    void operator()(google::protobuf::Closure* done) {
        return raft_run_closure_background(done);
    }
};

typedef std::unique_ptr<google::protobuf::Closure, RunClosureBackGround>
        AsyncClosureGuard;

class Counter {
public:
    Counter(bool exclusive) : _raw_ref_count(0), _exclusive(exclusive) {}
    uint32_t operator++() {
        if (_exclusive) {
            return ++ _raw_ref_count;
        }
        return _atomic_ref_count.fetch_add(1, std::memory_order_relaxed) + 1;
    }
    uint32_t operator++(int) {
        if (_exclusive) {
            return _raw_ref_count ++;
        }
        return _atomic_ref_count.fetch_add(1, std::memory_order_relaxed);
    }
    uint32_t operator--() {
        if (_exclusive) {
            return -- _raw_ref_count;
        }
        return _atomic_ref_count.fetch_sub(1, std::memory_order_relaxed) - 1;
    }
    uint32_t operator--(int) {
        if (_exclusive) {
            return _raw_ref_count --;
        }
        return _atomic_ref_count.fetch_sub(1, std::memory_order_relaxed);
    }
    operator uint32_t() const {
        if (_exclusive) {
            return _raw_ref_count;
        }
        return _atomic_ref_count.load(std::memory_order_relaxed);
    }

private:
    union {
        uint32_t _raw_ref_count;
        std::atomic<uint32_t> _atomic_ref_count;
    };
    bool _exclusive;
};

template <class T>
class RaftRefCounted {
public:
    explicit RaftRefCounted(bool exclusive) : _ref_count(exclusive) {}
    RaftRefCounted() : _ref_count(FLAGS_raft_use_polar) {}
    inline void AddRef() {
        ++ _ref_count;
    }
    inline void Release() {
        if ((--_ref_count) == 0) {
            delete this;
        }
    }
    bool has_one_ref() const {
        return _ref_count == 1;
    }

protected:
    virtual ~RaftRefCounted() {}

private:
    DISALLOW_COPY_AND_ASSIGN(RaftRefCounted);
    Counter _ref_count;
};

}  // namespace raft

// bthread run polar function, wait for completion
#define RAFT_SYNC_BTHREAD_RUN_ON_POLAR(expression, tid) ({                                      \
        using expr_type = decltype(expression);                                                 \
        using return_type = std::conditional<std::is_void_v<expr_type>, int, expr_type>::type;  \
        return_type ret;                                                                        \
        if (raft::FLAGS_raft_use_polar) {                                                       \
            raft::bthread_run_on_polar<expr_type>([&]() { return expression; }, tid, true, ret);\
        } else {                                                                                \
            raft::bthread_run_self<expr_type>([&]() { return expression; }, ret);               \
        }                                                                                       \
        ret;                                                                                    \
    })

// bthread run polar function, don't wait for completion
#define RAFT_BTHREAD_RUN_ON_POLAR(expression, tid) ({                                           \
        using expr_type = decltype(expression);                                                 \
        using return_type = std::conditional<std::is_void_v<expr_type>, int, expr_type>::type;  \
        return_type ret;                                                                        \
        if (raft::FLAGS_raft_use_polar) {                                                       \
            raft::bthread_run_on_polar<expr_type>([&]() { return expression; }, tid, false, ret);\
        } else {                                                                                \
            raft::bthread_run_self<expr_type>([&]() { return expression; }, ret);               \
        }                                                                                       \
        ret;                                                                                    \
    })

// dispatch ptr->method(cntl, request, response, done) to polar target_tidx
#define RAFT_DISPATCH_POLAR_WITH_INDEX(ptr, method, cntl, request, response, done_guard, target_tidx) do { \
    if (raft::FLAGS_raft_use_polar) { \
        int current_tidx = raft::current_polar_idx(); \
        CHECK_GE(current_tidx, 0); \
        CHECK_GE(target_tidx, 0); \
        LOG(TRACE) << "run " #method " current_polar_idx: " << current_tidx << ", target_polar_idx:" << target_tidx; \
        if (current_tidx != target_tidx) { \
            google::protobuf::Closure* same_done = new raft::SamePollerClosure(done_guard.release()); \
            auto l = [ptr, cntl, request, response, same_done] { \
                    ptr->method(cntl, request, response, same_done); \
            }; \
            raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx); \
        } else { \
            ptr->method(cntl, request, response, done_guard.release()); \
        } \
    } else { \
        ptr->method(cntl, request, response, done_guard.release()); \
    } \
    return; \
} while (0)

#define RAFT_DISPATCH_POLAR(ptr, method, cntl, request, response, done_guard) \
    RAFT_DISPATCH_POLAR_WITH_INDEX(ptr, method, cntl, request, response, done_guard, ptr->polar_worker_index())

// only use to call self function
#define RAFT_DISPATCH_SELF(method, cntl, request, response, done_guard, target_tidx) do { \
    if (raft::FLAGS_raft_use_polar) { \
        int current_tidx = raft::current_polar_idx(); \
        CHECK_GE(current_tidx, 0); \
        CHECK_GE(target_tidx, 0); \
        LOG(TRACE) << "run " #method " current_polar_idx: " << current_tidx << ", target_polar_idx:" << target_tidx; \
        if (current_tidx != target_tidx) { \
            google::protobuf::Closure* same_done = new raft::SamePollerClosure(done_guard.release()); \
            auto l = [this, cntl, request, response, same_done] { \
                this->method(cntl, request, response, same_done); \
            }; \
            raft::run_closure_in_polar(raft::create_pb_closure(std::move(l)), target_tidx);\
            return; \
        } \
    } \
} while (0)

