
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

#include "raft/rtc_helper.h"

namespace raft {

DEFINE_int64(add_poll_retry_interval_us, 1000, "Interval between retry when add poll failed.");

void SamePollerClosure::Run() {
    if (_polar_worker_index >= 0) {
        // ^^ in polar
        int cur_thread_index = -1;
        bool ret = baidu::hpn::polar::running_in_poll_thread(&cur_thread_index);
        LOG(TRACE) << "SamePollerClosure " << this << " now in thread: " << cur_thread_index
            << " will change to thread: " << _polar_worker_index;
        int thread_num = baidu::hpn::polar::poll_thread_num();
        if ((ret || thread_num > 0) && (cur_thread_index != _polar_worker_index)) {
            run_closure_in_polar(this, _polar_worker_index, true);
            return;
        }
    }
    CHECK(_done);
    _done->Run();
    delete this;
}

int run_on_polar_fn(void* args) {
    RaftBthreadRunOnPolarCallback* callback = static_cast<RaftBthreadRunOnPolarCallback*>(args);
    callback->fn(callback->args);
    if (callback->sync) {
        callback->event.signal();
    } else {
        delete callback;
    }
    return 0;
}

template <>
void bthread_run_on_polar(std::function<void()> f, const int tid, const bool sync, int& r) {
    RaftBthreadRunOnPolarCallback* sync_cb = new RaftBthreadRunOnPolarCallback;
    sync_cb->fn = [f](void*) { f(); };
    sync_cb->args = nullptr;
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
    r = 0;
}

template <>
void bthread_run_self(std::function<void()> f, int& r) {
    f();
    r = 0;
}

}  // namespace raft

