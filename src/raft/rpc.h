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

#include <baidu/hpn/rpc/socket.h>
#include <baidu/hpn/rpc/channel.h>
#include <baidu/hpn/rpc/controller.h>
#include <baidu/hpn/rpc/server.h>
#include <baidu/hpn/rpc/closure_guard.h>
#include <baidu/hpn/rpc/http_status_code.h>
#include <baidu/hpn/rpc/builtin/common.h>
#include <baidu/hpn/rpc/parallel_channel.h>
#define RAFT_VALIDATE_GFLAG HPN_RPC_VALIDATE_GFLAG
namespace rpc = baidu::hpn::rpc;

// #include <baidu/rpc/socket.h>
// #include <baidu/rpc/channel.h>
// #include <baidu/rpc/controller.h>
// #include <baidu/rpc/server.h>
// #include <baidu/rpc/closure_guard.h>
// #include <baidu/rpc/http_status_code.h>
// #include <baidu/rpc/builtin/common.h>
// #include <baidu/rpc/parallel_channel.h>
// #define RAFT_VALIDATE_GFLAG BAIDU_RPC_VALIDATE_GFLAG
// namespace rpc = baidu::rpc;
