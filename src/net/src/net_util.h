// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_UTIL_H_
#define NET_SRC_NET_UTIL_H_
#include <unistd.h>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace net {

int32_t Setnonblocking(int32_t sockfd);



}  // namespace net

#endif  //  NET_SRC_NET_UTIL_H_
