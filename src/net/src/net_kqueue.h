// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_KQUEUE_H_
#define NET_SRC_NET_KQUEUE_H_
#include <vector>

#include <sys/event.h>

#include "net/src/net_multiplexer.h"

namespace net {

class NetKqueue final : public NetMultiplexer {
 public:
  NetKqueue(int32_t queue_limit = kUnlimitedQueue);
  ~NetKqueue() override = default;

  int32_t NetAddEvent(int32_t fd, int32_t mask) override;
  int32_t NetDelEvent(int32_t fd, int32_t mask) override;
  int32_t NetModEvent(int32_t fd, int32_t old_mask, int32_t mask) override;

  int32_t NetPoll(int32_t timeout) override;

 private:
  std::vector<struct kevent> events_;
};

}  // namespace net
#endif  // NET_SRC_NET_EPOLL_H_
