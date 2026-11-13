// Copyright 2024 KVCache.AI
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

#ifndef URMA_COMPAT_H
#define URMA_COMPAT_H

#include <string>
#include <vector>

#include "urma_api.h"

namespace mooncake {

// Runtime compatibility layer for delegated rjetty and segment contexts,
// which are only present in newer UMDK releases. Production symbols are
// resolved via dlsym once per process; mock builds bind their in-process
// implementation directly. When the production symbols are absent, the
// legacy fixed-size structures are used.
//
// NOTE: do not call these optional APIs directly anywhere else. A direct
// reference would break linking against older liburma builds.
using urma_get_rjetty_fn_t = urma_status_t (*)(urma_jetty_t*, urma_rjetty_t**,
                                               uint32_t*);
using urma_put_rjetty_fn_t = void (*)(urma_rjetty_t*);
using urma_get_seg_ctx_fn_t = urma_status_t (*)(urma_target_seg_t*,
                                                urma_seg_t**, uint32_t*);
using urma_put_seg_ctx_fn_t = void (*)(urma_seg_t*);

extern urma_get_rjetty_fn_t g_urma_get_rjetty;
extern urma_put_rjetty_fn_t g_urma_put_rjetty;
extern urma_get_seg_ctx_fn_t g_urma_get_seg_ctx;
extern urma_put_seg_ctx_fn_t g_urma_put_seg_ctx;

void initUrmaCompat();
// Kept for source compatibility with the first rjetty-only compatibility
// layer. New code should use initUrmaCompat().
void initUrmaRjettyCompat();

bool decodeUrmaRjettyBlob(const std::string& encoded,
                          std::vector<unsigned char>& buffer,
                          std::string& error);

bool decodeUrmaSegBlob(const std::string& encoded,
                       std::vector<unsigned char>& buffer, std::string& error);

}  // namespace mooncake

#endif  // URMA_COMPAT_H
