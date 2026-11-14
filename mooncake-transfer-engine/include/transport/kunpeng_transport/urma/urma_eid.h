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

#ifndef MOONCAKE_URMA_EID_H
#define MOONCAKE_URMA_EID_H

#include <cstdint>
#include "urma_types.h"

namespace mooncake {

// Provider enumeration order need not match EID indices. Never substitute a
// different EID when a requested index is absent.
inline const urma_eid_info_t* findUrmaEid(const urma_eid_info_t* eids,
                                        uint32_t count, int index) {
    if (!eids || index < 0) return nullptr;
    for (uint32_t i = 0; i < count; ++i) {
        if (eids[i].eid_index == static_cast<uint32_t>(index)) return &eids[i];
    }
    return nullptr;
}

}  // namespace mooncake

#endif  // MOONCAKE_URMA_EID_H
