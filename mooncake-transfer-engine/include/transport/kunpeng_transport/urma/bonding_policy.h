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

#ifndef MOONCAKE_URMA_BONDING_POLICY_H
#define MOONCAKE_URMA_BONDING_POLICY_H

#include <cstdint>

namespace mooncake {

struct BondingChipIds {
    uint8_t src;
    uint8_t dst;
};

constexpr BondingChipIds selectBondingChipIds(bool electrical_network,
                                               uint8_t local_chip,
                                               uint8_t remote_chip) {
    return electrical_network ? BondingChipIds{remote_chip, remote_chip}
                              : BondingChipIds{local_chip, remote_chip};
}

}  // namespace mooncake

#endif  // MOONCAKE_URMA_BONDING_POLICY_H
