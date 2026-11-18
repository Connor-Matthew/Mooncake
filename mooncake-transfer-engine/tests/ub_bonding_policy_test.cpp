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

#include <gtest/gtest.h>

#include "transport/kunpeng_transport/urma/bonding_policy.h"

namespace mooncake {
namespace {

TEST(UbBondingPolicyTest, ElectricalNetworkUsesRemoteChipAtBothEnds) {
    constexpr auto ids = selectBondingChipIds(true, 1, 3);
    EXPECT_EQ(ids.src, 3);
    EXPECT_EQ(ids.dst, 3);
}

TEST(UbBondingPolicyTest, OpticalNetworkUsesLocalAndRemoteChips) {
    constexpr auto ids = selectBondingChipIds(false, 1, 3);
    EXPECT_EQ(ids.src, 1);
    EXPECT_EQ(ids.dst, 3);
}

}  // namespace
}  // namespace mooncake
