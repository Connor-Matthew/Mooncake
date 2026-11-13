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

#include "transport/kunpeng_transport/urma/urma_eid.h"

namespace mooncake {
namespace {

TEST(UrmaEidTest, SelectsByIndexAndKeepsMatchingAddress) {
    urma_eid_info_t eids[3] = {};
    // Sparse and deliberately shuffled, with eid0 not at offset zero.
    eids[0].eid_index = 5;
    eids[0].eid.raw[0] = 0x55;
    eids[1].eid_index = 0;
    eids[1].eid.raw[0] = 0x11;
    eids[2].eid_index = 1;
    eids[2].eid.raw[0] = 0x33;
    EXPECT_EQ(findUrmaEid(eids, 3, 5), &eids[0]);
    EXPECT_EQ(findUrmaEid(eids, 3, 0), &eids[1]);
    const auto* selected = findUrmaEid(eids, 3, 1);
    ASSERT_NE(selected, nullptr);
    EXPECT_EQ(selected->eid.raw[0], 0x33);
    EXPECT_EQ(selected->eid_index, 1u);
    // Index 2 is not present, although array offset 2 exists.
    EXPECT_EQ(findUrmaEid(eids, 3, 2), nullptr);
    EXPECT_EQ(findUrmaEid(eids, 3, -1), nullptr);
}

TEST(UrmaEidTest, RejectsEmptyEnumeration) {
    urma_eid_info_t eid = {};
    EXPECT_EQ(findUrmaEid(nullptr, 1, 0), nullptr);
    EXPECT_EQ(findUrmaEid(&eid, 0, 0), nullptr);
}

}  // namespace
}  // namespace mooncake
