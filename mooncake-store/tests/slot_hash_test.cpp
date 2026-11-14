#include "cvm/slot_hash.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "crc32c.h"
#include "cvm/cvm_types.h"
#include "partition/kv_hash_map.h"
#include "tenant_id.h"

namespace mooncake::test {
namespace {

TEST(SlotHashTest, RegistrationRankTakesPrecedenceOverMasterId) {
    cvm::MasterRegistration early;
    early.master_id = "z-early";
    early.create_revision = 10;
    cvm::MasterRegistration late;
    late.master_id = "a-late";
    late.create_revision = 20;
    EXPECT_TRUE(cvm::MasterRegistrationRankLess(early, late));
    late.create_revision = 10;
    EXPECT_TRUE(cvm::MasterRegistrationRankLess(late, early));
}

TEST(SlotHashTest, RegistrationOrderedRingAgreesWithOwnerLookup) {
    const std::vector<std::string> ids = {"z-early", "a-late"};
    for (const auto& id : ids) {
        const auto slots = cvm::ResolveOwnedSlotsOnRing(ids, id);
        // Sample the full range without rebuilding the ring for every slot.
        for (size_t i = 0; i < slots.size(); i += 127) {
            EXPECT_EQ(cvm::ResolveSlotOwnerOnRing(ids, slots[i]), id);
        }
    }
}

TEST(SlotHashTest, ParsesConfiguredSlotCount) {
    uint16_t parsed = 0;
    EXPECT_TRUE(cvm::ParseSlotCount("1", &parsed));
    EXPECT_EQ(parsed, 1u);
    EXPECT_TRUE(cvm::ParseSlotCount("1024", &parsed));
    EXPECT_EQ(parsed, 1024u);
    EXPECT_TRUE(cvm::ParseSlotCount("16384", &parsed));
    EXPECT_EQ(parsed, cvm::kDefaultSlotCount);
    EXPECT_FALSE(cvm::ParseSlotCount(nullptr, &parsed));
    EXPECT_FALSE(cvm::ParseSlotCount("", &parsed));
    EXPECT_FALSE(cvm::ParseSlotCount("0", &parsed));
    EXPECT_FALSE(cvm::ParseSlotCount("3", &parsed));
    EXPECT_FALSE(cvm::ParseSlotCount("16385", &parsed));
    EXPECT_FALSE(cvm::ParseSlotCount("1024x", &parsed));
    EXPECT_FALSE(cvm::ParseSlotCount("1024", nullptr));
}

TEST(SlotHashTest, SlotOfUsesConfiguredMask) {
    EXPECT_EQ(cvm::SlotOf(0u), 0u);
    EXPECT_EQ(cvm::SlotOf(1u), 1u);
    EXPECT_EQ(cvm::SlotOf(cvm::SlotCount() - 1), cvm::SlotCount() - 1);
    EXPECT_EQ(cvm::SlotOf(cvm::SlotCount()), 0u);
    EXPECT_EQ(cvm::SlotOf(0xFFFFFFFFu), cvm::SlotCount() - 1);
}

TEST(SlotHashTest, KeySlotInRange) {
    const std::vector<std::string> keys = {"", "a", "hello", "key-123",
                                           "a-longer-key-for-coverage"};
    for (const auto& key : keys) {
        EXPECT_LT(cvm::KeySlot(TenantId::Default(), key), cvm::SlotCount());
        EXPECT_LT(cvm::KeySlot(TenantId("tenant-a"), key), cvm::SlotCount());
    }
}

TEST(SlotHashTest, KeySlotDeterministic) {
    const std::string key = "deterministic-key";
    const TenantId tenant("tenant-x");
    const uint16_t first = cvm::KeySlot(tenant, key);
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(first, cvm::KeySlot(tenant, key));
    }
}

namespace {
uint16_t ManualDefaultKeySlot(const std::string& key) {
    Crc32c crc;
    crc.Extend(key.data(), key.size());
    return cvm::SlotOf(crc.Final());
}

uint16_t ManualScopedKeySlot(const std::string& tenant,
                             const std::string& key) {
    Crc32c crc;
    crc.Extend(tenant.data(), tenant.size());
    constexpr char kSeparator = '\0';
    crc.Extend(&kSeparator, 1);
    crc.Extend(key.data(), key.size());
    return cvm::SlotOf(crc.Final());
}
}  // namespace

TEST(SlotHashTest, KeySlotMatchesManualCrc) {
    const std::string key = "scope-me";
    // 默认 tenant：slot = hash(user_key)。
    EXPECT_EQ(cvm::KeySlot(TenantId::Default(), key),
              ManualDefaultKeySlot(key));
    // 非默认 tenant：slot = hash(tenant + '\0' + user_key)。
    EXPECT_EQ(cvm::KeySlot(TenantId("tenant-a"), key),
              ManualScopedKeySlot("tenant-a", key));
}

TEST(SlotHashTest, VNodePositionInRangeAndDeterministic) {
    const std::string master = "master-1:10001";
    for (uint16_t vnode = 0; vnode < cvm::kVnodeCount; ++vnode) {
        const uint16_t pos = cvm::VNodePosition(master, vnode);
        EXPECT_LT(pos, cvm::SlotCount());
        EXPECT_EQ(pos, cvm::VNodePosition(master, vnode));
    }
}

TEST(SlotHashTest, KvHashMapMatchesKeySlot) {
    // client 侧路由（KvHashMap）与 submaster 侧校验（KeySlot）必须用同一份
    // 哈希，否则 key 会路由到错误的 submaster。
    const std::string key = "consistency-key";
    const TenantId tenant("tenant-c");
    EXPECT_EQ(partition::KvHashMap::Compute(tenant, key),
              cvm::KeySlot(tenant, key));
}

TEST(SlotHashTest, ResolveOwnedSlotsOnRingSingleMaster) {
    // n=1 时退化：本机拥有全部 slot。
    const std::vector<std::string> ids = {"m-a"};
    const auto slots = cvm::ResolveOwnedSlotsOnRing(ids, "m-a");
    EXPECT_EQ(slots.size(), static_cast<size_t>(cvm::SlotCount()));
}

TEST(SlotHashTest, ResolveOwnedSlotsOnRingCoversAllSlots) {
    const std::vector<std::string> ids = {"m-a", "m-b", "m-c"};
    std::vector<bool> covered(cvm::SlotCount(), false);
    size_t total = 0;
    for (const auto& id : ids) {
        const auto slots = cvm::ResolveOwnedSlotsOnRing(ids, id);
        for (uint16_t s : slots) {
            EXPECT_FALSE(covered[s]) << "slot " << s << " owned by >1 master";
            covered[s] = true;
            ++total;
        }
    }
    // 每个 slot 恰好归属一个 primary（不重叠 + 全覆盖）。
    EXPECT_EQ(total, static_cast<size_t>(cvm::SlotCount()));
    for (size_t s = 0; s < cvm::SlotCount(); ++s) {
        EXPECT_TRUE(covered[s]) << "slot " << s << " has no owner";
    }
}

TEST(SlotHashTest, ResolveOwnedSlotsOnRingStableOnRemoval) {
    // 一致性哈希环核心性质：移除一个 primary 后，其余 primary 原有 slot 不
    // 丢（只增不减），仅被移除 primary 覆盖的 slot 发生重分配——即「非全员
    // 平移」。
    const std::vector<std::string> ids3 = {"m-a", "m-b", "m-c"};
    const auto a = cvm::ResolveOwnedSlotsOnRing(ids3, "m-a");
    const auto b = cvm::ResolveOwnedSlotsOnRing(ids3, "m-b");

    const std::vector<std::string> ids2 = {"m-a", "m-b"};
    const auto a2 = cvm::ResolveOwnedSlotsOnRing(ids2, "m-a");
    const auto b2 = cvm::ResolveOwnedSlotsOnRing(ids2, "m-b");

    const std::set<uint16_t> a_set(a.begin(), a.end());
    const std::set<uint16_t> a2_set(a2.begin(), a2.end());
    const std::set<uint16_t> b_set(b.begin(), b.end());
    const std::set<uint16_t> b2_set(b2.begin(), b2.end());

    for (uint16_t s : a_set) {
        EXPECT_TRUE(a2_set.count(s)) << "a lost slot " << s << " on removal";
    }
    for (uint16_t s : b_set) {
        EXPECT_TRUE(b2_set.count(s)) << "b lost slot " << s << " on removal";
    }
    // 移除后 a/b 覆盖全量 slot，且总量较移除前增加（接管了被移除者的 slot）。
    EXPECT_EQ(a2_set.size() + b2_set.size(),
              static_cast<size_t>(cvm::SlotCount()));
    EXPECT_GT(a2_set.size() + b2_set.size(), a_set.size() + b_set.size());
}

// ---------------------------------------------------------------------------
// RingSlot 槽位组（§16.5 P1）：SlotToRank / RankOwnedSlots 纯函数
// ---------------------------------------------------------------------------

TEST(SlotHashTest, SlotToRankSingleGroup) {
    // G=1 退化：全部 slot 归 rank 0（回滚兼容路径，§16.17.6）。
    for (uint16_t s : {0, 1, 8191, 8192, 16383}) {
        EXPECT_EQ(cvm::SlotToRank(s, 1u), 0u);
    }
}

TEST(SlotHashTest, SlotToRankTwoGroups) {
    // G=2 唯一精确对半划分。
    EXPECT_EQ(cvm::SlotToRank(0, 2u), 0u);
    EXPECT_EQ(cvm::SlotToRank(8191, 2u), 0u);
    EXPECT_EQ(cvm::SlotToRank(8192, 2u), 1u);
    EXPECT_EQ(cvm::SlotToRank(16383, 2u), 1u);
}

TEST(SlotHashTest, SlotToRankBoundaryEndpoints) {
    // 不变量①：slot=0 → rank 0；slot=16383 → rank G-1（对所有合法 G）。
    for (uint32_t g : {1u, 2u, 3u, 5u, 7u, 13u, 16383u, 16384u}) {
        EXPECT_EQ(cvm::SlotToRank(0, g), 0u) << "G=" << g;
        EXPECT_EQ(cvm::SlotToRank(cvm::SlotCount() - 1, g), g - 1)
            << "G=" << g;
    }
}

TEST(SlotHashTest, SlotToRankMonotonicNonDecreasing) {
    // 不变量②：slot 递增 → rank 不减（连续段性质）。
    for (uint32_t g : {3u, 5u, 100u}) {
        uint32_t prev = 0;
        for (uint16_t s = 0; s < cvm::SlotCount(); ++s) {
            const uint32_t rank = cvm::SlotToRank(s, g);
            EXPECT_GE(rank, prev) << "G=" << g << " slot=" << s;
            prev = rank;
        }
        EXPECT_EQ(prev, g - 1);
    }
}

TEST(SlotHashTest, RankOwnedSlotsIntervalMatchesFormula) {
    // 不变量③（上取整对偶）：段 == [ceil(r*16384/G), ceil((r+1)*16384/G))。
    const auto ceil_div = [](uint32_t a, uint32_t b) {
        return (a + b - 1) / b;
    };
    for (uint32_t g : {1u, 2u, 3u, 5u, 16383u}) {
        for (uint32_t r = 0; r < g; ++r) {
            const auto slots = cvm::RankOwnedSlots(r, g);
            ASSERT_FALSE(slots.empty()) << "G=" << g << " rank=" << r;
            EXPECT_EQ(slots.front(), ceil_div(r * cvm::SlotCount(), g));
            EXPECT_EQ(slots.back(),
                      ceil_div((r + 1) * cvm::SlotCount(), g) - 1);
            EXPECT_EQ(slots.size(),
                      static_cast<size_t>(
                          ceil_div((r + 1) * cvm::SlotCount(), g)) -
                          static_cast<size_t>(
                              ceil_div(r * cvm::SlotCount(), g)));
        }
    }
}

TEST(SlotHashTest, RankOwnedSlotsPartitionAllSlots) {
    // 不变量④：所有 rank 的段并集 == [0,16384) 且互不相交（双射）。
    // G 不整除 16384 时段长不均（如 G=3 → 5462/5461/5461）。
    std::vector<uint16_t> expected;
    for (uint16_t s = 0; s < cvm::SlotCount(); ++s) {
        expected.push_back(s);
    }
    for (uint32_t g : {1u, 2u, 3u, 5u, 7u, 13u}) {
        std::vector<uint16_t> actual;
        size_t total = 0;
        for (uint32_t r = 0; r < g; ++r) {
            const auto slots = cvm::RankOwnedSlots(r, g);
            total += slots.size();
            actual.insert(actual.end(), slots.begin(), slots.end());
        }
        EXPECT_EQ(total, static_cast<size_t>(cvm::SlotCount())) << "G=" << g;
        EXPECT_EQ(actual, expected) << "G=" << g;
    }
}

TEST(SlotHashTest, SlotToRankInverseOfRankOwnedSlots) {
    // 不变量⑤（双射反向全量）：段内所有 slot 的 rank 均为 r。正向性质
    // 「s ∈ seg(SlotToRank(s))」由本断言 + 不变量④（每 slot 恰属一段）
    // 联合推出，无需单独 spot-check。
    for (uint32_t g : {2u, 3u, 5u, 7u}) {
        for (uint32_t r = 0; r < g; ++r) {
            for (uint16_t s : cvm::RankOwnedSlots(r, g)) {
                EXPECT_EQ(cvm::SlotToRank(s, g), r)
                    << "G=" << g << " slot=" << s << " rank=" << r;
            }
        }
    }
}

TEST(SlotHashTest, RankOwnedSlotsDefensiveInputs) {
    // 防御：G=0、rank 越界 → 空。G=16384 时每 rank 恰 1 slot。
    EXPECT_TRUE(cvm::RankOwnedSlots(0, 0u).empty());
    EXPECT_TRUE(cvm::RankOwnedSlots(1, 1u).empty());
    const auto single = cvm::RankOwnedSlots(16383u, 16384u);
    EXPECT_EQ(single.size(), static_cast<size_t>(1));
    EXPECT_EQ(single.front(), 16383u);
}

TEST(SlotHashTest, SlotToRankArithmeticNoOverflow) {
    // 溢出边界：16383 * 16384 == 2^28，uint32_t 中间乘积不得截断。
    EXPECT_EQ(cvm::SlotToRank(16383, 16384u), 16383u);
    EXPECT_EQ(cvm::SlotToRank(16382, 16384u), 16382u);
    // G=16385（超域）：纯函数不校验，仅保证不 crash、结果有界可解释。
    const uint32_t r = cvm::SlotToRank(16383, 16385u);
    EXPECT_LT(r, 16385u);
}

TEST(SlotHashTest, SlotToRankNoCrackAtSegmentBoundary) {
    // 回归：G 不整除 16384 时的段界裂缝（floor/floor 不对偶）。
    // 旧实现（段首 floor）会让 slot=5461 落在 rank1 段内但
    // SlotToRank 返回 0——归属裂缝即环错位的另一种形式，必须为零。
    struct Case {
        uint32_t g;
        uint16_t boundary_slot;  // 恰在 rank r 段首的 slot
    };
    const std::vector<Case> cases = {
        {3u, 5462}, {3u, 10923}, {5u, 3277}, {5u, 6554}, {7u, 2341}};
    for (const auto& c : cases) {
        const uint32_t rank = cvm::SlotToRank(c.boundary_slot, c.g);
        const auto slots = cvm::RankOwnedSlots(rank, c.g);
        EXPECT_NE(std::find(slots.begin(), slots.end(), c.boundary_slot),
                  slots.end())
            << "G=" << c.g << " slot=" << c.boundary_slot;
        // 相邻 slot（段首-1）必须属于前一 rank。
        EXPECT_EQ(cvm::SlotToRank(static_cast<uint16_t>(c.boundary_slot - 1),
                                  c.g),
                  rank - 1)
            << "G=" << c.g << " slot=" << c.boundary_slot - 1;
    }
}

}  // namespace
}  // namespace mooncake::test
