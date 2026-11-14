#pragma once

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <system_error>
#include <vector>

#include "crc32c.h"
#include "cvm/cvm_types.h"
#include "tenant_id.h"

namespace mooncake {
namespace cvm {

// Number of logical slots. The default is aligned with Redis Cluster
// (16384 == 2^14), while smaller power-of-two values can be selected before
// process startup with MC_STORE_PARTITION_SLOT_COUNT. Every master and client
// in a cluster must use the same value. Changing it requires a fresh cluster
// namespace because it changes key hashing and partition ownership.
constexpr uint16_t kDefaultSlotCount = 16384;
constexpr char kSlotCountEnv[] = "MC_STORE_PARTITION_SLOT_COUNT";

inline bool ParseSlotCount(const char* value, uint16_t* slot_count) {
    if (value == nullptr || *value == '\0' || slot_count == nullptr) {
        return false;
    }
    uint32_t parsed = 0;
    const char* end = value + std::strlen(value);
    const auto [ptr, error] = std::from_chars(value, end, parsed);
    if (error != std::errc{} || ptr != end || parsed == 0 ||
        parsed > kDefaultSlotCount || (parsed & (parsed - 1)) != 0) {
        return false;
    }
    *slot_count = static_cast<uint16_t>(parsed);
    return true;
}

inline uint16_t SlotCount() {
    static const uint16_t slot_count = [] {
        const char* value = std::getenv(kSlotCountEnv);
        if (value == nullptr || *value == '\0') {
            return kDefaultSlotCount;
        }
        uint16_t parsed = 0;
        if (!ParseSlotCount(value, &parsed)) {
            std::fprintf(stderr,
                         "Invalid %s=%s: expected a power of two in [1, %u]\n",
                         kSlotCountEnv, value,
                         static_cast<unsigned>(kDefaultSlotCount));
            std::abort();
        }
        return parsed;
    }();
    return slot_count;
}

// Number of virtual nodes per master on the consistent-hash ring used for
// dynamic slot ownership. More vnodes yield a more balanced distribution at
// the cost of a larger ring to build/sort on each heartbeat. 128 keeps load
// skew low (~1%) while remaining cheap to recompute.
constexpr uint16_t kVnodeCount = 128;

// Maps a stable hash value to a slot by taking the low log2(SlotCount()) bits.
// This is equivalent to `hash % SlotCount()` but cheaper.
inline uint16_t SlotOf(uint32_t hash) {
    return static_cast<uint16_t>(hash & (SlotCount() - 1));
}

// Computes the logical slot for a tenant-scoped key.
//
// - Default tenant: slot = hash(user_key).
// - Non-default tenant: slot = hash(tenant + '\0' + user_key), so that keys
//   from different tenants are isolated while remaining stable across
//   processes/compilers (unlike std::hash).
inline uint16_t KeySlot(const TenantId& tenant, const std::string& user_key) {
    Crc32c crc;
    if (!tenant.IsDefault()) {
        crc.Extend(tenant.value().data(), tenant.value().size());
        constexpr char kSeparator = '\0';
        crc.Extend(&kSeparator, 1);
    }
    crc.Extend(user_key.data(), user_key.size());
    return SlotOf(crc.Final());
}

// Position of a master's virtual node on the consistent-hash ring, in
// [0, SlotCount()). Placement depends only on (master_id, vnode_index), so it
// is deterministic across processes/compilers and independent of the current
// master set. The vnode index is encoded as fixed little-endian bytes so the
// hash is stable regardless of host endianness.
inline uint16_t VNodePosition(const std::string& master_id, uint16_t vnode) {
    Crc32c crc;
    crc.Extend(master_id.data(), master_id.size());
    const uint8_t vnode_bytes[2] = {
        static_cast<uint8_t>(vnode & 0xFFu),
        static_cast<uint8_t>((vnode >> 8) & 0xFFu),
    };
    crc.Extend(reinterpret_cast<const char*>(vnode_bytes),
               sizeof(vnode_bytes));
    return SlotOf(crc.Final());
}

// 一致性哈希环分配：给定去重后的 primary master_id 列表 ids 与本机
// master_id，返回本机应拥有的 slot 集合。每个 primary 在环上放置
// kVnodeCount 个虚拟节点，slot 归属「顺时针最近的虚拟节点」。ids 应为
// 排序去重后的 primary 列表，且必须包含 master_id。
//
// 虚拟节点位置只依赖 (master_id, vnode_index)，因此 primary 增删时仅该
// primary 虚拟节点覆盖的 slot（约 1/n）发生迁移，其余 primary 的 slot 保持
// 不变，避免 naive 均分导致的「全员 slot 平移」。
inline std::vector<uint16_t> ResolveOwnedSlotsOnRing(
    const std::vector<std::string>& ids, const std::string& master_id) {
    const size_t n = ids.size();
    if (n <= 1) {
        std::vector<uint16_t> slots;
        slots.reserve(SlotCount());
        for (uint16_t s = 0; s < SlotCount(); ++s) {
            slots.push_back(s);
        }
        return slots;
    }

    struct VNode {
        uint16_t position;
        size_t owner_index;  // 指向 ids
    };
    std::vector<VNode> ring;
    ring.reserve(n * kVnodeCount);
    for (size_t i = 0; i < n; ++i) {
        for (uint16_t v = 0; v < kVnodeCount; ++v) {
            ring.push_back({VNodePosition(ids[i], v), i});
        }
    }
    // 稳定排序（position 相同按 owner_index）保证跨进程结果一致。
    std::sort(ring.begin(), ring.end(), [](const VNode& a, const VNode& b) {
        if (a.position != b.position) {
            return a.position < b.position;
        }
        return a.owner_index < b.owner_index;
    });

    std::vector<uint16_t> slots;
    slots.reserve(SlotCount() / n + 1);
    for (uint16_t s = 0; s < SlotCount(); ++s) {
        // 环上第一个 position >= s 的虚拟节点（越界则环绕到 ring[0]）。
        auto it = std::lower_bound(
            ring.begin(), ring.end(), s,
            [](const VNode& vn, uint16_t value) { return vn.position < value; });
        if (it == ring.end()) {
            it = ring.begin();
        }
        if (ids[it->owner_index] == master_id) {
            slots.push_back(s);
        }
    }
    return slots;
}

// 一致性哈希环反查：给定去重排序后的 primary master_id 列表 ids 与单个
// slot，返回该 slot 的 owner master_id。规则与 ResolveOwnedSlotsOnRing 一致
// （slot 归属顺时针最近的虚拟节点）。ids 为空返回空串，n == 1 直接返回
// 唯一成员（单主全量接管）。供读路径转发（非 owner → slot owner）使用。
inline std::string ResolveSlotOwnerOnRing(const std::vector<std::string>& ids,
                                          uint16_t slot) {
    const size_t n = ids.size();
    if (n == 0) {
        return {};
    }
    if (n == 1) {
        return ids[0];
    }

    struct VNode {
        uint16_t position;
        size_t owner_index;  // 指向 ids
    };
    std::vector<VNode> ring;
    ring.reserve(n * kVnodeCount);
    for (size_t i = 0; i < n; ++i) {
        for (uint16_t v = 0; v < kVnodeCount; ++v) {
            ring.push_back({VNodePosition(ids[i], v), i});
        }
    }
    // 稳定排序（position 相同按 owner_index）保证跨进程结果一致。
    std::sort(ring.begin(), ring.end(), [](const VNode& a, const VNode& b) {
        if (a.position != b.position) {
            return a.position < b.position;
        }
        return a.owner_index < b.owner_index;
    });

    auto it = std::lower_bound(
        ring.begin(), ring.end(), slot,
        [](const VNode& vn, uint16_t value) { return vn.position < value; });
    if (it == ring.end()) {
        it = ring.begin();
    }
    return ids[it->owner_index];
}

// ---------------------------------------------------------------------------
// RingSlot 槽位组（§16.3/§16.5）：slot → rank 连续段纯函数
//
// slot 归属推导链（P3 起）：
//   slot → rank  = SlotToRank(slot, G)   确定性纯函数，不持久化
//   rank → owner = ring_slots[rank]      稳定显式状态，持久化 O(G)
//
// G（槽位组数）在集群创建时设定、运行期恒不变（变更属冷操作，§16.8 前提）。
// 所有调用点必须使用同一个 G（从 RingMeta.slot_group_count 读取后传递），
// 服务端与客户端对同一输入必须推导出字节级一致的结果。
// ---------------------------------------------------------------------------

// 将 slot 映射到槽位组 rank ∈ [0, group_count)。
//
// 连续段划分（rank 单调、无空洞、无重叠、全覆盖）：
//   SlotToRank(s, G) = floor(s * G / S), S = SlotCount()
//   rank r 独占 s ∈ [ceil(r*S/G), ceil((r+1)*S/G))
// 两式的整除方向是对偶的（一向下、一向上），G 不整除 S 时也只有这一
// 组合能保证互逆，双向 floor 会
// 在段界 slot 上出现「SlotToRank(s)=r-1 但 s ∈ RankOwnedSlots(r)」的
// 归属裂缝。G=1 退化为单 primary 全量（与 §15 单主等价，回滚保证）。
inline uint32_t SlotToRank(uint16_t slot, uint32_t group_count) {
    // 先提升到 32 位再乘；slot_count 和 group_count 均不超过 16384，
    // 最大乘积小于 2^28，uint32_t 足够，
    // 但 uint16 直接乘可能截断，必须显式 cast。
    const uint32_t slot_count = SlotCount();
    return static_cast<uint32_t>(slot) * group_count / slot_count;
}

// 返回 rank 所拥有的 slot 段（升序、连续），即
// [ceil(rank*S/G), ceil((rank+1)*S/G))，S = SlotCount()。
//
// 防御：group_count == 0 或 rank >= group_count 返回空（正常不应发生）。
// 循环变量使用 uint32_t，避免中间算术发生 uint16_t 截断。
inline std::vector<uint16_t> RankOwnedSlots(uint32_t rank,
                                            uint32_t group_count) {
    if (group_count == 0 || rank >= group_count) {
        return {};
    }
    // 上取整对偶（见 SlotToRank 注释）：ceil(x/G) = (x + G - 1) / G。
    // rank < G <= S <= 16384，所以下面的 uint32_t 中间值无溢出。
    const uint32_t slot_count = SlotCount();
    const uint32_t first =
        (rank * slot_count + group_count - 1) / group_count;  // 段首（含）
    const uint32_t last = ((rank + 1) * slot_count + group_count - 1) /
                          group_count;  // 段尾（不含）
    std::vector<uint16_t> slots;
    slots.reserve(last - first);
    for (uint32_t s = first; s < last; ++s) {
        slots.push_back(static_cast<uint16_t>(s));
    }
    return slots;
}

// ---------------------------------------------------------------------------
// 统一 slot→owner 推导（§16.17.2）：新旧模型门控的唯一实现
//
// 「ring_slots 显式归属优先、缓存空回退 §15 环推导」这条策略曾有过多处
// 手写副本（客户端 PartitionRouter / master vsegment 路由 / master 归属
// 位图），任一处漏改即 manager 与路由大面积错位（STALE_ROUTE 刷屏）。
// 故收拢至此：新增路由消费点必须复用，禁止手写门控。
// ---------------------------------------------------------------------------

// ring_slots → rank→primary 查找表（建表形式，客户端 PartitionRouter 持有）：
// rank >= group_count 或 primary 为空的表项留空（无路由）。assigns 为空或
// group_count == 0（模型未启用）返回空表，调用方走 §15 回退。
inline std::vector<std::string> BuildRankToPrimary(
    const std::vector<RingSlotAssign>& assigns, uint32_t group_count) {
    if (group_count == 0) {
        return {};
    }
    std::vector<std::string> table(group_count);
    for (const auto& a : assigns) {
        if (a.rank < group_count && !a.primary_id.empty()) {
            table[a.rank] = a.primary_id;
        }
    }
    return table;
}

// slot → owner master_id（点查形式，master 侧逐 partition 解析）：
// ring_slots 启用时 SlotToRank 定位 rank、取其 primary（不区分 state：
// kMigrating 期间 primary_id 仍指迁移源，§16.16 阶段 1）；rank 缺失或
// primary 为空返回空串（无路由——客户端退避 / master 报 INVALID_VERSION，
// 空值语义由调用方各自处理）。模型未启用回退 §15 环推导。
inline std::string ResolveSlotOwnerUnified(
    const std::vector<RingSlotAssign>& assigns, uint32_t group_count,
    const std::vector<std::string>& legacy_primary_ids, uint16_t slot) {
    if (!assigns.empty() && group_count > 0) {
        const uint32_t rank = SlotToRank(slot, group_count);
        for (const auto& a : assigns) {
            if (a.rank == rank) {
                return a.primary_id;
            }
        }
        return {};
    }
    return ResolveSlotOwnerOnRing(legacy_primary_ids, slot);
}

}  // namespace cvm
}  // namespace mooncake
