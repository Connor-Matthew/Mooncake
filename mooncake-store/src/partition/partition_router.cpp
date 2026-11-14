#include "partition/partition_router.h"

#include <algorithm>
#include <set>
#include <utility>

#include <glog/logging.h>

#include "cvm/cvm_keys.h"
#include "cvm/etcd_view_store.h"
#include "cvm/slot_hash.h"
#include "etcd_helper.h"
#include "partition/kv_hash_map.h"

namespace mooncake {
namespace partition {

void PartitionRouter::LoadSlotOwners(
    const std::vector<cvm::SlotOwner>& owners) {
    std::unordered_map<uint16_t, std::string> next;
    next.reserve(owners.size());
    for (const auto& owner : owners) {
        if (!owner.primary_master_id.empty() &&
            owner.state == static_cast<int32_t>(cvm::SlotState::kStable)) {
            next[owner.slot] = owner.primary_master_id;
        }
    }

    const size_t valid = next.size();
    {
        SharedMutexLocker locker(&mutex_);
        // 显式逐 slot 加载属测试/旧路径：覆盖 rank 表，保证 ResolveSubmaster
        // 走 slot_to_submaster_ 兜底分支。
        group_count_ = 1;
        rank_to_primary_.clear();
        slot_to_submaster_ = std::move(next);
    }
    LOG(INFO) << "PartitionRouter loaded " << valid << " slot->submaster"
              << " entries (input " << owners.size() << " SlotOwner records)";
}

ErrorCode PartitionRouter::LoadFromEtcdSnapshot(
    const std::string& cluster_namespace) {
    // cluster_meta：G（slot_group_count）+ 回退用 submaster_count。
    cvm::RingMeta meta;
    ViewVersionId meta_revision = 0;
    ErrorCode err = cvm::EtcdViewStore::LoadClusterMeta(
        cluster_namespace, meta, meta_revision);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "PartitionRouter load cluster_meta failed: err=" << err
                     << " (cluster may not have published ring config yet)";
        return err;
    }
    if (meta.slot_count != cvm::SlotCount()) {
        LOG(ERROR) << "PartitionRouter slot count mismatch: cluster="
                   << meta.slot_count << ", local=" << cvm::SlotCount()
                   << ", env=" << cvm::kSlotCountEnv
                   << ". Use the same value on every master and client.";
        return ErrorCode::INVALID_PARAMS;
    }

    // ---- RingSlot 路由（§16.17.3）：ring_slots → rank 表 ----
    // G 防御 clamp：旧数据 slot_group_count 缺省 1（YLT_REFL 默认值）。
    const uint32_t group_count =
        std::max<uint32_t>(1, meta.slot_group_count);
    if (group_count > cvm::SlotCount()) {
        LOG(ERROR) << "PartitionRouter invalid ring metadata: groups="
                   << group_count << ", slot_count=" << cvm::SlotCount();
        return ErrorCode::INVALID_PARAMS;
    }

    std::vector<cvm::RingSlotAssign> assigns;
    ViewVersionId assigns_revision = 0;
    err = cvm::EtcdViewStore::LoadAllRingSlotAssigns(
        cluster_namespace, assigns, assigns_revision);
    if (err == ErrorCode::OK && !assigns.empty()) {
        // rank → primary 查找表：统一推导建表（§16.17.2
        // BuildRankToPrimary，与 master 侧 ResolveSlotOwnerUnified 同源，
        // 不区分 state：kMigrating 期间 primary_id 仍指源 A，读路由仍走
        // 源，§16.16 阶段 1）。缺失/空 primary 的 rank 无路由
        //（ResolveSubmaster → nullopt → 客户端退避）。
        std::vector<std::string> rank_to_primary =
            cvm::BuildRankToPrimary(assigns, group_count);
        size_t filled = 0;
        for (const auto& p : rank_to_primary) {
            if (!p.empty()) {
                ++filled;
            }
        }

        {
            SharedMutexLocker locker(&mutex_);
            group_count_ = group_count;
            rank_to_primary_ = std::move(rank_to_primary);
            slot_to_submaster_.clear();
        }
        LOG(INFO) << "PartitionRouter loaded ring_slots route: groups="
                  << group_count << ", filled_ranks=" << filled
                  << " (from " << assigns.size() << " ring_slots records)";
        return ErrorCode::OK;
    }
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "PartitionRouter load ring_slots failed: err=" << err
                     << ", falling back to local ring derivation";
    }

    // ---- 回退：本地建环（§16.17.6 新旧混跑，ring_slots 空的旧服务端）----
    // 读成员列表，用与服务端完全一致的确定性算法推导 slot → master_id。
    std::vector<cvm::MasterRegistration> members;
    ViewVersionId members_revision = 0;
    err = cvm::EtcdViewStore::LoadAllMasters(cluster_namespace, members,
                                             members_revision);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "PartitionRouter load masters failed: err=" << err;
        return err;
    }

    // 先到先得排序（by create_revision）→ 取前 submaster_count 作为 primary_ids。
    // 与服务端 ResolveOwnedSlotsForCvm 完全一致的推导规则（不依赖 role）。
    std::sort(members.begin(), members.end(), cvm::MasterRegistrationRankLess);
    std::vector<std::string> ids;
    ids.reserve(members.size());
    for (const auto& m : members) {
        if (!m.master_id.empty()) {
            ids.push_back(m.master_id);
        }
    }
    const uint32_t submaster_count = meta.submaster_count;
    if (submaster_count > 0 && ids.size() > submaster_count) {
        ids.resize(submaster_count);
    }

    std::unordered_map<uint16_t, std::string> next;
    next.reserve(cvm::SlotCount());
    for (uint16_t s = 0; s < cvm::SlotCount(); ++s) {
        std::string owner = cvm::ResolveSlotOwnerOnRing(ids, s);
        if (!owner.empty()) {
            next[s] = std::move(owner);
        }
    }

    const size_t valid = next.size();
    {
        SharedMutexLocker locker(&mutex_);
        group_count_ = 1;
        rank_to_primary_.clear();
        slot_to_submaster_ = std::move(next);
    }
    LOG(INFO) << "PartitionRouter derived " << valid
              << " slot->submaster entries locally (primaries=" << ids.size()
              << ", submaster_count=" << submaster_count
              << ", ring_slots empty, legacy fallback)";
    return ErrorCode::OK;
}

std::optional<std::string> PartitionRouter::ResolveSubmaster(
    uint16_t slot) const {
    SharedMutexLocker locker(&mutex_, shared_lock);
    // ring_slots rank 表优先（§16.17.2）：SlotToRank 纯函数定位，O(1)。
    // rank 越界 / 表项空 → nullopt（客户端 SLOT_NOT_OWNED 退避）。
    if (!rank_to_primary_.empty()) {
        const uint32_t rank = cvm::SlotToRank(slot, group_count_);
        if (rank >= rank_to_primary_.size() ||
            rank_to_primary_[rank].empty()) {
            LOG(WARNING) << "PartitionRouter no submaster for slot " << slot
                         << " (rank " << rank << " unrouted)";
            return std::nullopt;
        }
        return rank_to_primary_[rank];
    }
    auto it = slot_to_submaster_.find(slot);
    if (it == slot_to_submaster_.end()) {
        LOG(WARNING) << "PartitionRouter no submaster for slot " << slot;
        return std::nullopt;
    }
    return it->second;
}

std::optional<std::string> PartitionRouter::ResolveSubmasterQuiet(
    uint16_t slot) const {
    SharedMutexLocker locker(&mutex_, shared_lock);
    if (!rank_to_primary_.empty()) {
        const uint32_t rank = cvm::SlotToRank(slot, group_count_);
        if (rank >= rank_to_primary_.size() ||
            rank_to_primary_[rank].empty()) {
            return std::nullopt;
        }
        return rank_to_primary_[rank];
    }
    auto it = slot_to_submaster_.find(slot);
    if (it == slot_to_submaster_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::optional<std::string> PartitionRouter::Route(
    const TenantId& tenant, const std::string& key) const {
    return ResolveSubmaster(KvHashMap::Compute(tenant, key));
}

std::vector<std::string> PartitionRouter::GetAllPrimaryAddresses() const {
    SharedMutexLocker locker(&mutex_, shared_lock);
    std::vector<std::string> addresses;
    std::set<std::string> seen;
    const auto collect = [&](const std::string& address) {
        if (!address.empty() && seen.insert(address).second) {
            addresses.push_back(address);
        }
    };
    if (!rank_to_primary_.empty()) {
        for (const auto& primary : rank_to_primary_) {
            collect(primary);
        }
    } else {
        for (const auto& [slot, submaster] : slot_to_submaster_) {
            collect(submaster);
        }
    }
    return addresses;
}

void PartitionRouter::Clear() {
    size_t old_size = 0;
    {
        SharedMutexLocker locker(&mutex_);
        old_size = rank_to_primary_.size() + slot_to_submaster_.size();
        group_count_ = 1;
        rank_to_primary_.clear();
        slot_to_submaster_.clear();
    }
    LOG(INFO) << "PartitionRouter cleared " << old_size << " entries";
}

size_t PartitionRouter::Size() const {
    SharedMutexLocker locker(&mutex_, shared_lock);
    // rank 表模式下度量「有路由的 rank 数」（非 16384 条 slot 表的条数，
    // §16.20 单主判定 Size()==0 语义不变：全 rank 空表才为 0）。
    if (!rank_to_primary_.empty()) {
        size_t routed = 0;
        for (const auto& p : rank_to_primary_) {
            if (!p.empty()) {
                ++routed;
            }
        }
        return routed;
    }
    return slot_to_submaster_.size();
}

}  // namespace partition
}  // namespace mooncake
