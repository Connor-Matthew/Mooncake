#include "cvm/etcd_view_store.h"

#include <sstream>
#include <utility>
#include <vector>

#include <glog/logging.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "cvm/cvm_keys.h"
#include "etcd_helper.h"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"

namespace mooncake {
namespace cvm {

namespace {

// Parses the JSON array returned by EtcdHelper::GetRangeAsJson, which has the
// form [{"key":"...","value":"..."}, ...]. Also parses the optional etcd key
// create_revision field (defaults to 0), used by「先到先得」ordering
// (MasterRegistrationRankLess).
struct RangeKvWithRevision {
    std::string key;
    std::string value;
    int64_t create_revision{0};
};

ErrorCode ParseRangeJson(const std::string& json,
                         std::vector<RangeKvWithRevision>& kvs) {
    Json::Value root;
    Json::CharReaderBuilder reader;
    std::string errors;
    std::istringstream stream(json);
    if (!Json::parseFromStream(reader, stream, &root, &errors) ||
        !root.isArray()) {
        LOG(ERROR) << "Failed to parse etcd range JSON: " << errors;
        return ErrorCode::INTERNAL_ERROR;
    }

    kvs.clear();
    kvs.reserve(root.size());
    for (const auto& item : root) {
        if (!item.isObject() || !item["key"].isString() ||
            !item["value"].isString()) {
            return ErrorCode::INTERNAL_ERROR;
        }
        RangeKvWithRevision kv;
        kv.key = item["key"].asString();
        kv.value = item["value"].asString();
        if (item.isMember("create_revision") && item["create_revision"].isNumeric()) {
            kv.create_revision = item["create_revision"].asInt64();
        }
        kvs.push_back(std::move(kv));
    }
    return ErrorCode::OK;
}

}  // namespace

// ---- JSON serialization ----

ErrorCode EtcdViewStore::SerializeMasterRegistration(const MasterRegistration& reg,
                                                     std::string& out) {
    try {
        struct_json::to_json(reg, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeMasterRegistration failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeMasterRegistration(const std::string& in,
                                                       MasterRegistration& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeMasterRegistration failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SerializeRingMeta(const RingMeta& meta,
                                           std::string& out) {
    try {
        struct_json::to_json(meta, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeRingMeta failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeRingMeta(const std::string& in,
                                             RingMeta& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeRingMeta failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SerializePartitionRoute(
    const partition::PartitionRoute& route, std::string& out) {
    try {
        struct_json::to_json(route, out);
        return ErrorCode::OK;
    } catch (...) {
        return ErrorCode::SERIALIZE_FAIL;
    }
}

ErrorCode EtcdViewStore::DeserializePartitionRoute(
    const std::string& in, partition::PartitionRoute& out) {
    try {
        struct_json::from_json(out, in);
        return ErrorCode::OK;
    } catch (...) {
        return ErrorCode::DESERIALIZE_FAIL;
    }
}

ErrorCode EtcdViewStore::SavePartitionRoute(
    const std::string& ns, const partition::PartitionRoute& route) {
    if (route.partition_id.partition_id.empty() ||
        route.owner_submaster_id.empty() ||
        route.route_epoch == 0)
        return ErrorCode::INVALID_PARAMS;
    std::string value;
    auto error = SerializePartitionRoute(route, value);
    if (error != ErrorCode::OK) return error;
    const auto key =
        PartitionRouteKey(ns, route.partition_id.partition_id);
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdViewStore::LoadPartitionRoute(
    const std::string& ns, const std::string& partition_id,
    partition::PartitionRoute& out, ViewVersionId& version) {
    const auto key = PartitionRouteKey(ns, partition_id);
    std::string value;
    auto error = EtcdHelper::Get(key.data(), key.size(), value, version);
    return error == ErrorCode::OK ? DeserializePartitionRoute(value, out)
                                  : error;
}

ErrorCode EtcdViewStore::CASSwitchPartitionOwner(
    const std::string& ns, const std::string& partition_id,
    uint64_t expected_epoch, const std::string& new_owner,
    partition::PartitionState state, const std::string& target,
    partition::PartitionRoute& out) {
    partition::PartitionRoute current;
    ViewVersionId version = 0;
    auto error = LoadPartitionRoute(ns, partition_id, current, version);
    if (error != ErrorCode::OK) return error;
    if (current.route_epoch != expected_epoch) return ErrorCode::STALE_ROUTE;
    std::string old_value;
    SerializePartitionRoute(current, old_value);
    out = {{partition_id}, new_owner, expected_epoch + 1,
           static_cast<int32_t>(state), target};
    std::string new_value;
    SerializePartitionRoute(out, new_value);
    const auto key = PartitionRouteKey(ns, partition_id);
    error = EtcdHelper::TxnCompareAndPut(
        {{key, EtcdHelper::TxnCompareKind::kValueEquals, old_value}},
        {{key, new_value}});
    return error == ErrorCode::ETCD_TRANSACTION_FAIL ? ErrorCode::STALE_ROUTE
                                                      : error;
}


// ---- Segment neutral entity (segments/{segment_id}) ----

ErrorCode EtcdViewStore::SerializeSegmentDescriptor(
    const SegmentDescriptor& desc, std::string& out) {
    try {
        struct_json::to_json(desc, out);
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeSegmentDescriptor failed: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode EtcdViewStore::DeserializeSegmentDescriptor(
    const std::string& in, SegmentDescriptor& out) {
    try {
        struct_json::from_json(out, in);
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeSegmentDescriptor failed: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode EtcdViewStore::SaveSegmentDescriptor(
    const std::string& cluster_namespace, const SegmentDescriptor& desc) {
    const std::string key =
        SegmentNeutralEntityKey(cluster_namespace, desc.segment_id);
    std::string value;
    ErrorCode err = SerializeSegmentDescriptor(desc, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdViewStore::DeleteSegmentDescriptor(
    const std::string& cluster_namespace, const std::string& segment_id) {
    const std::string key =
        SegmentNeutralEntityKey(cluster_namespace, segment_id);
    const std::string end = PrefixEnd(key);
    return EtcdHelper::DeleteRange(key.data(), key.size(), end.data(),
                                   end.size());
}

ErrorCode EtcdViewStore::LoadAllSegmentDescriptors(
    const std::string& cluster_namespace,
    std::vector<SegmentDescriptor>& out, ViewVersionId& version) {
    out.clear();
    const std::string prefix = SegmentNeutralEntityPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<RangeKvWithRevision> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        SegmentDescriptor desc;
        err = DeserializeSegmentDescriptor(kv.value, desc);
        if (err != ErrorCode::OK) {
            return err;
        }
        out.push_back(std::move(desc));
    }
    return ErrorCode::OK;
}

// ---- Per-master segment mount (snapshot/{id}/segments/{seg}) ----

ErrorCode EtcdViewStore::SerializeMountEntry(const MountEntry& entry,
                                             std::string& out) {
    try {
        struct_json::to_json(entry, out);
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeMountEntry failed: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode EtcdViewStore::DeserializeMountEntry(const std::string& in,
                                               MountEntry& out) {
    try {
        struct_json::from_json(out, in);
        return ErrorCode::OK;
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeMountEntry failed: " << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

ErrorCode EtcdViewStore::SaveMountEntryWithLease(
    const std::string& cluster_namespace, const std::string& master_id,
    const MountEntry& entry, EtcdLeaseId lease_id) {
    const std::string key =
        SnapshotSegmentMountKey(cluster_namespace, master_id, entry.segment_id);
    std::string value;
    ErrorCode err = SerializeMountEntry(entry, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::DeleteMountEntry(
    const std::string& cluster_namespace, const std::string& master_id,
    const std::string& segment_id) {
    const std::string key =
        SnapshotSegmentMountKey(cluster_namespace, master_id, segment_id);
    const std::string end = PrefixEnd(key);
    return EtcdHelper::DeleteRange(key.data(), key.size(), end.data(),
                                   end.size());
}

ErrorCode EtcdViewStore::LoadAllMountEntries(
    const std::string& cluster_namespace,
    std::vector<std::pair<std::string, MountEntry>>& out,
    ViewVersionId& version) {
    out.clear();
    const std::string prefix = SnapshotPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<RangeKvWithRevision> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    // key = snapshot/{master_id}/segments/{segment_id}. Skip any other
    // snapshot subkey (e.g. future reg/oplog) that may appear in the scan.
    constexpr char kSegmentsMarker[] = "/segments/";
    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        const std::size_t pos = kv.key.rfind(kSegmentsMarker);
        if (pos == std::string::npos || pos <= prefix.size()) {
            continue;
        }
        MountEntry entry;
        err = DeserializeMountEntry(kv.value, entry);
        if (err != ErrorCode::OK) {
            return err;
        }
        const std::string master_id =
            kv.key.substr(prefix.size(), pos - prefix.size());
        out.emplace_back(master_id, std::move(entry));
    }
    return ErrorCode::OK;
}

// ---- Master registration ----

ErrorCode EtcdViewStore::RegisterMaster(const std::string& cluster_namespace,
                                        const MasterRegistration& reg,
                                        EtcdLeaseId lease_id) {
    const std::string key =
        MasterRegistrationKey(cluster_namespace, reg.master_id);
    std::string value;
    ErrorCode err = SerializeMasterRegistration(reg, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::UpdateMasterRole(const std::string& cluster_namespace,
                                          const std::string& master_id,
                                          MasterRole role,
                                          EtcdLeaseId lease_id) {
    const std::string key = MasterRegistrationKey(cluster_namespace, master_id);

    MasterRegistration reg;
    ViewVersionId version = 0;
    std::string existing;
    ErrorCode err =
        EtcdHelper::Get(key.data(), key.size(), existing, version);
    if (err == ErrorCode::OK) {
        err = DeserializeMasterRegistration(existing, reg);
        if (err != ErrorCode::OK) {
            return err;
        }
    }
    // On read failure (e.g. key missing) fall back to a minimal registration;
    // the caller only flips role on a previously-registered master, so this
    // path is defensive and preserves liveness via the lease.
    reg.master_id = master_id;
    reg.role = static_cast<int32_t>(role);

    std::string value;
    err = SerializeMasterRegistration(reg, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::PutWithLease(key.data(), key.size(), value.data(),
                                    value.size(), lease_id);
}

ErrorCode EtcdViewStore::LoadAllMasters(
    const std::string& cluster_namespace, std::vector<MasterRegistration>& out,
    ViewVersionId& version) {
    out.clear();
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<RangeKvWithRevision> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        MasterRegistration reg;
        err = DeserializeMasterRegistration(kv.value, reg);
        if (err != ErrorCode::OK) {
            return err;
        }
        reg.create_revision = kv.create_revision;
        out.push_back(std::move(reg));
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::SaveClusterMeta(const std::string& cluster_namespace,
                                         const RingMeta& meta) {
    const std::string key = ClusterMetaKey(cluster_namespace);
    std::string value;
    ErrorCode err = SerializeRingMeta(meta, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    return EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
}

ErrorCode EtcdViewStore::LoadClusterMeta(const std::string& cluster_namespace,
                                         RingMeta& out,
                                         ViewVersionId& version) {
    const std::string key = ClusterMetaKey(cluster_namespace);
    std::string value;
    ErrorCode err = EtcdHelper::Get(key.data(), key.size(), value, version);
    if (err != ErrorCode::OK) {
        return err;
    }
    return DeserializeRingMeta(value, out);
}

// ---- RingSlot 槽位组归属（ring_slots/{rank}）----

ErrorCode EtcdViewStore::SerializeRingSlotAssign(const RingSlotAssign& assign,
                                                  std::string& out) {
    try {
        struct_json::to_json(assign, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeRingSlotAssign failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::DeserializeRingSlotAssign(const std::string& in,
                                                   RingSlotAssign& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeRingSlotAssign failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::CreateRingSlotAssign(
    const std::string& cluster_namespace, const RingSlotAssign& assign) {
    if (assign.primary_id.empty() || assign.epoch == 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    std::string value;
    ErrorCode err = SerializeRingSlotAssign(assign, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    const std::string key = RingSlotAssignKey(cluster_namespace, assign.rank);
    return EtcdHelper::Create(key.data(), key.size(), value.data(),
                              value.size());
}

ErrorCode EtcdViewStore::LoadRingSlotAssign(
    const std::string& cluster_namespace, uint32_t rank, RingSlotAssign& out,
    ViewVersionId& version) {
    const std::string key = RingSlotAssignKey(cluster_namespace, rank);
    std::string value;
    ErrorCode err = EtcdHelper::Get(key.data(), key.size(), value, version);
    if (err != ErrorCode::OK) {
        return err;
    }
    err = DeserializeRingSlotAssign(value, out);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (out.rank != rank) {
        LOG(ERROR) << "ring_slots value rank=" << out.rank
                   << " mismatches key rank=" << rank << ", etcd corrupted";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::LoadAllRingSlotAssigns(
    const std::string& cluster_namespace,
    std::vector<RingSlotAssign>& out, ViewVersionId& version) {
    out.clear();
    const std::string prefix = RingSlotAssignPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<RangeKvWithRevision> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        RingSlotAssign assign;
        err = DeserializeRingSlotAssign(kv.value, assign);
        if (err != ErrorCode::OK) {
            return err;
        }
        uint32_t key_rank = 0;
        if (!ParseRankFromRingSlotKey(kv.key, prefix, key_rank) ||
            key_rank != assign.rank) {
            LOG(ERROR) << "ring_slots key '" << kv.key << "' rank mismatch, "
                       << "etcd corrupted";
            return ErrorCode::INTERNAL_ERROR;
        }
        out.push_back(std::move(assign));
    }
    return ErrorCode::OK;
}

ErrorCode EtcdViewStore::CASSwitchRingSlotOwner(
    const std::string& cluster_namespace, uint32_t rank,
    uint64_t expected_epoch, const std::string& new_primary_id,
    SlotState new_state, const std::string& migrating_to_id,
    RingSlotAssign& out, const std::vector<std::string>* new_standby_ids) {
    if (new_primary_id.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }

    RingSlotAssign current;
    ViewVersionId version = 0;
    ErrorCode err =
        LoadRingSlotAssign(cluster_namespace, rank, current, version);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (current.epoch != expected_epoch) {
        return ErrorCode::STALE_ROUTE;
    }

    std::string old_value;
    err = SerializeRingSlotAssign(current, old_value);
    if (err != ErrorCode::OK) {
        return err;
    }

    out = current;
    out.primary_id = new_primary_id;
    out.state = static_cast<int32_t>(new_state);
    out.migrating_to_id = migrating_to_id;
    out.epoch = expected_epoch + 1;
    if (new_standby_ids != nullptr) {
        out.standby_ids = *new_standby_ids;
    }

    std::string new_value;
    err = SerializeRingSlotAssign(out, new_value);
    if (err != ErrorCode::OK) {
        return err;
    }
    const std::string key = RingSlotAssignKey(cluster_namespace, rank);
    err = EtcdHelper::TxnCompareAndPut(
        {{key, EtcdHelper::TxnCompareKind::kValueEquals, old_value}},
        {{key, new_value}});
    return err == ErrorCode::ETCD_TRANSACTION_FAIL ? ErrorCode::STALE_ROUTE
                                                    : err;
}

ErrorCode EtcdViewStore::AdoptRankViaCAS(
    const std::string& cluster_namespace, uint32_t rank,
    uint64_t expected_epoch, const std::string& new_primary_id,
    SlotState new_state, const std::string& migrating_to_id,
    RingSlotAssign& out_assign, const std::vector<std::string>* new_standbys,
    const char* reason, const std::string& old_owner,
    bool success_as_warning, bool clear_intent) {
    // 归属推进组合原语（模板方法在调用方，此处是被复用的「过程骨架」）：
    // CAS 切换 + 可选幂等删 reshard_intent + 统一结构化日志。五处调用点
    //（晋升/补位接管/兼管自愈/driver 阶段1-2）的共性提取；竞争让位
    //（STALE）是正常结果打 INFO，防多 standby 并发时刷 WARNING。
    // clear_intent 见头文件：driver 流程内推进必须传 false（intent 是其
    // 断点续传依据），接管语义传 true（残留意图与新视图矛盾）。
    ErrorCode err = CASSwitchRingSlotOwner(
        cluster_namespace, rank, expected_epoch, new_primary_id, new_state,
        migrating_to_id, out_assign, new_standbys);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::STALE_ROUTE) {
            LOG(INFO) << "rank ownership CAS lost (concurrent winner): "
                         "rank="
                      << rank << ", reason=" << reason
                      << ", attempted_owner=" << new_primary_id;
        } else {
            LOG(WARNING) << "rank ownership CAS failed: rank=" << rank
                         << ", reason=" << reason << ", err=" << err;
        }
        return err;
    }

    // 可选 intent 清理（幂等，key 不存在视为 OK）。失败不阻断接管——
    // intent 随后由驱动路径（driver 重扫 / 晋升方）再次清理，记 WARNING
    // 供排查。
    if (clear_intent) {
        const ErrorCode del = DeleteReshardIntent(cluster_namespace, rank);
        if (del != ErrorCode::OK) {
            LOG(WARNING) << "rank ownership advanced but intent cleanup "
                            "failed: rank="
                         << rank << ", reason=" << reason << ", err=" << del
                         << " (will be retried by promotion paths)";
        }
    }

    const char* intent_note = clear_intent
                                  ? ", reshard_intent cleared (if any)"
                                  : ", reshard_intent kept (driver in progress)";
    if (success_as_warning) {
        LOG(WARNING) << "rank ownership advanced (" << reason
                     << "): rank=" << rank << ", old_owner=" << old_owner
                     << ", new_owner=" << out_assign.primary_id
                     << ", state="
                     << (new_state == SlotState::kMigrating ? "kMigrating"
                                                            : "kStable")
                     << (new_state == SlotState::kMigrating
                             ? ", migrating_to=" + migrating_to_id
                             : intent_note)
                     << ", new_epoch=" << out_assign.epoch;
    } else {
        LOG(INFO) << "rank ownership advanced (" << reason
                 << "): rank=" << rank << ", old_owner=" << old_owner
                 << ", new_owner=" << out_assign.primary_id
                 << ", state=" << (new_state == SlotState::kMigrating
                                       ? "kMigrating"
                                       : "kStable")
                 << (new_state == SlotState::kMigrating
                         ? ", migrating_to=" + migrating_to_id
                         : intent_note)
                 << ", new_epoch=" << out_assign.epoch;
    }
    return ErrorCode::OK;
}

// ---- Master membership watch ----

ErrorCode EtcdViewStore::WatchMasters(const std::string& cluster_namespace,
                                      ViewVersionId start_revision, void* ctx,
                                      WatchCallback cb) {
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    return EtcdHelper::WatchWithPrefixFromRevision(prefix.data(), prefix.size(),
                                                   start_revision, ctx, cb);
}

ErrorCode EtcdViewStore::CancelWatchMasters(
    const std::string& cluster_namespace) {
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    return EtcdHelper::CancelWatchWithPrefix(prefix.data(), prefix.size());
}

ErrorCode EtcdViewStore::WaitWatchMastersStopped(
    const std::string& cluster_namespace, int timeout_ms) {
    const std::string prefix = MasterRegistrationPrefix(cluster_namespace);
    return EtcdHelper::WaitWatchWithPrefixStopped(prefix.data(), prefix.size(),
                                                  timeout_ms);
}

// ---- RingSlot ownership watch ----

ErrorCode EtcdViewStore::WatchRingSlots(const std::string& cluster_namespace,
                                       ViewVersionId start_revision,
                                       void* ctx, WatchCallback cb) {
    const std::string prefix = RingSlotAssignPrefix(cluster_namespace);
    return EtcdHelper::WatchWithPrefixFromRevision(prefix.data(), prefix.size(),
                                                   start_revision, ctx, cb);
}

ErrorCode EtcdViewStore::CancelWatchRingSlots(
    const std::string& cluster_namespace) {
    const std::string prefix = RingSlotAssignPrefix(cluster_namespace);
    return EtcdHelper::CancelWatchWithPrefix(prefix.data(), prefix.size());
}

ErrorCode EtcdViewStore::WaitWatchRingSlotsStopped(
    const std::string& cluster_namespace, int timeout_ms) {
    const std::string prefix = RingSlotAssignPrefix(cluster_namespace);
    return EtcdHelper::WaitWatchWithPrefixStopped(prefix.data(), prefix.size(),
                                                  timeout_ms);
}

// ---- reshard 意图（reshard_intent/{rank}，§16.19.1）----

namespace {

ErrorCode SerializeReshardIntent(const ReshardIntent& intent,
                                 std::string& out) {
    try {
        struct_json::to_json(intent, out);
    } catch (const std::exception& e) {
        LOG(ERROR) << "SerializeReshardIntent failed: " << e.what();
        return ErrorCode::SERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

ErrorCode DeserializeReshardIntent(const std::string& in,
                                    ReshardIntent& out) {
    try {
        struct_json::from_json(out, in);
    } catch (const std::exception& e) {
        LOG(ERROR) << "DeserializeReshardIntent failed: " << e.what();
        return ErrorCode::DESERIALIZE_FAIL;
    }
    return ErrorCode::OK;
}

}  // namespace

ErrorCode EtcdViewStore::CreateReshardIntent(
    const std::string& cluster_namespace, const ReshardIntent& intent) {
    if (intent.source_primary_id.empty() ||
        intent.target_primary_id.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }
    std::string value;
    ErrorCode err = SerializeReshardIntent(intent, value);
    if (err != ErrorCode::OK) {
        return err;
    }
    const std::string key = ReshardIntentKey(cluster_namespace, intent.rank);
    return EtcdHelper::Create(key.data(), key.size(), value.data(),
                              value.size());
}

ErrorCode EtcdViewStore::DeleteReshardIntent(
    const std::string& cluster_namespace, uint32_t rank) {
    const std::string key = ReshardIntentKey(cluster_namespace, rank);
    const std::string end = PrefixEnd(key);
    // 幂等：key 不存在（已删除 / 从未写入）同样视为成功。
    const ErrorCode err = EtcdHelper::DeleteRange(
        key.data(), key.size(), end.data(), end.size());
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        return ErrorCode::OK;
    }
    return err;
}

ErrorCode EtcdViewStore::LoadAllReshardIntents(
    const std::string& cluster_namespace,
    std::vector<ReshardIntent>& out, ViewVersionId& version) {
    out.clear();
    const std::string prefix = ReshardIntentPrefix(cluster_namespace);
    const std::string end = PrefixEnd(prefix);
    std::string json;
    ErrorCode err = EtcdHelper::GetRangeAsJson(prefix.data(), prefix.size(),
                                               end.data(), end.size(),
                                               /*limit=*/0, json, version);
    if (err != ErrorCode::OK) {
        return err;
    }

    std::vector<RangeKvWithRevision> kvs;
    err = ParseRangeJson(json, kvs);
    if (err != ErrorCode::OK) {
        return err;
    }

    out.reserve(kvs.size());
    for (const auto& kv : kvs) {
        ReshardIntent intent;
        err = DeserializeReshardIntent(kv.value, intent);
        if (err != ErrorCode::OK) {
            return err;
        }
        uint32_t key_rank = 0;
        if (!ParseRankFromRingSlotKey(kv.key, prefix, key_rank) ||
            key_rank != intent.rank) {
            LOG(ERROR) << "reshard_intent key '" << kv.key
                       << "' rank mismatch, etcd corrupted";
            return ErrorCode::INTERNAL_ERROR;
        }
        out.push_back(std::move(intent));
    }
    return ErrorCode::OK;
}

}  // namespace cvm
}  // namespace mooncake
