#include "master_service.h"
#include "config/replica_selection_config.h"

#include <array>
#include <cassert>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <future>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <regex>
#include <unordered_set>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <ylt/util/tl/expected.hpp>
#include <ylt/standalone/cinatra/url_encode_decode.hpp>
#include <boost/algorithm/string.hpp>

#include "http_metadata_server.h"
#include "master_metric_manager.h"
#include "master_perf.h"
#include "mooncake_logging.h"
#include "common.h"
#include "common/network.h"
#include "config/dfs_enablement_config.h"
#include "config/master_metadata_config.h"
#include "environ.h"
#include "segment.h"
#include "segment/region_driver.h"
// nof_runtime.h is compiled unconditionally (returns a null initiator in
// non-USE_NOF builds); MasterService::DefaultProbeNofSegment relies on that.
#include "nof/nof_runtime.h"
#include "ha/ha_types.h"
#include "ha/kv/ha_kv_backend_factory.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/ordered_oplog_writer.h"
#ifdef USE_HTTP
#include "transfer_metadata_plugin.h"
#endif
#ifdef USE_NOF
#include "nof/nvmeof_initiator.h"
#endif
#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#include "cvm/etcd_view_store.h"
#include "cvm/cvm_keys.h"
#endif
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/oplog_batch_standby_reader.h"
#include "hot_standby_service.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "types.h"
#include "common/zstd_util.h"
#include "common/file_util.h"
#include "storage/distributed/shard_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/kvcs/kvcs_capacity_provider.h"
#include "storage/distributed/kvcs/kvcs_efc_target.h"
#include "random.h"
#include "kv_event/kv_event_config.h"
#include "ha_metric_manager.h"
#include "metadata_store.h"
#include "eviction/evictor.h"
#include "eviction/offloading_reclaimer.h"
#include "eviction/report_observers.h"
#include "io_pattern/runtime.h"
#include "io_pattern/cfm_service.h"
#include "vsegment/partition_quota_planner.h"
#include "vsegment/vsegment_ha.h"
#include "vsegment/vsegment_metrics.h"

namespace mooncake {

namespace {

uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs) {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

// Decides whether PutStart may proceed with the replicas that were
// actually allocated. Three deliberately different policies apply:
//
//  - Memory-only (nof_replica_num == 0): best-effort by default. Fewer
//    than config.replica_num replicas (but at least one) still succeed,
//    even though DetermineReplicaWriteMode() classifies such configs as
//    RELIABLE_MULTI_REPLICA. The shortfall is surfaced via a WARNING log
//    (action=put_start_partial_allocation) and the
//    master_put_start_partial_allocations_total metric.
//    When strict_memory_only is true (master flag
//    --strict_replica_allocation), the allocation must match
//    config.replica_num exactly, otherwise PutStart fails with
//    NO_AVAILABLE_HANDLE instead of degrading.
//  - FLEXIBLE_DUAL_REPLICA (1 memory + 1 NoF): allocating either side
//    alone is sufficient.
//  - Any other config with nof_replica_num > 0: strict. Both replica
//    types must match the requested counts exactly, otherwise PutStart
//    fails with NO_AVAILABLE_HANDLE.
//
// The "reliable" guarantee of RELIABLE_MULTI_REPLICA is enforced at the
// transfer stage (all allocated replicas must complete or the put is
// revoked), not at the allocation stage for memory-only configs unless
// strict_memory_only is enabled.
bool HasExpectedReplicaAllocation(const ReplicateConfig& config,
                                  size_t allocated_memory_replicas,
                                  size_t allocated_nof_replicas,
                                  bool strict_memory_only = false) {
    if (config.nof_replica_num == 0 && config.dfs_replica_num == 0) {
        if (strict_memory_only)
            return allocated_memory_replicas == config.replica_num;
        return allocated_memory_replicas > 0;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        return allocated_memory_replicas + allocated_nof_replicas > 0;
    }
    return allocated_memory_replicas == config.replica_num &&
           allocated_nof_replicas == config.nof_replica_num;
}

tl::expected<std::string, ErrorCode> GetGroupIdForKey(
    const ReplicateConfig& config, size_t key_count, size_t key_index) {
    if (!config.group_ids.has_value()) {
        return "";
    }
    if (config.group_ids->size() != key_count || key_index >= key_count) {
        MC_LOG(ERROR) << "group_ids.size()=" << config.group_ids->size()
                      << ", key_count=" << key_count
                      << ", error=invalid_group_ids";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return config.group_ids->at(key_index);
}

tl::expected<std::string, ErrorCode> GetResidencyAffinityIdForKey(
    const ReplicateConfig& config, size_t key_count, size_t key_index) {
    if (!config.residency_affinity_ids.has_value()) {
        return "";
    }
    if (config.residency_affinity_ids->size() != key_count ||
        key_index >= key_count) {
        LOG(ERROR) << "residency_affinity_ids.size()="
                   << config.residency_affinity_ids->size()
                   << ", key_count=" << key_count
                   << ", error=invalid_residency_affinity_ids";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return config.residency_affinity_ids->at(key_index);
}

}  // namespace

// Defined unconditionally (CreateNofRuntime() exists in every build and
// returns a null initiator without USE_NOF); the constructor wires it up only
// under USE_NOF. See the declaration in master_service.h for the rationale.
bool MasterService::DefaultProbeNofSegment(const std::string& te_endpoint,
                                           uint32_t timeout_ms,
                                           std::string* error_reason) {
    static const std::shared_ptr<NVMeoFInitiator> initiator =
        CreateNofRuntime().initiator;
    if (!initiator) {
        if (error_reason) {
            *error_reason = "nof_unavailable";
        }
        return false;
    }
    return initiator->ProbeSegment(te_endpoint, timeout_ms, error_reason);
}

// Memory eviction's ties to this service: memory replicas are what it takes,
// and the reclaimer that takes them follows the HA and offload settings.
struct MasterService::MemoryEviction {
    enum class ReplicaKind { kMemory, kKvcsDfs };

    // A completed memory replica nobody holds may go, except a managed
    // weight's. A soft-pinned object gives its memory only to make up a
    // round's minimum, and only when soft pins may be evicted at all.
    class Policy final : public eviction::EvictionPolicy {
       public:
        Policy(const MasterService& service, bool allow_soft_pinned,
               ReplicaKind kind = ReplicaKind::kMemory)
            : service_(service),
              allow_soft_pinned_(allow_soft_pinned),
              kind_(kind) {}

        tl::expected<eviction::Plan, eviction::SkipReason> Decide(
            const eviction::Candidate& candidate) const override {
            // A managed weight leaves memory only through its manager.
            if (candidate.metadata.IsGrouped() &&
                service_.weight_manager_.IsManagedGroup(
                    candidate.metadata.group_id)) {
                return tl::make_unexpected(
                    eviction::SkipReason::kExternalEvict);
            }
            if (service_.IsSoftPinActive(candidate.metadata, candidate.now)) {
                if (!allow_soft_pinned_) {
                    return tl::make_unexpected(
                        eviction::SkipReason::kSoftPinned);
                }
                if (!candidate.fallback) {
                    return tl::make_unexpected(eviction::SkipReason::kDeferred);
                }
            }
            eviction::Plan plan;
            for (const Replica& replica : candidate.metadata.GetAllReplicas()) {
                const bool evictable =
                    kind_ == ReplicaKind::kMemory
                        ? service_.IsEvictableMemoryReplica(replica)
                        : replica.is_dfs_replica() && replica.is_completed();
                if (evictable) {
                    plan.release.push_back(replica.id());
                }
            }
            if (plan.release.empty()) {
                return tl::make_unexpected(
                    eviction::SkipReason::kNothingToRelease);
            }
            return plan;
        }

       private:
        const MasterService& service_;
        const bool allow_soft_pinned_;
        const ReplicaKind kind_;
    };

    // Takes the replicas off the object on the spot, with the cache and
    // dynamic-replication bookkeeping that goes with them.
    class ImmediateReclaimer final : public eviction::Reclaimer {
       public:
        explicit ImmediateReclaimer(MasterService& service)
            : service_(service) {}

        tl::expected<eviction::Effect, ErrorCode> Apply(
            metadata::Namespace&, const route::WriteGuard& hold,
            const eviction::Plan& plan, const eviction::Report&) override {
            ObjectMetadata& metadata = hold.metadata();
            eviction::Effect effect;
            effect.detached = service_.PopReplicasWithCacheTotalAccounting(
                metadata, [&plan](const Replica& replica) {
                    return InPlan(plan, replica);
                });
            std::vector<ReplicaID> released_ids;
            released_ids.reserve(effect.detached.size());
            for (const auto& replica : effect.detached) {
                released_ids.push_back(replica.id());
            }
            service_.dynamic_replication_.RecordReplicaRemoval(metadata,
                                                               released_ids);
            effect.released_bytes =
                metadata.TotalLength() * effect.detached.size();
            effect.needs_teardown = !metadata.IsValid();
            effect.outcome = effect.needs_teardown
                                 ? eviction::Outcome::kDropped
                                 : eviction::Outcome::kDemoted;
            return effect;
        }

        void TearDown(metadata::Namespace& ns,
                      const route::WriteGuard& hold) override {
            service_.EraseMetadata(ns, hold, EraseMode::kFull);
        }

       private:
        MasterService& service_;
    };

    // HA strong consistency: records the object as it will be once the
    // replicas are gone and marks them removed; they are freed once the
    // record is durable, which also tears down an object left with nothing.
    class OpLogReclaimer final : public eviction::Reclaimer {
       public:
        explicit OpLogReclaimer(MasterService& service) : service_(service) {}

        tl::expected<eviction::Effect, ErrorCode> Apply(
            metadata::Namespace& ns, const route::WriteGuard& hold,
            const eviction::Plan& plan, const eviction::Report&) override {
            ObjectMetadata& metadata = hold.metadata();
            const std::string& key = hold.key();
            const auto in_plan = [&plan](const Replica& replica) {
                return InPlan(plan, replica);
            };
            auto remaining =
                service_.BuildRemainingReplicaDescriptors(metadata, in_plan);

            // Keep the removed IDs local until submission succeeds so a
            // consumed reservation cannot strand REMOVED replicas.
            auto reservation = service_.ReserveBatchOpLogSlot();
            if (!reservation) {
                LOG(WARNING)
                    << "Eviction: OpLog reservation failed for key=" << key
                    << ", err=" << static_cast<int>(reservation.error())
                    << ", stopping eviction scan";
                return tl::make_unexpected(reservation.error());
            }
            std::vector<ReplicaID> removed_ids;
            metadata.VisitReplicas(in_plan, [&removed_ids](Replica& replica) {
                removed_ids.push_back(replica.id());
                replica.mark_removed();
            });
            auto persisted = service_.AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()),
                remaining.empty() ? OpType::REMOVE : OpType::PUT_END,
                ns.id().value(), key,
                remaining.empty()
                    ? std::string{}
                    : service_.SerializeMetadataForOpLogFromReplicaDescriptors(
                          metadata, remaining),
                [this, removed_ids](const OpLogEntry& durable_entry) {
                    service_.FinalizeRemovedReplicasAfterDurable(durable_entry,
                                                                 removed_ids);
                });
            if (!persisted) {
                for (const auto& id : removed_ids) {
                    if (auto* replica = metadata.GetReplicaByID(id);
                        replica != nullptr) {
                        replica->cancel_remove();
                    }
                }
                LOG(WARNING) << "Eviction: OpLog persist failed for key=" << key
                             << ", err=" << static_cast<int>(persisted.error())
                             << ", stopping eviction scan";
                return tl::make_unexpected(persisted.error());
            }
            eviction::Effect effect;
            effect.released_bytes = metadata.TotalLength() * removed_ids.size();
            effect.outcome = remaining.empty() ? eviction::Outcome::kDropped
                                               : eviction::Outcome::kDemoted;
            return effect;
        }

        // The durable finalize tears the object down, so Apply never asks.
        void TearDown(metadata::Namespace& ns,
                      const route::WriteGuard& hold) override {
            service_.EraseMetadata(ns, hold, EraseMode::kFull);
        }

       private:
        MasterService& service_;
    };

    class OffloadQueue final : public eviction::OffloadQueue {
       public:
        explicit OffloadQueue(MasterService& service) : service_(service) {}

        bool Push(const TenantId& tenant_id, const std::string& key,
                  Replica& source, std::vector<UUID>* mirror_clients) override {
            return service_
                .PushOffloadingQueue(ObjectIdentity{tenant_id, key}, source,
                                     mirror_clients)
                .has_value();
        }

       private:
        MasterService& service_;
    };

    // Announces that an object lost memory. With the OpLog on, the durable
    // finalize announces it instead.
    class KvEventObserver final : public eviction::EvictionObserver {
       public:
        explicit KvEventObserver(MasterService& service,
                                 const char* medium = "cpu")
            : service_(service), medium_(medium) {}

        void OnApplied(const eviction::ObjectView& view,
                       const eviction::Effect& effect) override {
            if (effect.released_bytes > 0) {
                service_.PublishKvRemovedAfterEvict(
                    view.key, 1, medium_, view.metadata, view.tenant_id);
            }
        }

       private:
        MasterService& service_;
        const char* medium_;
    };

    struct Settings {
        bool through_oplog = false;
        bool allow_evict_soft_pinned = false;
        bool offload_on_evict = false;
        eviction::OffloadingReclaimer::Options offload;
    };

    MemoryEviction(MasterService& service, const Settings& settings)
        : policy(service, settings.allow_evict_soft_pinned),
          immediate(service),
          oplog(service),
          offload_queue(service),
          offloading(immediate, offload_queue, settings.offload),
          kv_events(service),
          log(settings.offload.cap),
          evictor(service.namespaces_, policy,
                  settings.through_oplog
                      ? static_cast<eviction::Reclaimer&>(oplog)
                  : settings.offload_on_evict
                      ? static_cast<eviction::Reclaimer&>(offloading)
                      : static_cast<eviction::Reclaimer&>(immediate),
                  Observers(settings)) {}

    static bool InPlan(const eviction::Plan& plan, const Replica& replica) {
        return std::find(plan.release.begin(), plan.release.end(),
                         replica.id()) != plan.release.end();
    }

    std::vector<eviction::EvictionObserver*> Observers(
        const Settings& settings) {
        std::vector<eviction::EvictionObserver*> observers{&metrics, &log};
        if (!settings.through_oplog) {
            observers.push_back(&kv_events);
        }
        return observers;
    }

    Policy policy;
    ImmediateReclaimer immediate;
    OpLogReclaimer oplog;
    OffloadQueue offload_queue;
    eviction::OffloadingReclaimer offloading;
    KvEventObserver kv_events;
    eviction::MetricsObserver metrics;
    eviction::LogObserver log;
    // Last, so everything it refers to is built first.
    eviction::Evictor evictor;
};

// KVCS reuses the common replica evictor above. Provider deletion and the DFS
// metadata mutation run under the same key lock, so a concurrent Put/Upsert
// cannot publish a replacement between them. A failed delete leaves metadata
// unchanged for the next capacity refresh to retry.
struct MasterService::KvcsDfsEviction {
    class Reclaimer final : public eviction::Reclaimer {
       public:
        explicit Reclaimer(MasterService& service)
            : service_(service), metadata_(service) {}

        tl::expected<eviction::Effect, ErrorCode> Apply(
            metadata::Namespace& ns, const route::WriteGuard& hold,
            const eviction::Plan& plan,
            const eviction::Report& report) override {
            auto deleted =
                service_.dfs_kvcs_capacity_provider_->DeleteObject(hold.key());
            if (deleted) return metadata_.Apply(ns, hold, plan, report);
            ++failures_;
            LOG(ERROR) << "KVCS DFS watermark provider delete failed, key="
                       << hold.key() << ", error=" << toString(deleted.error());
            return tl::make_unexpected(deleted.error());
        }

        void TearDown(metadata::Namespace& ns,
                      const route::WriteGuard& hold) override {
            metadata_.TearDown(ns, hold);
        }

        size_t TakeFailures() { return std::exchange(failures_, 0); }

       private:
        MasterService& service_;
        MemoryEviction::ImmediateReclaimer metadata_;
        size_t failures_{0};
    };

    KvcsDfsEviction(MasterService& service, bool allow_soft_pinned)
        : policy(service, allow_soft_pinned,
                 MemoryEviction::ReplicaKind::kKvcsDfs),
          reclaimer(service),
          kv_events(service, "disk"),
          evictor(service.namespaces_, policy, reclaimer,
                  {&metrics, &kv_events}) {}

    MemoryEviction::Policy policy;
    Reclaimer reclaimer;
    MemoryEviction::KvEventObserver kv_events;
    eviction::MetricsObserver metrics;
    eviction::Evictor evictor;
};

MasterService::MasterService() : MasterService(MasterServiceConfig()) {}

MasterService::MasterService(const MasterServiceConfig& config)
    : graceful_unmount_scheduler_(
          [this](const GracefulUnmountDeadlineRecord& record) {
              auto result =
                  this->UnmountSegment(record.segment_id, record.client_id);
              if (!result.has_value()) {
                  LOG(WARNING)
                      << "Failed to complete graceful unmount, segment_id="
                      << record.segment_id << ", client_id=" << record.client_id
                      << ", error=" << toString(result.error());
              }
          }),
      replica_cleanup_worker_([this] {
          // RFC #4700 slice 1: no metadata sweep while a promotion restore
          // is in flight; the restore itself owns the metadata transition.
          if (AdmitsBusinessWorkers()) {
              ClearInvalidHandles();
          }
      }),
      kvcs_capacity_refresh_worker_([this] { RefreshKvcsDfsUsage(); }),
      enable_async_segment_cleanup_(!config.enable_ha && !config.enable_cxl),
      default_kv_lease_ttl_(config.default_kv_lease_ttl),
      default_kv_soft_pin_ttl_(config.default_kv_soft_pin_ttl),
      max_kv_soft_pin_ttl_(config.max_kv_soft_pin_ttl),
      allow_evict_soft_pinned_objects_(config.allow_evict_soft_pinned_objects),
      eviction_ratio_(config.eviction_ratio),
      eviction_high_watermark_ratio_(config.eviction_high_watermark_ratio),
      nof_eviction_ratio_(config.nof_eviction_ratio),
      nof_eviction_high_watermark_ratio_(
          config.nof_eviction_high_watermark_ratio),
      view_version_(config.view_version),
      client_active_ttl_sec_(config.client_active_ttl_sec),
      client_suspicion_ttl_sec_(config.client_suspicion_ttl_sec),
      nof_heartbeat_interval_sec_(
          std::chrono::seconds(config.nof_heartbeat_interval_sec)),
      nof_heartbeat_probe_timeout_ms_(
          std::chrono::milliseconds(config.nof_heartbeat_probe_timeout_ms)),
      nof_heartbeat_failures_threshold_(
          config.nof_heartbeat_failures_threshold),
      enable_ha_(config.enable_ha),
      enable_offload_(config.enable_offload),
      dynamic_replication_(config.dynamic_replication_mode,
                           config.dynamic_replication_heat_window_seconds,
                           config.dynamic_replication_admission_qps_threshold,
                           config.dynamic_replication_max_memory_replicas),
      enable_oplog_(config.enable_ha && config.enable_oplog &&
                    ha::HaBackendSupportsOpLog(config.ha_backend_type)),
      weight_management_mutations_enabled_(
          !enable_oplog_ ||
          config.weight_management_oplog_capability_confirmed),
      oplog_batch_max_entries_(config.oplog_batch_max_entries),
      restore_repair_durable_timeout_ms_(
          config.restore_repair_durable_timeout_ms),
      cluster_id_(config.cluster_id),
      master_id_(config.master_id),
      cvm_http_port_(config.cvm_http_port),
      cvm_http_host_(config.cvm_http_host),
      submaster_count_(config.submaster_count),
      cvm_segment_default_medium_(config.cvm_segment_default_medium),
      cvm_segments_vsegment_exclusive_(config.cvm_segments_vsegment_exclusive),
      root_fs_dir_(config.root_fs_dir),
      enable_disk_eviction_(config.enable_disk_eviction),
      quota_bytes_(config.quota_bytes),
      enable_multi_tenants_(config.enable_multi_tenants),
      segment_manager_(config.memory_allocator, config.enable_cxl),
      nof_segment_manager_(config.memory_allocator),
      memory_allocator_type_(config.memory_allocator),
      allocation_strategy_type_(config.enable_cxl
                                    ? AllocationStrategyType::CXL
                                    : config.allocation_strategy_type),
      allocation_strategy_(CreateAllocationStrategy(allocation_strategy_type_,
                                                    local_ssd_manager_)),
      put_start_discard_timeout_sec_(config.put_start_discard_timeout_sec),
      put_start_release_timeout_sec_(config.put_start_release_timeout_sec),
      offloading_queue_limit_(config.offloading_queue_limit),
      task_manager_(config.task_manager_config),
      batch_oplog_writer_factory_(
          [](OrderedOpLogWriterConfig writer_config,
             OrderedOpLogWriter::WriteBatchFn write_batch) {
              return std::make_unique<OrderedOpLogWriter>(
                  std::move(writer_config), std::move(write_batch));
          }) {
    if (default_kv_soft_pin_ttl_ > max_kv_soft_pin_ttl_) {
        LOG(ERROR) << "Invalid soft-pin TTL configuration: default="
                   << default_kv_soft_pin_ttl_
                   << ", max=" << max_kv_soft_pin_ttl_;
        throw std::invalid_argument("Invalid soft-pin TTL configuration");
    }

    if (config.weight_import_idle_timeout_ms == 0) {
        throw std::invalid_argument(
            "weight_import_idle_timeout_ms must be positive");
    }
    weight_manager_.SetImportIdleTimeoutMs(
        config.weight_import_idle_timeout_ms);

    // Initialize the HTTP metadata key prefix from its component-owned
    // startup configuration.
    http_metadata_prefix_ =
        MasterMetadataConfig::FromEnvironment().HttpMetadataPrefix();
    if (allocation_strategy_type_ == AllocationStrategyType::LOCAL_FIRST) {
        LOG(INFO) << "Local-first allocation strategy enabled";
    }

    if (eviction_ratio_ < 0.0 || eviction_ratio_ > 1.0) {
        LOG(ERROR) << "Eviction ratio must be between 0.0 and 1.0, "
                   << "current value: " << eviction_ratio_;
        throw std::invalid_argument("Invalid eviction ratio");
    }
    if (eviction_high_watermark_ratio_ < 0.0 ||
        eviction_high_watermark_ratio_ > 1.0) {
        LOG(ERROR)
            << "Eviction high watermark ratio must be between 0.0 and 1.0, "
            << "current value: " << eviction_high_watermark_ratio_;
        throw std::invalid_argument("Invalid eviction high watermark ratio");
    }
    if (config.tenant_eviction_high_watermark_ratio < 0.0 ||
        config.tenant_eviction_high_watermark_ratio > 1.0) {
        LOG(ERROR) << "Tenant eviction high watermark ratio must be between "
                      "0.0 and 1.0, current value: "
                   << config.tenant_eviction_high_watermark_ratio;
        throw std::invalid_argument(
            "Invalid tenant eviction high watermark ratio");
    }
    // Multi-tenancy is the namespace policy every namespace is created under,
    // so it is in place before the first one is.
    if (enable_multi_tenants_) {
        tenant_quota_policy_ = std::make_unique<TenantQuotaPolicy>(
            policy_store_access_,
            TenantQuotaPolicy::Options{
                .eviction_high_watermark_ratio =
                    config.tenant_eviction_high_watermark_ratio,
                .eviction_ratio = eviction_ratio_});
        tenant_quota_policy_->OpenPolicyStoreOrThrow(
            config.tenant_quota_connector_type,
            config.tenant_quota_connector_uri, cluster_id_);
        policy_ = tenant_quota_policy_.get();
    }

    // Validate offload tuning knobs here (not only via gflags validator),
    // because values loaded from a configuration file bypass the gflags
    // validator chain.
    if (config.offload_cap_ratio < 0.0 || config.offload_cap_ratio > 1.0) {
        LOG(ERROR) << "offload_cap_ratio must be between 0.0 and 1.0, "
                   << "current value: " << config.offload_cap_ratio;
        throw std::invalid_argument("Invalid offload_cap_ratio");
    }
    if (offloading_queue_limit_ == 0) {
        LOG(ERROR) << "offloading_queue_limit must be greater than 0";
        throw std::invalid_argument("Invalid offloading_queue_limit");
    }
    if (offloading_queue_limit_ > 100'000'000ULL) {
        LOG(ERROR) << "offloading_queue_limit must be <= 100000000 to avoid "
                   << "overflow when computing offload_cap, current value: "
                   << offloading_queue_limit_;
        throw std::invalid_argument("Invalid offloading_queue_limit");
    }

    if (put_start_release_timeout_sec_ <= put_start_discard_timeout_sec_) {
        LOG(ERROR) << "put_start_release_timeout="
                   << put_start_release_timeout_sec_.count()
                   << " must be larger than put_start_discard_timeout_sec="
                   << put_start_discard_timeout_sec_.count();
        throw std::invalid_argument(
            "put_start_release_timeout must be larger than "
            "put_start_discard_timeout_sec");
    }

#ifdef USE_NOF
    if (nof_heartbeat_interval_sec_.count() <= 0) {
        LOG(ERROR) << "nof_heartbeat_interval_sec must be positive, current "
                   << nof_heartbeat_interval_sec_.count();
        throw std::invalid_argument("Invalid nof heartbeat interval");
    }
    if (nof_heartbeat_probe_timeout_ms_.count() <= 0) {
        LOG(ERROR) << "nof_heartbeat_probe_timeout_ms must be positive, "
                   << "current " << nof_heartbeat_probe_timeout_ms_.count();
        throw std::invalid_argument("Invalid nof heartbeat probe timeout");
    }
    if (nof_heartbeat_failures_threshold_ == 0) {
        LOG(ERROR) << "nof_heartbeat_failures_threshold must be positive";
        throw std::invalid_argument("Invalid nof heartbeat failure threshold");
    }

    nof_probe_fn_ = &DefaultProbeNofSegment;
#endif

    // Strict replica allocation: memory-only multi-replica requests must
    // allocate exactly replica_num replicas instead of degrading.
    strict_replica_allocation_ = config.strict_replica_allocation;
    if (strict_replica_allocation_) {
        MC_LOG(INFO) << "Strict replica allocation enabled: memory-only "
                        "multi-replica requests must be fully satisfied";
    }

    // Offload-on-evict: defer LOCAL_DISK offload to eviction time
    offload_on_evict_ = enable_offload_ && config.offload_on_evict;
    if (offload_on_evict_) {
        LOG(INFO) << "Offload-on-evict mode enabled: DRAM offload to "
                     "LOCAL_DISK will occur at eviction time instead of "
                     "PutEnd";
        if (config.offload_force_evict) {
            LOG(INFO) << "Force-evict enabled: objects exceeding offload "
                         "cap will be evicted without disk offload";
        }
    }
    // With offload-on-evict and offload_force_evict, a round that has queued
    // offload_cap_ratio of the offloading queue limit lets the rest of the
    // memory it takes go without an offload.
    memory_eviction_ = std::make_unique<MemoryEviction>(
        *this,
        MemoryEviction::Settings{
            .through_oplog = enable_oplog_,
            .allow_evict_soft_pinned = allow_evict_soft_pinned_objects_,
            .offload_on_evict = offload_on_evict_,
            .offload = {.cap = static_cast<long>(offloading_queue_limit_ *
                                                 config.offload_cap_ratio),
                        .force_evict = config.offload_force_evict}});
    // Promotion-on-hit: when Get observes a LOCAL_DISK-only key, queue an
    // async copy back to MEMORY. Only meaningful when offload is enabled
    // (otherwise no LOCAL_DISK replicas exist in the first place).
    promotion_on_hit_ = enable_offload_ && config.promotion_on_hit;
    promotion_admission_threshold_ = config.promotion_admission_threshold;
    promotion_queue_limit_ = config.promotion_queue_limit;
    promotion_max_per_heartbeat_ = config.promotion_max_per_heartbeat;
    // Clamp to >=1: 0 would make PromotionObjectHeartbeat return an empty
    // batch every call, silently disabling promotion delivery.
    if (promotion_max_per_heartbeat_ == 0) {
        promotion_max_per_heartbeat_ = 1;
    }
    // Defense-in-depth clamp: master.cpp clamps threshold into [1, 255]
    // at flag-parse time, but direct MasterServiceConfig construction
    // (tests, embedded users) bypasses that. Without the clamp here,
    // threshold=0 would silently bypass the frequency gate entirely
    // (freq < 0 is never true for uint8_t).
    if (promotion_admission_threshold_ == 0) {
        promotion_admission_threshold_ = 1;
    } else if (promotion_admission_threshold_ > 255) {
        promotion_admission_threshold_ = 255;
    }
    if (config.promotion_on_hit && !enable_offload_) {
        LOG(WARNING) << "promotion_on_hit=true was requested but "
                     << "enable_offload=false; promotion is silently "
                     << "disabled because it requires offload to produce "
                     << "LOCAL_DISK replicas. Set enable_offload=true to "
                     << "use this feature.";
    }
    if (promotion_on_hit_) {
        promotion_sketch_ = std::make_unique<CountMinSketch>();
        LOG(INFO) << "Promotion-on-hit mode enabled: LOCAL_DISK-only Gets "
                     "will queue async promotion to MEMORY (threshold="
                  << promotion_admission_threshold_
                  << ", queue_limit=" << promotion_queue_limit_
                  << ", max_per_heartbeat=" << promotion_max_per_heartbeat_
                  << ")";
    }

    InitDfsAllocatorFromEnvironment(config);
    io_pattern::IoPatternRuntime::Config io_pattern_config;
    // Report-driven execution: a SubMaster that merges ownership-addressed
    // client reports runs its own analysis -> decision -> execution cycle on
    // every accepted report (report_snapshot / report_metric_batch). This is
    // the driver that makes remote-mode CFM execution visible: without it
    // policy only runs on the local watermark thread, the local Put admission
    // path or explicit execute_* RPCs.
    io_pattern_config.report_driven_execution = true;
    // Mirror the master's own high/low watermark semantics so a merged-report
    // eviction cycle reclaims the same excess as the local eviction thread
    // (target ratio = high watermark - eviction ratio; clamped to >= 0).
    io_pattern_config.report_eviction_high_ratio =
        static_cast<float>(eviction_high_watermark_ratio_);
    io_pattern_config.report_eviction_target_ratio = static_cast<float>(
        std::max(0.0, eviction_high_watermark_ratio_ - eviction_ratio_));
    // Admission must stop before eviction starts: a negative configured
    // watermark means "derive it from the eviction high watermark", so the
    // store does not evict while still admitting new objects. A non-negative
    // value pins it.
    io_pattern_config.admission_watermark_ratio =
        config.io_pattern_admission_watermark_ratio >= 0.0
            ? static_cast<float>(config.io_pattern_admission_watermark_ratio)
            : static_cast<float>(eviction_high_watermark_ratio_);
    // Configurable so a deployment can restore the previous
    // admit-on-first-sight behaviour (threshold 1) without recompiling.
    io_pattern_config.admission_frequency_threshold =
        config.io_pattern_admission_frequency_threshold;
    // Cold-data eviction driver: allow merged reports to reclaim the coldest
    // real objects even below the memory watermark (opt-in via master flags).
    io_pattern_config.report_driven_cold_eviction =
        config.io_pattern_cold_eviction;
    io_pattern_config.report_driven_cold_eviction_bytes =
        config.io_pattern_cold_eviction_bytes_per_cycle;
    io_pattern_config.report_driven_cold_idle_threshold_us =
        config.io_pattern_cold_idle_threshold_us;
    if (config.io_pattern_cold_eviction) {
        LOG(INFO) << "Report-driven cold-data eviction enabled: per-cycle "
                     "budget="
                  << config.io_pattern_cold_eviction_bytes_per_cycle
                  << " bytes, idle threshold="
                  << config.io_pattern_cold_idle_threshold_us << " us";
    }
    // Policy-driven tier-down driver: below the memory watermark, copy the
    // coldest in-memory keys down to LOCAL_DISK while keeping their MEMORY
    // replica, so a later reclaim can discard them safely. The per-cycle budget
    // is the whole control: 0 (the default) keeps the driver off.
    io_pattern_config.tier_down_bytes_per_cycle =
        config.io_pattern_tier_down_bytes_per_cycle;
    if (config.io_pattern_tier_down_bytes_per_cycle != 0) {
        LOG(INFO) << "Policy-driven tier down enabled: per-cycle budget="
                  << config.io_pattern_tier_down_bytes_per_cycle << " bytes";
    }
    io_pattern_config.report_driven_observer =
        [](const io_pattern::IoPatternRuntime::ReportDrivenCycleReport& rpt) {
            auto& metrics = MasterMetricManager::instance();
            metrics.inc_io_pattern_report_cycles();
            if (rpt.degraded) metrics.inc_io_pattern_report_degraded();
            // A cycle "executes" a dimension when a plan reached the storage
            // handler. The report-driven cycle only forwards an eviction plan
            // with candidate keys (an empty-candidate pressure report is a
            // clean no-op, not a failure); prefetch/admission likewise only
            // reach their handler when candidates were planned. Failures are
            // counted when the storage handler rejected the plan.
            //
            // A tier-down cycle is excluded from the eviction counters: it
            // copies keys down and frees nothing, so counting it as a
            // report-driven eviction would overstate reclaim. Its outcome is
            // visible through MasterService's own
            // tier_down_attempt/success/failure counters.
            if (rpt.tier_down) {
                LOG(INFO) << "[IO-PATTERN-TIER-DOWN] report-driven tier-down "
                             "cycle="
                          << rpt.cycle_id
                          << " budget=" << rpt.eviction_target_bytes
                          << " candidates=" << rpt.eviction_candidates
                          << " status=" << static_cast<int>(rpt.eviction_status)
                          << " skipped=" << rpt.skipped_dimensions;
            } else if (rpt.eviction_candidates != 0) {
                metrics.inc_io_pattern_report_evictions();
                if (rpt.eviction_status != ErrorCode::OK) {
                    metrics.inc_io_pattern_report_eviction_failures();
                }
            }
            if (rpt.prefetch_candidates != 0) {
                metrics.inc_io_pattern_report_prefetches();
                if (rpt.prefetch_status != ErrorCode::OK) {
                    metrics.inc_io_pattern_report_prefetch_failures();
                }
            }
            if (rpt.admissions_admitted != 0) {
                metrics.inc_io_pattern_report_admissions();
                if (rpt.admission_status != ErrorCode::OK) {
                    metrics.inc_io_pattern_report_admission_failures();
                }
            }
            LOG(INFO) << "[IO-PATTERN-REPORT-CYCLE] cycle=" << rpt.cycle_id
                      << " keys_analyzed=" << rpt.keys_analyzed
                      << " analysis_elapsed_us=" << rpt.analysis_elapsed_us
                      << " degraded=" << rpt.degraded
                      << " skipped=" << rpt.skipped_dimensions
                      << " eviction(tier="
                      << static_cast<int>(rpt.eviction_tier)
                      << ", bytes=" << rpt.eviction_target_bytes
                      << ", candidates=" << rpt.eviction_candidates
                      << ", status=" << static_cast<int>(rpt.eviction_status)
                      << ", cold=" << rpt.cold_eviction
                      << ", tier_down=" << rpt.tier_down << ")"
                      << " prefetch(candidates=" << rpt.prefetch_candidates
                      << ", status=" << static_cast<int>(rpt.prefetch_status)
                      << ")"
                      << " admission(candidates=" << rpt.admission_candidates
                      << ", admitted=" << rpt.admissions_admitted
                      << ", status=" << static_cast<int>(rpt.admission_status)
                      << ")";
        };
    // Promotion is the Store's only cross-tier primitive, and it can decline
    // for structural reasons the policy cannot influence: promotion disabled,
    // no LOCAL_DISK source replica, or watermark / queue-cap / second-touch
    // backpressure. Those map to UNAVAILABLE_IN_CURRENT_MODE so the runtime
    // records a skip instead of a policy failure -- otherwise three such cycles
    // would permanently replace the workload policy with the legacy fallback.
    // Only a vanished object or a genuine enqueue failure stays an error.
    // Captured by value: a captureless lambda is an empty, copyable type, so no
    // local outlives this constructor.
    const auto promotion_outcome_to_error =
        [](PromotionQueueResult outcome) -> ErrorCode {
        switch (outcome) {
            case PromotionQueueResult::kQueued:
            case PromotionQueueResult::kAlreadyInFlight:
            case PromotionQueueResult::kMemoryReplicaPresent:
                return ErrorCode::OK;
            case PromotionQueueResult::kDisabled:
            case PromotionQueueResult::kFrequencyRejected:
            case PromotionQueueResult::kWatermarkRejected:
            case PromotionQueueResult::kQueueCapRejected:
            case PromotionQueueResult::kNoLocalDiskSource:
                return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
            case PromotionQueueResult::kNotFound:
            case PromotionQueueResult::kPushFailed:
                return ErrorCode::OBJECT_NOT_FOUND;
        }
        return ErrorCode::OBJECT_NOT_FOUND;
    };
    io_pattern_runtime_ = std::make_shared<io_pattern::IoPatternRuntime>(
        io_pattern::IoPatternRuntime::Handlers{
            .eviction =
                [this](const io_pattern::EvictionPlan& plan) {
                    struct TenantCandidates {
                        uint64_t bytes{0};
                        std::unordered_set<std::string> keys;
                    };
                    if (plan.target_bytes == 0) return ErrorCode::OK;
                    // A tier-down plan copies objects down and keeps their
                    // MEMORY replica, so it frees nothing. Only the reclaim
                    // share of the plan may drive the legacy fallback below:
                    // counting the demotion budget as a shortfall would make
                    // the fallback evict exactly the keys the driver just chose
                    // to keep.
                    const uint64_t tier_down_target = std::min(
                        plan.tier_down_target_bytes, plan.target_bytes);
                    const uint64_t reclaim_target =
                        plan.target_bytes - tier_down_target;
                    uint64_t total_freed = 0;
                    uint64_t tier_down_attempts = 0;
                    uint64_t tier_down_queued = 0;
                    uint64_t tier_down_in_flight = 0;
                    uint64_t tier_down_paved = 0;
                    uint64_t tier_down_failed = 0;
                    std::vector<std::string> tier_down_diag;
                    const auto tier_down_outcome_name =
                        [](TierDownOutcome outcome) -> const char* {
                        switch (outcome) {
                            case TierDownOutcome::kQueued:
                                return "queued";
                            case TierDownOutcome::kAlreadyInFlight:
                                return "already_in_flight";
                            case TierDownOutcome::kAlreadyPaved:
                                return "already_paved";
                            case TierDownOutcome::kNotFound:
                                return "not_found";
                            case TierDownOutcome::kPushFailed:
                                return "push_failed";
                        }
                        return "unknown";
                    };
                    std::unordered_map<TenantId, TenantCandidates, TenantIdHash>
                        targets;
                    for (const auto& candidate : plan.candidates) {
                        // The action is per candidate and comes from the plan
                        // (which got it from the driver), never from
                        // target_tier. A kTierDown candidate is copied down and
                        // keeps its MEMORY replica, so it is excluded from the
                        // quota eviction below and from freed accounting.
                        if (candidate.action ==
                            io_pattern::EvictionAction::kTierDown) {
                            ++tier_down_attempts;
                            const TierDownOutcome outcome = TryQueueTierDown(
                                ObjectIdentity{candidate.object.tenant_id,
                                               candidate.object.key});
                            switch (outcome) {
                                case TierDownOutcome::kQueued:
                                    ++tier_down_queued;
                                    break;
                                case TierDownOutcome::kAlreadyInFlight:
                                    ++tier_down_in_flight;
                                    break;
                                case TierDownOutcome::kAlreadyPaved:
                                    ++tier_down_paved;
                                    break;
                                case TierDownOutcome::kNotFound:
                                case TierDownOutcome::kPushFailed:
                                    ++tier_down_failed;
                                    break;
                            }
                            if (tier_down_diag.size() < 3) {
                                tier_down_diag.push_back(
                                    candidate.object.key + "=" +
                                    tier_down_outcome_name(outcome));
                            }
                            continue;
                        }
                        auto& target = targets[candidate.object.tenant_id];
                        target.bytes += candidate.bytes;
                        target.keys.insert(candidate.object.key);
                    }
                    if (tier_down_attempts != 0) {
                        tier_down_attempts_.fetch_add(
                            tier_down_attempts, std::memory_order_relaxed);
                        tier_down_successes_.fetch_add(
                            tier_down_queued, std::memory_order_relaxed);
                        tier_down_failures_.fetch_add(
                            tier_down_failed, std::memory_order_relaxed);
                        tier_down_skipped_in_flight_.fetch_add(
                            tier_down_in_flight, std::memory_order_relaxed);
                        tier_down_skipped_paved_.fetch_add(
                            tier_down_paved, std::memory_order_relaxed);
                        LOG(WARNING)
                            << "[IO-PATTERN-TIER-DOWN] io_pattern tier down "
                               "plan_target="
                            << tier_down_target
                            << " attempts=" << tier_down_attempts
                            << " queued=" << tier_down_queued
                            << " already_in_flight=" << tier_down_in_flight
                            << " already_paved=" << tier_down_paved
                            << " failed=" << tier_down_failed;
                        for (const auto& diag : tier_down_diag) {
                            LOG(WARNING)
                                << "[IO-PATTERN-TIER-DOWN]   candidate "
                                << diag;
                        }
                        if (tier_down_paved != 0 && io_pattern_runtime_) {
                            // The handler reads the authoritative metadata; the
                            // selector reads the collector snapshot. Log the
                            // snapshot's view so a disagreement is visible
                            // instead of looking like a driver that simply has
                            // nothing to do: metadata=paved with
                            // snapshot_paved=0 means the replica bit never
                            // reached the snapshot.
                            constexpr io_pattern::CacheTierMask kLowerTierBits =
                                static_cast<io_pattern::CacheTierMask>(
                                    io_pattern::CacheTierBit(
                                        io_pattern::CacheTier::kLocalDisk) |
                                    io_pattern::CacheTierBit(
                                        io_pattern::CacheTier::kL2Segment) |
                                    io_pattern::CacheTierBit(
                                        io_pattern::CacheTier::kL3NofSsd));
                            const auto snapshot =
                                io_pattern_runtime_->Snapshot();
                            size_t snapshot_paved = 0;
                            for (const auto& candidate : plan.candidates) {
                                if (candidate.action !=
                                    io_pattern::EvictionAction::kTierDown) {
                                    continue;
                                }
                                const auto it = std::find_if(
                                    snapshot.keys.begin(), snapshot.keys.end(),
                                    [&candidate](
                                        const io_pattern::KeyMetrics& key) {
                                        return key.object == candidate.object;
                                    });
                                if (it != snapshot.keys.end() &&
                                    (it->replica_tiers & kLowerTierBits) != 0) {
                                    ++snapshot_paved;
                                }
                            }
                            LOG(WARNING)
                                << "[IO-PATTERN-TIER-DOWN]   metadata=paved "
                                << tier_down_paved << " of "
                                << tier_down_attempts << ", snapshot=paved "
                                << snapshot_paved
                                << " (0 means the replica bit never reached "
                                   "the snapshot)";
                        }
                    }
                    for (const auto& [tenant, target] : targets) {
                        const auto result = EvictTenantMemoryForQuota(
                            tenant, target.bytes, &target.keys);
                        total_freed =
                            result.freed_bytes >
                                    std::numeric_limits<uint64_t>::max() -
                                        total_freed
                                ? std::numeric_limits<uint64_t>::max()
                                : total_freed + result.freed_bytes;
                        LOG(WARNING)
                            << "[IO-PATTERN-EVICT-DIAG] io_pattern eviction "
                               "tenant="
                            << tenant.value() << " target=" << target.bytes
                            << " freed=" << result.freed_bytes
                            << " evicted_objects=" << result.evicted_objects
                            << " candidate_keys=" << target.keys.size();
                        size_t shown = 0;
                        for (const auto& key : target.keys) {
                            if (shown++ >= 3) break;
                            LOG(WARNING)
                                << "[IO-PATTERN-EVICT-DIAG]   candidate key="
                                << key;
                        }
                        ReportEvictedKeysAsTierRemovals(tenant, target.keys);
                    }
                    LOG(WARNING)
                        << "[IO-PATTERN-EVICT-DIAG] io_pattern eviction "
                           "summary plan_target="
                        << plan.target_bytes
                        << " reclaim_target=" << reclaim_target
                        << " total_freed=" << total_freed
                        << " candidates=" << plan.candidates.size();
                    if (total_freed >= reclaim_target) {
                        return ErrorCode::OK;
                    }
                    // The plan under-delivered its *reclaim* target: its
                    // candidates may be stale, may still hold leases or pins,
                    // or may be empty. Fall back to the legacy lease-ordered
                    // eviction so the watermark request still makes progress.
                    // This covers both the local watermark
                    // thread and the report-driven worker, which is why the
                    // thread no longer runs its own fallback. The tier-down
                    // share of the plan is deliberately excluded from the
                    // shortfall: a demotion is not a failed reclaim.
                    const uint64_t shortfall = reclaim_target - total_freed;
                    LOG(WARNING)
                        << "[IO-PATTERN-EVICT-FALLBACK] policy plan under-"
                           "delivered reclaim_target="
                        << reclaim_target << " freed=" << total_freed
                        << " shortfall=" << shortfall;
                    return RunLegacyEvictionFallback(shortfall)
                               ? ErrorCode::OK
                               : ErrorCode::OBJECT_NOT_FOUND;
                },
            .prefetch = [this, promotion_outcome_to_error](
                            const io_pattern::PrefetchPlan& plan) -> ErrorCode {
                for (const auto& candidate : plan.candidates) {
                    // Store's safe promotion primitive is LOCAL_DISK ->
                    // MEMORY; HBM remains inference-runtime-owned and is
                    // never promoted from the master control plane.
                    if (candidate.target_tier ==
                        io_pattern::CacheTier::kL0Hbm) {
                        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
                    }
                    const ObjectIdentity object_id{candidate.object.tenant_id,
                                                   candidate.object.key};
                    const auto code = promotion_outcome_to_error(
                        TryPushPromotionQueue(object_id,
                                              /*record_candidate=*/false));
                    if (code != ErrorCode::OK) {
                        return code;
                    }
                }
                return ErrorCode::OK;
            },
            .admission =
                [this, promotion_outcome_to_error](
                    const io_pattern::AdmissionResult& result) -> ErrorCode {
                if (result.target_tier == io_pattern::CacheTier::kL0Hbm) {
                    return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
                }
                const ObjectIdentity object_id{result.object.tenant_id,
                                               result.object.key};
                return promotion_outcome_to_error(TryPushPromotionQueue(
                    object_id, /*record_candidate=*/false));
            }},
        std::move(io_pattern_config));
    MasterMetricManager::instance().set_io_pattern_runtime(io_pattern_runtime_);
    io_pattern_cfm_service_ =
        std::make_shared<io_pattern::CfmService>(io_pattern_runtime_);

    kv_event_publisher_ =
        std::make_unique<KvEventPublisher>(BuildKvEventConfig(config));
    kv_track_tenant_epochs_ = KvEventsEnabled();

    if (enable_oplog_ && !cluster_id_.empty()) {
        if (config.ha_backend_connstring.empty()) {
            LOG(INFO) << "Skipping automatic batch-record OpLog writer "
                         "initialization; no HA backend connstring configured";
        } else {
            auto backend_type = ha::ParseHABackendType(config.ha_backend_type);
            if (!backend_type.has_value()) {
                throw std::runtime_error(
                    "failed to create HA batch-record OpLog writer: invalid "
                    "HA backend");
            }
            auto backend = CreateHaKvBackend(ha::HABackendSpec{
                .type = backend_type.value(),
                .connstring = config.ha_backend_connstring,
                .cluster_namespace = cluster_id_,
            });
            if (!backend) {
                throw std::runtime_error(fmt::format(
                    "failed to create HA batch-record OpLog writer: {}",
                    toString(backend.error())));
            }
            // Direct MasterService construction has no leadership session;
            // supervisor-created services carry a non-zero acquired view.
            ErrorCode err = InitializeBatchOpLogWriter(
                std::move(backend.value()),
                /*require_fenced_writer=*/view_version_ > 0);
            if (err != ErrorCode::OK) {
                throw std::runtime_error(fmt::format(
                    "failed to create HA batch-record OpLog writer: {}",
                    toString(err)));
            }
        }
    }

    // This worker is part of the Client lifecycle protocol. Start it before
    // any raw std::thread so a startup failure can unwind the constructor
    // normally instead of encountering joinable thread destructors.
    client_offboarding_worker_.Start();
    VLOG(1) << "action=start_client_offboarding_worker";
    if (dfs_kvcs_capacity_provider_) kvcs_capacity_refresh_worker_.Start();
    // KV partition (CVM) ownership is now owned by the HA supervisor: the
    // supervisor creates the CvmController (etcd lease + master registration +
    // snapshot aggregation + membership loop), then injects the lease id and
    // drives SlotOwnerHeartbeat around serve start/stop via
    // SetCvmLeaseId() / StartSlotOwnerHeartbeat() / StopSlotOwnerHeartbeat().
    // MasterService itself only publishes slot/segment ownership records.

    eviction_running_ = true;
    eviction_thread_ = std::thread(&MasterService::EvictionThreadFunc, this);
    VLOG(1) << "action=start_eviction_thread";

    // Start client monitor thread in all modes so TTL/heartbeat works
    client_monitor_running_ = true;
    client_monitor_thread_ =
        std::thread(&MasterService::ClientMonitorFunc, this);
    VLOG(1) << "action=start_client_monitor_thread";

#ifdef USE_NOF
    nof_heartbeat_running_ = true;
    nof_heartbeat_thread_ =
        std::thread(&MasterService::NofHeartbeatThreadFunc, this);
    VLOG(1) << "action=start_nof_heartbeat_thread";
#endif

    // Start task cleanup thread
    task_cleanup_running_ = true;
    task_cleanup_thread_ =
        std::thread(&MasterService::TaskCleanupThreadFunc, this);
    VLOG(1) << "action=start_task_cleanup_thread";

    weight_reconciliation_running_ = true;
    weight_reconciliation_thread_ =
        std::thread(&MasterService::WeightReconciliationThreadFunc, this);

    replica_cleanup_worker_.Start();
#ifdef STORE_USE_ETCD
    // 方案一 A 异步化：旧代段清理 drain 线程。MountSegment 尾部仅入队
    // （EnqueueStaleGenerationEvict），本线程延迟执行完整清理，把同步
    // etcd 事务与锁竞争移出 mount RPC 关键路径。non-etcd 构建下
    // EvictStaleGenerationForHost 为空桩，不启动本线程。
    stale_evict_running_ = true;
    stale_evict_thread_ =
        std::thread(&MasterService::StaleEvictThreadFunc, this);
    VLOG(1) << "action=start_stale_evict_thread";
#endif

    // NOTE: The async HTTP metadata cleanup worker is started lazily in
    // setHttpMetadataRemoteUrl() once http_metadata_remote_url_ is set,
    // since that happens after this constructor returns (in
    // WrappedMasterService).

    job_dispatch_running_ = true;
    job_dispatch_thread_ =
        std::thread(&MasterService::JobDispatchThreadFunc, this);
    VLOG(1) << "action=start_job_dispatch_thread";

    dynamic_replication_.Start([this](const TenantId& tenant_id,
                                      const std::string& key) {
        // RFC #4700 slice 1: dynamic replica proposals mutate business
        // metadata, so they stay out of the restore window.
        if (!AdmitsBusinessWorkers()) {
            return;
        }
        std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
        TrySubmitDynamicReplicaProposal(ObjectIdentity{tenant_id, key});
    });

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
        if (config.global_file_segment_size ==
            std::numeric_limits<int64_t>::max()) {
            MasterMetricManager::instance().set_dfs_capacity_unlimited(true);
        } else {
            MasterMetricManager::instance().inc_total_file_capacity(
                config.global_file_segment_size);
        }
    }

    if (config.enable_cxl) {
        allocation_strategy_ = std::make_shared<CxlAllocationStrategy>();
        const auto result = segment_manager_.initializeCxlAllocator(
            config.cxl_path, config.cxl_size);
        LOG_IF(FATAL, result != ErrorCode::OK)
            << "Failed to initialize CXL allocator: "
            << static_cast<int>(result);
        VLOG(1) << "action=start_cxl_global_allocator";
    }
}

tl::expected<int, ErrorCode> MasterService::GetDfsShardCount() const {
    if (!shard_allocator_ || !shard_allocator_->IsInitialized()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return shard_allocator_->GetShardCount();
}

tl::expected<int, ErrorCode> MasterService::ExpandDfsShards(int shard_count) {
    if (!shard_allocator_ || !shard_allocator_->IsInitialized()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return shard_allocator_->ExpandShards(shard_count);
}

void MasterService::InitDfsAllocatorFromEnvironment(
    const MasterServiceConfig& config) {
    const auto dfs_config = DistributedStorageConfig::FromEnvironment();
    // KVCS is an object-storage backend and does not use the filesystem DFS
    // allocator, but it still enables the distributed-storage path.
    enable_dfs_ =
        DfsEnablementConfig::FromEnvironment().enabled || dfs_config.UsesKvcs();
    if (!enable_dfs_) return;

    const bool recovery_enabled = enable_oplog_;
    if (!dfs_config.Validate()) {
        LOG(ERROR) << "Invalid distributed storage config: {"
                   << dfs_config.FormatStr() << "}";
        enable_dfs_ = false;
        if (recovery_enabled) {
            throw std::invalid_argument(
                "Invalid distributed storage config in recovery mode");
        }
        return;
    }
    if (!dfs_config.single_tenant) {
        LOG(ERROR) << "Currently, DFS backend is not supported in "
                      "multi-tenant mode";
        enable_dfs_ = false;
        if (recovery_enabled) {
            throw std::invalid_argument(
                "Distributed storage recovery requires single-tenant mode");
        }
        return;
    }

    if (dfs_config.UsesKvcs()) {
        use_kvcs_ = true;
        auto target = LoadKvcsEfcTarget(dfs_config.object_storage_config_path);
        if (!target) {
            LOG(ERROR) << "Failed to load KVCS target capacity source";
            enable_dfs_ = false;
            use_kvcs_ = false;
            return;
        }
        auto provider = CreateKvcsCapacityProvider(*target);
        if (!provider) {
            if (provider.error() == ErrorCode::NOT_SUPPORTED) {
                LOG(WARNING) << "KVCS SDK is unavailable; capacity refresh "
                                "is disabled";
                LOG(INFO) << "KVCS distributed object storage initialized "
                             "without DFS extent allocator, config={"
                          << dfs_config.FormatStr() << "}";
                return;
            }
            LOG(ERROR) << "Failed to create KVCS capacity provider: "
                       << toString(provider.error());
            enable_dfs_ = false;
            use_kvcs_ = false;
            return;
        }
        dfs_kvcs_capacity_provider_ = std::move(*provider);
        dfs_kvcs_capacity_check_interval_ = dfs_config.eviction_check_interval;
        dfs_kvcs_eviction_enabled_ = dfs_config.eviction_enabled;
        dfs_kvcs_eviction_high_watermark_ = dfs_config.eviction_high_watermark;
        dfs_kvcs_eviction_low_watermark_ = dfs_config.eviction_low_watermark;
        InitKvcsDfsEviction();
        LOG(INFO) << "KVCS distributed object storage initialized without "
                     "DFS extent allocator, config={"
                  << dfs_config.FormatStr() << "}";
        return;
    }

    if (recovery_enabled) {
        LOG(ERROR) << "Filesystem DFS cannot be enabled with snapshot or oplog "
                      "recovery until allocator state restoration is supported";
        throw std::invalid_argument(
            "Filesystem DFS is incompatible with snapshot/oplog recovery");
    }

    shard_allocator_ = nullptr;
    bucket_allocator_ = nullptr;
    if (dfs_config.allocator_type == "bucket") {
        auto allocator = std::make_unique<ImmutableBucketAllocator>();
        bucket_allocator_ = allocator.get();
        dfs_allocator_ = std::move(allocator);
    } else {
        auto allocator = std::make_unique<ShardAllocator>();
        shard_allocator_ = allocator.get();
        dfs_allocator_ = std::move(allocator);
    }
    auto init_result = dfs_allocator_->Init(dfs_config);
    if (!init_result) {
        LOG(ERROR) << "Failed to initialize DFS allocator, error="
                   << init_result.error() << ", config={"
                   << dfs_config.FormatStr() << "}";
        dfs_allocator_.reset();
        shard_allocator_ = nullptr;
        bucket_allocator_ = nullptr;
        enable_dfs_ = false;
        return;
    }

    LOG(INFO) << "DFS allocator initialized, type="
              << ToString(dfs_allocator_->Type()) << ", config={"
              << dfs_config.FormatStr() << "}"
              << (shard_allocator_ != nullptr
                      ? ", ready_shard_count=" +
                            std::to_string(shard_allocator_->GetShardCount())
                      : "");
}

void MasterService::SetCvmLeaseId(EtcdLeaseId lease_id) {
    cvm_lease_id_ = lease_id;
}

void MasterService::SetCvmController(cvm::CvmController* controller) {
    cvm_controller_ = controller;
}

void MasterService::SetVSegmentAutoPlanPolicy(
    const vsegment::VSegmentUserPolicy& policy) {
    std::lock_guard<std::mutex> lock(vsegment_auto_plan_mutex_);
    vsegment_auto_plan_policy_ = policy;
}

std::vector<VSegmentLayoutEntry> MasterService::BuildVSegmentLayoutTable(
    const std::vector<std::pair<std::string, std::string>>& vsegments) {
    std::vector<VSegmentLayoutEntry> result;

    // Preserve the caller's state-lock context: several call sites already
    // hold master_state_mutex_ while building their response. The service is
    // published once and never reset, so initialization locking is unnecessary.
    std::shared_ptr<vsegment::VSegmentService> service = vsegment_service_;
    if (!service) return result;

    std::unordered_set<std::string> seen;
    for (const auto& [partition_id, vsegment_id] : vsegments) {
        if (!seen.insert(vsegment_id).second) continue;
        {
            std::lock_guard<std::mutex> lock(vsegment_layout_cache_mutex_);
            auto found = vsegment_layout_cache_.find(vsegment_id);
            if (found != vsegment_layout_cache_.end()) {
                result.push_back(found->second);
                continue;
            }
        }
        vsegment::VSegmentView view;
        if (service->LoadView(partition_id, vsegment_id, &view) !=
            ErrorCode::OK) {
            continue;
        }
        std::vector<UUID> member_ids;
        member_ids.reserve(view.members.size());
        bool ids_ok = true;
        for (const auto& member : view.members) {
            UUID id;
            if (!StringToUuid(member.segment_id, id)) {
                ids_ok = false;
                break;
            }
            member_ids.push_back(id);
        }
        if (!ids_ok) continue;

        std::vector<std::string> endpoints;
        std::vector<uint64_t> bases;
        if (!segment_manager_.GetSegmentLocations(member_ids, &endpoints,
                                                  &bases)) {
            continue;
        }
        VSegmentLayoutEntry entry;
        entry.vsegment_id = view.vsegment_id;
        entry.partition_id = view.partition_id;
        entry.stripe_size = view.stripe_size;
        entry.logical_capacity = view.logical_capacity;
        entry.mapping_algorithm = static_cast<uint32_t>(view.mapping_algorithm);
        entry.checksum = view.checksum;
        entry.members.reserve(view.members.size());
        for (size_t i = 0; i < view.members.size(); ++i) {
            entry.members.push_back(
                {view.members[i].segment_id, view.members[i].base_offset,
                 view.members[i].length, endpoints[i], bases[i]});
        }
        {
            std::lock_guard<std::mutex> lock(vsegment_layout_cache_mutex_);
            vsegment_layout_cache_[vsegment_id] = entry;
        }
        result.push_back(std::move(entry));
    }
    return result;
}

tl::expected<std::optional<PutStartResult>, ErrorCode>
MasterService::TryVSegmentPutStart(const UUID& client_id,
                                   const std::string& key,
                                   const TenantId& tenant_id,
                                   uint64_t slice_length,
                                   const ReplicateConfig& config) {
    if (!vsegment_service_) return std::optional<PutStartResult>{};
    if (config.replica_num == 0 || config.nof_replica_num != 0 ||
        config.dfs_replica_num != 0)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    auto normalized = ResolveTenantIdForWrite(tenant_id);
    if (!normalized) return tl::make_unexpected(normalized.error());
    auto plan = PrepareWriteStart(client_id, key, *normalized, slice_length,
                                  config, "vsegment_put_start");
    if (!plan) return tl::make_unexpected(plan.error());
    std::optional<std::unique_lock<std::mutex>> weight_lock;
    if (!plan->group_id.empty() &&
        weight_manager_.IsManagedGroup(plan->group_id)) {
        weight_lock.emplace(
            weight_manager_.LockGroup(*normalized, plan->group_id));
        if (!weight_manager_.AllowsGroupMemberMutation(plan->group_id))
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    const std::string partition_id =
        std::to_string(cvm::KeySlot(*normalized, key));
    std::shared_lock<std::shared_mutex> state_lock(master_state_mutex_);
    auto ready = CheckVSegmentServiceability(partition_id);
    if (ready != ErrorCode::OK) return tl::make_unexpected(ready);
    ready = CheckSlotWritable(cvm::KeySlot(*normalized, key));
    if (ready != ErrorCode::OK) return tl::make_unexpected(ready);
    std::vector<std::string> operation_ids(config.replica_num);
    for (auto& id : operation_ids) id = UuidToString(generate_uuid());
    auto reservations = vsegment_service_->StartPutReplicasOwned(
        partition_id, operation_ids, slice_length,
        config.vsegment_profile_name);
    if (!reservations) return tl::make_unexpected(reservations.error());
    std::vector<Replica> replicas;
    replicas.reserve(reservations->size());
    for (const auto& reservation : *reservations)
        replicas.emplace_back(reservation.replica, ReplicaStatus::PROCESSING);
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> inserted =
        tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    {
        MetadataAccessorRW accessor(this, ObjectIdentity{*normalized, key},
                                    MetadataAccessorRW::CreateKey{});
        if (!accessor.IsPublished()) {
            auto grown = accessor.Grow(
                SaturatingMultiply(slice_length, config.replica_num));
            if (grown) {
                inserted =
                    InsertMetadata(accessor, client_id, slice_length, config,
                                   plan->group_id, plan->residency_affinity_id,
                                   std::chrono::system_clock::now(),
                                   plan->soft_pin_request, std::move(replicas));
            } else {
                inserted = tl::make_unexpected(grown.error());
            }
        }
    }
    if (!inserted) {
        // Abort outside the object's lock: the allocator has its own durable
        // commit path and may wait for a callback while rolling back.
        for (const auto& reservation : *reservations) {
            const auto error = vsegment_service_->AbortPutOwned(
                reservation.replica.partition_id,
                reservation.replica.vsegment_id, reservation.operation_id);
            if (error != ErrorCode::OK)
                LOG(ERROR) << "VSegment reservation rollback failed: "
                           << toString(error);
        }
        return tl::make_unexpected(inserted.error());
    }
    PutStartResult result{
        operation_ids.size() == 1 ? operation_ids.front() : std::string{},
        std::move(*inserted)};
    EnrichVSegmentLayouts(&result);
    return std::optional<PutStartResult>(std::move(result));
}

vsegment::VSegmentPutStartResult MasterService::VSegmentPutStart(
    const std::string& partition_id, uint64_t route_epoch,
    const std::string& operation_id, uint64_t length,
    const std::string& profile_name) {
    std::shared_lock<std::shared_mutex> state_lock(master_state_mutex_);
    const auto ready = CheckVSegmentServiceability(partition_id);
    if (ready != ErrorCode::OK)
        return {ready, operation_id, {}, "Partition metadata is not ready"};
    if (!vsegment_service_)
        return {ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
                operation_id,
                {},
                "vsegment service is not initialized"};
    return vsegment_service_->StartPut(partition_id, route_epoch, operation_id,
                                       length, profile_name);
}

ErrorCode MasterService::VSegmentPutEnd(const VSegmentDescriptor& replica,
                                        uint64_t route_epoch,
                                        const std::string& operation_id,
                                        const std::string& object_id) {
    std::shared_lock<std::shared_mutex> state_lock(master_state_mutex_);
    const auto ready = CheckVSegmentServiceability(replica.partition_id);
    if (ready != ErrorCode::OK) return ready;
    if (!vsegment_service_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    return vsegment_service_->CommitPut(replica, route_epoch, operation_id,
                                        object_id);
}

ErrorCode MasterService::VSegmentPutRevoke(const std::string& partition_id,
                                           const std::string& vsegment_id,
                                           uint64_t route_epoch,
                                           const std::string& operation_id) {
    std::shared_lock<std::shared_mutex> state_lock(master_state_mutex_);
    const auto ready = CheckVSegmentServiceability(partition_id);
    if (ready != ErrorCode::OK) return ready;
    if (!vsegment_service_) return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    return vsegment_service_->AbortPut(partition_id, vsegment_id, route_epoch,
                                       operation_id);
}

void MasterService::EnrichVSegmentLayouts(GetReplicaListResponse* response) {
    if (response == nullptr) return;
    std::vector<std::pair<std::string, std::string>> vsegments;
    for (const auto& replica : response->replicas) {
        if (!replica.is_vsegment_replica()) continue;
        const auto& descriptor = replica.get_vsegment_descriptor();
        vsegments.emplace_back(descriptor.partition_id, descriptor.vsegment_id);
    }
    if (vsegments.empty()) return;
    auto layouts = BuildVSegmentLayoutTable(vsegments);
    if (!layouts.empty()) response->vsegment_layouts = std::move(layouts);
}

void MasterService::EnrichVSegmentLayouts(PutStartResult* response) {
    if (response == nullptr) return;
    std::vector<std::pair<std::string, std::string>> vsegments;
    for (const auto& replica : response->replicas) {
        if (!replica.is_vsegment_replica()) continue;
        const auto& descriptor = replica.get_vsegment_descriptor();
        vsegments.emplace_back(descriptor.partition_id, descriptor.vsegment_id);
    }
    if (vsegments.empty()) return;
    auto layouts = BuildVSegmentLayoutTable(vsegments);
    if (!layouts.empty()) response->vsegment_layouts = std::move(layouts);
}

void MasterService::KickSlotHeartbeat() {
    {
        std::lock_guard<std::mutex> lock(reshard_wake_mutex_);
        reshard_wake_pending_ = true;
    }
    reshard_wake_cv_.notify_all();
    std::lock_guard<std::mutex> lock(slot_heartbeat_kick_mutex_);
    if (slot_owner_heartbeat_) slot_owner_heartbeat_->Kick();
}

void MasterService::StopSlotOwnerHeartbeat() {
    if (cvm_controller_) cvm_controller_->SetSlotsChangedCallback({});
    std::unique_ptr<cvm::SlotOwnerHeartbeat> heartbeat;
    {
        // 锁外 Stop/Join：Join 期间心跳线程自身可能经回调链触发 Kick
        // （如 RefreshVSegmentOwnership → MaybeRetryVSegmentAutoPlan），持
        // 锁 join 会死锁。
        std::lock_guard<std::mutex> lock(slot_heartbeat_kick_mutex_);
        heartbeat = std::move(slot_owner_heartbeat_);
    }
    if (heartbeat) {
        heartbeat->Stop();
    }
}

#ifdef STORE_USE_ETCD
namespace {

ErrorCode LoadEffectivePartitionRoute(
    const std::string& cluster_id, const std::string& partition_id,
    partition::PartitionRoute& route, ViewVersionId& version,
    const std::vector<std::string>& primary_ids, ViewVersionId ring_revision,
    const cvm::RingSlotsView& ring_slots) {
    // Numeric KV Partitions use the same ownership model as KV slots:
    // slot→owner 统一经 ResolveSlotOwnerUnified（§16.17.2 的唯一实现，
    // 含 G 的 ring_slots 快照优先 + §15 回退），与客户端 PartitionRouter
    // 同源；kMigrating 期间空 owner 的语义见该函数注释。
    size_t parsed = 0;
    unsigned long numeric = 0;
    try {
        numeric = std::stoul(partition_id, &parsed);
    } catch (...) {
        return cvm::EtcdViewStore::LoadPartitionRoute(cluster_id, partition_id,
                                                      route, version);
    }
    if (parsed != partition_id.size() || numeric >= cvm::SlotCount()) {
        return cvm::EtcdViewStore::LoadPartitionRoute(cluster_id, partition_id,
                                                      route, version);
    }
    const auto slot = static_cast<uint16_t>(numeric);
    const std::string owner = cvm::ResolveSlotOwnerUnified(
        ring_slots.assigns(), ring_slots.group_count(), primary_ids, slot);
    if (owner.empty() || ring_revision <= 0) return ErrorCode::INVALID_VERSION;
    version = ring_revision;
    route.partition_id.partition_id = partition_id;
    route.owner_submaster_id = owner;
    route.route_epoch = static_cast<uint64_t>(version);
    route.state = static_cast<int32_t>(partition::PartitionState::kActive);
    route.target_submaster_id.clear();
    return ErrorCode::OK;
}

}  // namespace

ErrorCode MasterService::StartSlotOwnerHeartbeat() {
    const bool kv_partition_enabled =
        enable_ha_ && ha_backend_type_ == "etcd" && !master_id_.empty() &&
        !cluster_id_.empty();
    if (!kv_partition_enabled) {
        return ErrorCode::OK;
    }

    // The supervisor's CvmController already connected the etcd client before
    // this point; ConnectToEtcdStoreClient is idempotent so re-connecting is a
    // safe no-op for direct constructions / tests.
    ErrorCode connect_err =
        EtcdHelper::ConnectToEtcdStoreClient(ha_backend_connstring_);
    if (connect_err != ErrorCode::OK) {
        LOG(WARNING) << "StartSlotOwnerHeartbeat: failed to connect etcd: "
                     << connect_err;
        return connect_err;
    }

    if (slot_owner_heartbeat_) {
        return ErrorCode::OK;  // already running
    }

    // 复位运行期门控（supervisor 角色切换会在同一实例上 stop/start 心跳）：
    // 上一轮激活的 gate 若不复位，本函数尾部的初始调用会走异步门控路径收
    // NOT_READY，导致心跳启动失败。每次（重新）启动的初始调用都必须是
    // 同步冷启动语义。
    vsegment_runtime_gate_active_.store(false, std::memory_order_release);
    // 若上一 serve phase 的后台首建仍在执行（快照迟到窗口内发生角色切
    // 换），先 join 再走同步初始调用，避免初始 sweep 与后台 sweep 并发
    // （recovered_vsegment_snapshots_ 等无并发保护）。此时 RPC 服务尚未
    // 启动，join 无并发 spawn 面。
    {
        std::lock_guard<std::mutex> spawn_lock(vsegment_init_spawn_mutex_);
        if (vsegment_init_thread_.joinable()) {
            vsegment_init_thread_.join();
        }
    }

    const auto initial_slots = ResolveOwnedSlotsForCvm();
    UpdateExpectedSlots(initial_slots);
    auto vsegment_error = RefreshVSegmentOwnership();
    if (vsegment_error != ErrorCode::OK) return vsegment_error;
    // 心跳线程即将启动：此后 RefreshVSegmentOwnership 的运行期调用走异步
    // 门控（VSegmentInitState）——若快照迟到（auto-plan 运行期发布），
    // 「首建 + 首次全量 sweep」由后台线程执行，心跳线程只收可重试的
    // NOT_READY，绝不被分钟级初始化阻塞（masters lease 由 CvmController
    // 独立保活，不受影响）。
    vsegment_runtime_gate_active_.store(true, std::memory_order_release);

    cvm::SlotOwnerHeartbeat::Config hb_config;
    hb_config.cluster_namespace = cluster_id_;
    hb_config.master_id = master_id_;
    // Dynamic partition: recompute the owned slot set from the etcd master
    // membership on every heartbeat so multiple submaster instances split the
    // 16384 slots without overwriting each other.
    hb_config.dynamic_slot_resolver = [this]() {
        auto slots = ResolveOwnedSlotsForCvm();
        {
            // Drain writes admitted under the previous owned-slot bitmap
            // before staging a released Partition's immutable export.
            std::unique_lock<std::shared_mutex> lock(master_state_mutex_);
            UpdateExpectedSlots(slots);
        }
        const auto error = RefreshVSegmentOwnership();
        if (error == ErrorCode::VSEGMENT_SERVICE_NOT_READY) {
            // 后台首建进行中（快照迟到的首次拉起窗口）：本周期跳过
            // vsegment reconcile 属预期行为，静默处理防刷屏。
        } else if (error != ErrorCode::OK) {
            LOG(ERROR) << "Failed to refresh vsegment ownership: "
                       << toString(error);
        }
        // kNoQuota 稳态的低频重探：resolver 每心跳恰执行一次（心跳线程），
        // 绝不在 slot 认领热路径上 spawn。重探线程不触碰 state（见
        // MaybeSpawnVSegmentInit），放行率不受影响；运行期发布 quota 后由
        // 它推进 kInProgress → kReady。
        if (vsegment_runtime_gate_active_.load(std::memory_order_acquire) &&
            vsegment_init_state_.load(std::memory_order_acquire) ==
                VSegmentInitState::kNoQuota &&
            !vsegment_service_) {
            MaybeSpawnVSegmentInit();
        }
        return slots;
    };
    hb_config.lease_id = cvm_lease_id_;
    // live primary → live primary 元数据交接（§15.7 RPC 直传）：释放端把对象
    // 元数据 stage 到内存等新 owner 拉取，获得端经 InterMasterRpc 直传拉取导
    // 入并回 ack。数据字节始终留在 segment，不搬移。
    hb_config.on_slot_acquired = [this](uint16_t slot) {
        std::vector<ErrorCode> results;
        const auto error = OnSlotRangeAcquired(slot, slot, results);
        return error == ErrorCode::OK ? results.front() : error;
    };
    hb_config.on_slot_released = [this](uint16_t slot) {
        const auto exported = ExportSlotMetadata(slot);
        if (exported != ErrorCode::OK) {
            LOG(ERROR) << "Failed to export released vsegment Partition "
                       << slot << ": " << toString(exported);
            return;
        }
        // Retain the manager until the importer acknowledges the snapshot.
    };
    // slot 就绪加速（段级批量交接）：gained 聚成连续区间后按旧 owner 分组做
    // 段级 RPC 拉取（ExportSlotBatchGated），把逐 slot 串行 RPC 合并为段级；
    // 不可批量的 slot 由实现内部回退单 slot 路径，results 保持逐 slot 语义。
    hb_config.on_slot_range_acquired = [this](uint16_t first, uint16_t last,
                                              std::vector<ErrorCode>& results) {
        return OnSlotRangeAcquired(first, last, results);
    };
    hb_config.on_slot_range_released = [this](uint16_t first, uint16_t last) {
        auto exports = BuildSlotMetadataExportBatch(first, last, true);
        if (!exports)
            LOG(ERROR) << "Failed to stage released slots: " << exports.error();
    };
    const bool lease_bound = hb_config.lease_id != 0;
    {
        std::lock_guard<std::mutex> lock(slot_heartbeat_kick_mutex_);
        slot_owner_heartbeat_ =
            std::make_unique<cvm::SlotOwnerHeartbeat>(std::move(hb_config));
    }
    ErrorCode hb_err = slot_owner_heartbeat_->Start();
    if (hb_err != ErrorCode::OK) {
        LOG(WARNING) << "Failed to start SlotOwnerHeartbeat: " << hb_err;
        std::lock_guard<std::mutex> lock(slot_heartbeat_kick_mutex_);
        slot_owner_heartbeat_.reset();
        return hb_err;
    }
    if (cvm_controller_) {
        cvm_controller_->SetSlotsChangedCallback(
            [this] { KickSlotHeartbeat(); });
    }
    LOG(INFO) << "Started SlotOwnerHeartbeat: master_id=" << master_id_
              << ", cluster_namespace=" << cluster_id_
              << ", dynamic_partition=true"
              << ", lease_bound=" << lease_bound;
    return ErrorCode::OK;
}

// vsegment auto-plan 发布侧重试的最小间隔（心跳兜底与 mount 加速两个触发
// 点共享节流，防 mount 风暴/多 submaster 心跳打爆 etcd）。
static constexpr std::chrono::seconds kVSegmentAutoPlanMinRetryInterval{10};
// W6 静默期信号（集群级）：auto-plan 首写一次定型（first-writer-wins），
// member_count 刚满足的瞬间、滚动部署中段就发布会得到覆盖不完整拓扑的一次
// 性快照。墙钟毫秒写入 etcd 共享 key——任意 submaster 的 mount/remount 均刷
// 新，PlanAndPublishDiscoveredQuota 发布前读取并等待静默（对全部触发方统一
// 生效，多 submaster 滚动部署不会出现“从未见过 mount 的实例抢先发布”）。
// 写入失败不致命：最坏退化为该次活动未被记录，planner 端仍有零段/段数不足
// 校验兜底。mount 为低频端点，一次小 value Put 可忽略。
void MasterService::NoteVSegmentMountActivity() {
    const auto key = cvm::VSegmentMountActivityKey(cluster_id_);
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    const auto value = std::to_string(now_ms);
    const auto error =
        EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
    if (error != ErrorCode::OK) {
        VLOG(1) << "vsegment mount activity key write failed: "
                << toString(error);
    }
}

// vsegment auto-plan 发布侧重试（触发点：心跳 RefreshVSegmentOwnership 的
// KEY_NOT_EXIST 分支 + MountSegment/ReMountSegment 尾部，均低频）。返回
// true 表示快照现已发布（本机发布或并发赢家已发布）。节流 10s，A/B 共享
// 同一状态；锁存后 fast-path 短路，稳态零 etcd 开销。
bool MasterService::MaybeRetryVSegmentAutoPlan(const char* trigger) {
    {
        std::lock_guard<std::mutex> lock(vsegment_auto_plan_mutex_);
        if (vsegment_auto_plan_policy_.member_count == 0 ||
            vsegment_auto_plan_published_ || vsegment_auto_plan_gave_up_) {
            return false;
        }
    }
    std::string detail;
    std::string summary;
    vsegment::VSegmentUserPolicy policy;
    {
        std::lock_guard<std::mutex> lock(vsegment_auto_plan_mutex_);
        const auto now = std::chrono::steady_clock::now();
        if (now - vsegment_auto_plan_last_attempt_ <
            kVSegmentAutoPlanMinRetryInterval) {
            return false;
        }
        vsegment_auto_plan_last_attempt_ = now;
        policy = vsegment_auto_plan_policy_;
    }
    // W6 集群级静默期已下沉到 PlanAndPublishDiscoveredQuota 内部（读取 etcd
    // 共享活动 key），对 bootstrap/HTTP/本重试组件全部触发方统一生效。
    // 廉价前置检查（毫秒级 etcd Get）：本组件锁存仅内存态，进程重启后清零；
    // 若快照已被其他实例（或本进程重启前的上一代）发布，直接短路，避免重启
    // 后首次 mount 白白付出一次完整「发现→规划→序列化→Create 失败」并误报
    // concurrent publish。
    {
        vsegment::PartitionPhysicalQuotaSnapshot existing;
        vsegment::EtcdPartitionQuotaSnapshotStore store(cluster_id_);
        if (store.Load(&existing) == ErrorCode::OK) {
            {
                std::lock_guard<std::mutex> lock(vsegment_auto_plan_mutex_);
                vsegment_auto_plan_published_ = true;
            }
            LOG(INFO) << "vsegment auto-plan: quota generation "
                      << existing.config_generation
                      << " already published (via " << trigger << ")";
            // 快照已可用：Kick 心跳立即拉起首建/认领，不等下个周期。
            KickSlotHeartbeat();
            return true;
        }
    }
    const auto error = vsegment::PlanAndPublishDiscoveredQuota(
        cluster_id_, policy, vsegment::kDefaultQuotaMaxEtcdValueBytes, &detail,
        &summary);
    const bool first_attempt = [&] {
        std::lock_guard<std::mutex> lock(vsegment_auto_plan_mutex_);
        return ++vsegment_auto_plan_attempts_ == 1;
    }();
    switch (error) {
        case ErrorCode::OK:
        case ErrorCode::ETCD_TRANSACTION_FAIL: {
            // 原子首写输家：另一实例（或手工 POST /vsegment_quota）刚发布，
            // 与发布成功同等对待。
            std::lock_guard<std::mutex> lock(vsegment_auto_plan_mutex_);
            vsegment_auto_plan_published_ = true;
        }
            LOG(INFO) << "vsegment auto-plan published via " << trigger
                      << (error == ErrorCode::OK
                              ? ": " + summary
                              : std::string(": another instance published "
                                            "concurrently"));
            vsegment::LogVSegmentPoolFormed(cluster_id_);
            break;
        case ErrorCode::INVALID_PARAMS: {
            // 策略非法（非资源不足），重试无意义：终态锁存 + 单次 WARNING。
            std::lock_guard<std::mutex> lock(vsegment_auto_plan_mutex_);
            vsegment_auto_plan_gave_up_ = true;
        }
            LOG(WARNING) << "vsegment auto-plan gave up via " << trigger
                         << " (invalid policy): " << detail;
            break;
        case ErrorCode::VSEGMENT_STATIC_QUOTA_INSUFFICIENT:
        case ErrorCode::VSEGMENT_MOUNT_QUIET_PERIOD:
            // 等待更多 client 挂载 / mount 流未静默（W6 集群级门控）都是
            // 部署期正常稳态：首条 INFO 提示，此后 VLOG(1) 防刷屏（触发点
            // 本身低频 + 10s 节流双保险）。
            if (first_attempt) {
                LOG(INFO) << "vsegment auto-plan waiting for more mounts via "
                          << trigger << ": " << detail;
            } else {
                VLOG(1) << "vsegment auto-plan waiting for more mounts via "
                        << trigger << ": " << detail;
            }
            break;
        default:
            // etcd 读写失败等可重试错误：首条 WARNING，此后 VLOG(1)。
            if (first_attempt) {
                LOG(WARNING)
                    << "vsegment auto-plan retry failed via " << trigger << ": "
                    << toString(error) << " (" << detail << ")";
            } else {
                VLOG(1) << "vsegment auto-plan retry failed via " << trigger
                        << ": " << toString(error) << " (" << detail << ")";
            }
            break;
    }
    bool published = false;
    {
        std::lock_guard<std::mutex> lock(vsegment_auto_plan_mutex_);
        published = vsegment_auto_plan_published_;
    }
    // 发布成功（含并发赢家路径）：Kick 心跳立即拉起首建/认领。锁外触发，
    // 避免持有 auto_plan mutex 的同时再进 kick mutex 形成嵌套。
    if (published) {
        KickSlotHeartbeat();
    }
    return published;
}

ErrorCode MasterService::RefreshVSegmentOwnership(
    const std::string& acquiring, vsegment::VSegmentCommitBatch* batch) {
    if (!ordered_oplog_writer_) return ErrorCode::OK;
    if (vsegment_runtime_gate_active_.load(std::memory_order_acquire)) {
        const auto gate_state =
            vsegment_init_state_.load(std::memory_order_acquire);
        if (gate_state == VSegmentInitState::kIdle ||
            gate_state == VSegmentInitState::kInProgress) {
            MaybeSpawnVSegmentInit();
            return ErrorCode::VSEGMENT_SERVICE_NOT_READY;
        }
        // kNoQuota：quota 快照不存在（vsegment 关闭或尚未发布）。slot 就绪
        // 不依赖 vsegment——放行到 null-service 分支按冷启动同款语义处理，
        // 绝不能被永不完成的初始化门控阻塞（否则 vsegment-off 部署下所有
        // slot 永久 NOT_READY → put 永久 SLOT_MIGRATING）。
    }
    if (!vsegment_service_) {
        if (!vsegment_runtime_gate_active_.load(std::memory_order_acquire)) {
            // 冷启动同步路径（StartSlotOwnerHeartbeat 初始调用，gate 激活之
            // 前）：构造 + 首次全量 sweep 在调用线程内联完成，错误硬失败启动
            // ——与历史行为完全一致。双检锁：外层空判是 shared_ptr 原子读的
            // 快路径（稳态零成本）；进锁后复检——竞争输家（如 P4 reshard 线
            // 程与冷启动并发）直接复用赢家成果，绝不重复 Load + 构造。
            std::lock_guard<std::mutex> init_lock(vsegment_service_init_mutex_);
            if (!vsegment_service_) {
                vsegment::PartitionPhysicalQuotaSnapshot quota;
                std::string detail;
                vsegment::EtcdPartitionQuotaSnapshotStore store(cluster_id_);
                auto error = store.Load(&quota, &detail);
                if (error == ErrorCode::ETCD_KEY_NOT_EXIST &&
                    MaybeRetryVSegmentAutoPlan(acquiring.empty() ? "heartbeat"
                                                                 : "import")) {
                    // 发布侧自愈成功：立即重读快照并继续本周期拉起。
                    error = store.Load(&quota, &detail);
                }
                if (error == ErrorCode::ETCD_KEY_NOT_EXIST) {
                    // quota 缺席（vsegment off / 未发布）：slot 就绪不依赖
                    // vsegment。置 kNoQuota 稳态（运行期门控放行），消除第
                    // 一轮心跳遗留 kIdle 的慢热前段。
                    vsegment_init_state_.store(VSegmentInitState::kNoQuota,
                                               std::memory_order_release);
                    return ErrorCode::OK;
                }
                if (error != ErrorCode::OK) {
                    LOG(ERROR) << "Failed to load vsegment quota: " << detail;
                    return error;
                }
                // 先在局部完成全部配置，最后一步原子发布：并发读者要么见
                // null 要么见完整服务，绝不读到 provider 未注入的半成品。
                ConstructVSegmentService(std::move(quota));
                // 同步冷启动构造后置 kReady，与后台首建路径的状态语义一致
                // （首次 sweep 随后在本调用内联完成）。
                vsegment_init_state_.store(VSegmentInitState::kReady,
                                           std::memory_order_release);
            }
        } else {
            // 运行期异步门控：心跳线程绝不内联执行分钟级的「首建 + 首次全
            // 量 reconcile sweep」。kIdle/kInProgress 表示首建进行中——
            // NOT_READY 对全部运行期调用方均为可重试语义。
            //
            // kNoQuota（quota 缺席/读取失败的稳态）：与冷启动
            // KEY_NOT_EXIST 路径语义对齐——slot 就绪不依赖 vsegment，直接
            // 放行。★此路径绝不调用 MaybeSpawnVSegmentInit：重探由心跳
            // resolver 每周期低频驱动（见 dynamic_slot_resolver）；在 slot
            // 认领热路径上 spawn 会把 state 打回 kInProgress，使每轮心跳只
            // 放行 ~1 个 slot（16384 slot 需 22 小时）。绝不 fallthrough
            // 到 reconcile：service 为空。
            const auto gate_state =
                vsegment_init_state_.load(std::memory_order_acquire);
            if (gate_state == VSegmentInitState::kNoQuota) {
                return ErrorCode::OK;
            }
            MaybeSpawnVSegmentInit();
            return ErrorCode::VSEGMENT_SERVICE_NOT_READY;
        }
    }
    return RunVSegmentOwnershipReconcile(acquiring, batch);
}

// vsegment_service_ 构造 + provider 注入 + 原子发布（先配置后发布：并发
// 读者要么见 null 要么见完整服务，绝不读到 provider 未注入的半成品——否则
// 分配路径可能选中运行期已判死段）。调用前提：持有
// vsegment_service_init_mutex_ 且复检 vsegment_service_ 为空。
void MasterService::ConstructVSegmentService(
    vsegment::PartitionPhysicalQuotaSnapshot quota) {
    auto service =
        std::make_shared<vsegment::VSegmentService>(std::move(quota));
    // 配额发现活性校验的运行时补充：quota snapshot 是一次性首写（first-
    // writer-wins），发布后死掉的物理段会永久残留在快照里。此处注入
    // invalid_replica_endpoints_ 排除集提供者，让新 vsegment 的条带成员
    // 避开运行时已判死的段（worker 重挂载时 MountSegment erase 自愈）。
    // 锁序：manager mutex_ → invalid_endpoints_mutex_（与老路径 PutStart
    // 的 allocator 锁 → invalid 锁方向一致，无反向持有）。
    service->SetExcludedSegmentsProvider([this]() {
        std::shared_lock<std::shared_mutex> lock(invalid_endpoints_mutex_);
        // unordered_set → set：provider 约定返回有序集合（消费侧
        // PartitionQuotaAllocator::Allocate 按值语义持有快照）。
        return std::set<std::string>(invalid_replica_endpoints_.begin(),
                                     invalid_replica_endpoints_.end());
    });
    vsegment_service_ = std::move(service);
}

// vsegment reconcile 主体（原 RefreshVSegmentOwnership 后半），由冷启动同
// 步路径、运行期调用（service 已发布）与后台首建线程（首次全量 sweep）共
// 同复用。调用前提：vsegment_service_ 已发布非空。
ErrorCode MasterService::RunVSegmentOwnershipReconcile(
    const std::string& acquiring, vsegment::VSegmentCommitBatch* batch) {
    vsegment::VSegmentCommitBatch local_batch;
    auto* commits = batch ? batch : &local_batch;
    std::vector<std::string> primary_ids;
    ViewVersionId ring_revision = 0;
    {
        std::lock_guard<std::mutex> lock(cvm_resolver_mutex_);
        primary_ids = cvm_last_primary_ids_;
        ring_revision = cvm_ring_revision_;
    }
    // ring_slots 归属快照（含 G，单锁一致，§16.17.2；循环外一次构建，
    // 整个 reconcile 用同一视图）：owner 推导经 ResolveSlotOwnerUnified
    // 与客户端 PartitionRouter 同源，避免 manager 装错节点。
    cvm::RingSlotsView ring_slots;
    if (cvm_controller_ != nullptr) {
        ring_slots = cvm_controller_->BuildRingSlotsView();
    }
    // 周期聚合统计（防刷屏：绝不逐 partition 打印，仅周期末变更触发一条
    // INFO；reconcile 失败的逐条 ERROR 收拢为每周期一条 + 首个样本）。
    static const char* const kVSegStatNames[kVSegStatCount] = {
        "scanned",   "owned_by_me",    "foreign",  "no_route",
        "load_fail", "reconcile_fail", "stale_cas"};
    uint32_t vseg_stats[kVSegStatCount] = {};
    std::string reconcile_fail_first, load_fail_first;
    bool complete = true;
    std::unordered_set<std::string> recovered_owned_partitions;
    // Single-slot acquisition bypasses the full quota sweep. The caller can
    // supply a batch shared by a whole gained range.
    auto process_partition = [&](const std::string& partition_id,
                                 const vsegment::PartitionVSegmentSnapshot*
                                     state) {
        ++vseg_stats[0];
        partition::PartitionRoute route;
        ViewVersionId version = 0;
        auto error = LoadEffectivePartitionRoute(cluster_id_, partition_id,
                                                 route, version, primary_ids,
                                                 ring_revision, ring_slots);
        if (error == ErrorCode::ETCD_KEY_NOT_EXIST) {
            ++vseg_stats[3];
            if (state) complete = false;
            return;
        }
        if (error != ErrorCode::OK) {
            // 首失败样本（partition + 错误码），周期末一条 ERROR——与
            // reconcile_fail_first 同模式；典型样本即 ring_revision 门控的
            // INVALID_VERSION，能直接暴露本次这类死锁。
            if (load_fail_first.empty()) {
                load_fail_first = partition_id + ": " + toString(error);
            }
            ++vseg_stats[4];
            complete = false;
            return;
        }
        const bool numeric_slot =
            !partition_id.empty() &&
            partition_id.find_first_not_of("0123456789") == std::string::npos &&
            partition_id.size() <= 5 &&
            std::stoul(partition_id) < cvm::SlotCount();
        if (numeric_slot) {
            // The release hook stages state and ACK detaches it. Removing
            // here would lose the manager before the release hook can
            // export it.
            if (route.owner_submaster_id != master_id_) {
                ++vseg_stats[2];
                return;
            }
            if (acquiring != partition_id &&
                !OwnsSlot(static_cast<uint16_t>(std::stoul(partition_id)))) {
                ++vseg_stats[2];
                return;
            }
            vsegment::PartitionVSegmentSnapshot current;
            if (vsegment_service_->SnapshotPartition(partition_id, &current) ==
                ErrorCode::OK) {
                // Unrelated etcd writes must not invalidate in-flight
                // requests.
                route.route_epoch = current.route_epoch;
            } else if (state) {
                route.route_epoch =
                    std::max(route.route_epoch, state->route_epoch);
            }
        }
        auto committer =
            std::make_shared<vsegment::OrderedOpLogVSegmentCommitter>(
                ordered_oplog_writer_.get());
        error = vsegment_service_->ReconcilePartitionRoute(
            route, master_id_, std::move(committer), state, nullptr, commits);
        if (error != ErrorCode::OK && error != ErrorCode::STALE_ROUTE) {
            // 逐 partition ERROR 收拢：每周期一条（首个失败样本），计数
            // 进周期摘要——故障风暴（如 STALE_ROUTE 修复前的路由错位）
            // 不再 16384 条/周期刷屏。
            if (reconcile_fail_first.empty()) {
                reconcile_fail_first = partition_id + ": " + toString(error);
            }
            ++vseg_stats[5];
            complete = false;
        } else {
            if (error == ErrorCode::STALE_ROUTE) ++vseg_stats[6];
            ++vseg_stats[1];
            if (state && route.owner_submaster_id == master_id_) {
                recovered_owned_partitions.insert(partition_id);
            }
        }
    };

    auto process = [&](const std::string& partition_id) {
        if (!vsegment_service_->HasPartitionQuota(partition_id)) return;
        const vsegment::PartitionVSegmentSnapshot* state = nullptr;
        for (const auto& snapshot : recovered_vsegment_snapshots_) {
            if (snapshot.partition_id == partition_id) {
                state = &snapshot;
                break;
            }
        }
        process_partition(partition_id, state);
    };
    if (!acquiring.empty()) {
        process(acquiring);
    } else {
        size_t count = 0;
        for (const auto& partition_id : vsegment_service_->PartitionIds()) {
            process(partition_id);
            if (++count % 128 == 0 && commits->Flush() != ErrorCode::OK)
                complete = false;
        }
    }
    if (!batch) {
        if (local_batch.Flush() != ErrorCode::OK) complete = false;
        vsegment_service_->RefreshMetrics();
    }
    if (!reconcile_fail_first.empty()) {
        LOG(ERROR) << "vsegment reconcile failures: count=" << vseg_stats[5]
                   << ", first=" << reconcile_fail_first;
    }
    // load_fail（路由加载失败，含 ring_revision 门控拒绝的 INVALID_VERSION）
    // 同样收拢为每周期一条首样本——否则门控类全量失败 16384 条/周期只进
    // stats 计数，排查时无错误码可看（本次死锁定位耗时长的直接缺口）。
    if (!load_fail_first.empty()) {
        LOG(ERROR) << "vsegment reconcile load failures: count="
                   << vseg_stats[4] << ", first=" << load_fail_first;
    }
    if (complete && !recovered_owned_partitions.empty()) {
        std::unordered_map<std::string,
                           std::vector<vsegment::VSegmentObjectReference>>
            references;
        VisitNamespaces([&](const TenantId& tenant_id,
                            metadata::Namespace& ns) {
            for (auto object : ns.objects.ReadCursor()) {
                const auto allocation_id =
                    tenant_id.MakeScopedKey(object.key());
                for (const auto& replica : object.metadata().GetAllReplicas()) {
                    if (!replica.is_vsegment_replica()) continue;
                    const auto& descriptor = replica.get_vsegment_descriptor();
                    if (!recovered_owned_partitions.count(
                            descriptor.partition_id))
                        continue;
                    if (replica.status() != ReplicaStatus::COMPLETE &&
                        replica.status() != ReplicaStatus::PROCESSING &&
                        replica.status() != ReplicaStatus::INITIALIZED)
                        continue;
                    references[descriptor.partition_id].push_back(
                        {descriptor, allocation_id,
                         replica.status() == ReplicaStatus::COMPLETE});
                }
            }
        });
        for (const auto& partition_id : recovered_owned_partitions) {
            std::string detail;
            const auto error = vsegment_service_->ReconcileObjectReferences(
                partition_id, references[partition_id], &detail);
            if (error != ErrorCode::OK) {
                LOG(ERROR) << "Failed to reconcile recovered vsegment "
                           << "allocations for Partition " << partition_id
                           << ": " << detail;
                complete = false;
            }
        }
    }
    if (complete) {
        // Keep state for pending imports; a per-slot acquire must not discard
        // the other Partitions recovered from the primary/standby snapshot.
        recovered_vsegment_snapshots_.erase(
            std::remove_if(recovered_vsegment_snapshots_.begin(),
                           recovered_vsegment_snapshots_.end(),
                           [&](const auto& state) {
                               return recovered_owned_partitions.count(
                                          state.partition_id) != 0;
                           }),
            recovered_vsegment_snapshots_.end());
    }
    // 周期摘要（变更触发，防刷屏）：任一统计与上一周期不同才打 INFO，稳态
    // 静默；绝不逐 partition 打印。验证 STALE_ROUTE 修复时关注 owned_by_me
    // 应 ≈ 本机 rank 段 partition 数、foreign 由对端持有、stale_cas 稳态为 0。
    if (vseg_stats[0] > 0) {
        bool changed = false;
        for (int i = 0; i < kVSegStatCount; ++i) {
            if (vseg_stats[i] !=
                vseg_stat_last_[i].load(std::memory_order_relaxed)) {
                changed = true;
                break;
            }
        }
        if (changed) {
            std::string detail;
            for (int i = 0; i < kVSegStatCount; ++i) {
                if (i != 0) detail += ", ";
                detail += std::string(kVSegStatNames[i]) + "=" +
                          std::to_string(vseg_stats[i]);
                vseg_stat_last_[i].store(vseg_stats[i],
                                         std::memory_order_relaxed);
            }
            LOG(INFO) << "vsegment reconcile stats (changed): " << detail;
        }
    }
    return complete ? ErrorCode::OK : ErrorCode::PERSISTENT_FAIL;
}

// 后台首建线程主体：Load →（快照缺失时触发发布侧重试，含 10s 节流与
// 30s 静默期门控）→ 构造发布 → 首次全量 sweep → kReady。失败复位 kIdle
// 等待心跳重触发；异常兜底复位 kIdle，绝不滞留 kInProgress 导致
// NOT_READY 永久卡死。init 锁与冷启动 DCL 互斥；门控期间运行期调用方全
// 部收 NOT_READY，本线程独占 reconcile（recovered_vsegment_snapshots_
// 等无并发访问面）。
void MasterService::VSegmentInitThreadFunc() {
    // 探测/首建全程持有独立 in-flight 标志（出口自动清除）。探测阶段绝不
    // 置 kInProgress：quota 缺席的重探对 slot 就绪无影响，gate 保持 kNoQuota
    // 放行。仅当 Load 成功（quota 确认存在）才进入 kInProgress——首建
    // （Construct + 首扫）期间阻塞 slot 是设计原意。
    struct ProbeGuard {
        std::atomic<bool>& flag;
        ~ProbeGuard() { flag.store(false, std::memory_order_release); }
    } probe_guard{vsegment_init_probe_in_flight_};
    try {
        std::lock_guard<std::mutex> init_lock(vsegment_service_init_mutex_);
        if (vsegment_service_) {
            // 双检：冷启动路径已构造（探测竞态输家），直接收敛 kReady。
            vsegment_init_state_.store(VSegmentInitState::kReady,
                                       std::memory_order_release);
            return;
        }
        vsegment::PartitionPhysicalQuotaSnapshot quota;
        std::string detail;
        vsegment::EtcdPartitionQuotaSnapshotStore store(cluster_id_);
        auto error = store.Load(&quota, &detail);
        if (error == ErrorCode::ETCD_KEY_NOT_EXIST &&
            MaybeRetryVSegmentAutoPlan("init")) {
            // 发布侧自愈成功：重读快照继续拉起。
            error = store.Load(&quota, &detail);
        }
        if (error == ErrorCode::ETCD_KEY_NOT_EXIST) {
            // 快照仍未发布（重试受节流/静默期门控属正常稳态）：置 kNoQuota
            // 终态语义——quota 缺席不阻塞 slot 就绪（门控放行），但仍允许
            // 每心跳一次的重探（本函数经 MaybeSpawnVSegmentInit 的
            // kNoQuota 分支重拉），使运行期发布后自动转换到 kReady。
            vsegment_init_state_.store(VSegmentInitState::kNoQuota,
                                       std::memory_order_release);
            return;
        }
        if (error != ErrorCode::OK) {
            LOG(WARNING) << "vsegment background init load failed: " << detail
                         << " (will retry on next heartbeat)";
            // etcd 读取失败同样不阻塞 slot（slot 就绪不依赖 vsegment）：
            // 置 kNoQuota 放行 + 每心跳重探，绝不置 kIdle 挂起 slot。
            vsegment_init_state_.store(VSegmentInitState::kNoQuota,
                                       std::memory_order_release);
            return;
        }
        // quota 确认存在：进入首建阶段——此刻起阻塞 slot（kInProgress），
        // 直至 Construct + 首扫完成（kReady）。
        vsegment_init_state_.store(VSegmentInitState::kInProgress,
                                   std::memory_order_release);
        ConstructVSegmentService(std::move(quota));
        LOG(INFO) << "vsegment service published via background init; "
                     "running first full reconcile sweep";
        const auto sweep_error = RunVSegmentOwnershipReconcile("");
        vsegment_init_state_.store(VSegmentInitState::kReady,
                                   std::memory_order_release);
        // 事件驱动重试加速：首建期间 slot 认领全部 NOT_READY 挂起，此处
        // Kick 立即触发下一轮 reconcile，不等满 5s 心跳周期。注意 Kick 只
        // 置位标志，若当前 PublishOnce 正在心跳线程执行（另一实例并发场
        // 景不会——本函数在 gate 门控期间独占），语义退化为周期后立即补跑。
        KickSlotHeartbeat();
        if (sweep_error != ErrorCode::OK) {
            // sweep 幂等：残留工作由周期 reconcile 自然收尾，不视为初始化
            // 失败（kReady 已置，运行期调用恢复正常）。
            LOG(WARNING) << "vsegment background init first sweep rc="
                         << toString(sweep_error)
                         << " (residual work picked up by periodic reconcile)";
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "vsegment background init crashed: " << e.what()
                   << " (state set to kNoQuota, slots keep proceeding; "
                      "will retry on next heartbeat)";
        // 崩溃同样不永久挂起 slot：kNoQuota 放行 + 重探。
        vsegment_init_state_.store(VSegmentInitState::kNoQuota,
                                   std::memory_order_release);
    }
}

// 拉起后台 init/重探线程（幂等）。防重入使用独立的
// vsegment_init_probe_in_flight_ 原子标志——★绝不 CAS 修改
// vsegment_init_state_★：把防重入编码进 state（kIdle/kNoQuota →
// kInProgress 的 CAS）曾使 slot 认领热路径上的每次放行都制造一个
// kInProgress 窗口（init 线程的 etcd Get），把 slot 就绪压到 ~1 个/心跳。
// 现在 spawn 对 gate 完全不可见：kNoQuota 探测期间 state 保持 kNoQuota，
// slot 持续放行；仅 VSegmentInitThreadFunc 在 Load 确认 quota 存在后
// 才置 kInProgress（首建阶段阻塞 slot 属设计原意）。
void MasterService::MaybeSpawnVSegmentInit() {
    auto state = vsegment_init_state_.load(std::memory_order_acquire);
    if (state == VSegmentInitState::kReady) {
        // kReady 蕴含 vsegment_service_ 已发布；若此时仍为空（不变式被破
        // 坏），自愈回 kIdle 重新拉起，绝不 fallthrough 空指针解引用。
        if (vsegment_service_ != nullptr) return;
        vsegment_init_state_.store(VSegmentInitState::kIdle,
                                   std::memory_order_release);
        state = VSegmentInitState::kIdle;
    }
    if (state == VSegmentInitState::kInProgress) {
        return;  // 首建进行中：init 线程自身管理状态，无需重探
    }
    if (vsegment_init_probe_in_flight_.exchange(true,
                                                std::memory_order_acq_rel)) {
        return;  // 探测线程已在跑
    }
    std::lock_guard<std::mutex> spawn_lock(vsegment_init_spawn_mutex_);
    if (vsegment_init_thread_.joinable()) {
        // 回收上一轮已结束未 join 的线程（in-flight 标志保证其已退出）。
        vsegment_init_thread_.join();
    }
    try {
        vsegment_init_thread_ =
            std::thread([this] { VSegmentInitThreadFunc(); });
        // 等待窗口期间每心跳周期都会重触发拉起，VLOG(1) 防刷屏；发布/首扫
        // 等关键节点已有 INFO（见 VSegmentInitThreadFunc）。
        VLOG(1) << "vsegment background init thread spawned";
    } catch (const std::system_error& e) {
        // 线程创建失败（资源耗尽等）：清除 in-flight，下一心跳重试。
        LOG(ERROR) << "vsegment background init thread spawn failed: "
                   << e.what() << " (will retry next heartbeat)";
        vsegment_init_probe_in_flight_.store(false, std::memory_order_release);
    }
}

ErrorCode MasterService::StartInterMasterRpc() {
    const bool cvm_enabled = enable_ha_ && ha_backend_type_ == "etcd" &&
                             !master_id_.empty() && !cluster_id_.empty();
    if (!cvm_enabled) {
        return ErrorCode::OK;
    }

    // The supervisor's CvmController already connected the etcd client;
    // re-connecting is an idempotent no-op.
    ErrorCode connect_err =
        EtcdHelper::ConnectToEtcdStoreClient(ha_backend_connstring_);
    if (connect_err != ErrorCode::OK) {
        LOG(WARNING) << "StartInterMasterRpc: failed to connect etcd: "
                     << connect_err;
        return connect_err;
    }

    if (inter_master_rpc_) {
        return ErrorCode::OK;  // already running
    }

    inter_master_rpc_ = std::make_unique<cvm::InterMasterRpcClient>();
    ErrorCode rc = inter_master_rpc_->Start(cluster_id_, master_id_);
    if (rc != ErrorCode::OK) {
        LOG(WARNING) << "StartInterMasterRpc: refresh loop not started: " << rc
                     << " (manual member updates still work)";
        // Keep the client object for manual member updates; only the
        // etcd-driven refresh thread is unavailable.
    }
    LOG(INFO) << "Started InterMasterRpcClient: master_id=" << master_id_
              << ", cluster_namespace=" << cluster_id_;

    // P4 reshard driver（§16.19.1）：目标侧状态机线程，随 inter-master RPC
    // 生命周期启停（driver 的拉取/ack 依赖 inter_master_rpc_）。cvm_controller_
    // 未注入/缓存未就绪时 DriveReshardIntent 自行跳过等待，无顺序依赖。
    if (!reshard_driver_running_.exchange(true)) {
        reshard_driver_thread_ =
            std::thread(&MasterService::ReshardDriverLoop, this);
    }
    return ErrorCode::OK;
}

void MasterService::StopInterMasterRpc() {
    if (inter_master_rpc_) {
        inter_master_rpc_->Stop();
        inter_master_rpc_.reset();
    }
}

uint32_t MasterService::GetOwnedSlotCount() const {
    std::shared_lock<std::shared_mutex> lock(owned_slots_mutex_);
    if (!owned_slots_ready_) {
        return 0;
    }
    return static_cast<uint32_t>(
        std::count(ready_owned_.begin(), ready_owned_.end(), true));
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::InterMasterAllocateReplicas(
    const std::string& tenant_id, const std::string& key, uint64_t slice_length,
    uint64_t replica_num, const std::vector<std::string>& preferred_segments) {
    if (key.empty() || slice_length == 0 || replica_num == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ScopedAllocatorAccess allocator_access =
        segment_manager_.getAllocatorAccess();
    const auto& allocator_manager = allocator_access.getAllocatorManager();
    const auto& local_names = allocator_manager.getNames();
    if (local_names.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Strict mode: when preferred segments are given, only allocate in them.
    // Exclude every other local segment so the strategy's random fallback
    // cannot leak the allocation into unrelated segments.
    std::set<std::string> excluded;
    if (!preferred_segments.empty()) {
        const std::unordered_set<std::string> preferred(
            preferred_segments.begin(), preferred_segments.end());
        bool any_preferred_local = false;
        for (const auto& name : local_names) {
            if (preferred.count(name) > 0) {
                any_preferred_local = true;
            } else {
                excluded.insert(name);
            }
        }
        if (!any_preferred_local) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
    }

    auto allocation = allocation_strategy_->Allocate(
        allocator_manager, slice_length, replica_num, preferred_segments,
        excluded, ReplicaType::MEMORY);
    if (!allocation.has_value()) {
        return tl::make_unexpected(allocation.error());
    }
    // Partial allocation is not forwarded: the slot owner requires the full
    // replica count. Dropped here => destructors free the partial handles.
    if (allocation->size() != replica_num) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    std::vector<Replica::Descriptor> descriptors;
    descriptors.reserve(allocation->size());
    for (const auto& replica : allocation.value()) {
        descriptors.push_back(replica.get_descriptor());
    }
    const std::string scoped_key = TenantId(tenant_id).MakeScopedKey(key);
    {
        std::lock_guard<std::mutex> lock(inter_master_keepalive_mutex_);
        // Replace any stale keepalive entry for the same key: erasing frees
        // the old handles (idempotent re-PutStart after an aborted write).
        inter_master_keepalive_.erase(scoped_key);
        inter_master_keepalive_.emplace(scoped_key,
                                        std::move(allocation.value()));
    }
    LOG(INFO) << "InterMasterAllocateReplicas: allocated " << descriptors.size()
              << " replica(s) for scoped_key=" << scoped_key
              << ", slice_length=" << slice_length
              << ", preferred_segments=" << preferred_segments.size();
    return descriptors;
}

tl::expected<bool, ErrorCode> MasterService::InterMasterFreeReplicas(
    const std::string& tenant_id, const std::string& key) {
    const std::string scoped_key = TenantId(tenant_id).MakeScopedKey(key);
    std::vector<Replica> released;
    {
        std::lock_guard<std::mutex> lock(inter_master_keepalive_mutex_);
        auto it = inter_master_keepalive_.find(scoped_key);
        if (it == inter_master_keepalive_.end()) {
            return false;  // not the owner of this key's handles
        }
        released = std::move(it->second);
        inter_master_keepalive_.erase(it);
    }
    // Replica destructors free the handles at this (segment owning) master.
    LOG(INFO) << "InterMasterFreeReplicas: freed " << released.size()
              << " replica(s) for scoped_key=" << scoped_key;
    return true;
}

tl::expected<GetReplicaListResponse, ErrorCode>
MasterService::InterMasterGetReplicaList(const std::string& key,
                                         const std::string& tenant_id) {
    const TenantId tenant(tenant_id);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant);
    // peer 互信：不校验 OwnsSlot、不再次转发，直接本地查询（转发链止于
    // 第一跳，避免视图不一致时的循环转发）。
    return GetReplicaListLocal(object_id);
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::InterMasterBatchGetReplicaList(
    const std::vector<std::string>& keys, const std::string& tenant_id) {
    const TenantId tenant(tenant_id);
    return BatchGetReplicaListLocal(keys, tenant);
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::InterMasterPutStart(const UUID& client_id,
                                   const std::string& key,
                                   const std::string& tenant_id,
                                   uint64_t slice_length,
                                   const ReplicateConfig& config) {
    const TenantId tenant(tenant_id);
    // peer 互信：调用方已按 slot 归属解析本机为 owner，直接执行完整本地
    // PutStart（分配 + 写元数据 + keepalive）。本机 OwnsSlot==true，不会再
    // 二次转发（转发链止于第一跳，避免视图不一致时的循环转发）。
    return PutStart(client_id, key, tenant, slice_length, config);
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::InterMasterUpsertStart(const UUID& client_id,
                                      const std::string& key,
                                      const std::string& tenant_id,
                                      uint64_t slice_length,
                                      const ReplicateConfig& config) {
    const TenantId tenant(tenant_id);
    // Upsert 转发：本机为 slot owner，执行完整本地 UpsertStart 以保留
    // "已存在则覆盖（preemption）"语义；由 PutStart 转发走 PutStart 会丢失
    // 该覆盖语义。peer 互信，不二次转发。
    return UpsertStart(client_id, key, tenant, slice_length, config);
}

tl::expected<std::vector<Replica>, ErrorCode>
MasterService::TryAllocateReplicasRemotely(
    const std::string& key, const TenantId& tenant_id, uint64_t value_length,
    size_t replica_num, const std::vector<std::string>& preferred_segments) {
#ifdef STORE_USE_ETCD
    if (!inter_master_rpc_ || replica_num == 0) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    const std::string tenant_str = tenant_id.value();
    for (const auto& member : inter_master_rpc_->GetMembers()) {
        if (member.master_id.empty() || member.master_id == master_id_) {
            continue;
        }
        auto remote = inter_master_rpc_->AllocateReplicas(
            member.master_id, tenant_str, key, value_length,
            static_cast<uint64_t>(replica_num), preferred_segments);
        if (!remote.has_value()) {
            VLOG(1) << "Remote allocation on " << member.master_id
                    << " failed for key=" << key << ": "
                    << toString(remote.error());
            continue;
        }
        if (remote->size() < replica_num) {
            // Partial result: free it at the peer and keep probing.
            inter_master_rpc_->FreeReplicas(member.master_id, tenant_str, key);
            continue;
        }

        // Materialize dummy-allocator replicas from the descriptors (same
        // pattern as ImportSlotMetadata). The real handles stay alive at the
        // segment owner's keepalive registry.
        std::vector<Replica> replicas;
        replicas.reserve(remote->size());
        for (const auto& desc : remote.value()) {
            if (!desc.is_memory_replica()) {
                continue;
            }
            const auto& mem_desc = desc.get_memory_descriptor();
            const std::string& endpoint =
                mem_desc.buffer_descriptor.transport_endpoint_;
            std::shared_ptr<DummyBufferAllocator> alloc;
            {
                std::lock_guard<std::mutex> lock(
                    remote_replica_allocator_keepalive_mutex_);
                auto& slot = remote_replica_allocator_keepalive_[endpoint];
                if (!slot) {
                    slot = std::make_shared<DummyBufferAllocator>(endpoint,
                                                                  endpoint);
                }
                alloc = slot;
            }
            replicas.emplace_back(std::make_unique<AllocatedBuffer>(
                                      alloc, mem_desc.buffer_descriptor),
                                  desc.status);
        }
        if (replicas.size() != replica_num) {
            // Unexpected descriptor types: undo at the peer and fail.
            inter_master_rpc_->FreeReplicas(member.master_id, tenant_str, key);
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(remote_allocated_keys_mutex_);
            remote_allocated_keys_.insert(tenant_id.MakeScopedKey(key));
        }
        LOG(INFO) << "Allocated " << replicas.size()
                  << " remote replica(s) for key=" << key
                  << " on segment owner " << member.master_id;
        return replicas;
    }
    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
#else
    (void)key;
    (void)tenant_id;
    (void)value_length;
    (void)replica_num;
    (void)preferred_segments;
    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
#endif
}

void MasterService::EnqueueRemoteFreeIfTracked(const TenantId& tenant_id,
                                               const std::string& key,
                                               EraseMode quota_mode) {
#ifdef STORE_USE_ETCD
    {
        std::lock_guard<std::mutex> lock(remote_allocated_keys_mutex_);
        if (remote_allocated_keys_.erase(tenant_id.MakeScopedKey(key)) == 0) {
            return;  // handles (if any) are local
        }
    }
    if (quota_mode == EraseMode::kHandoff) {
        // Slot migration: the data bytes live on. The importing master
        // re-registers the key and owns the remote free from now on.
        return;
    }
    if (inter_master_rpc_) {
        inter_master_rpc_->EnqueueBroadcastFree(tenant_id.value(), key);
    }
#endif
}

tl::expected<SlotMetadataExport, ErrorCode>
MasterService::BuildSlotMetadataExport(uint16_t slot, bool require_not_owned) {
    auto batch = BuildSlotMetadataExportBatch(slot, slot, require_not_owned);
    if (!batch) return tl::make_unexpected(batch.error());
    if (batch->empty()) return tl::make_unexpected(ErrorCode::SLOT_MIGRATING);
    return std::move(batch->front());
}

tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
MasterService::BuildSlotMetadataExportBatch(uint16_t first_slot,
                                            uint16_t last_slot,
                                            bool require_not_owned) {
    if (first_slot > last_slot || last_slot >= cvm::SlotCount())
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    std::shared_ptr<vsegment::VSegmentService> service;
    {
        std::lock_guard<std::mutex> init_lock(vsegment_service_init_mutex_);
        service = vsegment_service_;
    }
    std::unique_lock<std::shared_mutex> lock(master_state_mutex_);
    std::vector<SlotMetadataExport> batch;
    const size_t count = static_cast<size_t>(last_slot) - first_slot + 1;
    batch.reserve(count);
    std::vector<int> missing(count, -1);
    for (uint32_t slot = first_slot; slot <= last_slot; ++slot) {
        const auto key = static_cast<uint16_t>(slot);
        if (require_not_owned &&
            CheckSlotServiceability(key) != ErrorCode::SLOT_NOT_OWNED)
            continue;
        {
            std::lock_guard<std::mutex> staged_lock(
                pending_slot_exports_mutex_);
            auto found = pending_slot_exports_.find(key);
            if (found != pending_slot_exports_.end()) {
                batch.push_back(found->second);
                continue;
            }
        }
        SlotMetadataExport payload;
        payload.slot = key;
        payload.source_master_id = master_id_;
        if (service) {
            vsegment::PartitionVSegmentSnapshot state;
            const auto error =
                service->SnapshotPartition(std::to_string(slot), &state);
            if (error == ErrorCode::OK)
                payload.vsegment_partition = std::move(state);
            else if (error != ErrorCode::STALE_ROUTE)
                return tl::make_unexpected(error);
        }
        missing[slot - first_slot] = static_cast<int>(batch.size());
        batch.push_back(std::move(payload));
    }
    // The route cursor pins each publication while its descriptors are copied.
    if (std::any_of(missing.begin(), missing.end(),
                    [](int index) { return index >= 0; })) {
        VisitNamespaces([&](const TenantId& tenant_id,
                            metadata::Namespace& ns) {
            for (auto object : ns.objects.ReadCursor()) {
                const auto slot = cvm::KeySlot(tenant_id, object.key());
                if (slot < first_slot || slot > last_slot) continue;
                const int index = missing[slot - first_slot];
                if (index < 0) continue;
                const auto& metadata = object.metadata();
                StandbyObjectEntry entry;
                entry.tenant_id = tenant_id.value();
                entry.key = object.key();
                entry.metadata.client_id = metadata.client_id;
                entry.metadata.size = metadata.TotalLength();
                entry.metadata.group_id = metadata.group_id;
                entry.metadata.data_type = metadata.data_type;
                entry.metadata.hard_pinned = metadata.hard_pinned;
                entry.metadata.session_id = metadata.session_id;
                entry.metadata.object_checksum = metadata.GetPart(0).checksum();
                entry.metadata.residency_affinity_id =
                    metadata.residency_affinity_id;
                for (const auto& replica : metadata.GetAllReplicas())
                    entry.metadata.replicas.push_back(replica.get_descriptor());
                batch[index].objects.push_back(std::move(entry));
            }
        });
    }
    // Cache while still holding master_state_mutex_, so ACK cannot race
    // staging.
    std::lock_guard<std::mutex> staged_lock(pending_slot_exports_mutex_);
    for (const auto& payload : batch)
        pending_slot_exports_.try_emplace(payload.slot, payload);
    return batch;
}

ErrorCode MasterService::DropSlotMetadataLocal(uint16_t slot) {
    return DropSlotMetadataRange(slot, slot);
}

ErrorCode MasterService::ExportSlotMetadata(uint16_t slot) {
    auto payload = BuildSlotMetadataExport(slot);
    return payload ? ErrorCode::OK : payload.error();
}

tl::expected<SlotMetadataExport, ErrorCode>
MasterService::InterMasterExportSlot(
    uint16_t slot, const std::string& /*requester_master_id*/) {
    return BuildSlotMetadataExport(slot);
}

tl::expected<bool, ErrorCode> MasterService::InterMasterAckSlotImported(
    uint16_t slot, const std::string& /*importer_master_id*/) {
    std::unique_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    if (CheckSlotServiceability(slot) != ErrorCode::SLOT_NOT_OWNED) {
        return tl::make_unexpected(ErrorCode::SLOT_MIGRATING);
    }
    bool had_staged = false;
    {
        std::lock_guard<std::mutex> lock(pending_slot_exports_mutex_);
        had_staged = pending_slot_exports_.erase(slot) > 0;
    }
    ErrorCode err = DropSlotMetadataLocal(slot);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "InterMasterAckSlotImported: drop failed slot=" << slot
                     << ", err=" << err;
        return tl::make_unexpected(err);
    }
    if (vsegment_service_) {
        const auto partition_id = std::to_string(slot);
        vsegment::PartitionVSegmentSnapshot state;
        err = vsegment_service_->SnapshotPartition(partition_id, &state);
        if (err == ErrorCode::OK) {
            err = vsegment_service_->RemovePartition(partition_id,
                                                     state.route_epoch);
            if (err != ErrorCode::OK) return tl::make_unexpected(err);
        } else if (err != ErrorCode::STALE_ROUTE) {
            return tl::make_unexpected(err);
        }
    }
    return had_staged;
}

tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
MasterService::InterMasterExportSlotBatch(
    uint16_t first_slot, uint16_t last_slot,
    const std::string& /*requester_master_id*/) {
    return BuildSlotMetadataExportBatch(first_slot, last_slot, false);
}

tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
MasterService::InterMasterExportSlotBatchGated(
    uint16_t first_slot, uint16_t last_slot,
    const std::string& /*requester_master_id*/) {
    return BuildSlotMetadataExportBatch(first_slot, last_slot, true);
}

ErrorCode MasterService::DropSlotMetadataRange(uint16_t first_slot,
                                               uint16_t last_slot) {
    if (first_slot > last_slot || last_slot >= cvm::SlotCount())
        return ErrorCode::INVALID_PARAMS;
    VisitNamespaces([&](const TenantId& tenant_id, metadata::Namespace& ns) {
        std::vector<route::ObjectRef> candidates;
        for (auto object : ns.objects.ReadCursor()) {
            const auto slot = cvm::KeySlot(tenant_id, object.key());
            if (slot >= first_slot && slot <= last_slot)
                candidates.push_back(object.ref());
        }
        // Release the cursor before acquiring any object's write lock.
        for (const auto& ref : candidates) {
            MetadataAccessorRW accessor(this, tenant_id, ref);
            if (accessor.IsPublished()) accessor.Erase(EraseMode::kHandoff);
        }
    });
    return ErrorCode::OK;
}

tl::expected<bool, ErrorCode> MasterService::InterMasterAckSlotRangeImported(
    uint16_t first_slot, uint16_t last_slot,
    const std::string& /*importer_master_id*/) {
    // §16.16.2 阶段4/5：目标安装完成后通知源清理。段内所有 slot 必须已
    // 观察到归属切走（本机 expected/ready 位图随心跳 resolver 刷新）；
    // 否则返回 SLOT_MIGRATING，目标稍后重试（源不清数据，重发无损）。
    std::unique_lock<std::shared_mutex> lock(master_state_mutex_);
    for (uint32_t slot = first_slot; slot <= last_slot; ++slot) {
        if (CheckSlotServiceability(static_cast<uint16_t>(slot)) !=
            ErrorCode::SLOT_NOT_OWNED) {
            return tl::make_unexpected(ErrorCode::SLOT_MIGRATING);
        }
    }
    bool had_staged = false;
    {
        std::lock_guard<std::mutex> staged_lock(pending_slot_exports_mutex_);
        for (uint32_t slot = first_slot; slot <= last_slot; ++slot) {
            had_staged =
                pending_slot_exports_.erase(static_cast<uint16_t>(slot)) > 0 ||
                had_staged;
        }
    }
    ErrorCode err = DropSlotMetadataRange(first_slot, last_slot);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "InterMasterAckSlotRangeImported: drop failed range=["
                     << first_slot << "," << last_slot << "], err=" << err;
        return tl::make_unexpected(err);
    }
    if (vsegment_service_) {
        for (uint32_t slot = first_slot; slot <= last_slot; ++slot) {
            const auto partition_id = std::to_string(slot);
            vsegment::PartitionVSegmentSnapshot state;
            err = vsegment_service_->SnapshotPartition(partition_id, &state);
            if (err == ErrorCode::OK) {
                err = vsegment_service_->RemovePartition(
                    partition_id, state.route_epoch, false);
                if (err != ErrorCode::OK) return tl::make_unexpected(err);
            } else if (err != ErrorCode::STALE_ROUTE) {
                return tl::make_unexpected(err);
            }
        }
    }
    if (vsegment_service_) vsegment_service_->RefreshMetrics(true);
    // 段级汇总一条日志（区间压缩语义），不逐 slot 打印。
    LOG(INFO) << "InterMasterAckSlotRangeImported: dropped slot range ["
              << first_slot << "," << last_slot << "]"
              << (had_staged ? " (staged exports cleared)" : "");
    return had_staged;
}

ErrorCode MasterService::ImportSlotMetadata(
    uint16_t slot, const SlotMetadataExport* prefetched_export,
    vsegment::VSegmentCommitBatch* batch) {
    if (vsegment_runtime_gate_active_.load(std::memory_order_acquire) &&
        vsegment_init_state_.load(std::memory_order_acquire) !=
            VSegmentInitState::kReady)
        return RefreshVSegmentOwnership(std::to_string(slot), batch);
    // 环互备领养 staging 门控（互备 1.4 ①）：slot 所在 rank 处于
    // ring-adopt staging（primary=本机且 kMigrating、migrating_to 为空）
    // 期间暂缓导入与就绪——物化由 supervisor RingStandbyRuntime 编排
    // （OpLog 流重放 + P4 过滤），kMigrating→kStable 收尾后本路径随
    // 下轮心跳重试自然通过。staging 窗口内提前 RefreshVSegmentOwnership
    // 有重建 allocator 覆盖存活 extent 的风险（4.2 fail-closed 语义），
    // 提前 MarkSlotsReady 则半物化 serve；返回 SLOT_MIGRATING 挂起
    // pending，SlotOwnerHeartbeat 下轮自动重试（on_slot_acquired 非 OK
    // 语义）。缓存读取零 etcd 开销；签名（primary=本机 + migrating_to
    // 空）与 reshard 阶段 1（primary=源、migrating_to=目标）不相交。
    if (cvm_controller_ != nullptr) {
        const uint32_t group_count = cvm_controller_->GetCachedSlotGroupCount();
        cvm::RingSlotAssign assign;
        if (group_count > 0 &&
            cvm_controller_->GetCachedRingSlotAssign(
                cvm::SlotToRank(slot, group_count), assign) &&
            static_cast<cvm::SlotState>(assign.state) ==
                cvm::SlotState::kMigrating &&
            assign.primary_id == master_id_ && assign.migrating_to_id.empty()) {
            return ErrorCode::SLOT_MIGRATING;
        }
    }
    // 推导迁移前一任 owner（旧环）。空环 / 无旧 owner（冷启动、旧 owner
    // 消亡）→ 无可拉取对象，直接视为就绪（元数据为空，客户端重建）。
    std::vector<std::string> prev_ids;
    std::vector<std::string> primary_ids;
    std::vector<std::string> alive_ids;
    ViewVersionId ring_revision = 0;
    {
        std::lock_guard<std::mutex> lock(cvm_resolver_mutex_);
        prev_ids = cvm_prev_primary_ids_;
        primary_ids = cvm_last_primary_ids_;
        ring_revision = cvm_ring_revision_;
        alive_ids = cvm_alive_master_ids_;
    }
    const std::string old_owner = cvm::ResolveSlotOwnerOnRing(prev_ids, slot);
    // old_owner 已从存活成员集中消亡（lease 过期删 key）时，无法再 RPC 拉取其
    // 元数据，应视为空元数据、由客户端重建，而不是去拉一个已死节点——否则会
    // 一直 INVALID_PARAMS 且每轮重试刷屏。
    const bool old_owner_gone =
        !alive_ids.empty() && std::find(alive_ids.begin(), alive_ids.end(),
                                        old_owner) == alive_ids.end();
    if (old_owner_gone && !old_owner.empty() && old_owner != master_id_ &&
        enable_oplog_) {
        auto recovery = TryRecoverFromDeadOwnerOpLog(old_owner);
        if (!recovery) {
            return recovery.error();
        }
    }
    if (old_owner_gone && !old_owner.empty() && old_owner != master_id_ &&
        vsegment_service_) {
        // 缺陷4修复：死 owner 的 OpLog 流仍持久在 etcd（compaction 窗口内），
        // 先重放恢复（RestoreFromStandbySnapshot 内 P4 过滤只落本机 owned
        // slots），恢复态同时填充 recovered_vsegment_snapshots_，使下方
        // vsegment 防御校验通过。重放不可用（未启用 / 越窗 / 流不存在）时
        // 回退旧语义：元数据判空、客户端重建。
        const auto partition_id = std::to_string(slot);
        const bool has_quota =
            vsegment_service_->HasPartitionQuota(partition_id);
        const bool has_recovered_state = std::any_of(
            recovered_vsegment_snapshots_.begin(),
            recovered_vsegment_snapshots_.end(), [&](const auto& state) {
                return state.partition_id == partition_id;
            });
        vsegment::PartitionVSegmentSnapshot installed;
        if (has_quota && !has_recovered_state &&
            vsegment_service_->SnapshotPartition(partition_id, &installed) !=
                ErrorCode::OK) {
            // A dead owner does not make its physical quota empty. Recreating
            // an allocator without its snapshot could overwrite live extents.
            // 4.2 修复：保留 fail-closed 语义（重建 allocator 可能覆盖存活
            // extent，风险高于暂不可用），但分区级日志去重，防 heartbeat
            // 周期重试刷屏。
            bool already_warned = false;
            {
                std::lock_guard<std::mutex> lock(oplog_replay_mutex_);
                // 上限保护：多 source × 多 partition 场景防集合无界增长
                // （16384 slots × masters 量级）。达到上限即清空重新收集
                // ——宁可短暂恢复逐次告警，不可丢 PERSISTENT_FAIL 信号。
                constexpr size_t kMaxMissingRecoveryWarnedPartitions = 1 << 20;
                if (missing_recovery_warned_partitions_.size() >=
                    kMaxMissingRecoveryWarnedPartitions) {
                    LOG(WARNING)
                        << "missing_recovery_warned_partitions_ reached "
                           "cap, resetting";
                    missing_recovery_warned_partitions_.clear();
                }
                already_warned =
                    !missing_recovery_warned_partitions_.emplace(partition_id)
                         .second;
            }
            if (!already_warned) {
                LOG(ERROR) << "Missing vsegment recovery state for lost "
                              "owner (retrying silently until recoverable), "
                           << "owner=" << old_owner
                           << ", Partition=" << partition_id;
            }
            return ErrorCode::PERSISTENT_FAIL;
        }
    }
    if (old_owner.empty() || old_owner == master_id_ || old_owner_gone) {
        if (old_owner == master_id_) {
            // 缺陷3补救：自身原主（重启后重新认领旧 slot）。supervisor 侧
            // 自恢复未覆盖（如瞬态失败回退）时，此处按 old_owner=self 从
            // 本机流补救重放一次；定局登记防重复。
            auto recovery = TryRecoverFromDeadOwnerOpLog(master_id_);
            if (!recovery) {
                return recovery.error();
            }
        }
        // 无旧 owner（冷启动 / 旧 owner 消亡 / 自身原主）：无可 RPC 拉取，
        // OpLog 重放已尽力（上方分支），剩余情形元数据视为空，客户端重建。
        return RefreshVSegmentOwnership(std::to_string(slot), batch);
    }

    // RPC 直传拉取旧 owner 的 slot 元数据导出（不落 etcd）。批量路径
    // （OnSlotRangeAcquired）传入已段级拉取的 prefetched 导出，跳过逐 slot
    // RPC；prefetched 模式下逐 slot ack 同样跳过——由调用方以段级
    // AckSlotRangeImported 收口（一次 RPC 替代逐 slot ack）。
    tl::expected<SlotMetadataExport, ErrorCode> pull_storage;
    const SlotMetadataExport* export_payload = nullptr;
    if (prefetched_export != nullptr) {
        if (prefetched_export->slot != slot) {
            LOG(ERROR) << "ImportSlotMetadata: prefetched export slot "
                          "mismatch, expected="
                       << slot << ", got=" << prefetched_export->slot;
            return ErrorCode::INVALID_PARAMS;
        }
        export_payload = prefetched_export;
    } else {
        if (!inter_master_rpc_) {
            LOG(WARNING) << "ImportSlotMetadata: inter_master_rpc_ not ready, "
                            "will retry slot="
                         << slot;
            return ErrorCode::INVALID_PARAMS;
        }
        pull_storage =
            inter_master_rpc_->ExportSlot(old_owner, slot, master_id_);
        if (!pull_storage.has_value()) {
            LOG(WARNING) << "ImportSlotMetadata: RPC pull failed slot=" << slot
                         << ", old_owner=" << old_owner
                         << ", err=" << toString(pull_storage.error());
            return pull_storage.error();
        }
        export_payload = &pull_storage.value();
    }

    // 路径路由（§16.17.2 后与客户端同模型）：ring_slots 优先、缓存空回退
    // 成员环推导；owner 必须是本机——本机因归属变化 gained 该 slot 才会
    // 进入此处。安装主体已抽取到 InstallSlotMetadataExport，供单 slot
    // 交接与 P4 段级批量导入复用。
    partition::PartitionRoute route;
    ViewVersionId route_version = 0;
    cvm::RingSlotsView ring_slots;
    if (cvm_controller_ != nullptr) {
        ring_slots = cvm_controller_->BuildRingSlotsView();
    }
    auto route_error = LoadEffectivePartitionRoute(
        cluster_id_, std::to_string(slot), route, route_version, primary_ids,
        ring_revision, ring_slots);
    if (route_error != ErrorCode::OK) {
        return route_error;
    }

    const ErrorCode install_error =
        InstallSlotMetadataExport(*export_payload, route, batch);
    if (install_error != ErrorCode::OK) {
        return install_error;
    }

    return ErrorCode::OK;
}

ErrorCode MasterService::OnSlotRangeAcquired(uint16_t first_slot,
                                             uint16_t last_slot,
                                             std::vector<ErrorCode>& results) {
    if (first_slot > last_slot || last_slot >= cvm::SlotCount())
        return ErrorCode::INVALID_PARAMS;
    const size_t count = static_cast<size_t>(last_slot) - first_slot + 1;
    results.assign(count, ErrorCode::SLOT_MIGRATING);
    std::vector<std::string> previous, alive;
    {
        std::lock_guard<std::mutex> lock(cvm_resolver_mutex_);
        previous = cvm_prev_primary_ids_;
        alive = cvm_alive_master_ids_;
    }
    std::unordered_set<uint32_t> migrating_ranks;
    uint32_t group_count = 0;
    if (cvm_controller_ && cvm_controller_->HasCachedRingSlotAssigns()) {
        group_count = cvm_controller_->GetCachedSlotGroupCount();
        std::vector<cvm::ReshardIntent> intents;
        ViewVersionId version = 0;
        const auto error = cvm::EtcdViewStore::LoadAllReshardIntents(
            cluster_id_, intents, version);
        if (error != ErrorCode::OK && error != ErrorCode::ETCD_KEY_NOT_EXIST) {
            results.assign(count, error);
            return ErrorCode::OK;
        }
        for (const auto& intent : intents) {
            if (intent.target_primary_id == master_id_ &&
                !intent.source_primary_id.empty() &&
                intent.source_primary_id != master_id_)
                migrating_ranks.insert(intent.rank);
        }
    }
    auto owner_of = [&](uint16_t slot) {
        return cvm::ResolveSlotOwnerOnRing(previous, slot);
    };
    // Bounded chunks also batch empty/cold-start slots. Installation never
    // publishes readiness or deletes source state before the durable barrier.
    for (size_t begin = 0; begin < count;) {
        const auto first = static_cast<uint16_t>(first_slot + begin);
        const auto owner = owner_of(first);
        size_t end = begin + 1;
        while (end < count && end - begin < 128 &&
               owner_of(static_cast<uint16_t>(first_slot + end)) == owner)
            ++end;
        const bool remote =
            !owner.empty() && owner != master_id_ &&
            (alive.empty() ||
             std::find(alive.begin(), alive.end(), owner) != alive.end());
        std::vector<SlotMetadataExport> payloads;
        if (remote && inter_master_rpc_ && end - begin > 1) {
            auto pull = inter_master_rpc_->ExportSlotBatchGated(
                owner, first, static_cast<uint16_t>(first_slot + end - 1),
                master_id_);
            if (pull) payloads = std::move(*pull);
        }
        std::unordered_map<uint16_t, const SlotMetadataExport*> prefetched;
        for (const auto& payload : payloads)
            prefetched.emplace(payload.slot, &payload);
        vsegment::VSegmentCommitBatch batch;
        std::unordered_set<uint16_t> acknowledge;
        for (size_t i = begin; i < end; ++i) {
            const auto slot = static_cast<uint16_t>(first_slot + i);
            if (OwnsSlot(slot)) {
                results[i] = ErrorCode::OK;
                continue;
            }
            if (group_count &&
                migrating_ranks.count(cvm::SlotToRank(slot, group_count)))
                continue;
            const auto found = prefetched.find(slot);
            const auto* payload =
                found == prefetched.end() ? nullptr : found->second;
            results[i] = ImportSlotMetadata(slot, payload, &batch);
            if (results[i] == ErrorCode::TASK_PENDING_LIMIT_EXCEEDED &&
                batch.Flush() == ErrorCode::OK)
                results[i] = ImportSlotMetadata(slot, payload, &batch);
            if (results[i] == ErrorCode::OK) acknowledge.insert(slot);
        }
        const auto durable = batch.Flush();
        std::vector<uint16_t> ready;
        for (size_t i = begin; i < end; ++i) {
            if (results[i] != ErrorCode::OK) continue;
            results[i] = durable;
            if (durable == ErrorCode::OK)
                ready.push_back(static_cast<uint16_t>(first_slot + i));
        }
        MarkSlotsReady(ready);
        // ACK only successfully installed, durable contiguous runs. Singles
        // and fallback pulls share this path, including old RPC peers.
        ready.erase(std::remove_if(ready.begin(), ready.end(),
                                   [&](uint16_t slot) {
                                       return acknowledge.count(slot) == 0;
                                   }),
                    ready.end());
        if (remote && inter_master_rpc_) {
            for (size_t a = 0; a < ready.size();) {
                size_t b = a;
                while (b + 1 < ready.size() && ready[b + 1] == ready[b] + 1)
                    ++b;
                auto ack = a == b ? inter_master_rpc_->AckSlotImported(
                                        owner, ready[a], master_id_)
                                  : inter_master_rpc_->AckSlotRangeImported(
                                        owner, ready[a], ready[b], master_id_);
                if (!ack)
                    LOG(WARNING) << "Slot handoff ACK failed: " << ack.error();
                a = b + 1;
            }
        }
        begin = end;
    }
    if (vsegment_init_state_.load(std::memory_order_acquire) ==
            VSegmentInitState::kReady &&
        vsegment_service_)
        vsegment_service_->RefreshMetrics(true);
    return ErrorCode::OK;
}

ErrorCode MasterService::ImportSlotRangeFrom(
    const std::string& source_master_id, uint32_t rank, uint16_t first_slot,
    uint16_t last_slot) {
    // §16.19.2 段级拉取 + 安装（P4 reshard 驱动主体）。调用前置：owner CAS
    // 已完成（ring_slots[rank].primary_id == 本机），源在 ack 前保留全部
    // 未清数据（重拉幂等）；快照稳定由两重保证——kMigrating 期间源写被
    // barrier 冻结、切 owner 后源停服，段内数据不再变化。
    // 空 source（空 rank 原生认领）无元数据可拉，直接返回。
    if (first_slot > last_slot) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (source_master_id.empty() || source_master_id == master_id_) {
        return ErrorCode::OK;
    }
    if (!inter_master_rpc_) {
        LOG(WARNING) << "ImportSlotRangeFrom: inter_master_rpc_ not ready, "
                        "rank="
                     << rank;
        return ErrorCode::INVALID_PARAMS;
    }

    // 安装前重读 ring_slots 显式路由：owner 必须仍是本机（InstallSlotMetadata
    // Export 的防御门控）；epoch 即本段当前归属代，作 vsegment route_epoch。
    // 若 owner 已被再次切走（管理员并发改判），放弃本轮，由下一扫描重读。
    cvm::RingSlotAssign assign;
    ViewVersionId version = 0;
    ErrorCode err = cvm::EtcdViewStore::LoadRingSlotAssign(cluster_id_, rank,
                                                           assign, version);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (assign.primary_id != master_id_) {
        LOG(WARNING) << "ImportSlotRangeFrom: rank=" << rank
                     << " owner changed to " << assign.primary_id
                     << " mid-import, aborting";
        return ErrorCode::STALE_ROUTE;
    }

    for (uint32_t first = first_slot; first <= last_slot; first += 128) {
        const auto last =
            static_cast<uint16_t>(std::min<uint32_t>(last_slot, first + 127));
        auto pull = inter_master_rpc_->ExportSlotBatchGated(
            source_master_id, static_cast<uint16_t>(first), last, master_id_);
        if (!pull) {
            // Old peers lack the gated batch method. The single-slot RPC
            // has the same lost-ownership fence and is safe during upgrade.
            std::vector<SlotMetadataExport> singles;
            for (uint32_t slot = first; slot <= last; ++slot) {
                auto item = inter_master_rpc_->ExportSlot(
                    source_master_id, static_cast<uint16_t>(slot), master_id_);
                if (!item) return item.error();
                singles.push_back(std::move(*item));
            }
            pull = std::move(singles);
        }
        if (pull->size() != last - first + 1) return ErrorCode::SLOT_MIGRATING;
        std::unordered_set<uint16_t> installed;
        vsegment::VSegmentCommitBatch batch;
        for (const auto& payload : *pull) {
            if (payload.slot < first || payload.slot > last ||
                !installed.insert(payload.slot).second)
                return ErrorCode::INVALID_PARAMS;
            partition::PartitionRoute route;
            route.partition_id.partition_id = std::to_string(payload.slot);
            route.owner_submaster_id = master_id_;
            route.route_epoch = assign.epoch;
            route.state =
                static_cast<int32_t>(partition::PartitionState::kActive);
            auto error = InstallSlotMetadataExport(payload, route, &batch);
            if (error == ErrorCode::TASK_PENDING_LIMIT_EXCEEDED &&
                batch.Flush() == ErrorCode::OK)
                error = InstallSlotMetadataExport(payload, route, &batch);
            if (error != ErrorCode::OK) {
                const auto durable = batch.Flush();
                return durable == ErrorCode::OK ? error : durable;
            }
        }
        const auto error = batch.Flush();
        if (error != ErrorCode::OK) return error;
    }
    if (vsegment_service_) vsegment_service_->RefreshMetrics(true);
    return ErrorCode::OK;
}

bool MasterService::DriveReshardIntent(const cvm::ReshardIntent& intent) {
    // §16.19.1 目标侧状态机（coordinator = target）。调用方已过滤
    // target==本机；返回 true = 已完成（含 ack）或意图失效，调用方删 intent。
    if (intent.target_primary_id != master_id_) {
        return false;
    }
    const uint32_t rank = intent.rank;

    // 同 rank 并发驱动互斥（driver 线程 + 潜在外部触发共用）。
    {
        std::lock_guard<std::mutex> lock(reshard_inflight_mutex_);
        if (reshard_inflight_.count(rank) > 0) {
            return false;
        }
        reshard_inflight_.emplace(rank, 0);
    }
    struct InflightGuard {
        std::mutex& mutex;
        std::map<uint32_t, uint64_t>& inflight;
        uint32_t rank;
        ~InflightGuard() {
            std::lock_guard<std::mutex> lock(mutex);
            inflight.erase(rank);
        }
    } inflight_guard{reshard_inflight_mutex_, reshard_inflight_, rank};

    // G 视图就绪判定 + rank 合法性。缓存未就绪（watch 首扫未完成）→ 等待；
    // rank >= G 属跨代残留（G 变更是冷操作，换代码后旧 intent 残留）→ 失效。
    if (cvm_controller_ == nullptr ||
        !cvm_controller_->HasCachedRingSlotAssigns()) {
        return false;
    }
    const uint32_t group_count = cvm_controller_->GetCachedSlotGroupCount();
    if (group_count == 0 || rank >= group_count) {
        LOG(WARNING) << "DriveReshardIntent: stale intent rank=" << rank
                     << " >= group_count=" << group_count << ", discarding";
        return true;
    }
    const auto slots = cvm::RankOwnedSlots(rank, group_count);
    if (slots.empty()) {
        return false;  // 防御，正常不发生
    }
    const uint16_t first_slot = slots.front();
    const uint16_t last_slot = slots.back();

    cvm::RingSlotAssign assign;
    ViewVersionId version = 0;
    ErrorCode err = cvm::EtcdViewStore::LoadRingSlotAssign(cluster_id_, rank,
                                                           assign, version);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        // 空 rank 原生认领（§16.15.3）：key 不存在 = 从未分配，无迁移阶段，
        // kKeyNotExists 事务声明本机为 primary（并发认领仅一个成功）。空段
        // 无元数据可拉，slot 就绪由心跳 gained→on_slot_acquired 路径完成。
        cvm::RingSlotAssign created;
        created.rank = rank;
        created.primary_id = master_id_;
        created.state = static_cast<int32_t>(cvm::SlotState::kStable);
        created.epoch = 1;  // CreateRingSlotAssign 守卫：epoch=0 视为未设置
        err = cvm::EtcdViewStore::CreateRingSlotAssign(cluster_id_, created);
        if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
            LOG(WARNING) << "DriveReshardIntent: claim race lost rank=" << rank
                         << ", discarding intent";
            return true;  // 他人已认领，本意图失效
        }
        if (err != ErrorCode::OK) {
            return false;
        }
        LOG(INFO) << "DriveReshardIntent: claimed empty rank=" << rank
                  << " primary=" << master_id_;
        return true;
    }
    if (err != ErrorCode::OK) {
        return false;  // etcd 瞬时故障，下一轮重试
    }
    {
        std::lock_guard<std::mutex> lock(reshard_inflight_mutex_);
        reshard_inflight_[rank] = assign.epoch;
    }

    if (assign.primary_id != master_id_) {
        if (assign.primary_id != intent.source_primary_id) {
            // 意图记录的源已非当前 owner（源死亡被晋升覆盖 / 管理员改判）。
            // 意图永久不可满足 → 失效，删（返回 true）——否则每 5s 扫描
            // 重复 WARNING 刷屏且永不清理。需要迁移由管理员重新发起
            // （§16.16.7）。
            LOG(WARNING) << "DriveReshardIntent: source no longer owner, "
                            "discarding stale intent rank="
                         << rank
                         << " intent_source=" << intent.source_primary_id
                         << " current_owner=" << assign.primary_id;
            return true;
        }
        if (static_cast<cvm::SlotState>(assign.state) ==
                cvm::SlotState::kMigrating &&
            assign.migrating_to_id != master_id_) {
            // 他方迁移进行中（同 rank 互斥，§16.19.1）：epoch 已被并发推进，
            // 本意图永久排队无意义 → 失效删除；需要时管理员重发。
            LOG(WARNING) << "DriveReshardIntent: another migration in "
                            "progress, discarding stale intent rank="
                         << rank << " migrating_to=" << assign.migrating_to_id;
            return true;
        }
        // 阶段1：CAS kMigrating（migrating_to=本机）。源 watch 即时冻结该段
        // 写（§16.19.3），读继续由源服务。kMigrating 语义下
        // AdoptRankViaCAS 不删 intent（本路径的配套状态）。
        cvm::RingSlotAssign migrating;
        err = cvm::EtcdViewStore::AdoptRankViaCAS(
            cluster_id_, rank, assign.epoch, assign.primary_id,
            cvm::SlotState::kMigrating, master_id_, migrating, nullptr,
            "reshard begin", assign.primary_id, /*success_as_warning=*/false,
            /*clear_intent=*/false);
        if (err == ErrorCode::STALE_ROUTE) {
            return false;  // 他方已动，下一轮重读
        }
        if (err != ErrorCode::OK) {
            return false;
        }
        assign = migrating;
        // 阶段2：CAS 切 owner（primary=本机, kStable）。客户端读路由原子切到
        // 本机；写冻结窗口持续到安装完成（§16.19.4，目标侧 expected 但
        // 未 ready 期间返回 SLOT_MIGRATING）。standby 列表保留在 rank 上
        // （nullptr 不替换），配对回放由 CvmController 重绑。intent 保留
        // （clear_intent=false）：阶段3 安装/阶段4 ack 未完成前是断点续传
        // 依据，driver 全流程完成后才删。
        cvm::RingSlotAssign owned;
        err = cvm::EtcdViewStore::AdoptRankViaCAS(
            cluster_id_, rank, assign.epoch, master_id_,
            cvm::SlotState::kStable, std::string(), owned, nullptr,
            "reshard owner switch", assign.primary_id,
            /*success_as_warning=*/false, /*clear_intent=*/false);
        if (err != ErrorCode::STALE_ROUTE && err != ErrorCode::OK) {
            return false;
        }
        // STALE：他方已切（如晋升覆盖），下一轮重读后走下方恢复路径。
    } else if (static_cast<cvm::SlotState>(assign.state) ==
               cvm::SlotState::kMigrating) {
        // 断点续传重入：阶段1 后崩溃（owner 仍源、kMigrating 指向本机）→
        // 补切 owner。
        if (assign.migrating_to_id != master_id_) {
            // 本机持有但正迁往他方（管理员改判）→ 本意图失效，删除。
            LOG(WARNING) << "DriveReshardIntent: rank owned by me but "
                            "migrating away, discarding stale intent rank="
                         << rank << " migrating_to=" << assign.migrating_to_id;
            return true;
        }
        cvm::RingSlotAssign owned;
        err = cvm::EtcdViewStore::AdoptRankViaCAS(
            cluster_id_, rank, assign.epoch, master_id_,
            cvm::SlotState::kStable, std::string(), owned, nullptr,
            "reshard resume", assign.primary_id, /*success_as_warning=*/false,
            /*clear_intent=*/false);
        if (err != ErrorCode::OK) {
            return false;
        }
    }
    // owner==本机且 kStable：阶段2 已完成（安装后崩溃 / ack 后残留）→
    // 重放拉取 + 安装（幂等：重复 key 跳过）+ 补 ack。

    // 阶段3：段级拉快照 + 安装。
    err = ImportSlotRangeFrom(intent.source_primary_id, rank, first_slot,
                              last_slot);
    if (err != ErrorCode::OK) {
        // 失败分类：源已消亡（lease 过期，masters/ 无记录）→ 元数据不可达，
        // 按「空元数据 + 客户端重建」收尾（与 §15.7 单 slot 交接的
        // old_owner_gone 语义一致）；已安装部分保留，缺口由客户端重写补齐。
        // 源仍存活 → 瞬时故障，下一轮重试。
        if (!intent.source_primary_id.empty() &&
            intent.source_primary_id != master_id_) {
            std::vector<cvm::MasterRegistration> masters;
            ViewVersionId masters_version = 0;
            if (cvm::EtcdViewStore::LoadAllMasters(
                    cluster_id_, masters, masters_version) == ErrorCode::OK &&
                !cvm::IsMemberAlive(masters, intent.source_primary_id)) {
                LOG(ERROR) << "DriveReshardIntent: source died mid-reshard, "
                              "finalizing rank with already-installed "
                              "metadata (gaps rebuilt by clients) rank="
                           << rank << " source=" << intent.source_primary_id;
                MarkSlotsReady(slots);
                return true;
            }
        }
        return false;
    }
    MarkSlotsReady(slots);

    // 阶段4：段级 ack（源清段内元数据 + staged 导出）。源尚未观察到归属切走
    // 时返回 SLOT_MIGRATING → 本轮放弃，下一轮恢复路径重放（重拉幂等）。
    if (!intent.source_primary_id.empty() &&
        intent.source_primary_id != master_id_) {
        if (!inter_master_rpc_) {
            return false;
        }
        auto ack = inter_master_rpc_->AckSlotRangeImported(
            intent.source_primary_id, first_slot, last_slot, master_id_);
        if (!ack.has_value()) {
            LOG(WARNING) << "DriveReshardIntent: ack failed rank=" << rank
                         << " (will retry) err=" << toString(ack.error());
            return false;
        }
    }
    LOG(INFO) << "DriveReshardIntent: completed rank=" << rank << " slots=["
              << first_slot << "," << last_slot
              << "] source=" << intent.source_primary_id
              << " target=" << master_id_;
    return true;
}

void MasterService::ReshardDriverLoop() {
    LOG(INFO) << "ReshardDriverLoop: started master_id=" << master_id_;
    // 扫描间隔：reshard 是低频操作（管理员扩缩容 / 新成员原生认领触发），
    // 5s 轮询足够及时；intent 持久化在 etcd，漏一轮下一轮补（断点续传）。
    constexpr auto kScanInterval = std::chrono::seconds(5);
    size_t last_logged_mine = static_cast<size_t>(-1);
    while (reshard_driver_running_.load()) {
        std::vector<cvm::ReshardIntent> intents;
        ViewVersionId version = 0;
        ErrorCode err = cvm::EtcdViewStore::LoadAllReshardIntents(
            cluster_id_, intents, version);
        bool pending = false;
        if (err == ErrorCode::OK) {
            size_t mine = 0;
            size_t completed = 0;
            for (const auto& intent : intents) {
                if (intent.target_primary_id != master_id_) {
                    continue;
                }
                ++mine;
                if (DriveReshardIntent(intent)) {
                    const ErrorCode del =
                        cvm::EtcdViewStore::DeleteReshardIntent(cluster_id_,
                                                                intent.rank);
                    if (del == ErrorCode::OK) {
                        ++completed;
                    } else {
                        LOG(WARNING) << "ReshardDriverLoop: delete intent "
                                        "failed rank="
                                     << intent.rank << " err=" << del
                                     << " (will re-drive)";
                    }
                }
            }
            pending = completed < mine;
            // 防刷屏：仅本机意图数变化时打印汇总（含归零）。
            if (mine != last_logged_mine) {
                LOG(INFO) << "ReshardDriverLoop: intents targeting me=" << mine
                          << (completed ? " (completed this scan)" : "");
                last_logged_mine = mine;
            }
        } else if (err != ErrorCode::ETCD_KEY_NOT_EXIST) {
            LOG(WARNING) << "ReshardDriverLoop: LoadAllReshardIntents "
                            "failed: "
                         << err;
        }
        std::unique_lock<std::mutex> lock(reshard_wake_mutex_);
        reshard_wake_cv_.wait_for(
            lock, pending ? std::chrono::milliseconds(100) : kScanInterval,
            [this] {
                return reshard_wake_pending_ || !reshard_driver_running_.load();
            });
        reshard_wake_pending_ = false;
    }
    LOG(INFO) << "ReshardDriverLoop: stopped";
}

ErrorCode MasterService::InstallSlotMetadataExport(
    const SlotMetadataExport& export_payload,
    const partition::PartitionRoute& effective_route,
    vsegment::VSegmentCommitBatch* batch) {
    const uint16_t slot = export_payload.slot;
    if (vsegment_runtime_gate_active_.load(std::memory_order_acquire) &&
        vsegment_init_state_.load(std::memory_order_acquire) !=
            VSegmentInitState::kReady)
        return RefreshVSegmentOwnership(std::to_string(slot), batch);
    // 防御：路由 owner 必须是本机（调用方已解析；不匹配说明视图已过期）。
    if (effective_route.owner_submaster_id != master_id_) {
        return ErrorCode::STALE_ROUTE;
    }

    if (export_payload.vsegment_partition.has_value()) {
        if (!vsegment_service_ || !ordered_oplog_writer_) {
            LOG(ERROR) << "InstallSlotMetadataExport: vsegment state has no "
                          "local runtime slot="
                       << slot;
            return ErrorCode::INVALID_PARAMS;
        }
        const std::string partition_id = std::to_string(slot);
        const auto& transferred = *export_payload.vsegment_partition;
        if (transferred.partition_id != partition_id) {
            LOG(ERROR) << "InstallSlotMetadataExport: vsegment Partition "
                          "mismatch";
            return ErrorCode::INVALID_PARAMS;
        }
        vsegment::PartitionVSegmentSnapshot current;
        const auto current_error =
            vsegment_service_->SnapshotPartition(partition_id, &current);
        if (current_error == ErrorCode::OK && !current.vsegments.empty()) {
            if (current.metadata_revision != transferred.metadata_revision) {
                LOG(ERROR) << "InstallSlotMetadataExport: target Partition "
                              "is not empty slot="
                           << slot;
                return ErrorCode::INVALID_VERSION;
            }
        } else {
            if (current_error == ErrorCode::OK) {
                auto remove_error = vsegment_service_->RemovePartition(
                    partition_id, effective_route.route_epoch);
                if (remove_error != ErrorCode::OK) return remove_error;
            } else if (current_error != ErrorCode::STALE_ROUTE) {
                return current_error;
            }
            auto state = transferred;
            state.route_epoch = effective_route.route_epoch;
            auto committer =
                std::make_shared<vsegment::OrderedOpLogVSegmentCommitter>(
                    ordered_oplog_writer_.get());
            std::string detail;
            auto add_error = vsegment_service_->AddPartition(
                partition_id, effective_route.route_epoch, std::move(committer),
                &state, &detail, batch);
            if (add_error != ErrorCode::OK) {
                LOG(ERROR) << "InstallSlotMetadataExport: failed to install "
                              "vsegment state slot="
                           << slot << ", detail=" << detail;
                return add_error;
            }
        }
    }

    const auto resolve = [](const StandbyObjectEntry& entry) {
        auto [scoped_tenant_id, user_key] = TenantId::ParseScopedKey(entry.key);
        TenantId tenant_id(entry.tenant_id);
        if (tenant_id.IsDefault() && !scoped_tenant_id.IsDefault()) {
            tenant_id = std::move(scoped_tenant_id);
        }
        return std::make_pair(std::move(tenant_id), std::move(user_key));
    };

    for (const auto& entry : export_payload.objects) {
        auto [tenant_id, user_key] = resolve(entry);
        if (!tenant_id.IsValid() || cvm::KeySlot(tenant_id, user_key) != slot)
            return ErrorCode::INVALID_PARAMS;
        const auto& standby_meta = entry.metadata;
        std::vector<Replica> replicas;
        replicas.reserve(standby_meta.replicas.size());
        bool processing = false;
        for (const auto& desc : standby_meta.replicas) {
            processing = processing ||
                         desc.status == ReplicaStatus::PROCESSING ||
                         desc.status == ReplicaStatus::INITIALIZED;
            if (desc.is_memory_replica() || desc.is_nof_replica()) {
                const auto& buffer =
                    desc.is_memory_replica()
                        ? desc.get_memory_descriptor().buffer_descriptor
                        : desc.get_nof_descriptor().buffer_descriptor;
                auto& alloc =
                    standby_allocator_keepalive_[buffer.transport_endpoint_];
                if (!alloc)
                    alloc = std::make_shared<DummyBufferAllocator>(
                        buffer.transport_endpoint_, buffer.transport_endpoint_);
                auto allocated =
                    std::make_unique<AllocatedBuffer>(alloc, buffer);
                if (desc.is_memory_replica()) {
                    auto owner = RecordRestoreKnownOwner(
                        restored_known_owners_, standby_meta.client_id);
                    allocated->bindClientLiveness(owner);
                    replicas.emplace_back(std::move(allocated), desc.status);
                } else {
                    replicas.emplace_back(std::move(allocated), desc.status,
                                          ReplicaType::NOF_SSD);
                }
            } else if (desc.is_disk_replica()) {
                const auto& disk = desc.get_disk_descriptor();
                replicas.emplace_back(disk.file_path, disk.object_size,
                                      desc.status);
            } else if (desc.is_local_disk_replica()) {
                const auto& disk = desc.get_local_disk_descriptor();
                replicas.emplace_back(disk.client_id, disk.object_size,
                                      disk.transport_endpoint, desc.status);
                replicas.back().bindClientLiveness(RecordRestoreKnownOwner(
                    restored_known_owners_, disk.client_id));
            } else if (desc.is_dfs_replica()) {
                replicas.emplace_back(desc.get_dfs_descriptor(), desc.status);
            } else if (desc.is_vsegment_replica()) {
                replicas.emplace_back(desc.get_vsegment_descriptor(),
                                      desc.status);
            }
        }
        MetadataAccessorRW accessor(
            this, ObjectIdentity{tenant_id, user_key},
            MetadataAccessorRW::CreateKey{.cleanup_invalid = false});
        if (accessor.IsPublished()) continue;
        accessor.Publish(
            standby_meta.client_id, std::chrono::system_clock::now(),
            standby_meta.size, std::move(replicas), std::nullopt,
            standby_meta.hard_pinned.value_or(false), standby_meta.data_type,
            standby_meta.group_id, tenant_id, user_key,
            standby_meta.residency_affinity_id.value_or(std::string{}));
        auto& metadata = accessor.Get();
        metadata.session_id = standby_meta.session_id.value_or(std::string{});
        metadata.MutablePart(0).set_checksum(
            standby_meta.object_checksum.value_or(0));
        accessor.GetState().is_processing = processing;
        SyncCacheTotalAccounting(metadata);
        PublishKvStored(user_key, metadata, tenant_id);
        {
            std::lock_guard<std::mutex> lock(remote_allocated_keys_mutex_);
            remote_allocated_keys_.insert(tenant_id.MakeScopedKey(user_key));
        }
    }
    // Complete vsegment activation before ACK: a failed activation must leave
    // the source's export intact for the next acquire attempt.
    const auto activation =
        RefreshVSegmentOwnership(std::to_string(slot), batch);
    if (activation != ErrorCode::OK) return activation;
    return ErrorCode::OK;
}
#else
ErrorCode MasterService::StartSlotOwnerHeartbeat() { return ErrorCode::OK; }
ErrorCode MasterService::StartInterMasterRpc() { return ErrorCode::OK; }
void MasterService::StopInterMasterRpc() {}
ErrorCode MasterService::RefreshVSegmentOwnership(
    const std::string&, vsegment::VSegmentCommitBatch*) {
    return ErrorCode::OK;
}
ErrorCode MasterService::RunVSegmentOwnershipReconcile(
    const std::string&, vsegment::VSegmentCommitBatch*) {
    return ErrorCode::OK;
}
bool MasterService::MaybeRetryVSegmentAutoPlan(const char*) { return false; }
void MasterService::NoteVSegmentMountActivity() {}
void MasterService::MaybeSpawnVSegmentInit() {}
void MasterService::VSegmentInitThreadFunc() {}
void MasterService::ConstructVSegmentService(
    vsegment::PartitionPhysicalQuotaSnapshot) {}
uint32_t MasterService::GetOwnedSlotCount() const { return 0; }
tl::expected<std::vector<Replica>, ErrorCode>
MasterService::TryAllocateReplicasRemotely(const std::string&, const TenantId&,
                                           uint64_t, size_t,
                                           const std::vector<std::string>&) {
    return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
}
void MasterService::EnqueueRemoteFreeIfTracked(const TenantId&,
                                               const std::string&, EraseMode) {}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::InterMasterAllocateReplicas(const std::string&,
                                           const std::string&, uint64_t,
                                           uint64_t,
                                           const std::vector<std::string>&) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}
tl::expected<bool, ErrorCode> MasterService::InterMasterFreeReplicas(
    const std::string&, const std::string&) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}
tl::expected<GetReplicaListResponse, ErrorCode>
MasterService::InterMasterGetReplicaList(const std::string&,
                                         const std::string&) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}
std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::InterMasterBatchGetReplicaList(
    const std::vector<std::string>& keys, const std::string&) {
    return std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>(
        keys.size(),
        tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));
}
tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::InterMasterPutStart(const UUID&, const std::string&,
                                   const std::string&, uint64_t,
                                   const ReplicateConfig&) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}
tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
MasterService::InterMasterUpsertStart(const UUID&, const std::string&,
                                      const std::string&, uint64_t,
                                      const ReplicateConfig&) {
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

tl::expected<SlotMetadataExport, ErrorCode>
MasterService::BuildSlotMetadataExport(uint16_t /*slot*/,
                                       bool /*require_not_owned*/) {
    return SlotMetadataExport{};
}

tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
MasterService::BuildSlotMetadataExportBatch(uint16_t /*first*/,
                                            uint16_t /*last*/,
                                            bool /*require_not_owned*/) {
    return std::vector<SlotMetadataExport>{};
}

ErrorCode MasterService::DropSlotMetadataLocal(uint16_t /*slot*/) {
    return ErrorCode::OK;
}

ErrorCode MasterService::DropSlotMetadataRange(uint16_t /*first_slot*/,
                                               uint16_t /*last_slot*/) {
    return ErrorCode::OK;
}

ErrorCode MasterService::ExportSlotMetadata(uint16_t /*slot*/) {
    return ErrorCode::OK;
}

ErrorCode MasterService::ImportSlotMetadata(
    uint16_t /*slot*/, const SlotMetadataExport* /*prefetched_export*/,
    vsegment::VSegmentCommitBatch* /*batch*/) {
    return ErrorCode::OK;
}

ErrorCode MasterService::InstallSlotMetadataExport(
    const SlotMetadataExport& /*export_payload*/,
    const partition::PartitionRoute& /*effective_route*/,
    vsegment::VSegmentCommitBatch* /*batch*/) {
    return ErrorCode::OK;
}

tl::expected<SlotMetadataExport, ErrorCode>
MasterService::InterMasterExportSlot(uint16_t /*slot*/,
                                     const std::string& /*requester*/) {
    return SlotMetadataExport{};
}

tl::expected<bool, ErrorCode> MasterService::InterMasterAckSlotImported(
    uint16_t /*slot*/, const std::string& /*importer*/) {
    return false;
}

tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
MasterService::InterMasterExportSlotBatch(uint16_t /*first_slot*/,
                                          uint16_t /*last_slot*/,
                                          const std::string& /*requester*/) {
    return std::vector<SlotMetadataExport>{};
}

tl::expected<std::vector<SlotMetadataExport>, ErrorCode>
MasterService::InterMasterExportSlotBatchGated(
    uint16_t /*first_slot*/, uint16_t /*last_slot*/,
    const std::string& /*requester*/) {
    return std::vector<SlotMetadataExport>{};
}

tl::expected<bool, ErrorCode> MasterService::InterMasterAckSlotRangeImported(
    uint16_t /*first_slot*/, uint16_t /*last_slot*/,
    const std::string& /*importer*/) {
    return false;
}

ErrorCode MasterService::ImportSlotRangeFrom(
    const std::string& /*source_master_id*/, uint32_t /*rank*/,
    uint16_t /*first_slot*/, uint16_t /*last_slot*/) {
    return ErrorCode::OK;
}

bool MasterService::DriveReshardIntent(const cvm::ReshardIntent& /*intent*/) {
    return false;
}

void MasterService::ReshardDriverLoop() {}
#endif

#ifdef STORE_USE_ETCD
void MasterService::PublishSegmentOwnerForCvm(const Segment& segment) {
    if (!enable_ha_ || ha_backend_type_ != "etcd" || master_id_.empty() ||
        cluster_id_.empty()) {
        return;
    }
    const std::string seg_id = UuidToString(segment.id);

    // 查询 allocator 实时已用字节数，供 planner 按 [used_bytes, capacity)
    // 切分 vsegment 可分配范围，避免与其他分配器占用范围重叠。查询失败
    // 时保守上报 0（等同 exclusive 空集群），由 planner 后续校验兜底。
    size_t used_bytes = 0;
    size_t capacity_bytes = 0;
    {
        auto segment_access = segment_manager_.getSegmentAccess();
        segment_access.QuerySegments(segment.name, used_bytes, capacity_bytes);
    }

    // 1. Write neutral SegmentDescriptor to segments/{seg_id} (idempotent,
    //    without lease — one copy per segment, no owner).资源事实字段
    //    （medium / io_alignment / supports_unaligned_io / failure_domain /
    //    used_bytes / vsegment_exclusive）由本集群配置 + allocator 实时
    //    状态填充，供 vsegment 自动发现使用，不应由用户在配额文件中手写。
    cvm::SegmentDescriptor desc;
    desc.segment_id = seg_id;
    desc.segment_name = segment.name;
    desc.capacity = segment.size;
    desc.te_endpoint = segment.te_endpoint;
    desc.protocol = segment.protocol;
    desc.host_id = segment.host_id;
    // 优先用 client 上报的 segment.medium（支持混合介质集群：不同 segment
    // 可上报不同 medium，如 "cpu:0"/"cuda:0"/"ssd:0"）；缺省回退集群默认。
    desc.medium =
        segment.medium.empty() ? cvm_segment_default_medium_ : segment.medium;
    desc.io_alignment = 1;  // 保守默认；Planner 取 max(profile, segment)
    desc.supports_unaligned_io = true;
    desc.failure_domain =
        segment.host_id.empty() ? desc.segment_id : segment.host_id;
    desc.used_bytes = used_bytes;
    desc.vsegment_exclusive = cvm_segments_vsegment_exclusive_;
    // vsegment/psegment 术语的注册可见性日志（存量日志不动，仅此新增行）。
    // numa_node 从 medium "cpu:N" 解析（N 为 NUMA 节点号，非核号）；
    // 非 cpu: 前缀（cuda:/ssd:/集群默认值）打印 n/a。
    {
        const std::string kNumaPrefix = "cpu:";
        std::string numa_node = "n/a";
        if (desc.medium.rfind(kNumaPrefix, 0) == 0 &&
            desc.medium.size() > kNumaPrefix.size()) {
            try {
                numa_node =
                    "numa" + std::to_string(std::stoi(
                                 desc.medium.substr(kNumaPrefix.size())));
            } catch (const std::exception&) {
            }
        }
        LOG(INFO) << "PSegment registered: id=" << desc.segment_id
                  << ", store=" << desc.te_endpoint << ", host=" << desc.host_id
                  << ", numa_node=" << numa_node << ", medium=" << desc.medium
                  << ", capacity=" << desc.capacity
                  << ", used=" << desc.used_bytes << ", available="
                  << (desc.capacity > desc.used_bytes
                          ? desc.capacity - desc.used_bytes
                          : 0)
                  << ", protocol=" << desc.protocol
                  << ", io_alignment=" << desc.io_alignment << ", unaligned_io="
                  << (desc.supports_unaligned_io ? "supported" : "unsupported")
                  << ", failure_domain=" << desc.failure_domain
                  << ", mounted_on=" << master_id_;
    }
    ErrorCode err =
        cvm::EtcdViewStore::SaveSegmentDescriptor(cluster_id_, desc);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "PublishSegmentOwnerForCvm SaveSegmentDescriptor "
                        "failed: segment="
                     << seg_id << " err=" << err;
    }

    // 2. Write per-master MountEntry to
    //    snapshot/{master_id}/segments/{seg_id} (with lease — auto-cleaned on
    //    master death; key=(master_id, segment_id) naturally supports
    //    one-segment-multi-master without overwrite conflicts).
    cvm::MountEntry mount;
    mount.segment_id = seg_id;
    mount.mounted_at_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    if (cvm_lease_id_ != 0) {
        err = cvm::EtcdViewStore::SaveMountEntryWithLease(
            cluster_id_, master_id_, mount, cvm_lease_id_);
    } else {
        // Fallback: write persistent MountEntry without lease.
        const std::string key =
            cvm::SnapshotSegmentMountKey(cluster_id_, master_id_, seg_id);
        std::string value;
        cvm::EtcdViewStore::SerializeMountEntry(mount, value);
        err =
            EtcdHelper::Put(key.data(), key.size(), value.data(), value.size());
    }
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "PublishSegmentOwnerForCvm SaveMountEntryWithLease "
                        "failed: segment="
                     << seg_id << " master=" << master_id_ << " err=" << err;
    } else {
        LOG(INFO) << "PublishSegmentOwnerForCvm done: segment=" << seg_id
                  << " master=" << master_id_;
    }
}

void MasterService::RemoveSegmentOwnerForCvm(const UUID& segment_id) {
    if (!enable_ha_ || ha_backend_type_ != "etcd" || master_id_.empty() ||
        cluster_id_.empty()) {
        return;
    }
    const std::string id = UuidToString(segment_id);

    // 先删除本 master 的挂载记录；描述符是否清理取决于是否还有其他 master
    // 仍挂载该 segment（案 A：最后一个挂载者卸载时删除描述符）。
    ErrorCode err =
        cvm::EtcdViewStore::DeleteMountEntry(cluster_id_, master_id_, id);
    if (err != ErrorCode::OK) {
        // 删除失败时本 master 的挂载记录可能仍在，后续扫描会因「排除了本机」
        // 而误判为「无其它挂载」，进而误删描述符。保守返回，不删描述符。
        LOG(WARNING) << "RemoveSegmentOwnerForCvm DeleteMountEntry failed: "
                        "segment="
                     << id << " master=" << master_id_ << " err=" << err;
        return;
    }

    // 感知「是否还有其他 master 挂载」：全量扫描 snapshot/*/segments/，过滤
    // 出 segment_id == id 且 master_id != 本机 的挂载记录。
    std::vector<std::pair<std::string, cvm::MountEntry>> mounts;
    ViewVersionId version = 0;
    err = cvm::EtcdViewStore::LoadAllMountEntries(cluster_id_, mounts, version);
    if (err != ErrorCode::OK) {
        // 无法确认时保守返回，不删描述符（避免误删仍被其它 master
        // 使用的描述符）。
        LOG(WARNING) << "RemoveSegmentOwnerForCvm LoadAllMountEntries failed: "
                        "segment="
                     << id << " master=" << master_id_ << " err=" << err;
        return;
    }

    for (const auto& m : mounts) {
        if (m.second.segment_id == id && m.first != master_id_) {
            LOG(INFO) << "RemoveSegmentOwnerForCvm kept descriptor (still "
                         "mounted elsewhere): segment="
                      << id << " master=" << master_id_;
            return;  // 仍有其它 master 挂载，保留描述符。
        }
    }

    // 最后一个挂载者已卸载：删除中立描述符。
    // NOTE：与「新挂载者写挂载记录」存在跨节点 check-then-act 竞态窗口（挂载
    // 顺序为 描述符→挂载记录，卸载为 挂载记录→扫描→描述符），极端交错下可能
    // 误删描述符；影响仅限 HTTP /segment_view 的描述符字段，可被后续 mount
    // 幂等重写自愈，故此处容忍该窗口，不做 key 布局/事务原子化。
    err = cvm::EtcdViewStore::DeleteSegmentDescriptor(cluster_id_, id);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "RemoveSegmentOwnerForCvm DeleteSegmentDescriptor "
                        "failed: segment="
                     << id << " master=" << master_id_ << " err=" << err;
        return;
    }
    LOG(INFO) << "RemoveSegmentOwnerForCvm done (last mount removed "
                 "descriptor): segment="
              << id << " master=" << master_id_;
}

void MasterService::EnqueueStaleGenerationEvict(const Segment& incoming,
                                                const UUID& mounting_client) {
    // 无法归因（缺 name/host/endpoint 任一标识）不入队，交由 TTL reaper
    // 兜底（与 helper 前置判同款，宁可漏清理也不误杀）。
    if (incoming.name.empty() || incoming.te_endpoint.empty() ||
        incoming.host_id.empty()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(stale_evict_queue_mutex_);
        // (host_id, segment_name) 去重 + 最新参考 client 胜出：快速连续
        // 重启场景下旧请求的参考 client 已过时（drain 前置校验会作废
        // 它），此处直接以最新代覆盖，省一次空 drain。
        for (auto& request : stale_evict_queue_) {
            if (request.host_id == incoming.host_id &&
                request.segment_name == incoming.name) {
                request.te_endpoint = incoming.te_endpoint;
                request.mounting_client = mounting_client;
                stale_evict_cv_.notify_one();
                return;
            }
        }
        StaleEvictRequest request;
        request.host_id = incoming.host_id;
        request.segment_name = incoming.name;
        request.te_endpoint = incoming.te_endpoint;
        request.mounting_client = mounting_client;
        stale_evict_queue_.push_back(std::move(request));
        stale_evict_pending_.fetch_add(1, std::memory_order_release);
    }
    stale_evict_cv_.notify_one();
}

void MasterService::StaleEvictThreadFunc() {
    // 纯 cv 唤醒（无空轮询开销）：仅入队或停机时唤醒。debounce 合并挂载
    // 风暴——worker 启动连挂 N 段触发 N 次入队，去重 + 1s 窗口后一次
    // drain 完成，而非 N 次全 host 扫描。
    constexpr auto kDrainDebounce = std::chrono::seconds(1);
    while (stale_evict_running_.load(std::memory_order_acquire)) {
        std::vector<StaleEvictRequest> requests;
        {
            std::unique_lock<std::mutex> lock(stale_evict_queue_mutex_);
            stale_evict_cv_.wait(lock, [&] {
                return !stale_evict_queue_.empty() ||
                       !stale_evict_running_.load(std::memory_order_acquire);
            });
            if (!stale_evict_running_.load(std::memory_order_acquire)) {
                break;
            }
            stale_evict_cv_.wait_for(lock, kDrainDebounce, [&] {
                return !stale_evict_running_.load(std::memory_order_acquire);
            });
            if (!stale_evict_running_.load(std::memory_order_acquire)) {
                break;
            }
            requests.swap(stale_evict_queue_);
        }
        for (const auto& request : requests) {
            if (!stale_evict_running_.load(std::memory_order_acquire)) {
                break;
            }
            {
                // 竞态防护：swap 后有同 (host_id, segment_name) 的新请求
                // 入队（参考 client 已换代）时，本（旧参考）请求作废，
                // 交由下一轮 drain 以最新参考执行——防止旧参考 drain 误杀
                // 刚挂载的新代段（快速连续重启场景；入队去重仅覆盖仍在
                // 队列中的请求，此检查覆盖已 swap 出队的在途请求）。
                std::lock_guard<std::mutex> lock(stale_evict_queue_mutex_);
                const bool superseded = std::any_of(
                    stale_evict_queue_.begin(), stale_evict_queue_.end(),
                    [&](const StaleEvictRequest& pending) {
                        return pending.host_id == request.host_id &&
                               pending.segment_name == request.segment_name &&
                               pending.mounting_client !=
                                   request.mounting_client;
                    });
                if (superseded) {
                    VLOG(1) << "stale_generation_evict_skip "
                               "reason=superseded_by_newer_mount"
                            << ", segment_name=" << request.segment_name
                            << ", host_id=" << request.host_id;
                    stale_evict_pending_.fetch_sub(1,
                                                   std::memory_order_release);
                    continue;
                }
            }
            Segment incoming;
            incoming.name = request.segment_name;
            incoming.host_id = request.host_id;
            incoming.te_endpoint = request.te_endpoint;
            // 锁序与原 MountSegment 尾部同步调用一致：helper 按「调用方
            // 持 snapshot 共享锁」设计（snapshot → client → segment →
            // shard），移出 RPC 线程后由此处补持。drain 前置校验在
            // helper 内（reference_client_gone 时整个请求作废）。
            std::shared_lock<std::shared_mutex> snapshot_lock(
                master_state_mutex_);
            EvictStaleGenerationForHost(incoming, request.mounting_client);
            stale_evict_pending_.fetch_sub(1, std::memory_order_release);
        }
    }
}

void MasterService::FlushStaleEvictQueueForTesting() {
    // 测试专用：轮询等待 pending 归零（覆盖 1s debounce + 在途请求），
    // 5s 上限防死等。生产代码勿用。
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (stale_evict_pending_.load(std::memory_order_acquire) > 0) {
        if (std::chrono::steady_clock::now() > deadline) {
            LOG(WARNING) << "FlushStaleEvictQueueForTesting timed out,"
                            " pending="
                         << stale_evict_pending_.load(
                                std::memory_order_acquire);
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void MasterService::EvictStaleGenerationForHost(const Segment& incoming,
                                                const UUID& mounting_client) {
    // 无法归因（缺 name/host/endpoint 任一标识）则不动，交由 TTL reaper 与
    // 方案一 B（etcd lease 兜底）处理，宁可漏清理也不误杀。
    if (incoming.name.empty() || incoming.te_endpoint.empty() ||
        incoming.host_id.empty()) {
        LOG(WARNING) << "stale_generation_evict_skip reason=missing_identity"
                     << ", segment_name=" << incoming.name
                     << ", endpoint=" << incoming.te_endpoint
                     << ", host_id=" << incoming.host_id;
        return;
    }

    // 异步 drain 有效性前置校验：参考 client（mounting_client）必须仍持有
    // 同 name + 同 host 的段。入队→drain 之间存在秒级窗口，若参考 client
    // 在窗口内再次重启（换新 client_id + 新段），以旧参考执行清理会误杀
    // 新代段——此时本请求作废，交由新一代挂载时入队的请求处理。判据不含
    // te_endpoint：参考 client 幂等重挂（ReMount）在窗口内可能换了端口。
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        std::vector<std::pair<Segment, UUID>> all_segments;
        if (segment_access.GetAllSegmentsWithOwner(all_segments) !=
            ErrorCode::OK) {
            LOG(WARNING) << "stale_generation_evict_skip reason=list_failed"
                         << ", segment_name=" << incoming.name
                         << ", host_id=" << incoming.host_id;
            return;
        }
        const bool reference_present =
            std::any_of(all_segments.begin(), all_segments.end(),
                        [&](const std::pair<Segment, UUID>& pair) {
                            return pair.second == mounting_client &&
                                   pair.first.name == incoming.name &&
                                   pair.first.host_id == incoming.host_id;
                        });
        if (!reference_present) {
            VLOG(1) << "stale_generation_evict_skip "
                       "reason=reference_client_gone"
                    << ", segment_name=" << incoming.name
                    << ", host_id=" << incoming.host_id;
            return;
        }
    }

    // LOCAL_DISK 对称清理：同 host 旧代 client 的本地盘段（快速重注册场景
    // 下不等 TTL reaper）。判据与内存段一致（host 归并 + client_id 代际），
    // 叠加 P2-8-2 name 级 co-tenant 过滤（segment_name 已登记且与 incoming
    // 不同 → 旁路 client，跳过）；TTL reaper 对过期 client 执行的正是同一
    // LocalSsdManager 注销 + ClearInvalidHandles，此处仅提前触发。
    // 锁序与 ClientMonitorFunc 一致：snapshot(调用方持有) → client →
    // segment；client 锁释放后再取 segment 锁，避免倒挂。
    {
        std::vector<UUID> stale_ld_clients;
        {
            std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
            for (const auto& [cid, host] : client_host_id_) {
                if (cid != mounting_client && host == incoming.host_id) {
                    stale_ld_clients.push_back(cid);
                }
            }
        }
        if (!stale_ld_clients.empty()) {
            for (const auto& cid : stale_ld_clients) {
                // P2-8-2：name 级过滤。未登记（legacy / 仅
                // LOCAL_DISK client）保持 host 级归并原行为。
                const auto registered_name =
                    local_ssd_manager_.GetClientSegmentName(cid);
                if (registered_name && !registered_name->empty() &&
                    *registered_name != incoming.name) {
                    LOG(WARNING) << "client_id=" << cid
                                 << ", host_id=" << incoming.host_id
                                 << ", registered_segment_name="
                                 << *registered_name
                                 << ", action=skip_stale_ld_cotenant";
                    continue;
                }
                const auto reported_capacity =
                    local_ssd_manager_.UnregisterClient(cid);
                if (reported_capacity && *reported_capacity > 0) {
                    MasterMetricManager::instance().dec_total_file_capacity(
                        *reported_capacity);
                }
                LOG(WARNING)
                    << "client_id=" << cid << ", host_id=" << incoming.host_id
                    << ", action=unmount_stale_generation_local_disk";
            }
            // 旧代 client 的 LOCAL_DISK 副本元数据随段卸载转为 invalid，
            // 与 TTL reaper 路径对称地立即清扫（内部不获取 snapshot 锁）。
            ClearInvalidHandles();
        }
    }

    const auto evict_t0 = std::chrono::steady_clock::now();

    struct StaleSegInfo {
        UUID id;
        UUID client_id;
        std::string name;
        std::string endpoint;
        size_t dec_capacity = 0;
    };

    // 阶段 1（段锁内）：检测旧代段并 Prepare（删 allocator）。
    // Prepare 失败 = 段已被并发卸载（reaper / 主动 unmount / 重复触发），
    // 幂等跳过。旧代判定（同 name + 同 host 前提下任一满足）：
    // a) te_endpoint 不同：P2PHANDSHAKE 重启端口漂移（原逻辑）；
    // b) owner client 不同：非 P2P 快速重注册（endpoint 为固定 hostname
    //    跨重启不变）。name=local_hostname_ 按 host 唯一，同 name 不同
    //    client_id 即旧进程代；活 client 幂等重挂 client_id 相同，不误杀。
    // owner 解析必须用 GetAllSegmentsWithOwner（client_segments_ 反查）：
    // 新旧代段同名，pair 版 GetAllSegments 经 client_by_name_ 会把旧段归因
    // 到新 client，commit 时在 client_segments_ 留下死 id 并误报 ERROR。
    std::vector<StaleSegInfo> stale;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        std::vector<std::pair<Segment, UUID>> all_segments;
        if (segment_access.GetAllSegmentsWithOwner(all_segments) !=
            ErrorCode::OK) {
            LOG(WARNING) << "stale_generation_evict_skip reason=list_failed"
                         << ", segment_name=" << incoming.name
                         << ", host_id=" << incoming.host_id;
            return;
        }
        for (auto& [seg, cid] : all_segments) {
            if (seg.name != incoming.name || seg.host_id != incoming.host_id ||
                seg.te_endpoint.empty()) {
                continue;
            }
            // 自身幂等重挂（同 endpoint + 同 owner）跳过；其余（端点不同
            // 或 owner 不同）均按旧代处理。
            if (seg.te_endpoint == incoming.te_endpoint &&
                cid == mounting_client) {
                continue;
            }
            StaleSegInfo info;
            info.id = seg.id;
            info.client_id = cid;
            info.name = seg.name;
            info.endpoint = seg.te_endpoint;
            if (segment_access.PrepareUnmountSegment(
                    seg.id, info.dec_capacity) == ErrorCode::OK) {
                stale.push_back(std::move(info));
            } else {
                VLOG(1) << "stale_generation_prepare_skip segment_id="
                        << UuidToString(seg.id) << " reason=concurrent_unmount";
            }
        }
    }
    if (stale.empty()) {
        return;
    }

    LOG(WARNING) << "stale_generation_evict host_id=" << incoming.host_id
                 << ", segment_name=" << incoming.name
                 << ", new_endpoint=" << incoming.te_endpoint
                 << ", stale_count=" << stale.size() << ", detect_ms="
                 << std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - evict_t0)
                        .count();

    // 阶段 2（段锁释放后）：清理对象元数据。与 TTL reaper 的锁序一致
    // （snapshot shared lock 持有下调用，内部不获取 snapshot 锁）。
    ClearInvalidHandles();

    // 阶段 3（段锁内）：Commit 卸载，把旧段从注册表移除。
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (auto& info : stale) {
            segment_access.CommitUnmountSegment(info.id, info.client_id,
                                                info.dec_capacity);
            LOG(WARNING) << "client_id=" << info.client_id
                         << ", segment_name=" << info.name
                         << ", action=unmount_stale_generation_segment"
                         << ", endpoint=" << info.endpoint;
            cleanupHttpMetadata(info.name);
        }
    }

    // 阶段 4（无锁）：同步清理 etcd（挂载记录 + 最后挂载者卸载时的描述符）。
    // 多 submaster 下各自独立 evict 本机内存态；RemoveSegmentOwnerForCvm
    // 内部按「是否仍有其它 master 挂载」决定是否删描述符，天然防误删。
    // 此段是 MountSegment 30s 卡顿的头号嫌疑（逐段同步 etcd 事务），
    // 单独计时。
    {
        const auto etcd_t0 = std::chrono::steady_clock::now();
        for (const auto& info : stale) {
            RemoveSegmentOwnerForCvm(info.id);
        }
        LOG(WARNING) << "stale_generation_evict_etcd_done"
                     << ", segment_name=" << incoming.name
                     << ", host_id=" << incoming.host_id
                     << ", stale_count=" << stale.size() << ", etcd_ms="
                     << std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - etcd_t0)
                            .count();
    }

    // 阶段 5：invalid 集合卫生——死端点/段名不再有对应段，从排除集中清除，
    // 防止多次 worker 重启导致集合无界增长（与 MountSegment 自愈 erase 对称）。
    {
        std::lock_guard<std::shared_mutex> lock(invalid_endpoints_mutex_);
        for (const auto& info : stale) {
            invalid_replica_endpoints_.erase(info.endpoint);
            invalid_replica_endpoints_.erase(info.name);
            // UUID 形式（ReportInvalidReplicaEndpoints 三形式插入的对称
            // 清除）：vsegment 物理分配过滤按 quota.segment_id=UUID 匹配。
            invalid_replica_endpoints_.erase(UuidToString(info.id));
        }
    }

    RecomputeTenantEffectiveQuotas();
}

std::vector<uint16_t> MasterService::ResolveOwnedSlotsForCvm() {
    // ring_slots 缓存优先（§16.15.5）：本机是哪些 rank 的 primary_id，拥有
    // 这些 rank 的连续 slot 段（兼管多 rank 取并集）。缓存空（shadow write
    // 未启用的新集群）回退 §15 环推导，两模型并行灰度。
    // 启用判定必须用「存在归属记录」而非 G>0：旧集群（§15）的 cluster_meta
    // 反序列化后 G 也落默认 1，用 G 判定会把旧集群 primary 误判为 standby
    // （0 slot）。
    if (cvm_controller_ != nullptr &&
        cvm_controller_->HasCachedRingSlotAssigns()) {
        // 单锁快照（含 G）：与 vsegment 路由 / 客户端 router 的归属判定
        // 同源，并消除原「逐 rank 单点查询」的多次加锁开销。语义与原
        // 实现等价（rank 越界记录由 RankOwnedSlots 防御性返回空剔除，
        // 不影响本机 owned 集合）。
        const cvm::RingSlotsView view = cvm_controller_->BuildRingSlotsView();
        // 缓存对应的 etcd revision（range read ModRevision，锁外取——与上方
        // BuildRingSlotsView 同模式，避免 controller 锁内再套 resolver 锁）。
        // ring 模式下作为 route_epoch 门控数据写入 cvm_ring_revision_：修复
        // ring 分支只更新 owned 集合、revision 恒为 0，导致 numeric partition
        // 的 LoadEffectivePartitionRoute 恒被「ring_revision<=0」拒绝（全量
        // load_fail → PERSISTENT_FAIL → slot 永不 ready → 全部写入
        // SLOT_MIGRATING 的单主新集群死锁）。§15 回退分支用 masters
        // revision，此处用 ring 前缀 revision，语义一致：归属 CAS 推进 → 递增 →
        // 客户端 STALE_ROUTE 比对可感知归属变化。
        const int64_t ring_revision =
            cvm_controller_->GetCachedRingSlotsRevision();
        const uint32_t group_count = view.group_count();
        std::vector<cvm::RingSlotAssign> mine;
        for (const auto& a : view.assigns()) {
            if (a.rank < group_count && a.primary_id == master_id_) {
                mine.push_back(a);
            }
        }
        if (!mine.empty()) {
            std::vector<uint16_t> owned;
            for (const auto& a : mine) {
                const auto segment = cvm::RankOwnedSlots(a.rank, group_count);
                owned.insert(owned.end(), segment.begin(), segment.end());
            }
            std::lock_guard<std::mutex> lock(cvm_resolver_mutex_);
            cvm_ring_revision_ = ring_revision;
            const std::size_t new_count = owned.size();
            if (!owned_slot_count_logged_ ||
                new_count != last_logged_owned_count_) {
                LOG(INFO) << "ResolveOwnedSlotsForCvm: owned slot count "
                             "changed (ring_slots)"
                          << ", master_id=" << master_id_
                          << ", groups=" << group_count
                          << ", owned_slots=" << new_count
                          << ", previous=" << last_logged_owned_count_;
                owned_slot_count_logged_ = true;
                last_logged_owned_count_ = new_count;
            }
            cvm_last_resolved_owned_slots_ = owned;
            // ★ ring_slots 视图的 etcd 版本：vsegment 的
            // LoadEffectivePartitionRoute 要求 ring_revision > 0，
            // 否则所有 Partition 都会被判成 INVALID_VERSION。
            cvm_ring_revision_ = view.revision();
            return owned;
        }
        // 缓存有记录但本机非任何 rank 的 primary：本机 standby，返回空段。
        // 同样走 sticky 更新（下述旧路径的 sticky 语义在此不再适用——
        // standby 拥有 0 slot 是显式状态，非解析失败）。降级转移（如
        // primary N→0）打一次变更日志，稳态零输出。
        {
            std::lock_guard<std::mutex> lock(cvm_resolver_mutex_);
            // standby（非任何 rank 的 primary）同样维护 revision 门控：其对
            // foreign partition 的路由缓存加载也走同一判定。
            cvm_ring_revision_ = ring_revision;
            if (!owned_slot_count_logged_ || last_logged_owned_count_ != 0) {
                LOG(INFO) << "ResolveOwnedSlotsForCvm: owned slot count "
                             "changed (ring_slots)"
                          << ", master_id=" << master_id_ << ", owned_slots=0"
                          << ", previous=" << last_logged_owned_count_
                          << " (standby: not primary of any rank)";
                owned_slot_count_logged_ = true;
                last_logged_owned_count_ = 0;
            }
            cvm_last_resolved_owned_slots_.clear();
            cvm_ring_revision_ = view.revision();
        }
        return {};
    }

    // etcd 读取失败：sticky 策略——沿用上一轮成功解析的结果（可能为空）。
    // 绝不回退为全量接管：瞬时抖动引发的全量认领会与对端产生覆盖战
    // （双方反复重写对方 slot 记录，导致 slot 分布持续震荡不收敛）。
    // 回退瞬态打点（节流）：expected 集推导的幽灵风险观测（§16.14.6）。
    MaybeWarnGhostFallback("ResolveOwnedSlots", 0);
    std::vector<cvm::MasterRegistration> masters;
    ViewVersionId version;
    ErrorCode err =
        cvm::EtcdViewStore::LoadAllMasters(cluster_id_, masters, version);
    if (err != ErrorCode::OK) {
        std::lock_guard<std::mutex> lock(cvm_resolver_mutex_);
        LOG(WARNING) << "ResolveOwnedSlotsForCvm: LoadAllMasters failed: "
                     << err << ", keeping previous owned set (sticky), count="
                     << cvm_last_resolved_owned_slots_.size();
        return cvm_last_resolved_owned_slots_;
    }

    // 确定性哈希（§15.2）：primary_ids =
    // 先到先得排序(存活成员)[0:submaster_count]。 不再依赖 etcd 的 role
    // 字段过滤——role 由「本机是否在 primary_ids 内」 纯函数推导，消除 role
    // 未收敛时 standby 随机拿 slot 的竞态（§15.8 P2）。
    std::sort(masters.begin(), masters.end(), cvm::MasterRegistrationRankLess);
    std::vector<std::string> ids;
    ids.reserve(masters.size());
    for (const auto& m : masters) {
        if (!m.master_id.empty()) {
            ids.push_back(m.master_id);
        }
    }
    if (submaster_count_ > 0 && ids.size() > submaster_count_) {
        ids.resize(submaster_count_);
    }

    // 本机是否在推导出的 primary_ids 内（而非 etcd role 字段）。
    // ids 已按 create_revision 排序（非 master_id 字典序），不能用
    // binary_search，改线性查找（submaster_count 很小）。
    const bool self_primary =
        std::find(ids.begin(), ids.end(), master_id_) != ids.end();
    std::vector<uint16_t> slots;
    if (self_primary) {
        // 技术债 3.1：一致性哈希环分配已提取到 cvm::ResolveOwnedSlotsOnRing
        // 纯函数（可单元测试），本机 owned slots 由 (ids, master_id_) 决定。
        slots = cvm::ResolveOwnedSlotsOnRing(ids, master_id_);
    } else {
        LOG(WARNING) << "ResolveOwnedSlotsForCvm: self not in primary_ids "
                        "(primaries="
                     << ids.size() << "), owning no slots this cycle";
    }

    {
        std::lock_guard<std::mutex> lock(cvm_resolver_mutex_);
        const std::size_t new_count = slots.size();
        if (!owned_slot_count_logged_ ||
            new_count != last_logged_owned_count_) {
            LOG(INFO) << "ResolveOwnedSlotsForCvm: owned slot count changed"
                      << ", master_id=" << master_id_
                      << ", primaries=" << ids.size()
                      << ", owned_slots=" << new_count
                      << ", previous=" << last_logged_owned_count_;
            owned_slot_count_logged_ = true;
            last_logged_owned_count_ = new_count;
        }

        // P1：成员增删（根因）变化时才打印一次，避免心跳周期刷屏。
        if (cvm_last_primary_ids_ != ids) {
            // 迁移旧环（Phase 3）：保存变化前的 primary 列表，供 gained slot
            // 推导旧 owner（RPC 直传的「上一任 owner」）。仅 membership 变化时
            // 更新；不变时维持旧值，使 acquire 失败后的下一轮重试仍能定位旧
            // owner。
            cvm_prev_primary_ids_ = cvm_last_primary_ids_;
            std::vector<std::string> joined;
            std::vector<std::string> left;
            // Membership is ranked by registration revision, not by id.
            // Sort copies for set_difference; preserve ring tie-break order.
            auto current_ids = ids;
            auto previous_ids = cvm_last_primary_ids_;
            std::sort(current_ids.begin(), current_ids.end());
            std::sort(previous_ids.begin(), previous_ids.end());
            std::set_difference(current_ids.begin(), current_ids.end(),
                                previous_ids.begin(), previous_ids.end(),
                                std::back_inserter(joined));
            std::set_difference(previous_ids.begin(), previous_ids.end(),
                                current_ids.begin(), current_ids.end(),
                                std::back_inserter(left));
            auto join = [](const std::vector<std::string>& v) {
                std::string s;
                for (const auto& e : v) {
                    if (!s.empty()) s += ",";
                    s += e;
                }
                return s;
            };
            LOG(INFO) << "ResolveOwnedSlotsForCvm: membership changed"
                      << ", master_id=" << master_id_ << ", joined=["
                      << join(joined) << "]"
                      << ", left=[" << join(left) << "]"
                      << ", primaries=" << ids.size();
            cvm_last_primary_ids_ = ids;
        }

        cvm_last_resolved_owned_slots_ = slots;
        cvm_ring_revision_ = version;

        // 存活成员集（primary + standby）：供 ImportSlotMetadata 判断旧 owner
        // 是否已消亡。旧 owner 消亡（lease 过期删 key）时无法 RPC 拉取，改走
        // 空元数据 + 客户端重建。每次成功解析后刷新；LoadAllMasters 失败时
        // sticky 沿用旧值（与 owned slots 一致）。
        cvm_alive_master_ids_.clear();
        cvm_alive_master_ids_.reserve(masters.size());
        for (const auto& m : masters) {
            if (!m.master_id.empty()) {
                cvm_alive_master_ids_.push_back(m.master_id);
            }
        }
    }
    return slots;
}

std::optional<std::string> MasterService::ResolveSlotOwnerMasterId(
    uint16_t slot) const {
    // ring_slots 缓存优先（§16.15.5）：SlotToRank 纯函数定位 rank，读该
    // rank 的 primary_id。缓存空回退 §15 环反查，两模型并行灰度。
    // 回退瞬态打点（节流）：写/转发判定的幽灵风险观测（§16.14.6）。
    if (cvm_controller_ != nullptr) {
        const uint32_t group_count = cvm_controller_->GetCachedSlotGroupCount();
        cvm::RingSlotAssign a;
        if (group_count > 0 && cvm_controller_->GetCachedRingSlotAssign(
                                   cvm::SlotToRank(slot, group_count), a)) {
            if (a.primary_id.empty()) {
                return std::nullopt;  // 晋升未完成 → 调用方 SLOT_NOT_OWNED
            }
            return a.primary_id;
        }
        MaybeWarnGhostFallback("ResolveSlotOwner", slot);
    }

    std::vector<cvm::MasterRegistration> masters;
    ViewVersionId version;
    ErrorCode err =
        cvm::EtcdViewStore::LoadAllMasters(cluster_id_, masters, version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "ResolveSlotOwnerMasterId: LoadAllMasters failed: "
                     << err << ", slot=" << slot;
        return std::nullopt;
    }

    std::sort(masters.begin(), masters.end(), cvm::MasterRegistrationRankLess);
    std::vector<std::string> ids;
    ids.reserve(masters.size());
    for (const auto& m : masters) {
        if (!m.master_id.empty()) {
            ids.push_back(m.master_id);
        }
    }
    if (submaster_count_ > 0 && ids.size() > submaster_count_) {
        ids.resize(submaster_count_);
    }

    // 与 ResolveOwnedSlotsForCvm 保持一致：primary_ids = 先到先得排序(存活成员)
    // [0:submaster_count]，不依赖 etcd role 字段，客户端/服务端推导同环。
    // 解析结果为空（无存活成员）时由调用方跳过转发。
    return cvm::ResolveSlotOwnerOnRing(ids, slot);
}
#else
void MasterService::PublishSegmentOwnerForCvm(const Segment&) {}
void MasterService::RemoveSegmentOwnerForCvm(const UUID&) {}
void MasterService::EvictStaleGenerationForHost(const Segment&, const UUID&) {}
void MasterService::EnqueueStaleGenerationEvict(const Segment&, const UUID&) {}
void MasterService::StaleEvictThreadFunc() {}
void MasterService::FlushStaleEvictQueueForTesting() {}
std::vector<uint16_t> MasterService::ResolveOwnedSlotsForCvm() { return {}; }
void MasterService::MaybeWarnGhostFallback(const char*, uint16_t) const {}
std::optional<std::string> MasterService::ResolveSlotOwnerMasterId(
    uint16_t /*slot*/) const {
    return std::nullopt;
}
#endif

void MasterService::UpdateExpectedSlots(const std::vector<uint16_t>& slots) {
    std::unique_lock<std::shared_mutex> lock(owned_slots_mutex_);
    expected_owned_.assign(cvm::SlotCount(), false);
    for (uint16_t slot : slots) {
        if (slot < cvm::SlotCount()) {
            expected_owned_[slot] = true;
        }
    }
    // Retained slots remain ready. Newly acquired slots are not serviceable
    // until both object metadata and their vsegment state have been installed.
    ready_owned_.resize(cvm::SlotCount(), false);
    uint32_t ready_count = 0;
    for (size_t slot = 0; slot < ready_owned_.size(); ++slot) {
        ready_owned_[slot] = ready_owned_[slot] && expected_owned_[slot];
        if (ready_owned_[slot]) {
            ++ready_count;
        }
    }
    // ready/expected 计数同步（饱和日志观测点，见成员注释）。
    expected_slot_count_ = static_cast<uint32_t>(slots.size());
    ready_slot_count_ = ready_count;
    owned_slots_ready_ = true;
    // 一致性哈希环分配结果（3.1）：仅在数量变化（含首次解析）时打印，
    // 记录本机当前负责的 slot 规模；心跳周期内重复不变则静默。
    if (!owned_slot_count_logged_ || slots.size() != last_logged_owned_count_) {
        LOG(INFO) << "Owned slots updated (consistent-hash ring): count="
                  << slots.size() << "/" << cvm::SlotCount()
                  << (owned_slot_count_logged_ ? " (changed)" : "");
        owned_slot_count_logged_ = true;
        last_logged_owned_count_ = slots.size();
    }
    VLOG(1) << "UpdateExpectedSlots: " << slots.size() << " slots";
}

void MasterService::MarkSlotsReady(const std::vector<uint16_t>& slots) {
    std::unique_lock<std::shared_mutex> lock(owned_slots_mutex_);
    if (ready_owned_.size() != cvm::SlotCount()) {
        ready_owned_.assign(ready_owned_.size(), false);
        ready_owned_.resize(cvm::SlotCount(), false);
    }
    for (uint16_t slot : slots) {
        if (slot < ready_owned_.size() && slot < expected_owned_.size() &&
            expected_owned_[slot]) {
            if (!ready_owned_[slot]) {
                ++ready_slot_count_;
            }
            ready_owned_[slot] = true;
        }
    }
    // A former owner may regain a slot after an earlier ACK failed. Its
    // previous immutable export must never survive into the new serving epoch.
    {
        std::lock_guard<std::mutex> staged_lock(pending_slot_exports_mutex_);
        for (uint16_t slot : slots) {
            if (slot < expected_owned_.size() && expected_owned_[slot])
                pending_slot_exports_.erase(slot);
        }
    }
    // 饱和转变日志（进入打一条，离开静默复位）：ready==expected 即「全部
    // 已归属 slot 元数据就绪、写入可放行」的明确观测点——vsegment 发布
    // 成功（reconcile recovered）之后、客户端恢复写入之前的最后一环。
    if (expected_slot_count_ > 0 && ready_slot_count_ == expected_slot_count_) {
        if (!all_ready_logged_) {
            LOG(INFO) << "All owned slots ready: ready=" << ready_slot_count_
                      << "/" << expected_slot_count_
                      << ", master is ready to serve writes"
                      << ", master_id=" << master_id_;
            all_ready_logged_ = true;
        }
    } else if (all_ready_logged_) {
        all_ready_logged_ = false;
    }
}

bool MasterService::OwnsSlot(uint16_t slot) const {
    std::shared_lock<std::shared_mutex> lock(owned_slots_mutex_);
    if (!owned_slots_ready_) {
        return true;  // partition 未启用或尚未解析过，放行
    }
    return slot < ready_owned_.size() && ready_owned_[slot];
}

ErrorCode MasterService::CheckSlotServiceability(uint16_t slot) const {
    std::shared_lock<std::shared_mutex> lock(owned_slots_mutex_);
    if (!owned_slots_ready_) {
        return ErrorCode::OK;  // partition 未启用或尚未解析，放行
    }
    if (slot < ready_owned_.size() && ready_owned_[slot]) {
        return ErrorCode::OK;
    }
    if (slot < expected_owned_.size() && expected_owned_[slot]) {
        return ErrorCode::SLOT_MIGRATING;
    }
    return ErrorCode::SLOT_NOT_OWNED;
}

#ifdef STORE_USE_ETCD
void MasterService::MaybeWarnGhostFallback(const char* where,
                                           uint16_t slot) const {
    // 幽灵写瞬态告警（§16.14.6 验证辅助）：模型曾启用（seen）但本 slot
    // 的归属缓存未命中（G 未加载 / rank 记录缺失）→ 判定正在走旧环回退，
    // 结果可能与 etcd 真实归属不一致。旧集群（从未 seen）回退是正常
    // 稳态，不告警。数据面 QPS 高：30s 节流一条，CAS 保证多线程只打一次。
    if (cvm_controller_ == nullptr ||
        !cvm_controller_->HasSeenRingSlotsModel()) {
        return;
    }
    const uint32_t group_count = cvm_controller_->GetCachedSlotGroupCount();
    cvm::RingSlotAssign assign;
    if (group_count > 0 && cvm_controller_->GetCachedRingSlotAssign(
                               cvm::SlotToRank(slot, group_count), assign)) {
        return;  // 缓存命中：正常路径
    }
    const int64_t now_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    const int64_t last =
        last_ghost_fallback_warn_ms_.load(std::memory_order_relaxed);
    if (now_ms - last < 30000) {
        return;
    }
    int64_t expected = last;  // compare_exchange 需可变引用
    if (last_ghost_fallback_warn_ms_.compare_exchange_strong(
            expected, now_ms, std::memory_order_relaxed)) {
        LOG(WARNING) << "GHOST-WRITE-RISK " << where
                     << ": ring_slots model active but slot ownership "
                        "cache missed, legacy-ring fallback in use "
                        "(may diverge from etcd): master_id="
                     << master_id_ << ", slot=" << slot
                     << ", cached_group_count=" << group_count;
    }
}
#endif

ErrorCode MasterService::CheckSlotWritable(uint16_t slot) const {
    // §16.19.3 写路径校验（读写分离）：ready/expected 判定复用读路径的
    // CheckSlotServiceability，其上叠加 reshard 迁出 barrier——本机是该
    // slot 所在 rank 的 primary，但 ring_slots 显示 kMigrating 且
    // migrating_to 非本机（正在迁出）→ 冻结写返回 SLOT_MIGRATING。读路径
    // 继续走 CheckSlotServiceability（源在 kMigrating 期间仍服务读）。
    // fail-safe 取向「宁可误冻结，不可漏冻结」：kMigrating 置位由
    // RingSlotsWatch 即时驱动（watch 滞后仅短暂多冻/漏窗口 < 刷新周期）。
    const ErrorCode svc = CheckSlotServiceability(slot);
    if (svc != ErrorCode::OK) {
        return svc;  // SLOT_MIGRATING（gaining）/ SLOT_NOT_OWNED 原样透传
    }
    if (cvm_controller_ != nullptr) {
        const uint32_t group_count = cvm_controller_->GetCachedSlotGroupCount();
        cvm::RingSlotAssign assign;
        if (group_count > 0 &&
            cvm_controller_->GetCachedRingSlotAssign(
                cvm::SlotToRank(slot, group_count), assign) &&
            static_cast<cvm::SlotState>(assign.state) ==
                cvm::SlotState::kMigrating &&
            assign.migrating_to_id != master_id_) {
            return ErrorCode::SLOT_MIGRATING;  // 迁出 barrier：冻结写
        }
    }
    // 判定通过（返回 OK）：若处于「模型已启用但缓存空」的旧环回退瞬态，
    // 此 OK 可能基于过时推导——打点（节流）供线上复现定位。
    MaybeWarnGhostFallback("CheckSlotWritable", slot);
    return ErrorCode::OK;
}

ErrorCode MasterService::CheckVSegmentServiceability(
    const std::string& partition_id) const {
    if (!partition_id.empty() && partition_id.size() <= 5 &&
        partition_id.find_first_not_of("0123456789") == std::string::npos) {
        const auto slot = std::stoul(partition_id);
        if (slot < cvm::SlotCount())
            return CheckSlotServiceability(static_cast<uint16_t>(slot));
    }
    return ErrorCode::OK;  // Named Partitions use explicit PartitionRoute.
}
MasterService::~MasterService() {
    // Join the vsegment background init thread BEFORE stopping the oplog
    // writer（首次全量 sweep 逐 partition 使用 ordered_oplog_writer_ 构造
    // committer，须先 join 再 Stop，避免 stop 后使用）。持 spawn 锁防御并发
    // spawn 面。
    {
        std::lock_guard<std::mutex> spawn_lock(vsegment_init_spawn_mutex_);
        if (vsegment_init_thread_.joinable()) {
            vsegment_init_thread_.join();
        }
    }

    // Stop the SlotOwnerHeartbeat thread before tearing down the rest.
    StopSlotOwnerHeartbeat();

    // Stop the P4 reshard driver before the inter-master RPC client it uses
    // （拉取/ack 依赖 inter_master_rpc_，须先 join 再销毁客户端）。
    {
        std::lock_guard<std::mutex> lock(reshard_wake_mutex_);
        reshard_driver_running_ = false;
    }
    reshard_wake_cv_.notify_all();
    if (reshard_driver_thread_.joinable()) {
        reshard_driver_thread_.join();
    }

    StopInterMasterRpc();

    // Stop and join the threads
    eviction_running_ = false;
    client_monitor_running_ = false;

    task_cleanup_running_ = false;
    {
        std::lock_guard lock(weight_reconciliation_mutex_);
        weight_reconciliation_running_ = false;
    }
    job_dispatch_running_ = false;
    http_metadata_cleanup_running_ = false;
    // Resolve durability waits while every callback target is still alive,
    // before joining workers that may be waiting for an OpLog publication.
    // This releases publication waiters; in-flight backend I/O still follows
    // the writer's shutdown contract and is not cancelled by the worker flag.
    if (ordered_oplog_writer_) {
        ordered_oplog_writer_->Stop();
    }
    stale_evict_running_ = false;
    graceful_unmount_scheduler_.Stop();
    replica_cleanup_worker_.Stop();
    kvcs_capacity_refresh_worker_.Stop();
#ifdef USE_NOF
    nof_heartbeat_running_ = false;
#endif

    // Wake sleepers so join() doesn't block for long sleep intervals.
    task_cleanup_cv_.notify_all();
    weight_reconciliation_cv_.notify_all();
    http_metadata_cleanup_cv_.notify_all();
    stale_evict_cv_.notify_all();

    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
#ifdef USE_NOF
    if (nof_heartbeat_thread_.joinable()) {
        nof_heartbeat_thread_.join();
    }
#endif
    if (task_cleanup_thread_.joinable()) {
        task_cleanup_thread_.join();
    }
    if (weight_reconciliation_thread_.joinable()) {
        weight_reconciliation_thread_.join();
    }
    if (http_metadata_cleanup_thread_.joinable()) {
        http_metadata_cleanup_thread_.join();
    }
    if (job_dispatch_thread_.joinable()) {
        job_dispatch_thread_.join();
    }
    dynamic_replication_.Stop();
    if (stale_evict_thread_.joinable()) {
        stale_evict_thread_.join();
    }

    // Its admission worker executes handlers that capture this service. Stop
    // and join it while all handler dependencies are still alive.
    MasterMetricManager::instance().clear_io_pattern_runtime(
        io_pattern_runtime_.get());
    io_pattern_cfm_service_.reset();
    io_pattern_runtime_.reset();

    client_offboarding_worker_.Stop();
    for (const auto& [segment, bytes] : standby_accounted_memory_bytes_) {
        MasterMetricManager::instance().dec_allocated_mem_size(
            segment, static_cast<int64_t>(bytes));
        MasterMetricManager::instance().remove_segment_metrics(segment);
    }

    // Segments still mounted here never went through CommitUnmountSegment;
    // release their capacity contribution so the process-lifetime
    // MasterMetricManager stays consistent when the next leadership term
    // constructs a fresh MasterService and the clients remount.
    segment_manager_.releaseCapacityMetrics();

    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    auto& metrics = MasterMetricManager::instance();
    for (const auto& [_, record] : client_liveness_records_) {
        metrics.on_client_liveness_record_removed(record->state());
    }
    client_liveness_records_.clear();
}

void MasterService::SetBatchOpLogTerminalCallback(
    OrderedOpLogWriter::TerminalCallback callback) {
    if (ordered_oplog_writer_) {
        ordered_oplog_writer_->SetTerminalCallback(std::move(callback));
    }
}

void MasterService::StopBatchOpLogWriter() {
    if (ordered_oplog_writer_) ordered_oplog_writer_->Stop();
}

TieredStorageUsageSnapshot MasterService::GetStorageUsageSnapshot() const {
    TieredStorageUsageSnapshot snapshot{
        .memory = segment_manager_.GetMemoryUsageSnapshot(),
        .nof = nof_segment_manager_.GetUsageSnapshot(),
        .dfs = {},
    };
    if (enable_dfs_ && dfs_allocator_ && dfs_allocator_->IsInitialized()) {
        snapshot.dfs = {
            .enabled = true,
            .used_bytes = dfs_allocator_->GetUsedBytes(),
            .capacity_bytes = dfs_allocator_->GetTotalCapacity(),
            .file_count = dfs_allocator_->GetFileCount(),
        };
    } else if (enable_dfs_ && dfs_kvcs_capacity_provider_) {
        std::lock_guard lock(dfs_kvcs_usage_mutex_);
        snapshot.dfs = dfs_kvcs_usage_;
    }
    return snapshot;
}

auto MasterService::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    ErrorCode mount_result = ErrorCode::INTERNAL_ERROR;
    {
        std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
        auto [record_it, inserted] = client_liveness_records_.try_emplace(
            client_id, std::make_shared<ClientLivenessRecord>(
                           ClientLivenessRecord::Clock::now()));
        const auto record = record_it->second;
        if (inserted) {
            MasterMetricManager::instance().client_liveness_record_created();
        }
        const auto observation =
            record->ObserveAndRun(ClientLivenessRecord::Clock::now(), [&] {
                std::shared_lock<std::shared_mutex> snapshot_lock(
                    master_state_mutex_);
                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                LOG(INFO) << "client_id=" << client_id
                          << ", action=mount_segment, segment_name="
                          << segment.name;
                mount_result =
                    segment_access.MountSegment(segment, client_id, record);
                return mount_result == ErrorCode::OK ||
                       mount_result == ErrorCode::SEGMENT_ALREADY_EXISTS;
            });
        if (observation == ClientLivenessObservation::REJECTED_OFFLINE) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        if (mount_result != ErrorCode::OK &&
            mount_result != ErrorCode::SEGMENT_ALREADY_EXISTS) {
            if (inserted) {
                client_liveness_records_.erase(client_id);
                MasterMetricManager::instance()
                    .on_client_liveness_record_removed(
                        ClientLivenessState::ACTIVE);
            }
            return tl::make_unexpected(mount_result);
        }
        if (observation == ClientLivenessObservation::RECOVERED_ACTIVE) {
            MasterMetricManager::instance().client_liveness_recovered();
            LOG(INFO) << "client_id=" << client_id
                      << ", action=client_liveness_recovered, "
                         "signal=memory_mount";
        }
    }

    if (mount_result == ErrorCode::OK && enable_oplog_ &&
        ordered_oplog_writer_) {
        SegmentMountOp op;
        op.segment_name = segment.name;
        op.transport_endpoint = segment.te_endpoint;
        op.capacity = segment.size;
        op.is_memory_segment = true;
        op.file_path.clear();
        auto bytes = struct_pack::serialize(op);
        PersistSegmentOpForHA("MountSegment", OpType::SEGMENT_MOUNT,
                              segment.te_endpoint,
                              std::string(bytes.begin(), bytes.end()));
    }
    if (mount_result == ErrorCode::OK) {
        UpdateClientHostId(client_id, segment.host_id);
        NotifyPolicyCapacityChanged();
    }
    // segment（重）挂载成功后，该 endpoint 不再视为 invalid。升主恢复
    // （RestoreFromStandbySnapshot）在 client 尚未重挂时会把 segment 的
    // transport_endpoint/name 标记进 invalid_replica_endpoints_，若此处不清除，
    // 这些 replica 会因 IsReplicaReadable 永久返回 false 而
    // REPLICA_IS_NOT_READY。
    {
        std::lock_guard<std::shared_mutex> lock(invalid_endpoints_mutex_);
        invalid_replica_endpoints_.erase(segment.te_endpoint);
        invalid_replica_endpoints_.erase(segment.name);
        // UUID 形式（ReportInvalidReplicaEndpoints 三形式插入的对称清除）：
        // vsegment 物理分配过滤按 quota.segment_id=UUID 匹配排除集。
        invalid_replica_endpoints_.erase(UuidToString(segment.id));
    }
    PublishSegmentOwnerForCvm(segment);
    // P2-8-2：补登该 client 的 LOCAL_DISK segment_name（首次内存段挂载
    // 时记录），供代际驱逐做 name 级 co-tenant 过滤。LOCAL_DISK-only
    // client 无 name 上下文，保持空值（退化为 host 级归并）。
    local_ssd_manager_.SetClientSegmentName(client_id, segment.name);
    // 方案一 A（异步化）：新代段挂载完成后，将同 host 同 name 旧代死段的
    // 清理（P2PHANDSHAKE 端口漂移 / 非 P2P owner 代际替换 / LOCAL_DISK
    // 对称卸载 / etcd 清理）入队转交 stale_evict_thread_ 延迟执行——RPC
    // 线程仅入队（µs 级），把同步 etcd 事务与锁竞争移出 mount 关键路径
    // （30s 卡顿两大主因）。「不等 TTL reaper」语义不变：drain debounce
    // 仅秒级延迟。
    EnqueueStaleGenerationEvict(segment, client_id);
    // vsegment auto-plan 加速触发点（低频端点）：配额快照尚未发布时立即重试一次
    // 「发现→规划→发布」（W6 静默期门控下通常静默返回，由心跳兜底在
    // mount 流静默后发布）。发布后的 VSegment 拉起仍由下一次心跳的
    // RefreshVSegmentOwnership 承担（重路径，绝不挂在 mount RPC 内）。
    NoteVSegmentMountActivity();
    MaybeRetryVSegmentAutoPlan("mount");
    return {};
}

auto MasterService::MountNoFSegment(const NoFSegment& segment,
                                    const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id << ", segment_name=" << segment.name
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    ScopedNoFSegmentAccess nof_segment_access =
        nof_segment_manager_.getNoFSegmentAccess();

    LOG(INFO) << "NoF segment mount: "
              << "client_id=" << client_id
              << ", action=mount_segment, segment_name=" << segment.name;

    auto err = nof_segment_access.MountSegment(segment, client_id);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
#endif
}

ErrorCode MasterService::ValidateStandbyRemountSegment(
    const Segment& segment) const {
    const StandbySegmentInfo* match = nullptr;
    for (const auto& standby : standby_memory_segments_) {
        if (standby.transport_endpoint == segment.te_endpoint ||
            standby.segment_name == segment.name) {
            if (match != nullptr && match != &standby) {
                return ErrorCode::INVALID_PARAMS;
            }
            match = &standby;
        }
    }
    if (match != nullptr && segment.protocol == "cxl") {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }
    if (match != nullptr && (match->segment_name != segment.name ||
                             match->transport_endpoint != segment.te_endpoint ||
                             match->capacity != segment.size)) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

auto MasterService::ReMountSegment(const std::vector<Segment>& segments,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    InvalidateVSegmentLayoutCache();
    std::unique_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    {
        std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
        auto [record_it, record_inserted] =
            client_liveness_records_.try_emplace(
                client_id, std::make_shared<ClientLivenessRecord>(
                               ClientLivenessRecord::Clock::now()));
        const auto record = record_it->second;
        if (record_inserted) {
            MasterMetricManager::instance().client_liveness_record_created();
        }
        auto remount_guard = record->TryAcquireRetainingGuard();
        if (!remount_guard) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        const auto discard_provisional_record = [&] {
            if (!record_inserted) {
                return;
            }
            client_liveness_records_.erase(client_id);
            MasterMetricManager::instance().on_client_liveness_record_removed(
                ClientLivenessState::ACTIVE);
        };
        std::unique_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
        for (const auto& segment : segments) {
            if (!segment.host_id.empty()) {
                client_host_id_[client_id] = segment.host_id;
                break;
            }
        }
        {
            auto segment_access = segment_manager_.getSegmentAccess();
            for (const auto& segment : segments) {
                auto standby_validation =
                    ValidateStandbyRemountSegment(segment);
                if (standby_validation != ErrorCode::OK) {
                    discard_provisional_record();
                    return tl::make_unexpected(standby_validation);
                }
                auto validation =
                    segment_access.ValidateRemountSegment(segment, client_id);
                if (validation != ErrorCode::OK) {
                    discard_provisional_record();
                    return tl::make_unexpected(validation);
                }
            }
        }
        if (ok_client_.contains(client_id)) {
            LOG(WARNING) << "client_id=" << client_id
                         << ", warn=client_already_remounted";
            // Return OK because this is an idempotent operation
            return {};
        }

        struct SegmentRestore {
            Segment segment;
            std::shared_ptr<BufferAllocatorBase> old_allocator;
            std::shared_ptr<BufferAllocatorBase> restored_allocator;
            std::vector<Replica*> replicas;
            std::vector<AllocatedBuffer::Descriptor> descriptors;
            std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
            uint64_t imported_size{0};
        };
        std::vector<SegmentRestore> restores;
        restores.reserve(segments.size());
        std::vector<bool> segment_existed(segments.size());
        auto rollback_new_segments = [&] {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            for (size_t i = 0; i < segments.size(); ++i) {
                if (segment_existed[i] ||
                    !segment_access.GetAllocator(segments[i].id)) {
                    continue;
                }
                size_t capacity = 0;
                if (segment_access.PrepareUnmountSegment(
                        segments[i].id, capacity) != ErrorCode::OK) {
                    LOG(ERROR) << "segment_name=" << segments[i].name
                               << ", error=remount_rollback_prepare_failed";
                    continue;
                }
                if (segment_access.CommitUnmountSegment(
                        segments[i].id, client_id, capacity) != ErrorCode::OK) {
                    LOG(ERROR) << "segment_name=" << segments[i].name
                               << ", error=remount_rollback_commit_failed";
                }
            }
        };
        auto fail_remount =
            [&](ErrorCode error) -> tl::expected<void, ErrorCode> {
            rollback_new_segments();
            discard_provisional_record();
            return tl::make_unexpected(error);
        };

        ErrorCode remount_error = ErrorCode::OK;
        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            for (size_t i = 0; i < segments.size(); ++i) {
                segment_existed[i] =
                    segment_access.GetAllocator(segments[i].id) != nullptr;
            }

            remount_error =
                segment_access.ReMountSegment(segments, client_id, record);
            if (remount_error == ErrorCode::OK) {
                segment_access.BindClientLiveness(client_id, record);
                for (const auto& segment : segments) {
                    auto allocator = segment_access.GetAllocator(segment.id);
                    Segment authoritative;
                    if (!allocator ||
                        !segment_access.GetSegment(segment.id, authoritative)) {
                        remount_error = ErrorCode::INTERNAL_ERROR;
                        break;
                    }
                    restores.push_back({std::move(authoritative),
                                        std::move(allocator),
                                        nullptr,
                                        {},
                                        {},
                                        {},
                                        0});
                }
            }
        }
        if (remount_error != ErrorCode::OK) {
            return fail_remount(remount_error);
        }

        bool ambiguous_endpoint = false;
        bool unsupported_cxl = false;
        // master_state_mutex_ is held exclusively, so no object operation runs
        // until the repair below: the replicas this walk collects stay put,
        // and each object it touched is locked again there by its publication.
        // SP930 port: track pre-remount readability so the first successful
        // recovery starts a fresh lease without extending already-readable
        // objects.
        std::unordered_map<ObjectMetadata*, bool> restored_objects;
        std::vector<std::pair<TenantId, route::ObjectRef>> affected_objects;
        bool any_standby_kept_alive = std::any_of(
            segments.begin(), segments.end(), [this](const Segment& segment) {
                return standby_accounted_memory_bytes_.contains(segment.name);
            });
        if (any_standby_kept_alive) {
            VisitNamespaces([&](const TenantId& tenant_id,
                                metadata::Namespace& ns) {
                for (auto object : ns.objects.WriteCursor()) {
                    bool affected = false;
                    object.metadata().VisitReplicas(
                        [](const Replica& replica) {
                            return replica.is_memory_replica() &&
                                   replica.status() != ReplicaStatus::REMOVED &&
                                   replica.status() != ReplicaStatus::FAILED;
                        },
                        [&](Replica& replica) {
                            auto descriptor = replica.get_descriptor()
                                                  .get_memory_descriptor()
                                                  .buffer_descriptor;
                            SegmentRestore* match = nullptr;
                            bool endpoint_match = false;
                            for (auto& restore : restores) {
                                if (descriptor.transport_endpoint_ ==
                                        restore.segment.te_endpoint ||
                                    descriptor.transport_endpoint_ ==
                                        restore.segment.name) {
                                    endpoint_match = true;
                                    // Reject CXL via the existing check below.
                                    if (descriptor.protocol_ != "cxl" &&
                                        (descriptor.buffer_address_ <
                                             restore.segment.base ||
                                         descriptor.buffer_address_ -
                                                 restore.segment.base >=
                                             restore.segment.size))
                                        continue;
                                    if (match != nullptr) {
                                        ambiguous_endpoint = true;
                                        return;
                                    }
                                    match = &restore;
                                }
                            }
                            if (endpoint_match && match == nullptr) {
                                ambiguous_endpoint = true;
                                return;
                            }
                            if (match != nullptr) {
                                if (descriptor.protocol_ == "cxl") {
                                    unsupported_cxl = true;
                                    return;
                                }
                                descriptor.transport_endpoint_ =
                                    match->segment.te_endpoint;
                                if (!restored_objects.contains(
                                        &object.metadata())) {
                                    restored_objects.emplace(
                                        &object.metadata(),
                                        object.metadata().HasReplica(
                                            [this](const Replica& candidate) {
                                                return candidate
                                                           .is_memory_replica() &&
                                                       IsReplicaReadable(
                                                           candidate);
                                            }));
                                }
                                match->replicas.push_back(&replica);
                                match->descriptors.push_back(descriptor);
                                affected = true;
                            }
                        });
                    if (affected) {
                        affected_objects.emplace_back(tenant_id, object.ref());
                    }
                }
            });
        }
        if (ambiguous_endpoint) {
            return fail_remount(ErrorCode::INVALID_PARAMS);
        }
        if (unsupported_cxl) {
            return fail_remount(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
        }

        for (auto& restore : restores) {
            if (restore.descriptors.empty()) {
                continue;
            }
            const RegionResourceSpec spec{
                restore.segment.id, restore.segment.name, restore.segment.base,
                restore.segment.size, restore.segment.te_endpoint};
            auto allocations =
                BuildRegionLiveAllocations(spec, restore.descriptors);
            if (!allocations) {
                return fail_remount(allocations.error());
            }
            if (std::dynamic_pointer_cast<OffsetBufferAllocator>(
                    restore.old_allocator)) {
                auto imported = ImportOffsetBufferAllocator(
                    restore.segment.name, restore.segment.base,
                    restore.segment.size, restore.segment.te_endpoint,
                    restore.segment.protocol, *allocations);
                if (!imported) {
                    return fail_remount(ErrorCode::INVALID_PARAMS);
                }
                restore.restored_allocator = std::move(imported->allocator);
                restore.buffers = std::move(imported->buffers);
            } else if (std::dynamic_pointer_cast<CachelibBufferAllocator>(
                           restore.old_allocator)) {
                auto imported = ImportCachelibBufferAllocator(
                    restore.segment.name, restore.segment.base,
                    restore.segment.size, restore.segment.te_endpoint,
                    restore.segment.protocol, *allocations);
                if (!imported) {
                    return fail_remount(ErrorCode::INVALID_PARAMS);
                }
                restore.restored_allocator = std::move(imported->allocator);
                restore.buffers = std::move(imported->buffers);
            } else {
                return fail_remount(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
            }
        }

        std::vector<ScopedSegmentAccess::AllocatorReplacement>
            allocator_replacements;
        for (auto& restore : restores) {
            if (restore.restored_allocator) {
                if (restore.buffers.size() != restore.replicas.size() ||
                    std::any_of(restore.buffers.begin(), restore.buffers.end(),
                                [](const auto& buffer) { return !buffer; })) {
                    return fail_remount(ErrorCode::INTERNAL_ERROR);
                }
                restore.imported_size = std::accumulate(
                    restore.descriptors.begin(), restore.descriptors.end(),
                    uint64_t{0}, [](uint64_t sum, const auto& descriptor) {
                        return sum + descriptor.size_;
                    });
                auto accounted =
                    standby_accounted_memory_bytes_.find(restore.segment.name);
                if (accounted == standby_accounted_memory_bytes_.end() ||
                    accounted->second < restore.imported_size) {
                    return fail_remount(ErrorCode::INTERNAL_ERROR);
                }
                allocator_replacements.push_back({restore.segment.id,
                                                  restore.old_allocator,
                                                  restore.restored_allocator});
            }
        }
        bool allocators_replaced = false;
        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            allocators_replaced =
                segment_access.ReplaceAllocators(allocator_replacements);
        }
        if (!allocators_replaced) {
            return fail_remount(ErrorCode::INTERNAL_ERROR);
        }
        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            for (auto& restore : restores) {
                for (auto& buffer : restore.buffers) {
                    segment_access.BindBufferToSegment(restore.segment.id,
                                                       *buffer);
                }
            }
        }
        for (auto& restore : restores) {
            if (restore.imported_size != 0) {
                MasterMetricManager::instance().dec_allocated_mem_size(
                    restore.segment.name,
                    static_cast<int64_t>(restore.imported_size));
                auto accounted =
                    standby_accounted_memory_bytes_.find(restore.segment.name);
                accounted->second -= restore.imported_size;
                if (accounted->second == 0) {
                    standby_accounted_memory_bytes_.erase(accounted);
                }
            }
            for (size_t i = 0; i < restore.replicas.size(); ++i) {
                (void)restore.replicas[i]->replace_memory_buffer(
                    std::move(restore.buffers[i]));
            }
            invalid_replica_endpoints_.erase(restore.segment.te_endpoint);
            invalid_replica_endpoints_.erase(restore.segment.name);
            standby_allocator_keepalive_.erase(restore.segment.te_endpoint);
            standby_allocator_keepalive_.erase(restore.segment.name);
        }
        for (const auto& [tenant_id, object] : affected_objects) {
            MetadataAccessorRW accessor(this, tenant_id, object);
            if (accessor.IsPublished()) {
                accessor.Get().GrantReadLease(
                    std::chrono::milliseconds(default_kv_lease_ttl_));
            }
        }
        // SP930 port: a replica whose readable state was restored by this
        // remount earns a fresh lease (with the soft pin), but only when it
        // was not readable before: the lease clock must not be extended for
        // objects that were serving traffic all along.
        for (const auto& [metadata, was_readable] : restored_objects) {
            if (!was_readable &&
                metadata->HasReplica([this](const Replica& replica) {
                    return replica.is_memory_replica() &&
                           IsReplicaReadable(replica);
                })) {
                const auto soft_pin_deadline = metadata->GrantLease(
                    std::chrono::milliseconds(default_kv_lease_ttl_),
                    std::chrono::milliseconds(default_kv_soft_pin_ttl_));
                if (soft_pin_deadline) {
                    soft_pin_deadline_index_.Upsert(
                        metadata->tenant_id.MakeScopedKey(metadata->user_key),
                        *soft_pin_deadline);
                }
            }
        }

        // Change the client status to OK
        ok_client_.insert(client_id);
        MasterMetricManager::instance().inc_active_clients();
    }

    if (enable_oplog_ && ordered_oplog_writer_) {
        for (const auto& seg : segments) {
            SegmentMountOp op;
            op.segment_name = seg.name;
            op.transport_endpoint = seg.te_endpoint;
            op.capacity = seg.size;
            op.is_memory_segment = true;
            op.file_path.clear();
            auto bytes = struct_pack::serialize(op);
            PersistSegmentOpForHA("ReMountSegment", OpType::SEGMENT_MOUNT,
                                  seg.name,
                                  std::string(bytes.begin(), bytes.end()));
        }
    }
    NotifyPolicyCapacityChanged();

    for (const auto& seg : segments) {
        PublishSegmentOwnerForCvm(seg);
    }

    // vsegment auto-plan 加速触发点（同 MountSegment 尾部；submaster 重启后
    // 旧 mount lease 失效、client 经本路径重连补挂，是「重启自愈」的关键
    // 入口）。仅发布快照，VSegment 拉起留给下一次心跳。W6：remount 同样
    // 刷新静默期时间戳（重启补挂流也可能增量补齐段）。
    NoteVSegmentMountActivity();
    MaybeRetryVSegmentAutoPlan("remount");

    return {};
}

auto MasterService::ReMountNoFSegment(const std::vector<NoFSegment>& segments,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id
               << ", segments_count=" << segments.size()
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    ScopedNoFSegmentAccess nof_segment_access =
        nof_segment_manager_.getNoFSegmentAccess();
    ErrorCode err = nof_segment_access.ReMountSegment(segments, client_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
#endif
}

std::shared_ptr<ClientLivenessRecord> MasterService::FindClientRecord(
    const UUID& client_id) const {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    const auto it = client_liveness_records_.find(client_id);
    return it == client_liveness_records_.end() ? nullptr : it->second;
}

std::unordered_set<UUID, boost::hash<UUID>>
MasterService::GetRetainingClientIdsLocked() const {
    std::unordered_set<UUID, boost::hash<UUID>> clients;
    clients.reserve(client_liveness_records_.size());
    for (const auto& [client_id, record] : client_liveness_records_) {
        if (record->ShouldRetainResources()) {
            clients.insert(client_id);
        }
    }
    return clients;
}

void MasterService::UpdateClientHostId(const UUID& client_id,
                                       const std::string& host_id) {
    if (host_id.empty()) {
        return;
    }
    {
        std::shared_lock<std::shared_mutex> lock(client_mutex_);
        auto it = client_host_id_.find(client_id);
        if (it != client_host_id_.end() && it->second == host_id) {
            return;
        }
    }

    std::unique_lock<std::shared_mutex> lock(client_mutex_);
    auto it = client_host_id_.find(client_id);
    if (it == client_host_id_.end() || it->second != host_id) {
        client_host_id_[client_id] = host_id;
    }
}

std::string MasterService::GetClientHostId(const UUID& client_id) const {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    auto it = client_host_id_.find(client_id);
    return it == client_host_id_.end() ? std::string() : it->second;
}

const TenantId& MasterService::ResolveRequestTenantId(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    return tenant_id;
}

MasterService::ObjectIdentity MasterService::MakeObjectIdentityForRequest(
    const std::string& user_key, const TenantId& tenant_id) const {
    return {ResolveRequestTenantId(tenant_id), user_key};
}

bool MasterService::IsTenantRegistered(const TenantId& tenant_id) const {
    return tenant_quota_policy_ == nullptr ||
           tenant_quota_policy_->IsTenantRegistered(tenant_id);
}

tl::expected<TenantId, ErrorCode> MasterService::ResolveTenantIdForWrite(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    if (!IsTenantRegistered(tenant_id)) {
        return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    return tenant_id;
}

uint64_t MasterService::AllocatableMemoryBytes() {
    uint64_t capacity = 0;
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::pair<Segment, UUID>> segments;
    if (segment_access.GetAllSegments(segments) != ErrorCode::OK) {
        return 0;
    }
    for (const auto& [segment, _] : segments) {
        if (capacity > std::numeric_limits<uint64_t>::max() - segment.size) {
            return std::numeric_limits<uint64_t>::max();
        }
        capacity += segment.size;
    }
    return capacity;
}

void MasterService::NotifyPolicyRestored() {
    if (policy_ != nullptr) {
        policy_->OnRestored();
    }
}

void MasterService::NotifyPolicyCapacityChanged() {
    if (policy_ != nullptr) {
        policy_->OnCapacityChanged();
    }
}

void MasterService::PolicyStoreAccess::VisitNamespaces(
    const std::function<void(const TenantId&, route::ObjectRoute&)>& fn) {
    service_.namespaces_.Visit(
        [&](const TenantId& tenant_id, metadata::Namespace& ns) {
            fn(tenant_id, ns.objects);
        });
}

size_t MasterService::PolicyStoreAccess::ObjectCount(
    const TenantId& tenant_id) const {
    const metadata::Namespace* ns = service_.namespaces_.Lookup(tenant_id);
    return ns == nullptr ? 0 : ns->objects.ObjectCount();
}

uint64_t MasterService::PolicyStoreAccess::AllocatableMemoryBytes() const {
    return service_.AllocatableMemoryBytes();
}

StoreControl::EvictionResult
MasterService::PolicyStoreAccess::EvictNamespaceMemory(
    const TenantId& tenant_id, uint64_t target_bytes) {
    return service_.EvictNamespaceMemory(tenant_id, target_bytes);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
MasterService::BeginWeightImport(const BeginWeightImportRequest& request) {
    return weight_manager_.BeginWeightImport(request);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
MasterService::CommitWeightImport(const CommitWeightImportRequest& request) {
    return weight_manager_.CommitWeightImport(request);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
MasterService::AbortWeightImport(const AbortWeightImportRequest& request) {
    return weight_manager_.AbortWeightImport(request);
}

WeightMetadataStore::Result<WeightRevisionView>
MasterService::GetWeightRevision(
    const GetWeightRevisionRequest& request) const {
    return weight_manager_.GetWeightRevision(request);
}

WeightMetadataStore::Result<ListWeightRevisionsResponse>
MasterService::ListWeightRevisions(
    const ListWeightRevisionsRequest& request) const {
    return weight_manager_.ListWeightRevisions(request);
}

WeightMetadataStore::Result<WeightRevisionLease>
MasterService::AcquireWeightRevisionLease(
    const AcquireWeightRevisionLeaseRequest& request) {
    return weight_manager_.AcquireWeightRevisionLease(request);
}

WeightMetadataStore::Result<WeightRevisionLease>
MasterService::RenewWeightRevisionLease(
    const RenewWeightRevisionLeaseRequest& request) {
    return weight_manager_.RenewWeightRevisionLease(request);
}

WeightMetadataStore::Result<void> MasterService::ReleaseWeightRevisionLease(
    const ReleaseWeightRevisionLeaseRequest& request) {
    return weight_manager_.ReleaseWeightRevisionLease(request);
}

WeightMetadataStore::Result<WeightResidencyOperation>
MasterService::StartWeightResidencyOperation(
    const StartWeightResidencyOperationRequest& request) {
    return weight_manager_.StartWeightResidencyOperation(request);
}

WeightMetadataStore::Result<WeightResidencyOperation>
MasterService::QueryWeightOperation(
    const QueryWeightOperationRequest& request) const {
    return weight_manager_.QueryWeightOperation(request);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
MasterService::ReconcileWeightRevision(
    const ReconcileWeightRevisionRequest& request) {
    return weight_manager_.ReconcileWeightRevision(request);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
MasterService::DeleteWeightRevision(
    const DeleteWeightRevisionRequest& request) {
    return weight_manager_.DeleteWeightRevision(request);
}

std::vector<std::string> MasterService::GetGroupMemberKeys(
    const TenantId& tenant_id, const std::string& group_id) const {
    const metadata::Namespace* ns = namespaces_.Lookup(tenant_id);
    if (ns == nullptr) {
        return {};
    }
    return ns->objects.GroupMembers(group_id);
}

void MasterService::EvictManagedWeightMembersToCold(
    const WeightRevisionMetadata& revision,
    const std::vector<std::string>& member_keys) {
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    if (weight_manager_.HasActiveLease(revision.identity,
                                       revision.metadata_generation, now_ms)) {
        return;
    }
    const TenantId tenant_id(revision.identity.tenant_id);
    metadata::Namespace* ns = namespaces_.Lookup(tenant_id);
    if (ns == nullptr || member_keys.empty()) {
        return;
    }
    const std::unordered_set<std::string> selected_members(member_keys.begin(),
                                                           member_keys.end());
    // Not const: ObjectMetadata::IsLeaseExpired takes it by reference.
    auto now = std::chrono::system_clock::now();
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    // Memory eviction's own reclaimer, without the offload: a member goes cold
    // only when it holds a readable cold replica already.
    eviction::Reclaimer& reclaimer =
        enable_oplog_
            ? static_cast<eviction::Reclaimer&>(memory_eviction_->oplog)
            : memory_eviction_->immediate;
    for (const std::string& member_key :
         ns->objects.GroupMembers(revision.manifest.payload_group_id)) {
        if (!selected_members.contains(member_key)) {
            continue;
        }
        // Freed once the member's lock is let go.
        std::vector<Replica> detached;
        const std::optional<route::WriteGuard> hold =
            ns->objects.Write(member_key);
        if (!hold) {
            continue;
        }
        ObjectMetadata& metadata = hold->metadata();
        if (metadata.group_id != revision.manifest.payload_group_id ||
            metadata.data_type != ObjectDataType::WEIGHT) {
            continue;
        }
        const bool has_cold =
            metadata.HasReplica([this](const Replica& replica) {
                return !replica.is_memory_replica() &&
                       IsReplicaReadable(replica);
            });
        if (!has_cold || !metadata.IsLeaseExpired(now) ||
            IsSoftPinActive(metadata, now)) {
            continue;
        }
        eviction::Plan plan;
        for (const Replica& replica : metadata.GetAllReplicas()) {
            if (IsEvictableMemoryReplica(replica)) {
                plan.release.push_back(replica.id());
            }
        }
        if (plan.release.empty()) {
            continue;
        }
        auto effect = reclaimer.Apply(*ns, *hold, plan, eviction::Report{});
        if (!effect) {
            break;
        }
        // With the OpLog on, the durable finalize announces it instead.
        if (!enable_oplog_) {
            PublishKvRemovedAfterEvict(member_key, effect->detached.size(),
                                       "cpu", metadata, tenant_id);
        }
        if (effect->needs_teardown) {
            reclaimer.TearDown(*ns, *hold);
        }
        detached = std::move(effect->detached);
    }
}

bool MasterService::HasCompletedMemoryCacheReplica(
    const ObjectMetadata& metadata) {
    return metadata.HasReplica([](const Replica& replica) {
        return (replica.is_memory_replica() || replica.is_vsegment_replica()) &&
               replica.is_completed();
    });
}

bool MasterService::HasCompletedDiskCacheReplica(
    const ObjectMetadata& metadata) {
    return metadata.HasReplica([](const Replica& replica) {
        return (replica.is_disk_replica() || replica.is_local_disk_replica()) &&
               replica.is_completed();
    });
}

void MasterService::SyncCacheTotalAccounting(ObjectMetadata& metadata) {
    const bool has_memory_cache_replica =
        HasCompletedMemoryCacheReplica(metadata);
    const bool has_disk_cache_replica = HasCompletedDiskCacheReplica(metadata);

    if (!metadata.memory_cache_total_accounted && has_memory_cache_replica) {
        MasterMetricManager::instance().inc_mem_cache_nums();
        metadata.memory_cache_total_accounted = true;
    } else if (metadata.memory_cache_total_accounted &&
               !has_memory_cache_replica) {
        MasterMetricManager::instance().dec_mem_cache_nums();
        metadata.memory_cache_total_accounted = false;
    }

    if (!metadata.disk_cache_total_accounted && has_disk_cache_replica) {
        MasterMetricManager::instance().inc_file_cache_nums();
        metadata.disk_cache_total_accounted = true;
    } else if (metadata.disk_cache_total_accounted && !has_disk_cache_replica) {
        MasterMetricManager::instance().dec_file_cache_nums();
        metadata.disk_cache_total_accounted = false;
    }
}

void MasterService::AccountCacheTotalRemoval(ObjectMetadata& metadata) {
    if (metadata.memory_cache_total_accounted) {
        MasterMetricManager::instance().dec_mem_cache_nums();
        metadata.memory_cache_total_accounted = false;
    }
    if (metadata.disk_cache_total_accounted) {
        MasterMetricManager::instance().dec_file_cache_nums();
        metadata.disk_cache_total_accounted = false;
    }
}

std::vector<Replica> MasterService::PopReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& pred_fn) {
    ReleaseVSegmentReplicaState(metadata, pred_fn);
    auto replicas = metadata.PopReplicas(pred_fn);
    SyncCacheTotalAccounting(metadata);
    return replicas;
}

std::vector<Replica> MasterService::PopReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata) {
    ReleaseVSegmentReplicaState(metadata, [](const Replica&) { return true; });
    auto replicas = metadata.PopReplicas();
    SyncCacheTotalAccounting(metadata);
    return replicas;
}

void MasterService::ReleaseVSegmentReplicaState(
    const ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& pred_fn) {
    if (!vsegment_service_) return;
    const auto allocation_id =
        metadata.tenant_id.MakeScopedKey(metadata.user_key);
    metadata.VisitReplicas(
        [&](const Replica& replica) {
            return replica.is_vsegment_replica() && pred_fn(replica);
        },
        [&](const Replica& replica) {
            const auto& descriptor = replica.get_vsegment_descriptor();
            ErrorCode result = ErrorCode::OBJECT_NOT_FOUND;
            if ((replica.status() == ReplicaStatus::PROCESSING ||
                 replica.status() == ReplicaStatus::INITIALIZED) &&
                !descriptor.operation_id.empty()) {
                result = vsegment_service_->AbortPutOwned(
                    descriptor.partition_id, descriptor.vsegment_id,
                    descriptor.operation_id);
                // Same-size Upsert marks an already committed replica as
                // PROCESSING. INVALID_WRITE distinguishes that state from a
                // live reservation, so release its committed allocation.
                if (result == ErrorCode::INVALID_WRITE) {
                    result = vsegment_service_->ReleaseObject(descriptor,
                                                              allocation_id);
                }
            } else {
                result =
                    vsegment_service_->ReleaseObject(descriptor, allocation_id);
            }
            if (result != ErrorCode::OK) {
                LOG(ERROR) << "Failed to discard vsegment replica state, key="
                           << metadata.user_key
                           << ", error=" << toString(result);
            }
        });
}

size_t MasterService::EraseReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& pred_fn,
    std::vector<ReplicaID>* erased_replica_ids) {
    auto erased_replicas =
        PopReplicasWithCacheTotalAccounting(metadata, pred_fn);
    if (erased_replica_ids != nullptr) {
        erased_replica_ids->reserve(erased_replica_ids->size() +
                                    erased_replicas.size());
        for (const auto& replica : erased_replicas) {
            erased_replica_ids->push_back(replica.id());
        }
    }
    std::vector<ReplicaID> erased_ids;
    erased_ids.reserve(erased_replicas.size());
    for (const auto& replica : erased_replicas) {
        erased_ids.push_back(replica.id());
    }
    dynamic_replication_.RecordReplicaRemoval(metadata, erased_ids);
    // Release SSD/local-disk usage for any local-disk replicas being removed.
    // No-op for memory/noF replicas, so it is safe to call unconditionally.
    ReleaseLocalDiskUsage(erased_replicas);
    FreeDfsReplicas(metadata.user_key, erased_replicas);
    return erased_replicas.size();
}

void MasterService::FinalizeRemovedReplicasAfterDurable(
    const OpLogEntry& durable_entry,
    const std::vector<ReplicaID>& replica_ids) {
    if (replica_ids.empty()) {
        return;
    }

    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    // Replica ids are never reused, so the replicas name the publication they
    // were removed from and no generation check is needed.
    MetadataAccessorRW accessor(
        this, ObjectIdentity{tenant_id, durable_entry.object_key});
    if (!accessor.IsPublished()) {
        return;
    }

    std::unordered_set<ReplicaID> ids(replica_ids.begin(), replica_ids.end());
    auto& metadata = accessor.Get();
    // The replicas being finalized are already marked REMOVED, so the normal
    // completed-replica view no longer contains their media. Preserve the
    // pre-removal view for the event delta.
    const auto previous_media = KvRemovalSnapshot(metadata);
    auto erased_replicas = PopReplicasWithCacheTotalAccounting(
        metadata, [&ids](const Replica& replica) {
            return replica.status() == ReplicaStatus::REMOVED &&
                   ids.contains(replica.id());
        });
    if (erased_replicas.empty()) {
        return;
    }
    std::vector<ReplicaID> erased_replica_ids;
    erased_replica_ids.reserve(erased_replicas.size());
    for (const auto& replica : erased_replicas) {
        erased_replica_ids.push_back(replica.id());
    }
    dynamic_replication_.RecordReplicaRemoval(metadata, erased_replica_ids);
    // SP930 port: durable erase of LOCAL_DISK replicas must notify every
    // holder client so it can physically delete the SSD file; collect the
    // holder ids for the RemoveObjectHeartbeat queue below.
    const bool erased_local_disk = std::any_of(
        erased_replicas.begin(), erased_replicas.end(),
        [](const Replica& replica) { return replica.is_local_disk_replica(); });
    std::vector<UUID> local_disk_holders;
    for (const auto& replica : erased_replicas) {
        if (!replica.is_local_disk_replica()) continue;
        local_disk_holders.push_back(
            replica.get_descriptor().get_local_disk_descriptor().client_id);
    }
    ReleaseLocalDiskUsage(erased_replicas);
    FreeDfsReplicas(metadata.user_key, erased_replicas);
    CancelPromotionTaskForRemovedReplicas(metadata, accessor.GetState(),
                                          erased_replica_ids);
    if (!metadata.IsValid()) {
        accessor.Erase(previous_media);
    } else {
        SyncKvObjectState(durable_entry.object_key, metadata, tenant_id,
                          previous_media);
        if (metadata.AllReplicas(&Replica::fn_is_completed)) {
            accessor.EraseFromProcessing();
        }
    }
    if (erased_local_disk) {
        EnqueueRemoveTasks(
            local_disk_holders,
            RemoveTaskItem{tenant_id.value(), durable_entry.object_key});
    }
}

void MasterService::FinalizeMetadataEraseAfterDurable(
    route::ObjectRef object, const TenantId& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    // Act on the pinned publication only: a same-key recreate that landed
    // while the erase was in flight must not be erased, and an object already
    // torn down is not released a second time (refcounts, KV removal
    // events).
    MetadataAccessorRW accessor(this, tenant_id, object);
    if (!accessor.IsPublished()) {
        return;
    }
    accessor.Erase();
}

void MasterService::FinalizeExpiredProcessingReplicasAfterDurable(
    route::ObjectRef object, const OpLogEntry& durable_entry,
    const std::chrono::system_clock::time_point& ttl) {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    // Identity gate: the durable cleanup was requested for the publication
    // that is still routed, so a same-key recreate that landed while the
    // cleanup was in flight keeps its replicas.
    MetadataAccessorRW accessor(this, tenant_id, object);
    if (!accessor.Exists()) {
        return;
    }

    auto& metadata = accessor.Get();
    // Snapshot before popping: the pop is what makes a medium disappear, and
    // the publisher needs the pre-pop set to compute the removal delta. Mirrors
    // FinalizeExpiredReplicationTaskAfterDurable.
    const auto previous_media = KvRemovalSnapshot(metadata);
    auto replicas = PopReplicasWithCacheTotalAccounting(
        metadata, &Replica::fn_is_processing);
    if (!replicas.empty()) {
        FreeDfsReplicas(metadata.user_key, replicas);
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.emplace_back(std::move(replicas), ttl);
    }
    if (!metadata.IsValid()) {
        accessor.Erase(previous_media);
    } else {
        SyncKvObjectState(durable_entry.object_key, metadata, tenant_id,
                          previous_media);
        if (accessor.InProcessing()) {
            accessor.EraseFromProcessing();
        }
    }
}

void MasterService::FinalizeExpiredReplicationTaskAfterDurable(
    const OpLogEntry& durable_entry, ReplicaID source_id,
    const std::vector<ReplicaID>& target_ids,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch,
    const std::chrono::system_clock::time_point& ttl) {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    MetadataAccessorRW accessor(this, MakeObjectIdentityForRequest(
                                          durable_entry.object_key, tenant_id));
    if (!accessor.Exists()) {
        return;
    }

    if (!accessor.HasReplicationTask()) {
        return;
    }
    auto& task = accessor.GetReplicationTask();
    if (!task.durable_cleanup_pending || task.source_id != source_id ||
        task.replica_ids != target_ids ||
        task.dynamic_replication_lease_id != dynamic_replication_lease_id ||
        task.dynamic_replication_version_epoch !=
            dynamic_replication_version_epoch) {
        return;
    }

    auto& metadata = accessor.Get();
    if (auto source = metadata.GetReplicaByID(source_id); source != nullptr) {
        source->dec_refcnt();
    }

    // Snapshot the visible media before popping: targets are matched by id
    // regardless of status, so a completed replica can be dropped here and its
    // medium would otherwise be unrecoverable for the remove delta.
    const auto previous_media = KvRemovalSnapshot(metadata);
    std::unordered_set<ReplicaID> ids(target_ids.begin(), target_ids.end());
    auto replicas = PopReplicasWithCacheTotalAccounting(
        metadata,
        [&ids](const Replica& replica) { return ids.contains(replica.id()); });
    std::vector<ReplicaID> erased_replica_ids;
    erased_replica_ids.reserve(replicas.size());
    for (const auto& replica : replicas) {
        erased_replica_ids.push_back(replica.id());
    }
    const bool dynamic_task = dynamic_replication_lease_id != UUID{} ||
                              dynamic_replication_version_epoch != 0;
    dynamic_replication_.RecordReplicaRemoval(metadata, erased_replica_ids);
    if (!replicas.empty()) {
        FreeDfsReplicas(metadata.user_key, replicas);
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.emplace_back(std::move(replicas), ttl);
    }
    if (dynamic_task) {
        dynamic_replication_.ClearPendingLocked(accessor.GetTenantId(),
                                                durable_entry.object_key,
                                                accessor.GetState());
    }
    if (!metadata.IsValid()) {
        accessor.Erase(previous_media);
    } else {
        // Publish before EraseReplicationTask, which may drop an emptied
        // tenant and invalidate the metadata reference.
        SyncKvObjectState(durable_entry.object_key, metadata, tenant_id,
                          previous_media);
        accessor.EraseReplicationTask();
    }
}

MasterService::StaleHandleCleanupPlan
MasterService::BuildStaleHandleCleanupPlan(
    const ObjectMetadata& metadata,
    const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients)
    const {
    return BuildStaleHandleCleanupPlan(
        metadata, [&retaining_clients](const Replica& replica) {
            return (replica.has_invalid_mem_handle() ||
                    replica.has_invalid_nof_handle() ||
                    replica.has_stale_local_disk_client(retaining_clients)) &&
                   replica.is_completed();
        });
}

MasterService::StaleHandleCleanupPlan
MasterService::BuildStaleHandleCleanupPlan(
    const ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& is_stale) const {
    StaleHandleCleanupPlan plan;
    bool has_valid_after_cleanup = false;
    for (const auto& replica : metadata.GetAllReplicas()) {
        if (is_stale(replica)) {
            plan.removed_ids.push_back(replica.id());
            continue;
        }
        if (!replica.is_memory_replica() || !replica.has_invalid_mem_handle()) {
            has_valid_after_cleanup = true;
        }
    }
    plan.would_invalidate =
        metadata.TotalLength() == 0 || !has_valid_after_cleanup;
    return plan;
}

tl::expected<void, ErrorCode> MasterService::PersistStaleHandleCleanupForHA(
    const std::string& why, const TenantId& tenant_id, const std::string& key,
    ObjectMetadata& metadata, const StaleHandleCleanupPlan& plan) {
    if (plan.removed_ids.empty() || !enable_oplog_) {
        return {};
    }

    const std::unordered_set<ReplicaID> ids(plan.removed_ids.begin(),
                                            plan.removed_ids.end());
    const auto op_type =
        plan.would_invalidate ? OpType::REMOVE : OpType::PUT_END;
    std::string payload;
    if (!plan.would_invalidate) {
        std::vector<Replica::Descriptor> remaining;
        for (const auto& replica : metadata.GetAllReplicas()) {
            if (!ids.contains(replica.id()) &&
                replica.status() == ReplicaStatus::COMPLETE) {
                remaining.push_back(replica.get_descriptor());
            }
        }
        payload = SerializeMetadataForOpLogFromReplicaDescriptors(metadata,
                                                                  remaining);
    }

    auto reservation = ReserveBatchOpLogSlot();
    if (!reservation) {
        return tl::make_unexpected(reservation.error());
    }
    metadata.VisitReplicas(
        [&ids](const Replica& replica) { return ids.contains(replica.id()); },
        [](Replica& replica) { replica.mark_removed(); });
    auto result = AppendReservedOpLogWithDurableFinalize(
        std::move(reservation.value()), op_type, tenant_id.value(), key,
        payload,
        [this,
         removed_ids = plan.removed_ids](const OpLogEntry& durable_entry) {
            FinalizeRemovedReplicasAfterDurable(durable_entry, removed_ids);
        });
    if (!result) {
        metadata.VisitReplicas(
            [&ids](const Replica& replica) {
                return ids.contains(replica.id());
            },
            [](Replica& replica) { replica.cancel_remove(); });
        LOG(WARNING) << why
                     << ": stale cleanup OpLog queue failed for key=" << key
                     << ", err=" << static_cast<int>(result.error());
        return tl::make_unexpected(result.error());
    }
    return {};
}

void MasterService::VisitNamespaces(
    const std::function<void(const TenantId&, metadata::Namespace&)>& visit)
    const {
    namespaces_.Visit([&](const TenantId& tenant_id, metadata::Namespace& ns) {
        if (ns.objects.ObjectCount() != 0) {
            visit(tenant_id, ns);
        }
    });
}

void MasterService::CleanupInvalidMemoryReplicas(MetadataAccessorRW& accessor) {
    // Only memory replicas are checked here, to avoid a lock order violation
    // (client_mutex_ must be acquired before a key). local_disk
    // replicas are cleaned up by ClearInvalidHandles() in ClientMonitorFunc.
    if (enable_ha_ && enable_oplog_) {
        return;
    }
    const TenantId& tenant_id = accessor.GetTenantId();
    ObjectMetadata& metadata = accessor.Get();
    // Gate the snapshot on the publisher being live: this runs on every
    // read-write metadata access, and the VisitReplicas walk plus vector
    // allocation is pure overhead when KV events are off (the default).
    const auto previous_kv_media = KvMediaSnapshot(metadata);
    // Erase invalid memory replicas (those with unmounted segments). No
    // client_mutex_ needed since we only check memory replicas.
    std::vector<ReplicaID> removed_replica_ids;
    EraseReplicasWithCacheTotalAccounting(
        metadata,
        [](const Replica& replica) { return replica.has_invalid_mem_handle(); },
        &removed_replica_ids);
    CancelPromotionTaskForRemovedReplicas(metadata, accessor.GetState(),
                                          removed_replica_ids);
    SyncKvObjectState(accessor.GetKey(), metadata, tenant_id,
                      previous_kv_media);
    // If no valid replicas remain, delete the whole object, and with it every
    // task in flight for it.
    if (!metadata.IsValid()) {
        accessor.Erase();
    }
}

// EraseMetadata deletes the object and, with it, every task in flight for
// it. It gives back what each task holds first: the offload's and the
// replication's source refcount. Callers no longer need to clean these up
// before calling. What the object charged its tenant goes with it when the
// key's lock is let go.
ErrorCode MasterService::EraseMetadata(
    metadata::Namespace& ns, const route::WriteGuard& hold,
    EraseMode erase_mode, const std::vector<std::string>& previous_media_hint) {
    auto& metadata = hold.metadata();
    auto& state = hold.state();
    const std::string& key = hold.key();
    const TenantId& tenant_id = ns.id();

    // Release logical VSegment allocations before changing publication or
    // policy state. A failure must not leave a still-published object with
    // its policy and event bookkeeping already torn down.
    if (erase_mode != EraseMode::kHandoff && vsegment_service_) {
        for (const auto& replica : metadata.GetAllReplicas()) {
            if (!replica.is_vsegment_replica()) continue;
            const auto result = vsegment_service_->ReleaseObject(
                replica.get_vsegment_descriptor(), tenant_id.MakeScopedKey(key));
            if (result != ErrorCode::OK) {
                LOG(ERROR) << "Failed to release vsegment allocation, key="
                           << key << ", error=" << toString(result);
                return result;
            }
        }
    }
    if (erase_mode == EraseMode::kFull) {
        ns.BeforeTearDown(hold);
    }

    // Preserve the publisher's state while a size-changing upsert temporarily
    // removes the old metadata. A complete erase emits one remove event per
    // media that was previously visible.
    if (erase_mode == EraseMode::kReplace) {
        // Union rather than replace: the caller hint may only describe one
        // tier, so a medium still visible in metadata must not be dropped
        // from the remove delta.
        auto previous_media = KvRemovalSnapshot(metadata);
        previous_media.insert(previous_media.end(), previous_media_hint.begin(),
                              previous_media_hint.end());
        SyncKvObjectState(key, metadata, tenant_id, previous_media);
    } else {
        PublishKvRemoved(key, metadata, tenant_id, previous_media_hint);
    }

    // Clean up offloading_tasks + dec_refcnt before erasing metadata.
    // When BatchEvict deletes metadata, Store Worker may still have an
    // in-flight offload for this key. Without this cleanup the task
    // becomes an orphan that only expires after 600s.
    if (state.offloading_task) {
        auto source = metadata.GetReplicaByID(state.offloading_task->source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }
        state.offloading_task.reset();

        // The mailbox entry must be dropped too, otherwise the next
        // OffloadObjectHeartbeat drains a task-less key back to the client and
        // produces an orphan bucket.
        local_ssd_manager_.RemoveOffloadFromAll(tenant_id, key);
    }
    if (state.replication_task) {
        auto source =
            metadata.GetReplicaByID(state.replication_task->source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }
    }
    ErasePromotionTaskIfPresent(state);
    dynamic_replication_.ClearPendingLocked(tenant_id, key, state);
    // The candidate leaves the retry index with the object, or the index would
    // hold its slot toward the candidate limit until the retry loop reached it.
    promotion_candidates_.EraseLocked(tenant_id, key, state);

    if (erase_mode != EraseMode::kHandoff) {
        ReleaseLocalDiskUsage(metadata.GetAllReplicas());
        FreeDfsReplicas(key, metadata.GetAllReplicas());
    }
    EnqueueRemoteFreeIfTracked(tenant_id, key, erase_mode);
    AccountCacheTotalRemoval(metadata);
    if (metadata.GetCommittedSoftPinTimeout()) {
        soft_pin_deadline_index_.Remove(tenant_id.MakeScopedKey(key));
    }
    hold.TearDown();
    return ErrorCode::OK;
}

void MasterService::ReleaseLocalDiskUsage(std::span<const Replica> replicas) {
    std::unordered_map<UUID, int64_t, boost::hash<UUID>> bytes_by_client;
    for (const auto& replica : replicas) {
        if (!replica.is_local_disk_replica()) {
            continue;
        }
        const auto descriptor =
            replica.get_descriptor().get_local_disk_descriptor();
        if (descriptor.object_size > 0) {
            bytes_by_client[descriptor.client_id] += descriptor.object_size;
        }
    }
    if (bytes_by_client.empty()) {
        return;
    }

    for (const auto& [client_id, bytes] : bytes_by_client) {
        local_ssd_manager_.AdjustUsedBytes(client_id, -bytes);
    }
}

void MasterService::SoftPinDeadlineIndex::MaybeCompactLocked() {
    const size_t live_count = registrations_.size();
    const size_t ratio_limit =
        live_count > std::numeric_limits<size_t>::max() / kCompactionRatio
            ? std::numeric_limits<size_t>::max()
            : live_count * kCompactionRatio;
    const size_t threshold = std::max(kMinCompactionThreshold, ratio_limit);
    if (heap_.size() <= threshold) {
        return;
    }

    decltype(heap_) rebuilt;
    for (const auto& [scoped_key, registration] : registrations_) {
        rebuilt.push(Entry{registration.deadline, scoped_key});
    }
    heap_.swap(rebuilt);
}

void MasterService::SoftPinDeadlineIndex::Upsert(std::string scoped_key,
                                                 const TimePoint& deadline) {
    std::lock_guard lock(mutex_);
    const auto it = registrations_.find(scoped_key);
    if (it != registrations_.end() && it->second.deadline == deadline) {
        return;
    }

    auto [registration_it, inserted] =
        registrations_.insert_or_assign(scoped_key, Registration{deadline});
    (void)inserted;
    heap_.push(Entry{deadline, registration_it->first});
    MaybeCompactLocked();
}

void MasterService::SoftPinDeadlineIndex::Remove(
    const std::string& scoped_key) {
    std::lock_guard lock(mutex_);
    if (registrations_.erase(scoped_key) > 0) {
        MaybeCompactLocked();
    }
}

void MasterService::SoftPinDeadlineIndex::RemoveIfMatches(
    const std::string& scoped_key, const TimePoint& deadline) {
    std::lock_guard lock(mutex_);
    const auto it = registrations_.find(scoped_key);
    if (it != registrations_.end() && it->second.deadline == deadline) {
        registrations_.erase(it);
        MaybeCompactLocked();
    }
}

std::vector<MasterService::SoftPinDeadlineIndex::Entry>
MasterService::SoftPinDeadlineIndex::PopExpired(const TimePoint& now) {
    std::vector<Entry> expired;
    std::lock_guard lock(mutex_);
    while (!heap_.empty() && heap_.top().deadline <= now) {
        Entry entry = heap_.top();
        heap_.pop();

        const auto it = registrations_.find(entry.scoped_key);
        if (it == registrations_.end() ||
            it->second.deadline != entry.deadline) {
            continue;
        }
        registrations_.erase(it);
        expired.push_back(std::move(entry));
    }
    MaybeCompactLocked();
    return expired;
}

void MasterService::SoftPinDeadlineIndex::Clear() {
    std::lock_guard lock(mutex_);
    registrations_.clear();
    decltype(heap_) empty;
    heap_.swap(empty);
}

auto MasterService::ResolveSoftPinRequest(const ReplicateConfig& config) const
    -> tl::expected<ResolvedSoftPinRequest, ErrorCode> {
    switch (config.soft_pin_action) {
        case SoftPinAction::PRESERVE:
        case SoftPinAction::DISABLE:
            if (config.soft_pin_ttl_ms.has_value()) {
                LOG(ERROR) << "soft_pin_action=" << config.soft_pin_action
                           << ", soft_pin_ttl_ms=" << *config.soft_pin_ttl_ms
                           << ", error=ttl_requires_enable";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            return ResolvedSoftPinRequest{config.soft_pin_action,
                                          std::chrono::milliseconds::zero()};
        case SoftPinAction::ENABLE: {
            const uint64_t ttl_ms =
                config.soft_pin_ttl_ms.value_or(default_kv_soft_pin_ttl_);
            if (ttl_ms > max_kv_soft_pin_ttl_) {
                LOG(ERROR) << "soft_pin_ttl_ms=" << ttl_ms
                           << ", max_kv_soft_pin_ttl=" << max_kv_soft_pin_ttl_
                           << ", error=soft_pin_ttl_exceeds_limit";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            return ResolvedSoftPinRequest{config.soft_pin_action,
                                          std::chrono::milliseconds(ttl_ms)};
        }
    }
    LOG(ERROR) << "soft_pin_action="
               << static_cast<uint32_t>(config.soft_pin_action)
               << ", error=invalid_soft_pin_action";
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

auto MasterService::BatchUpdateSoftPin(const std::vector<std::string>& keys,
                                       const TenantId& tenant_id,
                                       uint64_t ttl_ms)
    -> std::vector<tl::expected<void, ErrorCode>> {
    ReplicateConfig config;
    config.soft_pin_action = SoftPinAction::ENABLE;
    config.soft_pin_ttl_ms = ttl_ms;
    auto request = ResolveSoftPinRequest(config);
    if (!tenant_id.IsValid() || !request) {
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    const TenantId normalized_tenant = ResolveRequestTenantId(tenant_id);
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        if (key.empty()) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        MetadataAccessorRW accessor(this,
                                    ObjectIdentity{normalized_tenant, key});
        if (!accessor.Exists()) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
            continue;
        }
        auto& metadata = accessor.Get();
        if (accessor.InProcessing() || metadata.pending_soft_pin_action ||
            !metadata.HasReplica(&Replica::fn_is_completed)) {
            results.emplace_back(
                tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY));
            continue;
        }
        ApplySoftPinEvaluation(
            metadata,
            metadata.ApplySoftPin(*request, std::chrono::system_clock::now()));
        results.emplace_back();
    }
    return results;
}

void MasterService::ApplySoftPinMetricDelta(int metric_delta) {
    if (metric_delta > 0) {
        MasterMetricManager::instance().inc_soft_pin_key_count(metric_delta);
    } else if (metric_delta < 0) {
        MasterMetricManager::instance().dec_soft_pin_key_count(-metric_delta);
    }
}

void MasterService::ApplySoftPinEvaluation(
    const ObjectMetadata& metadata,
    const ObjectMetadata::SoftPinEvaluation& result) const {
    if (!result.deadline_to_index && !result.removed_deadline) {
        ApplySoftPinMetricDelta(result.metric_delta);
        return;
    }

    const auto scoped_key = metadata.tenant_id.MakeScopedKey(metadata.user_key);
    if (result.deadline_to_index) {
        soft_pin_deadline_index_.Upsert(scoped_key, *result.deadline_to_index);
    } else if (result.removed_deadline) {
        soft_pin_deadline_index_.RemoveIfMatches(scoped_key,
                                                 *result.removed_deadline);
    }
    ApplySoftPinMetricDelta(result.metric_delta);
}

bool MasterService::IsSoftPinActive(
    const ObjectMetadata& metadata,
    const std::chrono::system_clock::time_point& now) const {
    const auto evaluation = metadata.EvaluateSoftPin(now);
    ApplySoftPinEvaluation(metadata, evaluation);
    return evaluation.active;
}

void MasterService::CleanupExpiredSoftPins(
    const std::chrono::system_clock::time_point& now) {
    auto expired_entries = soft_pin_deadline_index_.PopExpired(now);

    // Each deadline is resolved on its own, under the key the scoped key
    // names: the key may have been republished since, and only the deadline
    // the index still holds may expire a soft pin.
    int expired_count = 0;
    for (const auto& entry : expired_entries) {
        auto [tenant_id, key] = TenantId::ParseScopedKey(entry.scoped_key);
        MetadataAccessorRO accessor(
            this, ObjectIdentity{std::move(tenant_id), std::move(key)});
        if (accessor.IsPublished() &&
            accessor.Get().ExpireSoftPinIfDeadlineMatches(entry.deadline,
                                                          now)) {
            ++expired_count;
        }
    }
    if (expired_count > 0) {
        MasterMetricManager::instance().dec_soft_pin_key_count(expired_count);
    }
}

void MasterService::ClearInvalidHandles() {
    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();
    ClearInvalidHandles(retaining_clients);
}

void MasterService::ClearInvalidHandles(
    const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients) {
    (void)ClearStaleHandles([&retaining_clients](const Replica& replica) {
        return (replica.has_invalid_mem_handle() ||
                replica.has_invalid_nof_handle() ||
                replica.has_stale_local_disk_client(retaining_clients)) &&
               replica.is_completed();
    });
}

void MasterService::ClearLocalDiskHandlesOwnedBy(const UUID& owner) {
    (void)ClearStaleHandles([&owner](const Replica& replica) {
        return replica.is_local_disk_replica() && replica.is_completed() &&
               replica.get_local_disk_client_id() == owner;
    });
}

tl::expected<void, ErrorCode> MasterService::ClearStaleHandles(
    const std::function<bool(const Replica&)>& is_stale) {
    std::optional<ErrorCode> first_persist_error;
    VisitNamespaces([&](const TenantId& tenant_id, metadata::Namespace& ns) {
        // Reader pre-check: an object with nothing to sweep never takes the
        // write lock. Each picked object is classified again under its own
        // write lock below, since it may have changed, and one torn down or
        // published again meanwhile is left alone.
        std::vector<route::ObjectRef> to_sweep;
        for (auto object : ns.objects.ReadCursor()) {
            const auto& metadata = object.metadata();
            if (metadata.HasReplica(is_stale) || !metadata.IsValid()) {
                to_sweep.push_back(object.ref());
            }
        }
        for (const auto& object : to_sweep) {
            // The by-reference accessor skips the invalid-replica cleanup,
            // which would drop the stale replicas before this pass could see
            // what it erased.
            MetadataAccessorRW accessor(this, tenant_id, object);
            if (!accessor.IsPublished()) {
                continue;
            }
            auto& metadata = accessor.Get();
            const auto cleanup_plan =
                BuildStaleHandleCleanupPlan(metadata, is_stale);
            if (!cleanup_plan.removed_ids.empty()) {
                if (enable_ha_ && enable_oplog_) {
                    // The erase runs from the durable-finalize callback.
                    auto persist_result = PersistStaleHandleCleanupForHA(
                        "ClearStaleHandles", tenant_id, object.key, metadata,
                        cleanup_plan);
                    if (!persist_result && !first_persist_error) {
                        first_persist_error = persist_result.error();
                    }
                    continue;
                }
                if (!CleanupStaleHandles(object.key, tenant_id, metadata,
                                         accessor.GetState(), is_stale)) {
                    continue;
                }
            } else if (metadata.IsValid()) {
                continue;
            } else if (enable_ha_) {
                if (enable_oplog_) {
                    auto persist_result = AppendOpLogWithDurableFinalize(
                        OpType::REMOVE, tenant_id.value(), object.key, {},
                        [this, object](const OpLogEntry& durable_entry) {
                            FinalizeMetadataEraseAfterDurable(
                                object, TenantId(durable_entry.tenant_id));
                        });
                    if (!persist_result) {
                        LOG(WARNING)
                            << "ClearStaleHandles(last replica)"
                            << ": REMOVE persist failed for key=" << object.key
                            << ", err="
                            << static_cast<int>(persist_result.error());
                        if (!first_persist_error) {
                            first_persist_error = persist_result.error();
                        }
                    }
                }
                continue;
            }
            accessor.Erase();
        }
    });
    if (first_persist_error) {
        return tl::make_unexpected(*first_persist_error);
    }
    return {};
}

bool MasterService::ProcessClientOffboardingJob(ClientOffboardingJob& job) {
    bool quota_recompute_needed = false;
    {
        std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);

        if (!job.pending_prepare_segments.empty()) {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            for (auto it = job.pending_prepare_segments.begin();
                 it != job.pending_prepare_segments.end();) {
                SegmentStatus status = SegmentStatus::UNDEFINED;
                const auto status_err =
                    segment_access.GetSegmentStatusById(it->segment_id, status);
                if (status_err == ErrorCode::SEGMENT_NOT_FOUND) {
                    it = job.pending_prepare_segments.erase(it);
                    continue;
                }
                if (status_err == ErrorCode::OK &&
                    status == SegmentStatus::UNMOUNTING) {
                    ++it;
                    continue;
                }

                size_t metrics_dec_capacity = 0;
                const auto err = segment_access.PrepareUnmountSegment(
                    it->segment_id, metrics_dec_capacity);
                if (err == ErrorCode::OK) {
                    job.prepared_segments.push_back(
                        {.segment_id = it->segment_id,
                         .segment_name = it->segment_name,
                         .transport_endpoint = it->transport_endpoint,
                         .metrics_dec_capacity = metrics_dec_capacity});
                    it = job.pending_prepare_segments.erase(it);
                    continue;
                }
                if (err == ErrorCode::SEGMENT_NOT_FOUND) {
                    it = job.pending_prepare_segments.erase(it);
                    continue;
                }
                LOG(ERROR) << "client_id=" << job.client_id
                           << ", segment_name=" << it->segment_name
                           << ", action=prepare_client_offboarding"
                           << ", error=" << toString(err);
                ++it;
            }
            if (!job.pending_prepare_segments.empty()) {
                return false;
            }
        }

        if (!job.metadata_cleanup_accepted) {
            const auto cleanup_result =
                ClearStaleHandles([&job](const Replica& replica) {
                    if (!replica.is_completed()) {
                        return false;
                    }
                    if (replica.is_local_disk_replica()) {
                        return replica.get_local_disk_client_id() ==
                               job.client_id;
                    }
                    return replica.is_memory_replica() &&
                           replica.isAffiliatedWith(job.liveness);
                });
            if (!cleanup_result) {
                LOG(ERROR) << "client_id=" << job.client_id
                           << ", action=queue_client_offboarding_metadata"
                           << ", error=" << toString(cleanup_result.error());
                return false;
            }
            bool unfinished_affiliated_replica = false;
            VisitNamespaces([&](const TenantId& /*tenant_id*/,
                                metadata::Namespace& ns) {
                if (unfinished_affiliated_replica) {
                    return;
                }
                for (auto object : ns.objects.ReadCursor()) {
                    if (object.metadata().HasReplica(
                            [&job](const Replica& replica) {
                                return replica.is_processing() &&
                                       replica.isAffiliatedWith(job.liveness);
                            })) {
                        unfinished_affiliated_replica = true;
                        break;
                    }
                }
            });
            if (unfinished_affiliated_replica) {
                LOG(ERROR)
                    << "client_id=" << job.client_id
                    << ", action=wait_client_offboarding_processing_replica";
                return false;
            }
            job.metadata_cleanup_accepted = true;
        }

        if (!job.local_ssd_unregistered) {
            auto capacity = local_ssd_manager_.UnregisterClient(job.client_id);
            if (capacity && *capacity > 0) {
                MasterMetricManager::instance().dec_total_file_capacity(
                    *capacity);
                quota_recompute_needed = true;
            }
            job.local_ssd_unregistered = true;
        }

        for (auto it = job.prepared_segments.begin();
             it != job.prepared_segments.end();) {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            const auto commit_result = segment_access.CommitUnmountSegment(
                it->segment_id, job.client_id, it->metrics_dec_capacity,
                /*retain_name_registration=*/true);
            if (commit_result != ErrorCode::OK &&
                commit_result != ErrorCode::SEGMENT_NOT_FOUND) {
                LOG(ERROR) << "client_id=" << job.client_id
                           << ", segment_name=" << it->segment_name
                           << ", action=commit_client_offboarding"
                           << ", error=" << toString(commit_result);
                ++it;
                continue;
            }

            if (enable_oplog_ && ordered_oplog_writer_ &&
                !it->transport_endpoint.empty()) {
                SegmentUnmountOp op{it->transport_endpoint};
                auto bytes = struct_pack::serialize(op);
                auto persist_result = AppendOpLogVisibleBeforeDurable(
                    OpType::SEGMENT_UNMOUNT, TenantId::Default().value(),
                    it->transport_endpoint,
                    std::string(bytes.begin(), bytes.end()));
                if (!persist_result) {
                    LOG(ERROR)
                        << "client_id=" << job.client_id
                        << ", segment_name=" << it->segment_name
                        << ", action=queue_client_offboarding_unmount"
                        << ", error=" << toString(persist_result.error());
                    ++it;
                    continue;
                }
            }

            cleanupHttpMetadata(it->segment_name);
            segment_access.ReleaseUnmountedSegmentName(it->segment_id,
                                                       it->segment_name);
            LOG(INFO) << "client_id=" << job.client_id
                      << ", segment_name=" << it->segment_name
                      << ", action=unmount_offline_mem_segment";
            quota_recompute_needed = true;
            it = job.prepared_segments.erase(it);
        }
    }

    if (quota_recompute_needed) {
        NotifyPolicyCapacityChanged();
    }
    if (!job.prepared_segments.empty()) {
        return false;
    }

    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    const auto current = client_liveness_records_.find(job.client_id);
    if (current != client_liveness_records_.end() &&
        current->second == job.liveness) {
        const auto state = current->second->state();
        client_liveness_records_.erase(current);
        MasterMetricManager::instance().on_client_liveness_record_removed(
            state);
    }
    return true;
}

void MasterService::TaskCleanupThreadFunc() {
    LOG(INFO) << "Task cleanup thread started";
    while (task_cleanup_running_) {
        // Wait for the next cleanup interval, but allow fast shutdown.
        {
            std::unique_lock<std::mutex> lk(task_cleanup_mutex_);
            task_cleanup_cv_.wait_for(
                lk, std::chrono::milliseconds(kTaskCleanupThreadSleepMs),
                [&] { return !task_cleanup_running_.load(); });
        }

        if (!task_cleanup_running_) {
            break;
        }

        // RFC #4700 slice 1: task pruning, soft-pin expiry and dynamic
        // replication cleanup all mutate business metadata.
        if (!AdmitsBusinessWorkers()) {
            continue;
        }

        std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
        {
            auto write_access = task_manager_.get_write_access();
            write_access.prune_expired_tasks();
            write_access.prune_finished_tasks();
        }
        CleanupExpiredSoftPins(std::chrono::system_clock::now());
        CleanupExpiredDynamicReplicationState();
    }
    LOG(INFO) << "Task cleanup thread stopped";
}

void MasterService::WeightReconciliationThreadFunc() {
    LOG(INFO) << "Weight reconciliation thread started";
    while (weight_reconciliation_running_) {
        {
            std::unique_lock lock(weight_reconciliation_mutex_);
            weight_reconciliation_cv_.wait_for(
                lock, std::chrono::milliseconds(kTaskCleanupThreadSleepMs),
                [&] { return !weight_reconciliation_running_.load(); });
        }
        if (!weight_reconciliation_running_) {
            break;
        }
        const auto now_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
        weight_manager_.ReconcileWeightMetadataStoreOnce(now_ms, 32);
    }
    LOG(INFO) << "Weight reconciliation thread stopped";
}

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    size_t metrics_dec_capacity = 0;  // to update the metrics

    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();
    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        ErrorCode err = segment_access.PrepareUnmountSegment(
            segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            // Return OK because this is an idempotent operation
            return {};
        }
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }

    // Keep HA, snapshot, and CXL behavior unchanged. Regular memory segments
    // become unreadable as soon as PrepareUnmountSegment releases their
    // allocator; only the physical metadata sweep is deferred.
    if (enable_async_segment_cleanup_) {
        replica_cleanup_worker_.Schedule();
    } else {
        ClearInvalidHandles(retaining_clients);
    }

    // Cache endpoint before commit removes segment from registry.
    std::string segment_name;
    std::string te_endpoint;
    if (!segment_manager_.GetSegmentBasicInfo(segment_id, segment_name,
                                              te_endpoint)) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    // 3. Commit the unmount operation
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                       metrics_dec_capacity);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }

    if (enable_oplog_ && ordered_oplog_writer_ && !te_endpoint.empty()) {
        SegmentUnmountOp op{te_endpoint};
        auto bytes = struct_pack::serialize(op);
        PersistSegmentOpForHA("UnmountSegment", OpType::SEGMENT_UNMOUNT,
                              te_endpoint,
                              std::string(bytes.begin(), bytes.end()));
    }
    NotifyPolicyCapacityChanged();
    return {};
}

auto MasterService::GracefulUnmountSegment(const UUID& segment_id,
                                           const UUID& client_id,
                                           uint64_t grace_period_ms)
    -> tl::expected<void, ErrorCode> {
    std::unique_lock<std::shared_mutex> lock(master_state_mutex_);
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Verify ownership: the segment must belong to the calling client
    std::vector<Segment> client_segments;
    auto err = segment_access.GetClientSegments(client_id, client_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    bool owned = false;
    for (auto& seg : client_segments) {
        if (seg.id == segment_id) {
            owned = true;
            break;
        }
    }
    if (!owned) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    err = segment_access.PrepareGracefulUnmountSegment(segment_id);
    if (err == ErrorCode::SEGMENT_NOT_FOUND) {
        return {};
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    auto expire_time = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(grace_period_ms);
    graceful_unmount_scheduler_.Schedule({segment_id, client_id}, expire_time);
    return {};
}

auto MasterService::UnmountNoFSegment(const UUID& segment_id,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id << ", segment_id=" << segment_id
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    size_t metrics_dec_capacity = 0;  // to update the metrics

    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();

    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedNoFSegmentAccess segment_access =
            nof_segment_manager_.getNoFSegmentAccess();
        ErrorCode err = segment_access.PrepareUnmountSegment(
            segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            // Return OK because this is an idempotent operation
            return {};
        }
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }  // Release the segment mutex before long-running step 2 and avoid
       // deadlocks

    // 2. Remove the metadata of the related objects
    ClearInvalidHandles(retaining_clients);

    // 3. Commit the unmount operation
    ScopedNoFSegmentAccess segment_access =
        nof_segment_manager_.getNoFSegmentAccess();
    auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                   metrics_dec_capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    {
        std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
        nof_heartbeat_states_.erase(segment_id);
    }
    return {};
#endif
}

auto MasterService::ExistKey(const std::string& key, const TenantId& tenant_id)
    -> tl::expected<bool, ErrorCode> {
    return ExistKeyImpl(key, tenant_id, /*grant_lease=*/true);
}

auto MasterService::ProbeKey(const std::string& key, const TenantId& tenant_id)
    -> tl::expected<bool, ErrorCode> {
    return ExistKeyImpl(key, tenant_id, /*grant_lease=*/false);
}

auto MasterService::ExistKeyImpl(const std::string& key,
                                 const TenantId& tenant_id, bool grant_lease)
    -> tl::expected<bool, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRO accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        MC_VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    return ReadExistence(accessor.Get(), grant_lease);
}

bool MasterService::ReadExistence(const ObjectMetadata& metadata,
                                  bool grant_lease) const {
    if (!HasReadableReplica(metadata)) {
        return false;
    }

    if (grant_lease) {
        // Grant a lease to the object as it may be further used by the client.
        // Read path is group-agnostic: only the object's own lease is
        // refreshed.
        metadata.GrantReadLease(
            std::chrono::milliseconds(default_kv_lease_ttl_));
    }
    return true;
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKey(
    const std::vector<std::string>& keys, const TenantId& tenant_id) {
    return BatchExistKeyImpl(keys, tenant_id, /*grant_lease=*/true);
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchProbeKey(
    const GrantLeasePolicy& policy, const std::vector<std::string>& keys,
    const TenantId& tenant_id) {
    if (keys.empty()) return {};
    std::string_view invalid_reason;
    if (policy.lease_mode != ProbeLeaseMode::None &&
        policy.lease_mode != ProbeLeaseMode::LastHitOnly) {
        invalid_reason = "unknown lease mode";
    } else if (policy.lease_mode == ProbeLeaseMode::LastHitOnly) {
        if (policy.candidate_size == 0) {
            invalid_reason = "candidate_size is zero";
        } else if (keys.size() % policy.candidate_size != 0) {
            invalid_reason = "key count is not divisible by candidate_size";
        }
    }
    if (!invalid_reason.empty()) {
        LOG(WARNING) << "BatchProbeKey: " << invalid_reason
                     << ", lease_mode=" << static_cast<int>(policy.lease_mode)
                     << ", candidate_size=" << policy.candidate_size
                     << ", key_count=" << keys.size();
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }

    auto results = BatchExistKeyImpl(keys, tenant_id, /*grant_lease=*/false);
    if (policy.lease_mode == ProbeLeaseMode::None) return results;

    const auto is_readable = [](const auto& result) {
        return result.has_value() && result.value();
    };
    for (size_t end = keys.size(); end > 0; end -= policy.candidate_size) {
        const size_t begin = end - policy.candidate_size;
        if (!std::all_of(results.begin() + begin, results.begin() + end,
                         is_readable)) {
            continue;
        }

        const std::vector<std::string> candidate(keys.begin() + begin,
                                                 keys.begin() + end);
        auto leased =
            BatchExistKeyImpl(candidate, tenant_id, /*grant_lease=*/true);
        // Recheck under each key's own lock. Publish the fresh
        // results so a candidate lost since probing cannot remain all-true.
        // Failed candidates may retain partial leases until their TTL expires.
        std::copy(leased.begin(), leased.end(), results.begin() + begin);
        if (std::all_of(leased.begin(), leased.end(), is_readable)) {
            break;
        }
    }
    return results;
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKeyImpl(
    const std::vector<std::string>& keys, const TenantId& tenant_id,
    bool grant_lease) {
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    std::vector<tl::expected<bool, ErrorCode>> results(keys.size());
    if (keys.empty()) {
        return results;
    }

    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    for (size_t i = 0; i < keys.size(); ++i) {
        MetadataAccessorRO accessor(this, normalized_tenant, keys[i]);
        if (!accessor.Exists()) {
            VLOG(1) << "key=" << keys[i] << ", tenant_id=" << normalized_tenant
                    << ", info=object_not_found";
            results[i] = false;
            continue;
        }
        results[i] = ReadExistence(accessor.Get(), grant_lease);
    }
    return results;
}

auto MasterService::GetAllKeys(const TenantId& tenant_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const metadata::Namespace* ns =
        namespaces_.Lookup(ResolveRequestTenantId(tenant_id));
    if (ns == nullptr) {
        return all_keys;
    }
    for (auto object : ns->objects.ReadCursor()) {
        const auto& metadata = object.metadata();
        if (!HasReadableReplica(metadata)) {
            continue;
        }
        all_keys.push_back(metadata.user_key.empty() ? object.key()
                                                     : metadata.user_key);
    }
    return all_keys;
}

auto MasterService::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::string> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return all_segments;
}

auto MasterService::GetAllNoFSegments()
    -> tl::expected<std::vector<NoFSegment>, ErrorCode> {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);

    std::vector<NoFSegment> result;
    for (const auto& segment : mounted_segments) {
        result.push_back(segment.segment);
    }

    return result;
}

auto MasterService::GetNoFSegmentsByName(const std::string& segment_name)
    -> tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode> {
    return nof_segment_manager_.GetSegmentsByName(segment_name);
}

auto MasterService::GetSegmentsDetail()
    -> tl::expected<std::vector<SegmentDetailInfo>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Get full info of all segments (including Segment and client_id)
    std::vector<std::pair<Segment, UUID>> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::vector<SegmentDetailInfo> result;
    result.reserve(all_segments.size());

    for (const auto& [segment, client_id] : all_segments) {
        SegmentDetailInfo info;
        info.segment_name = segment.name;
        info.segment_id = segment.id;
        info.client_id = client_id;
        info.base_address = segment.base;
        info.size_bytes = segment.size;
        info.te_endpoint = segment.te_endpoint;
        info.protocol = segment.protocol;

        // Query segment status
        segment_access.GetSegmentStatusByName(segment.name, info.status);

        // Query allocator used/capacity
        size_t used = 0, capacity = 0;
        segment_access.QuerySegments(segment.name, used, capacity);
        info.allocator_used_bytes = used;
        info.allocator_capacity_bytes = capacity;

        result.push_back(std::move(info));
    }

    return result;
}

auto MasterService::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    size_t used, capacity;
    auto err = segment_access.QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

auto MasterService::QuerySegmentStatus(const std::string& segment_name)
    -> tl::expected<SegmentStatus, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    SegmentStatus status = SegmentStatus::UNDEFINED;
    auto err = segment_access.GetSegmentStatusByName(segment_name, status);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return status;
}

auto MasterService::QuerySegmentStatusById(const UUID& segment_id)
    -> tl::expected<SegmentStatus, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    SegmentStatus status = SegmentStatus::UNDEFINED;
    auto err = segment_access.GetSegmentStatusById(segment_id, status);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return status;
}

namespace {

// Same scoping rule the bounded path uses: an explicit tenant_id on the
// entry wins over the scoped key spelling.
std::pair<TenantId, std::string> ResolveStandbyObject(
    const StandbyObjectEntry& entry) {
    auto [scoped_tenant_id, user_key] = TenantId::ParseScopedKey(entry.key);
    TenantId tenant_id(entry.tenant_id);
    if (tenant_id.IsDefault() && !scoped_tenant_id.IsDefault()) {
        tenant_id = std::move(scoped_tenant_id);
    }
    return std::make_pair(std::move(tenant_id), std::move(user_key));
}

}  // namespace

struct MasterService::PreparedObject {
    const StandbyObjectEntry* entry;
    TenantId tenant_id;
    std::string user_key;
    std::vector<Replica> replicas;
};

// Shared state for the tolerant legacy-snapshot restore (#3760): one bad
// entry or descriptor must not cost the whole index. Validation runs in
// three phases — cheap per-entry validation, ambiguity-discard overlap
// resolution, then construction for the survivors — and only segment-level
// structural corruption stays fail-fast. The bounded handoff path keeps its
// fail-fast chunk loop: its chunks stream from a metadata store the standby
// wrote from its own live index, so the contents are internally consistent
// by construction, and deferring overlap resolution across chunk installs
// would break the bounded-memory shape.
struct MasterService::LegacyRestoreContext {
    const std::vector<StandbyObjectEntry>& objects;
    const std::unordered_map<std::string, const StandbySegmentInfo*>&
        segments_by_alias;
    std::unordered_map<std::string, std::shared_ptr<BufferAllocatorBase>>&
        allocators;
    std::unordered_map<std::string, uint64_t>& accounted_bytes;
    size_t& rejected_count;
    size_t& already_existing_count;
    std::vector<std::pair<TenantId, std::string>>& repair_remove_keys;
    std::vector<std::pair<TenantId, std::string>>& repair_canonical_keys;
    std::vector<std::pair<TenantId, route::ObjectRef>>& installed;
    std::unordered_map<UUID, std::shared_ptr<ClientLivenessRecord>,
                       boost::hash<UUID>>& new_known_owner_records;

    struct AcceptedEntry {
        const StandbyObjectEntry* entry;
        TenantId tenant_id;
        std::string user_key;
    };
    std::vector<AcceptedEntry> accepted;
    std::unordered_set<std::string> object_ids;
    // segment -> (address, size, index into `accepted`, descriptor index)
    std::unordered_map<
        const StandbySegmentInfo*,
        std::vector<std::tuple<uintptr_t, uint64_t, size_t, size_t>>>
        pending_ranges;
    std::unordered_map<size_t, std::unordered_set<size_t>> discarded_replicas;
    std::vector<PreparedObject> prepared;
};

void MasterService::NoteLegacyStandbyRejection(LegacyRestoreContext& ctx,
                                               const StandbyObjectEntry& entry,
                                               const char* reason) {
    ++ctx.rejected_count;
    LOG(WARNING) << "RestoreFromStandbySnapshot: skipping object, key="
                 << entry.key << ", reason=" << reason;
    MasterMetricManager::instance().inc_standby_restore_rejected_objects(
        reason);
}

std::shared_ptr<ClientLivenessRecord> MasterService::RecordRestoreKnownOwner(
    std::unordered_map<UUID, std::shared_ptr<ClientLivenessRecord>,
                       boost::hash<UUID>>& new_known_owner_records,
    const UUID& owner) {
    if (const auto existing = client_liveness_records_.find(owner);
        existing != client_liveness_records_.end()) {
        return existing->second;
    }
    auto [record, inserted] = new_known_owner_records.try_emplace(
        owner, std::make_shared<ClientLivenessRecord>(
                   ClientLivenessRecord::Clock::now()));
    (void)inserted;
    return record->second;
}

tl::expected<void, ErrorCode> MasterService::ValidateLegacyStandbyEntries(
    LegacyRestoreContext& ctx) {
    for (const auto& entry : ctx.objects) {
        auto [tenant_id, user_key] = ResolveStandbyObject(entry);
        if (!tenant_id.IsValid()) {
            NoteLegacyStandbyRejection(ctx, entry, "invalid_tenant_id");
            continue;
        }
        if (!ctx.object_ids.insert(tenant_id.MakeScopedKey(user_key)).second) {
            NoteLegacyStandbyRejection(ctx, entry, "duplicate_object");
            continue;
        }
        if (MetadataAccessorRO(this, {tenant_id, user_key}).IsPublished()) {
            NoteLegacyStandbyRejection(ctx, entry, "object_already_exists");
            ++ctx.already_existing_count;
            continue;
        }
        bool valid = true;
        // (segment, address, size, descriptor index) for this object's
        // live memory replicas. They join ctx.pending_ranges only after the
        // object is ctx.accepted, so a rejected object never leaves ranges
        // whose owner index a later ctx.accepted object would reuse.
        std::vector<
            std::tuple<const StandbySegmentInfo*, uintptr_t, uint64_t, size_t>>
            object_ranges;
        for (size_t desc_idx = 0; desc_idx < entry.metadata.replicas.size();
             ++desc_idx) {
            const auto& desc = entry.metadata.replicas[desc_idx];
            if (!desc.is_memory_replica()) {
                continue;
            }
            const auto& buffer = desc.get_memory_descriptor().buffer_descriptor;
            auto segment_it =
                ctx.segments_by_alias.find(buffer.transport_endpoint_);
            if (segment_it == ctx.segments_by_alias.end()) {
                NoteLegacyStandbyRejection(ctx, entry, "unknown_endpoint");
                valid = false;
                break;
            }
            if (buffer.size_ != entry.metadata.size || buffer.size_ == 0 ||
                buffer.buffer_address_ >
                    std::numeric_limits<uintptr_t>::max() - buffer.size_) {
                NoteLegacyStandbyRejection(ctx, entry,
                                           "invalid_memory_descriptor");
                valid = false;
                break;
            }
            if (desc.status != ReplicaStatus::REMOVED &&
                desc.status != ReplicaStatus::FAILED) {
                object_ranges.emplace_back(segment_it->second,
                                           buffer.buffer_address_, buffer.size_,
                                           desc_idx);
            }
        }
        if (!valid) {
            continue;
        }
        const size_t owner = ctx.accepted.size();
        ctx.accepted.push_back(
            {&entry, std::move(tenant_id), std::move(user_key)});
        for (const auto& [segment, addr, size, desc_idx] : object_ranges) {
            ctx.pending_ranges[segment].emplace_back(addr, size, owner,
                                                     desc_idx);
        }
    }

    // Overlap is ambiguity, not recency: the promotion context carries no
    // replay order (the standby snapshot enumerates unordered maps), so an
    // overlapping pair cannot prove which descriptor is newer. Every
    // descriptor in a transitively overlapping group is discarded,
    // replicas independent of the conflict are kept, and an object is
    // dropped only when no reliable replica remains (#3760).
    for (auto& [segment, ranges] : ctx.pending_ranges) {
        std::sort(ranges.begin(), ranges.end());
        size_t run_start = 0;
        while (run_start < ranges.size()) {
            const auto range_end = [&](size_t idx) -> uintptr_t {
                return std::get<0>(ranges[idx]) + std::get<1>(ranges[idx]);
            };
            size_t run_last = run_start;
            uintptr_t run_end = range_end(run_start);
            while (run_last + 1 < ranges.size() &&
                   std::get<0>(ranges[run_last + 1]) < run_end) {
                ++run_last;
                run_end = std::max(run_end, range_end(run_last));
            }
            if (run_last > run_start) {
                for (size_t j = run_start; j <= run_last; ++j) {
                    const auto& [addr, size, owner, desc_idx] = ranges[j];
                    ctx.discarded_replicas[owner].insert(desc_idx);
                    LOG(WARNING)
                        << "RestoreFromStandbySnapshot: discarding "
                        << "ambiguous overlapping replica, segment="
                        << segment->segment_name << ", addr=0x" << std::hex
                        << addr << "+0x" << size << std::dec
                        << ", key=" << ctx.accepted[owner].entry->key;
                }
            }
            run_start = run_last + 1;
        }
    }
    for (size_t owner = 0; owner < ctx.accepted.size(); ++owner) {
        auto& accepted_entry = ctx.accepted[owner];
        if (accepted_entry.entry == nullptr) {
            continue;
        }
        const auto discarded_it = ctx.discarded_replicas.find(owner);
        size_t reliable = 0;
        for (size_t desc_idx = 0;
             desc_idx < accepted_entry.entry->metadata.replicas.size();
             ++desc_idx) {
            const auto& desc =
                accepted_entry.entry->metadata.replicas[desc_idx];
            if (desc.status == ReplicaStatus::REMOVED ||
                desc.status == ReplicaStatus::FAILED ||
                (discarded_it != ctx.discarded_replicas.end() &&
                 discarded_it->second.contains(desc_idx))) {
                continue;
            }
            ++reliable;
        }
        if (reliable == 0) {
            // Covers both overlap casualties and objects that arrived with
            // every replica already REMOVED/FAILED: an object with no
            // readable replica is dropped instead of lingering as
            // metadata-only, and the durable REMOVE below stops a later
            // promotion from replaying it.
            NoteLegacyStandbyRejection(ctx, *accepted_entry.entry,
                                       "no_reliable_replica");
            ctx.repair_remove_keys.emplace_back(accepted_entry.tenant_id,
                                                accepted_entry.user_key);
            accepted_entry.entry = nullptr;
        } else if (discarded_it != ctx.discarded_replicas.end()) {
            // Survives with some descriptors discarded: the canonical
            // repair record carries only the survivors.
            ctx.repair_canonical_keys.emplace_back(accepted_entry.tenant_id,
                                                   accepted_entry.user_key);
        }
    }

    return {};
}

tl::expected<void, ErrorCode> MasterService::ConstructLegacyStandbyReplicas(
    LegacyRestoreContext& ctx) {
    // Construction phase: build replicas and accounting for survivors
    // only.
    for (size_t owner = 0; owner < ctx.accepted.size(); ++owner) {
        const auto& accepted_entry = ctx.accepted[owner];
        if (accepted_entry.entry == nullptr) {
            continue;
        }
        const auto& entry = *accepted_entry.entry;
        const auto& standby_meta = entry.metadata;
        const auto discarded_it = ctx.discarded_replicas.find(owner);
        std::vector<Replica> replicas;
        replicas.reserve(standby_meta.replicas.size());
        bool construction_ok = true;
        // Per-object bytes join the segment totals only once the whole
        // object validates, so a mid-construction rejection never leaves
        // phantom accounting behind for later capacity checks or metrics.
        std::unordered_map<std::string, uint64_t> object_bytes;

        for (size_t desc_idx = 0; desc_idx < standby_meta.replicas.size();
             ++desc_idx) {
            if (discarded_it != ctx.discarded_replicas.end() &&
                discarded_it->second.contains(desc_idx)) {
                continue;
            }
            const auto& desc = standby_meta.replicas[desc_idx];
            if (desc.is_memory_replica()) {
                const auto& buffer =
                    desc.get_memory_descriptor().buffer_descriptor;
                auto alloc = ctx.allocators.at(buffer.transport_endpoint_);
                const auto* segment =
                    ctx.segments_by_alias.at(buffer.transport_endpoint_);
                if (desc.status != ReplicaStatus::REMOVED &&
                    desc.status != ReplicaStatus::FAILED) {
                    const uint64_t used =
                        ctx.accounted_bytes[segment->segment_name] +
                        object_bytes[segment->segment_name];
                    if (used > segment->capacity ||
                        buffer.size_ > segment->capacity - used) {
                        NoteLegacyStandbyRejection(ctx, entry,
                                                   "capacity_overflow");
                        construction_ok = false;
                        break;
                    }
                    object_bytes[segment->segment_name] += buffer.size_;
                }

                replicas.push_back(Replica(
                    desc.id, std::make_unique<AllocatedBuffer>(alloc, buffer),
                    desc.status));
            } else if (desc.is_nof_replica()) {
                const auto& buffer =
                    desc.get_nof_descriptor().buffer_descriptor;
                if (buffer.size_ != standby_meta.size || buffer.size_ == 0) {
                    NoteLegacyStandbyRejection(ctx, entry,
                                               "invalid_nof_descriptor");
                    construction_ok = false;
                    break;
                }
                auto& alloc = ctx.allocators[buffer.transport_endpoint_];
                if (!alloc) {
                    alloc = std::make_shared<DummyBufferAllocator>(
                        buffer.transport_endpoint_, buffer.transport_endpoint_);
                }
                replicas.push_back(Replica(
                    desc.id, std::make_unique<AllocatedBuffer>(alloc, buffer),
                    desc.status, ReplicaType::NOF_SSD));
            } else if (desc.is_disk_replica()) {
                const auto& disk_desc = desc.get_disk_descriptor();
                if (disk_desc.object_size != standby_meta.size) {
                    NoteLegacyStandbyRejection(ctx, entry,
                                               "invalid_disk_descriptor");
                    construction_ok = false;
                    break;
                }
                replicas.push_back(Replica(desc.id, disk_desc.file_path,
                                           disk_desc.object_size, desc.status));
            } else if (desc.is_local_disk_replica()) {
                const auto& local_disk_desc = desc.get_local_disk_descriptor();
                if (local_disk_desc.object_size != standby_meta.size) {
                    NoteLegacyStandbyRejection(ctx, entry,
                                               "invalid_local_disk_descriptor");
                    construction_ok = false;
                    break;
                }
                replicas.push_back(Replica(
                    desc.id, local_disk_desc.client_id,
                    local_disk_desc.object_size,
                    local_disk_desc.transport_endpoint, desc.status,
                    RecordRestoreKnownOwner(ctx.new_known_owner_records,
                                            local_disk_desc.client_id)));
            } else if (desc.is_dfs_replica()) {
                const auto& dfs_desc = desc.get_dfs_descriptor();
                if (!use_kvcs_ || dfs_desc.object_size != standby_meta.size) {
                    NoteLegacyStandbyRejection(ctx, entry,
                                               "invalid_dfs_descriptor");
                    construction_ok = false;
                    break;
                }
                replicas.push_back(Replica(desc.id, dfs_desc, desc.status));
            } else {
                LOG(ERROR) << "RestoreFromStandbySnapshot: unsupported replica "
                           << "descriptor, tenant="
                           << accepted_entry.tenant_id.value()
                           << ", key=" << accepted_entry.user_key;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
        }
        if (!construction_ok) {
            continue;
        }
        for (const auto& [segment_name, bytes] : object_bytes) {
            ctx.accounted_bytes[segment_name] += bytes;
        }
        ctx.prepared.push_back(
            {accepted_entry.entry, std::move(accepted_entry.tenant_id),
             std::move(accepted_entry.user_key), std::move(replicas)});
    }

    return {};
}

tl::expected<size_t, ErrorCode> MasterService::InstallLegacyStandbyObjects(
    LegacyRestoreContext& ctx) {
    const size_t restored_count = ctx.prepared.size();
    // A snapshot whose objects all fail to land is a failed restore, not
    // an empty cluster: reporting success here would let the supervisor
    // serve on zero state. Duplicates of already-served objects are the
    // exception, since their state is present already. How partial
    // restores gate serving is N07/N08 scope (#3808), not this stop-gap.
    if (!ctx.objects.empty() && restored_count == 0 &&
        ctx.already_existing_count < ctx.objects.size()) {
        LOG(ERROR) << "RestoreFromStandbySnapshot: every standby object "
                   << "was rejected, objects=" << ctx.objects.size()
                   << ", rejected=" << ctx.rejected_count;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (auto published = PublishRestoredObjects(ctx.prepared, ctx.installed);
        !published) {
        return tl::make_unexpected(published.error());
    }
    return restored_count;
}

tl::expected<void, ErrorCode> MasterService::PublishRestoredObjects(
    std::vector<PreparedObject>& objects,
    std::vector<std::pair<TenantId, route::ObjectRef>>& installed) {
    for (const auto& object : objects) {
        if (MetadataAccessorRO(this, {object.tenant_id, object.user_key})
                .IsPublished()) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
    }

    const auto now = std::chrono::system_clock::now();
    for (auto& object : objects) {
        const auto& standby_meta = object.entry->metadata;
        MetadataAccessorRW accessor(this, {object.tenant_id, object.user_key},
                                    MetadataAccessorRW::CreateKey{});
        if (accessor.IsPublished()) {
            // The pass above found none, and a restore runs with every
            // writer held off, so this is a key the objects name twice.
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        const route::Generation generation = accessor.Publish(
            standby_meta.client_id, now, standby_meta.size,
            std::move(object.replicas), std::nullopt,
            standby_meta.hard_pinned.value_or(false), standby_meta.data_type,
            standby_meta.group_id, object.tenant_id, object.user_key,
            standby_meta.residency_affinity_id.value_or(""));
        auto& metadata = accessor.Get();
        metadata.MutablePart(0).SetChecksum(standby_meta.object_checksum);
        metadata.session_id = standby_meta.session_id.value_or("");
        installed.emplace_back(object.tenant_id,
                               route::ObjectRef{object.user_key, generation});
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::SetSegmentStatus(
    const std::string& segment_name, SegmentStatus status) {
    if (status != SegmentStatus::OK && status != SegmentStatus::DRAINING) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(job_mutex_);
    for (const auto& [_, job] : drain_jobs_) {
        std::lock_guard<std::mutex> job_lock(job->mutex);
        if (job->status == JobStatus::SUCCEEDED ||
            job->status == JobStatus::FAILED ||
            job->status == JobStatus::CANCELED) {
            continue;
        }
        const auto& sources = job->request.segments;
        if (std::find(sources.begin(), sources.end(), segment_name) !=
            sources.end()) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    SegmentStatus current = SegmentStatus::UNDEFINED;
    auto err = segment_access.GetSegmentStatusByName(segment_name, current);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    if (current != SegmentStatus::OK && current != SegmentStatus::DRAINING) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    err = segment_access.SetSegmentStatusByName(segment_name, status);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::RestoreFromStandbySnapshot(
    const std::vector<StandbyObjectEntry>& objects,
    uint64_t initial_oplog_sequence_id,
    const std::vector<StandbySegmentInfo>& segments,
    const WeightMetadataSnapshot& weight_metadata,
    const std::vector<vsegment::PartitionVSegmentSnapshot>&
        vsegment_partitions) {
    auto result = RestoreFromStandbyState(
        &objects, nullptr, initial_oplog_sequence_id, segments, objects.size(),
        std::nullopt, &weight_metadata);
    if (result) {
        AccumulateRecoveredVSegmentSnapshots(vsegment_partitions);
    }
    return result;
}

// SP930 keeps recovered partition states for the reconcile that runs once
// the VSegment service is installed. Accumulate (not replace): several
// restores/replays may each contribute different partitions. Only called
// after a successful restore, so a failed attempt never leaves duplicates.
void MasterService::AccumulateRecoveredVSegmentSnapshots(
    const std::vector<vsegment::PartitionVSegmentSnapshot>& partitions) {
    recovered_vsegment_snapshots_.insert(recovered_vsegment_snapshots_.end(),
                                         partitions.begin(), partitions.end());
}

tl::expected<void, ErrorCode> MasterService::MaterializeReplayedState(
    const std::vector<StandbyObjectEntry>& objects,
    const std::vector<StandbySegmentInfo>& segments,
    const std::vector<vsegment::PartitionVSegmentSnapshot>&
        vsegment_partitions) {
    // Additive replay: self-recovery before serve, cold takeover during the
    // slot-import barrier and ring adoption while the slot is under the
    // migration write barrier. Those callers guarantee no concurrent write for
    // the affected slots, but replay may still overlap state installed by an
    // earlier partial attempt, so existing keys are skipped rather than
    // treated as conflicts.
    //
    // Multi-submaster replay covers every source stream, so filter materialized
    // objects to the slots this master currently owns; other masters replay
    // their own share. Single-source deployments have no slot partition and
    // keep the full set.
    std::vector<StandbyObjectEntry> owned_objects;
    const std::vector<StandbyObjectEntry>* objects_to_install = &objects;
    const bool kv_partition_enabled = enable_ha_ &&
                                      ha_backend_type_ == "etcd" &&
                                      !master_id_.empty() && !cluster_id_.empty();
    if (kv_partition_enabled) {
        const std::vector<uint16_t> owned_slots = ResolveOwnedSlotsForCvm();
        if (!owned_slots.empty()) {
            std::vector<bool> owned(cvm::SlotCount(), false);
            for (uint16_t slot : owned_slots) owned[slot] = true;
            owned_objects.reserve(objects.size());
            for (const auto& entry : objects) {
                auto [tenant_id, user_key] = ResolveStandbyObject(entry);
                if (!tenant_id.IsValid()) continue;
                if (!owned[cvm::KeySlot(tenant_id, user_key)]) continue;
                owned_objects.push_back(entry);
            }
            objects_to_install = &owned_objects;
        }
    }

    // The legacy path validates a weight snapshot pointer, but replay carries
    // no weight section: pass the neutral empty snapshot.
    const WeightMetadataSnapshot empty_weight_metadata{};
    const auto result = RestoreFromStandbyStateCore(
        objects_to_install, /*metadata_store=*/nullptr,
        /*initial_oplog_sequence_id=*/0, segments, objects_to_install->size(),
        /*expected_max_replica_id=*/std::nullopt, &empty_weight_metadata,
        /*skip_existing_objects=*/true);
    if (!result) {
        LOG(ERROR) << "OpLog replay materialization failed: objects="
                   << objects_to_install->size() << ", segments="
                   << segments.size() << ", vsegment_partitions="
                   << vsegment_partitions.size()
                   << ", err=" << toString(result.error());
        return result;
    }
    AccumulateRecoveredVSegmentSnapshots(vsegment_partitions);
    LOG(INFO) << "OpLog replay materialized: objects="
              << objects_to_install->size() << ", segments=" << segments.size()
              << ", vsegment_partitions=" << vsegment_partitions.size();
    return {};
}

tl::expected<void, ErrorCode> MasterService::RestoreFromBatchOpLogPromotion(
    const BatchOpLogPromotionHandoff& handoff, size_t chunk_object_count) {
    if (!handoff.metadata_store) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return RestoreFromBatchOpLogPromotion(
        *handoff.metadata_store, handoff.applied_cursor, handoff.segments,
        handoff.max_replica_id, chunk_object_count,
        handoff.vsegment_partitions);
}

tl::expected<void, ErrorCode> MasterService::RestoreFromBatchOpLogPromotion(
    const StandbyMetadataStore& metadata_store, DurablePrefix applied_cursor,
    const std::vector<StandbySegmentInfo>& segments, ReplicaID max_replica_id,
    size_t chunk_object_count,
    const std::vector<vsegment::PartitionVSegmentSnapshot>&
        vsegment_partitions) {
    if (chunk_object_count == 0 ||
        ((applied_cursor.batch_id == 0) != (applied_cursor.last_seq == 0))) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = RestoreFromStandbyState(
        nullptr, &metadata_store, applied_cursor.last_seq, segments,
        chunk_object_count, max_replica_id, nullptr);
    if (result) {
        AccumulateRecoveredVSegmentSnapshots(vsegment_partitions);
    }
    return result;
}

// Restore lifecycle (RFC #4700, slice 1). Defined next to the restore
// implementation it guards.
namespace {

const char* RestoreLifecycleStateToString(
    MasterService::RestoreLifecycleState state) {
    switch (state) {
        case MasterService::RestoreLifecycleState::kConstructed:
            return "Constructed";
        case MasterService::RestoreLifecycleState::kRecovering:
            return "Recovering";
        case MasterService::RestoreLifecycleState::kReady:
            return "Ready";
        case MasterService::RestoreLifecycleState::kServing:
            return "Serving";
        case MasterService::RestoreLifecycleState::kFailed:
            return "Failed";
    }
    return "Unknown";
}

}  // namespace

tl::expected<void, ErrorCode> MasterService::RestoreFromStandbyState(
    const std::vector<StandbyObjectEntry>* legacy_objects,
    const StandbyMetadataStore* metadata_store,
    uint64_t initial_oplog_sequence_id,
    const std::vector<StandbySegmentInfo>& segments, size_t chunk_object_count,
    std::optional<ReplicaID> expected_max_replica_id,
    const WeightMetadataSnapshot* legacy_weight_metadata) {
    // Lifecycle bookkeeping around the restore (RFC #4700, slice 1). The
    // restore itself keeps its existing retry semantics: a replacement
    // restore from Ready or Failed is still allowed and is exercised by
    // the HA tests (allocator rebind, discard-repair retry). The one
    // refused entry is Serving: the RFC's proposed no-cycles-out-of-
    // Serving constraint. master_state_mutex_ stays in place, so this
    // slice adds no locking behavior.
    //
    // Admission is single-flight: the state transition into Recovering is
    // the admission token, so a second restore that overlaps an in-flight
    // one is refused instead of running. Without that, two overlapping
    // callers could each publish their own outcome and the first one to
    // finish would return the lifecycle to Ready while the other is still
    // rewriting metadata. Overlapping restores are not a supported way to
    // retry -- a retry starts after the previous attempt returned.
    RestoreLifecycleState expected =
        lifecycle_state_.load(std::memory_order_acquire);
    if (expected == RestoreLifecycleState::kServing) {
        LOG(ERROR) << "RestoreFromStandbyState: refusing restore of a "
                      "serving service (lifecycle state="
                   << RestoreLifecycleStateToString(expected) << ")";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (expected == RestoreLifecycleState::kRecovering) {
        LOG(ERROR) << "RestoreFromStandbyState: refusing a second restore "
                      "while one is already in flight";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (!lifecycle_state_.compare_exchange_strong(
            expected, RestoreLifecycleState::kRecovering,
            std::memory_order_acq_rel)) {
        LOG(ERROR) << "RestoreFromStandbyState: lost the admission race "
                      "(lifecycle state="
                   << RestoreLifecycleStateToString(expected) << ")";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    auto result = RestoreFromStandbyStateCore(
        legacy_objects, metadata_store, initial_oplog_sequence_id, segments,
        chunk_object_count, expected_max_replica_id, legacy_weight_metadata);

    lifecycle_state_.store(result.has_value() ? RestoreLifecycleState::kReady
                                              : RestoreLifecycleState::kFailed,
                           std::memory_order_release);
    LOG(INFO) << "Restore lifecycle -> "
              << RestoreLifecycleStateToString(lifecycle_state_.load());
    return result;
}

MasterService::RestoreLifecycleState MasterService::GetRestoreLifecycleState()
    const {
    return lifecycle_state_.load(std::memory_order_acquire);
}

void MasterService::MarkReadyForServing() {
    RestoreLifecycleState expected = RestoreLifecycleState::kConstructed;
    if (!lifecycle_state_.compare_exchange_strong(
            expected, RestoreLifecycleState::kReady)) {
        LOG(ERROR) << "MarkReadyForServing: refusing transition from "
                   << RestoreLifecycleStateToString(lifecycle_state_.load());
        return;
    }
    LOG(INFO) << "Restore lifecycle -> Ready (non-HA initialization)";
}

void MasterService::ActivateServing() {
    RestoreLifecycleState expected = RestoreLifecycleState::kReady;
    if (!lifecycle_state_.compare_exchange_strong(
            expected, RestoreLifecycleState::kServing)) {
        LOG(ERROR) << "ActivateServing: refusing transition from "
                   << RestoreLifecycleStateToString(lifecycle_state_.load());
        return;
    }
    LOG(INFO) << "Restore lifecycle -> Serving";
}

bool MasterService::AdmitsBusinessWorkers() const {
    // Admission is evaluated once per worker iteration, so it gates the
    // start of business work, not work already in progress: a worker that
    // passed this check before the restore window opened runs its current
    // iteration to completion. In this slice that is safe because the
    // restore core still holds master_state_mutex_ exclusively, which is
    // what actually excludes in-flight business work. A slice that removes
    // that lock from these paths must add an explicit quiesce step
    // (drain or epoch) instead of relying on this predicate.
    switch (lifecycle_state_.load(std::memory_order_acquire)) {
        case RestoreLifecycleState::kRecovering:
        case RestoreLifecycleState::kFailed:
            return false;
        case RestoreLifecycleState::kConstructed:
        case RestoreLifecycleState::kReady:
        case RestoreLifecycleState::kServing:
            return true;
    }
    return false;
}

tl::expected<void, ErrorCode> MasterService::RestoreFromStandbyStateCore(
    const std::vector<StandbyObjectEntry>* legacy_objects,
    const StandbyMetadataStore* metadata_store,
    uint64_t initial_oplog_sequence_id,
    const std::vector<StandbySegmentInfo>& segments, size_t chunk_object_count,
    std::optional<ReplicaID> expected_max_replica_id,
    const WeightMetadataSnapshot* legacy_weight_metadata,
    bool skip_existing_objects) {
    if (enable_dfs_ && !use_kvcs_) {
        LOG(ERROR) << "RestoreFromStandbySnapshot: DFS allocator state "
                      "restoration is not supported";
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if ((legacy_objects == nullptr) == (metadata_store == nullptr) ||
        (metadata_store && chunk_object_count == 0) ||
        (legacy_objects && legacy_weight_metadata == nullptr)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const WeightMetadataSnapshot weight_metadata =
        metadata_store ? metadata_store->SnapshotWeightMetadata()
                       : *legacy_weight_metadata;
    if (!ValidateWeightMetadataSnapshot(weight_metadata)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // The ordered writer initializes its sequence from durable_prefix.
    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    std::unique_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    std::unordered_map<UUID, std::shared_ptr<ClientLivenessRecord>,
                       boost::hash<UUID>>
        new_known_owner_records;
    const auto record_for_known_owner = [&](const UUID& owner) {
        return RecordRestoreKnownOwner(new_known_owner_records, owner);
    };

    const auto& resolve_standby_object = ResolveStandbyObject;

    ReplicaID max_live_id = 0;
    if (metadata_store) {
        if (!expected_max_replica_id ||
            !metadata_store->ValidateReplicaIds(max_live_id) ||
            max_live_id != *expected_max_replica_id) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    } else {
        for (const auto& entry : *legacy_objects) {
            auto [tenant_id, user_key] = resolve_standby_object(entry);
            std::unordered_set<ReplicaID> object_replica_ids;
            for (const auto& desc : entry.metadata.replicas) {
                if (desc.id == 0 ||
                    desc.id == std::numeric_limits<ReplicaID>::max() ||
                    !object_replica_ids.insert(desc.id).second) {
                    LOG(ERROR)
                        << "RestoreFromStandbySnapshot: invalid or duplicate "
                        << "replica id=" << desc.id
                        << ", tenant=" << tenant_id.value()
                        << ", key=" << user_key;
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                max_live_id = std::max(max_live_id, desc.id);
            }
        }
    }

    std::vector<StandbySegmentInfo> restored_memory_segments;
    std::unordered_map<std::string, const StandbySegmentInfo*>
        memory_segments_by_alias;
    std::unordered_map<std::string, std::shared_ptr<BufferAllocatorBase>>
        restored_allocators;
    std::unordered_set<std::string> restored_invalid_endpoints;
    for (const auto& seg : segments) {
        if (!seg.is_memory_segment) {
            continue;
        }
        if (seg.segment_name.empty() || seg.transport_endpoint.empty() ||
            seg.capacity == 0) {
            LOG(ERROR) << "RestoreFromStandbySnapshot: invalid memory segment "
                       << "name=" << seg.segment_name
                       << ", endpoint=" << seg.transport_endpoint
                       << ", capacity=" << seg.capacity;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const auto add_alias = [&](const std::string& alias) {
            return memory_segments_by_alias.emplace(alias, &seg).second;
        };
        if (!add_alias(seg.transport_endpoint) ||
            (seg.segment_name != seg.transport_endpoint &&
             !add_alias(seg.segment_name))) {
            LOG(ERROR)
                << "RestoreFromStandbySnapshot: ambiguous memory segment "
                << "name=" << seg.segment_name
                << ", endpoint=" << seg.transport_endpoint;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        restored_memory_segments.push_back(seg);
        // Reuse the live standby allocator only on an exact endpoint
        // match. Restored buffers hold their allocator by weak_ptr, so a
        // later snapshot that swaps the keepalive must not expire the
        // buffers an earlier snapshot restored onto the same endpoint. A
        // name match with a changed endpoint is a different physical
        // segment: reusing its allocator would make the replica report
        // the stale endpoint and look readable before the new endpoint
        // remounts.
        std::shared_ptr<BufferAllocatorBase> allocator;
        if (auto keepalive =
                standby_allocator_keepalive_.find(seg.transport_endpoint);
            keepalive != standby_allocator_keepalive_.end()) {
            allocator = keepalive->second;
        } else {
            allocator = std::make_shared<DummyBufferAllocator>(
                seg.segment_name, seg.transport_endpoint);
        }
        restored_allocators[seg.transport_endpoint] = allocator;
        if (seg.segment_name != seg.transport_endpoint) {
            restored_allocators[seg.segment_name] = allocator;
        }
        if (!segment_manager_.HasSegmentByEndpoint(seg.transport_endpoint)) {
            restored_invalid_endpoints.insert(seg.transport_endpoint);
            if (seg.segment_name != seg.transport_endpoint) {
                restored_invalid_endpoints.insert(seg.segment_name);
            }
        }
    }

    // Batch restore spans chunks, so it needs an ordered cross-chunk index.
    std::unordered_map<const StandbySegmentInfo*, std::map<uintptr_t, uint64_t>>
        bounded_memory_ranges;
    std::unordered_map<std::string, uint64_t> restored_accounted_memory_bytes;
    size_t restored_object_count = 0;
    size_t dropped_stale_replica_count = 0;
    size_t skipped_stale_object_count = 0;

    size_t rejected_count = 0;
    // Objects skipped because the live index already holds them are not
    // losses: their state is present, just not from this snapshot.
    size_t already_existing_count = 0;
    // Conflict-derived discards become durable repair records after the index
    // installs, so a later promotion cannot replay them: full drops as
    // key-level REMOVE, partial discards as a canonical PUT_END carrying only
    // the survivors.
    std::vector<std::pair<TenantId, std::string>> repair_remove_keys;
    std::vector<std::pair<TenantId, std::string>> repair_canonical_keys;
    // The publications this call installed, so a failed restore can roll its
    // own state back before reporting the error.
    std::vector<std::pair<TenantId, route::ObjectRef>> installed;

    const auto restore_chunk =
        [&](const std::vector<StandbyObjectEntry>& objects)
        -> tl::expected<void, ErrorCode> {
        std::vector<PreparedObject> prepared;
        std::unordered_set<std::string> chunk_object_ids;
        size_t chunk_skipped_objects = 0;
        for (const auto& entry : objects) {
            auto [tenant_id, user_key] = resolve_standby_object(entry);
            if (!tenant_id.IsValid()) {
                LOG(ERROR) << "RestoreFromStandbySnapshot: invalid tenant_id="
                           << entry.tenant_id << ", key=" << entry.key;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (!chunk_object_ids.insert(tenant_id.MakeScopedKey(user_key))
                     .second) {
                LOG(ERROR)
                    << "RestoreFromStandbySnapshot: duplicate object, tenant="
                    << tenant_id.value() << ", key=" << user_key;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (MetadataAccessorRO(this, {tenant_id, user_key}).IsPublished()) {
                if (skip_existing_objects) {
                    // OpLog replay is additive: a previous partial attempt may
                    // already have installed this key, and re-installing it
                    // would both fail the whole replay and pin stale indexes.
                    VLOG(1) << "RestoreFromStandbySnapshot: replay skips an "
                               "already published object, tenant="
                            << tenant_id.value() << ", key=" << user_key;
                    ++chunk_skipped_objects;
                    continue;
                }
                LOG(ERROR)
                    << "RestoreFromStandbySnapshot: object already exists, "
                    << "tenant=" << tenant_id.value() << ", key=" << user_key;
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
            const auto& standby_meta = entry.metadata;
            std::vector<Replica> replicas;
            replicas.reserve(standby_meta.replicas.size());
            bool dropped_stale_replica = false;

            for (const auto& desc : standby_meta.replicas) {
                if (desc.is_memory_replica()) {
                    const auto& buffer =
                        desc.get_memory_descriptor().buffer_descriptor;
                    auto segment_it = memory_segments_by_alias.find(
                        buffer.transport_endpoint_);
                    if (segment_it == memory_segments_by_alias.end()) {
                        // The snapshot can still carry replicas on segments
                        // that were unloaded before the handoff. Those are
                        // gone for good, so drop just these replicas instead
                        // of failing the whole promotion.
                        LOG(WARNING)
                            << "RestoreFromStandbySnapshot: dropping replica "
                            << "id=" << desc.id << " on unknown memory "
                            << "endpoint=" << buffer.transport_endpoint_
                            << ", tenant=" << tenant_id.value()
                            << ", key=" << user_key;
                        ++dropped_stale_replica_count;
                        dropped_stale_replica = true;
                        continue;
                    }
                    if (buffer.size_ != standby_meta.size ||
                        buffer.size_ == 0 ||
                        buffer.buffer_address_ >
                            std::numeric_limits<uintptr_t>::max() -
                                buffer.size_) {
                        LOG(ERROR)
                            << "RestoreFromStandbySnapshot: invalid memory "
                            << "descriptor, tenant=" << tenant_id.value()
                            << ", key=" << user_key;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }

                    const auto* segment = segment_it->second;
                    if (desc.status != ReplicaStatus::REMOVED &&
                        desc.status != ReplicaStatus::FAILED) {
                        auto& bytes =
                            restored_accounted_memory_bytes[segment
                                                                ->segment_name];
                        if (bytes > segment->capacity ||
                            buffer.size_ > segment->capacity - bytes) {
                            LOG(ERROR)
                                << "RestoreFromStandbySnapshot: memory "
                                   "descriptors exceed segment capacity, "
                                   "segment="
                                << segment->segment_name;
                            return tl::make_unexpected(
                                ErrorCode::INVALID_PARAMS);
                        }
                        auto& ranges = bounded_memory_ranges[segment];
                        auto next = ranges.lower_bound(buffer.buffer_address_);
                        const uintptr_t end =
                            buffer.buffer_address_ + buffer.size_;
                        if ((next != ranges.end() && end > next->first) ||
                            (next != ranges.begin() &&
                             std::prev(next)->first + std::prev(next)->second >
                                 buffer.buffer_address_)) {
                            LOG(ERROR) << "RestoreFromStandbySnapshot: "
                                          "overlapping "
                                          "memory descriptors";
                            return tl::make_unexpected(
                                ErrorCode::INVALID_PARAMS);
                        }
                        ranges.emplace(buffer.buffer_address_, buffer.size_);
                        bytes += buffer.size_;
                    }

                    auto alloc =
                        restored_allocators.at(buffer.transport_endpoint_);
                    auto restored_buffer =
                        std::make_unique<AllocatedBuffer>(alloc, buffer);
                    replicas.push_back(Replica(
                        desc.id, std::move(restored_buffer), desc.status));
                } else if (desc.is_nof_replica()) {
                    const auto& buffer =
                        desc.get_nof_descriptor().buffer_descriptor;
                    if (buffer.size_ != standby_meta.size ||
                        buffer.size_ == 0) {
                        LOG(ERROR) << "RestoreFromStandbySnapshot: invalid NoF "
                                   << "descriptor, tenant=" << tenant_id.value()
                                   << ", key=" << user_key;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    auto& alloc =
                        restored_allocators[buffer.transport_endpoint_];
                    if (!alloc) {
                        alloc = std::make_shared<DummyBufferAllocator>(
                            buffer.transport_endpoint_,
                            buffer.transport_endpoint_);
                    }
                    auto restored_buffer =
                        std::make_unique<AllocatedBuffer>(alloc, buffer);
                    replicas.push_back(
                        Replica(desc.id, std::move(restored_buffer),
                                desc.status, ReplicaType::NOF_SSD));
                } else if (desc.is_disk_replica()) {
                    const auto& disk_desc = desc.get_disk_descriptor();
                    if (disk_desc.object_size != standby_meta.size) {
                        LOG(ERROR)
                            << "RestoreFromStandbySnapshot: invalid disk "
                            << "descriptor, tenant=" << tenant_id.value()
                            << ", key=" << user_key;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    replicas.push_back(Replica(desc.id, disk_desc.file_path,
                                               disk_desc.object_size,
                                               desc.status));
                } else if (desc.is_local_disk_replica()) {
                    const auto& local_disk_desc =
                        desc.get_local_disk_descriptor();
                    if (local_disk_desc.object_size != standby_meta.size) {
                        LOG(ERROR)
                            << "RestoreFromStandbySnapshot: invalid local disk "
                            << "descriptor, tenant=" << tenant_id.value()
                            << ", key=" << user_key;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    replicas.push_back(Replica(
                        desc.id, local_disk_desc.client_id,
                        local_disk_desc.object_size,
                        local_disk_desc.transport_endpoint, desc.status,
                        record_for_known_owner(local_disk_desc.client_id)));
                } else if (desc.is_dfs_replica()) {
                    const auto& dfs_desc = desc.get_dfs_descriptor();
                    if (!use_kvcs_ ||
                        dfs_desc.object_size != standby_meta.size) {
                        LOG(ERROR)
                            << "RestoreFromStandbySnapshot: invalid DFS "
                               "descriptor, tenant="
                            << tenant_id.value() << ", key=" << user_key
                            << ", descriptor_size=" << dfs_desc.object_size
                            << ", expected_size=" << standby_meta.size;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    replicas.push_back(Replica(desc.id, dfs_desc, desc.status));
                } else {
                    LOG(ERROR)
                        << "RestoreFromStandbySnapshot: unsupported replica "
                        << "descriptor, tenant=" << tenant_id.value()
                        << ", key=" << user_key;
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
            }
            if (replicas.empty() && dropped_stale_replica) {
                LOG(WARNING)
                    << "RestoreFromStandbySnapshot: skipping object with no "
                    << "surviving replicas, tenant=" << tenant_id.value()
                    << ", key=" << user_key;
                ++chunk_skipped_objects;
                continue;
            }
            prepared.push_back({&entry, std::move(tenant_id),
                                std::move(user_key), std::move(replicas)});
        }

        if (auto published = PublishRestoredObjects(prepared, installed);
            !published) {
            return tl::make_unexpected(published.error());
        }
        restored_object_count += objects.size() - chunk_skipped_objects;
        skipped_stale_object_count += chunk_skipped_objects;
        return {};
    };

    if (legacy_objects) {
        LegacyRestoreContext legacy_ctx{
            *legacy_objects,     memory_segments_by_alias,
            restored_allocators, restored_accounted_memory_bytes,
            rejected_count,      already_existing_count,
            repair_remove_keys,  repair_canonical_keys,
            installed,           new_known_owner_records};
        if (auto r = ValidateLegacyStandbyEntries(legacy_ctx); !r) {
            return tl::make_unexpected(r.error());
        }
        if (auto r = ConstructLegacyStandbyReplicas(legacy_ctx); !r) {
            return tl::make_unexpected(r.error());
        }
        auto installed = InstallLegacyStandbyObjects(legacy_ctx);
        if (!installed) {
            return tl::make_unexpected(installed.error());
        }
        restored_object_count += *installed;
    } else {
        // Copy, do not drain. The supervisor keeps the promotion handoff
        // until serving is established so a failed restore or a lost lease
        // can still PreservePromotionBaseline from the applied cursor.
        std::vector<StandbyObjectEntry> objects;
        auto cursor = metadata_store->BeginSnapshotTraversal();
        for (;;) {
            if (!metadata_store->CopyNextSnapshotChunk(chunk_object_count,
                                                       cursor, objects)) {
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            if (objects.empty()) {
                break;
            }
            auto restored = restore_chunk(objects);
            if (!restored) {
                return restored;
            }
        }
    }

    const auto prev_standby_accounted_memory_bytes =
        standby_accounted_memory_bytes_;
    const auto prev_standby_memory_segments = standby_memory_segments_;
    const auto prev_standby_allocator_keepalive = standby_allocator_keepalive_;
    const auto prev_invalid_replica_endpoints = invalid_replica_endpoints_;

    // A failed restore leaves nothing behind: installed keys go back out
    // through the canonical removal, bookkeeping returns to its pre-restore
    // snapshots, and the new liveness records come out with their counters.
    // The exported memory gauges are only swapped in once the restore can no
    // longer fail, so rollback never has to undo a published metric delta.
    const auto rollback_restored_state = [&]() {
        for (const auto& [tenant, ref] : installed) {
            MetadataAccessorRW accessor(this, tenant, ref);
            if (accessor.IsPublished()) {
                accessor.Erase();
            }
        }
        standby_accounted_memory_bytes_ = prev_standby_accounted_memory_bytes;
        standby_memory_segments_ = prev_standby_memory_segments;
        standby_allocator_keepalive_ = prev_standby_allocator_keepalive;
        invalid_replica_endpoints_ = prev_invalid_replica_endpoints;
        for (const auto& [client_id, record] : new_known_owner_records) {
            client_liveness_records_.erase(client_id);
            MasterMetricManager::instance().on_client_liveness_record_removed(
                record->state());
        }
        NotifyPolicyRestored();
    };

    standby_accounted_memory_bytes_ =
        std::move(restored_accounted_memory_bytes);
    standby_memory_segments_ = std::move(restored_memory_segments);
    standby_allocator_keepalive_ = std::move(restored_allocators);
    invalid_replica_endpoints_ = std::move(restored_invalid_endpoints);
    for (auto& [client_id, record] : new_known_owner_records) {
        // Copy, not move: rollback_restored_state() still needs a valid
        // record (state() for the removal metric) when a later durable
        // repair step fails the restore.
        client_liveness_records_.emplace(client_id, record);
        MasterMetricManager::instance().client_liveness_record_created();
    }

    if (max_live_id != 0) {
        const ReplicaID desired_next_id = max_live_id + 1;
        ReplicaID current_next_id = Replica::next_id_.load();
        while (current_next_id < desired_next_id &&
               !Replica::next_id_.compare_exchange_weak(current_next_id,
                                                        desired_next_id)) {
        }
    }

    NotifyPolicyRestored();

    auto restored_weight_metadata =
        weight_manager_.RestoreSnapshot(weight_metadata);
    if (!restored_weight_metadata) {
        // Same fail-closed contract as the repair paths below: leave nothing
        // behind, including the freshly installed liveness records.
        rollback_restored_state();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Durable repair for conflict-derived discards: a later promotion must
    // not replay what this one dropped. The whole repair set goes out as one
    // indivisible batch record (REMOVE for full drops, canonical PUT_END
    // carrying only the survivors for partial ones), and the restore fails
    // unless that record becomes durable, so the candidate never serves an
    // index whose discards could resurrect. Committing the entries one by
    // one would let the set split across batch records, and a durable
    // prefix covering only part of it leaves exactly one side of a conflict
    // pair able to survive a later replay on its own. With no OpLog
    // configured there is nothing to replay into, so the local filter
    // stands alone. With OpLog enabled but no writer available (e.g. no HA
    // backend connection string), a filtered discard would be silently
    // unrepaired, so the restore fails explicitly instead.
    if ((!repair_remove_keys.empty() || !repair_canonical_keys.empty()) &&
        enable_oplog_ && !ordered_oplog_writer_) {
        LOG(ERROR) << "RestoreFromStandbySnapshot: conflict discards need "
                      "durable repair but no OpLog writer is available; "
                      "failing the restore";
        rollback_restored_state();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if ((!repair_remove_keys.empty() || !repair_canonical_keys.empty()) &&
        enable_oplog_ && ordered_oplog_writer_) {
        std::vector<OpLogEntry> repair_entries;
        repair_entries.reserve(repair_remove_keys.size() +
                               repair_canonical_keys.size());
        for (const auto& [tenant, key] : repair_remove_keys) {
            OpLogEntry entry;
            entry.op_type = OpType::REMOVE;
            entry.tenant_id = tenant.value();
            entry.object_key = key;
            repair_entries.push_back(std::move(entry));
        }
        for (const auto& [tenant, key] : repair_canonical_keys) {
            MetadataAccessorRO accessor(this, {tenant, key});
            OpLogEntry entry;
            entry.tenant_id = tenant.value();
            entry.object_key = key;
            if (!accessor.IsPublished()) {
                // Survived the conflict but failed construction (e.g.
                // capacity): nothing installed, so the canonical state is
                // absence. Repudiate the key outright.
                entry.op_type = OpType::REMOVE;
            } else {
                entry.op_type = OpType::PUT_END;
                entry.payload = SerializeMetadataForOpLog(accessor.Get());
            }
            repair_entries.push_back(std::move(entry));
        }

        const size_t total = repair_entries.size();
        auto remaining = std::make_shared<std::atomic<size_t>>(total);
        auto done = std::make_shared<std::promise<void>>();
        std::future<void> durable_future = done->get_future();
        auto on_durable = [remaining, done](const OpLogEntry&) {
            if (remaining->fetch_sub(1, std::memory_order_acq_rel) == 1) {
                done->set_value();
            }
        };

        ErrorCode repair_err = ErrorCode::OK;
        auto appended = AppendOpLogBatchWithDurableFinalize(
            std::move(repair_entries), on_durable);
        if (!appended) {
            repair_err = appended.error();
        }
        if (repair_err == ErrorCode::OK) {
            // Wait on the writer's durability callback. 0 means no cap: a
            // slow backend should not turn a recoverable delay into a
            // promotion failure. A positive timeout still fail-closes, and
            // a later durable completion is left in the log for the retry
            // to observe rather than rolling it back locally.
            std::future_status durable_status = std::future_status::ready;
            if (restore_repair_durable_timeout_ms_ == 0) {
                durable_future.wait();
            } else {
                durable_status =
                    durable_future.wait_for(std::chrono::milliseconds(
                        restore_repair_durable_timeout_ms_));
            }
            if (durable_status != std::future_status::ready) {
                LOG(ERROR) << "RestoreFromStandbySnapshot: repair batch not "
                              "durable within "
                           << restore_repair_durable_timeout_ms_
                           << "ms, failing the restore";
                repair_err = ErrorCode::INTERNAL_ERROR;
            }
        }
        if (repair_err != ErrorCode::OK) {
            LOG(ERROR) << "RestoreFromStandbySnapshot: durable repair failed, "
                          "error="
                       << toString(repair_err);
            rollback_restored_state();
            return tl::make_unexpected(repair_err);
        }
        LOG(INFO) << "RestoreFromStandbySnapshot: durable repair records="
                  << total;
    }

    // Publish the accounting swap only after nothing can fail anymore: the
    // fail-closed paths above must stay a no-op for the exported gauges, and
    // the client/snapshot mutexes held here mean no concurrent reader can see
    // the gauge drift between the map swap and this commit.
    for (const auto& [segment, bytes] : prev_standby_accounted_memory_bytes) {
        MasterMetricManager::instance().dec_allocated_mem_size(
            segment, static_cast<int64_t>(bytes));
    }
    for (const auto& [segment, bytes] : standby_accounted_memory_bytes_) {
        MasterMetricManager::instance().inc_allocated_mem_size(
            segment, static_cast<int64_t>(bytes));
    }

    LOG(INFO) << "Restored from standby: " << restored_object_count
              << " objects, " << segments.size()
              << " segments, initial_seq_id=" << initial_oplog_sequence_id
              << ", invalid_endpoints=" << invalid_replica_endpoints_.size()
              << ", rejected_objects=" << rejected_count;
    if (dropped_stale_replica_count > 0) {
        LOG(WARNING) << "Restored from standby dropped "
                     << dropped_stale_replica_count
                     << " stale replicas on unloaded segments, skipped "
                     << skipped_stale_object_count << " objects";
    }
    return {};
}

auto MasterService::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<Segment> segments;
    ErrorCode err = segment_access.GetClientSegments(client_id, segments);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            VLOG(1) << "QueryIp: client_id=" << client_id
                    << " not found or has no segments";
            return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
        }

        LOG(ERROR) << "QueryIp: failed to get segments for client_id="
                   << client_id << ", error=" << toString(err);

        return tl::make_unexpected(err);
    }

    std::unordered_set<std::string> unique_ips;
    unique_ips.reserve(segments.size());
    for (const auto& segment : segments) {
        if (!segment.te_endpoint.empty()) {
            unique_ips.emplace(getHostNameWithoutPort(segment.te_endpoint));
        }
    }

    if (unique_ips.empty()) {
        LOG(WARNING) << "QueryIp: client_id=" << client_id
                     << " has no valid IP addresses";
        return {};
    }
    std::vector<std::string> result(unique_ips.begin(), unique_ips.end());
    return result;
}

auto MasterService::BatchQueryIp(const std::vector<UUID>& client_ids)
    -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode> {
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>
        results;
    results.reserve(client_ids.size());
    for (const auto& client_id : client_ids) {
        auto ip_result = QueryIp(client_id);
        if (ip_result.has_value()) {
            results.emplace(client_id, std::move(ip_result.value()));
        }
    }
    return results;
}

auto MasterService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name, const std::string& tenant_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    std::vector<std::string> cleared_keys;
    cleared_keys.reserve(object_keys.size());
    const bool clear_all_segments = segment_name.empty();
    const TenantId requested_tenant(tenant_id);
    if (!requested_tenant.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const TenantId& normalized_tenant =
        ResolveRequestTenantId(requested_tenant);

    for (const auto& key : object_keys) {
        if (key.empty()) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " empty key, skipping";
            continue;
        }
        MetadataAccessorRW accessor(this,
                                    MakeObjectIdentity(key, normalized_tenant));
        if (!accessor.Exists()) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " key=" << key << " not found, skipping";
            continue;
        }

        auto& metadata = accessor.Get();
        const auto previous_kv_media = KvMediaSnapshot(metadata);

        // Security check: Ensure the requesting client owns the object.
        if (metadata.client_id != client_id) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " key=" << key << " belongs to different client_id="
                         << metadata.client_id << ", expected=" << client_id
                         << ", skipping";
            continue;
        }

        // Safety check: Do not clear an object that has an active lease.
        if (!metadata.IsLeaseExpired()) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " key=" << key << " has active lease, skipping";
            continue;
        }

        if (clear_all_segments) {
            // Check if all replicas are complete. Incomplete replicas could
            // indicate an ongoing Put operation, and clearing during this time
            // could lead to an inconsistent state or interfere with the write.
            if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                LOG(WARNING)
                    << "BatchReplicaClear: tenant=" << normalized_tenant
                    << " key=" << key << " has incomplete replicas, skipping";
                continue;
            }

            if (enable_ha_) {
                if (enable_oplog_) {
                    auto reservation = ReserveBatchOpLogSlot();
                    if (!reservation) {
                        continue;
                    }
                    std::vector<ReplicaID> removed_ids;
                    metadata.VisitReplicas(
                        &Replica::fn_is_completed,
                        [&removed_ids](Replica& replica) {
                            removed_ids.push_back(replica.id());
                            replica.mark_removed();
                        });
                    auto persist_result =
                        AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::REMOVE,
                            normalized_tenant.value(), key, {},
                            [this, removed_ids = std::move(removed_ids)](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    durable_entry, removed_ids);
                            });
                    if (!persist_result) {
                        continue;
                    }
                    cleared_keys.emplace_back(key);
                    VLOG(1)
                        << "BatchReplicaClear: tenant=" << normalized_tenant
                        << " successfully cleared all replicas for key=" << key
                        << " for client_id=" << client_id;
                    continue;
                }
            }

            // Erase the entire metadata (all replicas will be deallocated)
            accessor.Erase();
            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: tenant=" << normalized_tenant
                    << " successfully cleared all replicas for key=" << key
                    << " for client_id=" << client_id;
        } else {
            // Clear only replicas on the specified segment_name
            const auto match_replica_on_segment =
                [&](const Replica& replica) -> bool {
                if (!replica.is_completed()) {
                    return false;
                }
                const auto segment_names = replica.get_segment_names();
                for (const auto& seg_name : segment_names) {
                    if (seg_name.has_value() &&
                        seg_name.value() == segment_name) {
                        return true;
                    }
                }
                return false;
            };

            if (!metadata.HasReplica(match_replica_on_segment)) {
                LOG(WARNING)
                    << "BatchReplicaClear: tenant=" << normalized_tenant
                    << " key=" << key
                    << " has no replica on segment_name=" << segment_name
                    << ", skipping";
                continue;
            }

            if (enable_ha_) {
                if (enable_oplog_) {
                    auto reservation = ReserveBatchOpLogSlot();
                    if (!reservation) {
                        continue;
                    }
                    auto remaining = BuildRemainingReplicaDescriptors(
                        metadata,
                        [&match_replica_on_segment](const Replica& r) {
                            return match_replica_on_segment(r);
                        });
                    std::vector<ReplicaID> removed_ids;
                    metadata.VisitReplicas(
                        match_replica_on_segment,
                        [&removed_ids](Replica& replica) {
                            removed_ids.push_back(replica.id());
                            replica.mark_removed();
                        });

                    tl::expected<OpLogEntry, ErrorCode> persist_result;
                    if (remaining.empty()) {
                        persist_result = AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::REMOVE,
                            normalized_tenant.value(), key, {},
                            [this, removed_ids = std::move(removed_ids)](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    durable_entry, removed_ids);
                            });
                    } else {
                        persist_result = AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::PUT_END,
                            normalized_tenant.value(), key,
                            SerializeMetadataForOpLogFromReplicaDescriptors(
                                metadata, remaining),
                            [this, removed_ids = std::move(removed_ids)](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    durable_entry, removed_ids);
                            });
                    }
                    if (!persist_result) {
                        continue;
                    }
                    cleared_keys.emplace_back(key);
                    VLOG(1) << "BatchReplicaClear: tenant=" << normalized_tenant
                            << " successfully cleared replicas on segment_name="
                            << segment_name << " for key=" << key
                            << " for client_id=" << client_id;
                    continue;
                }
            }

            EraseReplicasWithCacheTotalAccounting(metadata,
                                                  match_replica_on_segment);

            // If no valid replicas remain, erase the entire metadata
            if (!metadata.IsValid()) {
                accessor.Erase();
            } else {
                SyncKvObjectState(key, metadata, normalized_tenant,
                                  previous_kv_media);
            }

            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: tenant=" << normalized_tenant
                    << " successfully cleared replicas on segment_name="
                    << segment_name << " for key=" << key
                    << " for client_id=" << client_id;
        }
    }

    return cleared_keys;
}

bool MasterService::IsAwaitingRemount(const Replica& replica) const {
    // The set is empty outside the window between a standby restore and the
    // owners' remounts, so the endpoint is looked up only inside it.
    if (invalid_replica_endpoints_.empty()) {
        return false;
    }
    const auto endpoint = replica.transport_endpoint();
    return endpoint && invalid_replica_endpoints_.contains(*endpoint);
}

bool MasterService::TryGetReadableReplicaDescriptor(
    const Replica& replica, Replica::Descriptor& descriptor) const {
    return IsReplicaReadable(replica) &&
           replica.getDescriptorIfAvailable(descriptor);
}

std::vector<Replica::Descriptor> MasterService::GetReadableReplicaDescriptors(
    const ObjectMetadata& metadata) const {
    std::vector<Replica::Descriptor> descriptors;
    descriptors.reserve(metadata.CountReplicas());
    metadata.VisitReplicas(
        [](const Replica&) { return true; },
        [this, &descriptors](const Replica& replica) {
            Replica::Descriptor descriptor;
            if (TryGetReadableReplicaDescriptor(replica, descriptor)) {
                descriptors.push_back(std::move(descriptor));
            }
        });
    return descriptors;
}

bool MasterService::IsReplicaReadable(const Replica& replica) const {
    return replica.is_completed() && replica.is_available() &&
           !IsAwaitingRemount(replica);
}

bool MasterService::HasReadableReplica(const ObjectMetadata& metadata) const {
    return metadata.HasReplica(
        [this](const Replica& replica) { return IsReplicaReadable(replica); });
}

bool MasterService::IsEvictableMemoryReplica(const Replica& replica) const {
    return replica.is_memory_replica() && IsReplicaReadable(replica) &&
           replica.get_refcnt() == 0;
}

auto MasterService::GetReplicaListByRegex(const std::string& regex_pattern,
                                          const TenantId& tenant_id)
    -> tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode> {
    std::unordered_map<std::string, std::vector<Replica::Descriptor>> results;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const metadata::Namespace* ns =
        namespaces_.Lookup(ResolveRequestTenantId(tenant_id));
    if (ns == nullptr) {
        return results;
    }
    for (auto object : ns->objects.ReadCursor()) {
        const std::string& key = object.key();
        if (std::regex_search(key, pattern)) {
            const auto& metadata = object.metadata();
            auto replica_list = GetReadableReplicaDescriptors(metadata);

            if (replica_list.empty()) {
                LOG(WARNING)
                    << "key=" << key
                    << " matched by regex, but has no complete replicas.";
                continue;
            }

            results.emplace(key, std::move(replica_list));
            metadata.GrantReadLease(
                std::chrono::milliseconds(default_kv_lease_ttl_));
        }
    }

    return results;
}

auto MasterService::GetOffloadEndpoints()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::unordered_set<std::string> unique_endpoints;
    std::shared_lock<std::shared_mutex> state_lock(master_state_mutex_);
    VisitNamespaces([&](const TenantId&, metadata::Namespace& ns) {
        for (auto object : ns.objects.ReadCursor()) {
            object.metadata().VisitReplicas(
                [this](const Replica& replica) {
                    return replica.is_local_disk_replica() &&
                           replica.is_completed() && IsReplicaReadable(replica);
                },
                [&](const Replica& replica) {
                    const auto endpoint = replica.transport_endpoint();
                    if (endpoint && !endpoint->empty())
                        unique_endpoints.insert(*endpoint);
                });
        }
    });
    return std::vector<std::string>(unique_endpoints.begin(),
                                    unique_endpoints.end());
}

auto MasterService::GetReplicaList(const std::string& key,
                                   const TenantId& tenant_id)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);

    ReadFollowUps follow_ups;
    auto resp = [&]() -> tl::expected<GetReplicaListResponse, ErrorCode> {
        MetadataAccessorRO accessor(this, object_id);
                MasterMetricManager::instance().inc_total_get_nums();

        if (!accessor.Exists()) {
            VLOG(1) << "key=" << key << ", info=object_not_found";
                                    return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        return ReadReplicaList(object_id.tenant_id, key, accessor.Get(),
                               follow_ups);
    }();
    // RO accessor released. Safe to take a fresh RW accessor now.
    follow_ups.Run(*this);
    return resp;
}

auto MasterService::ReadReplicaList(const TenantId& tenant_id,
                                    const std::string& key,
                                    const ObjectMetadata& metadata,
                                    ReadFollowUps& follow_ups)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    auto replica_list = GetReadableReplicaDescriptors(metadata);
    if (dfs_allocator_) {
        for (const auto& replica : replica_list) {
            if (replica.is_dfs_replica()) {
                const auto& desc = replica.get_dfs_descriptor();
                dfs_allocator_->UpdateAccess(key, desc);
            }
        }
    }

    if (replica_list.empty()) {
        if (metadata.AllReplicas([](const Replica& replica) {
                return replica.status() == ReplicaStatus::REMOVED;
            })) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // TODO: NoF SSD support (ranhaojia)
    if (replica_list[0].is_memory_replica()) {
        MasterMetricManager::instance().inc_mem_cache_hit_nums();
        MasterMetricManager::instance().inc_mem_cache_hit_bytes(
            static_cast<int64_t>(metadata.TotalLength()));
    } else if (replica_list[0].is_local_disk_replica() ||
               replica_list[0].is_disk_replica()) {
        MasterMetricManager::instance().inc_file_cache_hit_nums();
        MasterMetricManager::instance().inc_file_cache_hit_bytes(
            static_cast<int64_t>(metadata.TotalLength()));
    }
    MasterMetricManager::instance().inc_valid_get_nums();
    // Grant a lease to the object so it will not be removed
    // when the client is reading it. Read path is group-agnostic: only the
    // object's own lease is refreshed.
    metadata.GrantReadLease(std::chrono::milliseconds(default_kv_lease_ttl_));

    // Promotion-on-hit eligibility: only when no MEMORY replica is
    // present but at least one LOCAL_DISK replica is. Decided here while
    // the key is held; the actual enqueue happens once it is released, to
    // avoid lock-upgrade complexity.
    if (promotion_on_hit_) {
        const bool any_memory =
            std::any_of(replica_list.begin(), replica_list.end(),
                        [](const auto& descriptor) {
                            return descriptor.is_memory_replica();
                        });
        const bool any_local_disk =
            std::any_of(replica_list.begin(), replica_list.end(),
                        [](const auto& descriptor) {
                            return descriptor.is_local_disk_replica();
                        });
        if (!any_memory && any_local_disk) {
            follow_ups.Promote(tenant_id, key);
        }
    }
    if (dynamic_replication_.Enabled()) {
        const size_t memory_replicas =
            std::count_if(replica_list.begin(), replica_list.end(),
                          [](const auto& descriptor) {
                              return descriptor.is_memory_replica();
                          });
        if (dynamic_replication_.WantsAnotherMemoryReplica(memory_replicas)) {
            follow_ups.RecordAccess(tenant_id, key);
        }
    }

    return GetReplicaListResponse(std::move(replica_list),
                                  default_kv_lease_ttl_,
                                  metadata.GetPart(0).checksum());
}

void MasterService::ReadFollowUps::Run(MasterService& service) const {
    for (const auto& object_id : promotions_) {
        service.TryPushPromotionQueue(object_id);
    }
    for (const auto& object_id : accesses_) {
        service.dynamic_replication_.RecordAccess(object_id.tenant_id,
                                                  object_id.user_key);
    }
}

auto MasterService::GetReplicaListForAdmin(const std::string& key,
                                           const TenantId& tenant_id)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    assert(tenant_id.IsValid());
    const auto object_id = MakeObjectIdentity(key, tenant_id);

    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRO accessor(this, object_id);

    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto resp = ReadReplicaListForAdmin(accessor.Get());
    if (!resp.has_value()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
    }
    return resp;
}

auto MasterService::ReadReplicaListForAdmin(const ObjectMetadata& metadata)
    const -> tl::expected<GetReplicaListResponse, ErrorCode> {
    auto replica_list = GetReadableReplicaDescriptors(metadata);

    if (replica_list.empty()) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    return GetReplicaListResponse(std::move(replica_list),
                                  default_kv_lease_ttl_,
                                  metadata.GetPart(0).checksum());
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::BatchGetReplicaList(const std::vector<std::string>& keys,
                                   const TenantId& tenant_id) {
    assert(tenant_id.IsValid());

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> results(
        keys.size(), tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    if (keys.empty()) {
        return results;
    }

    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);

    // A1 + 方案 B 第二阶段：先按 slot 所有权把 keys 拆成「本地组」与
    // 「转发组（按 owner master_id 分组）」。本地组走本地批量查询，转发组
    // 通过 InterMasterRpcClient 转发给对应 slot owner。
    std::vector<size_t> local_indices;
    std::map<std::string, std::vector<size_t>> forward_groups;
    for (size_t i = 0; i < keys.size(); ++i) {
        const uint16_t slot = cvm::KeySlot(normalized_tenant, keys[i]);
        const ErrorCode svc = CheckSlotServiceability(slot);
        if (svc == ErrorCode::OK) {
            local_indices.push_back(i);
            continue;
        }
        if (svc == ErrorCode::SLOT_MIGRATING) {
            results[i] = tl::make_unexpected(ErrorCode::SLOT_MIGRATING);
            continue;
        }
#ifdef STORE_USE_ETCD
        auto owner = ResolveSlotOwnerMasterId(slot);
        if (owner && !owner->empty() && *owner != master_id_ &&
            inter_master_rpc_) {
            forward_groups[*owner].push_back(i);
            continue;
        }
#endif
        LOG(WARNING) << "BatchGetReplicaList: key=" << keys[i]
                     << " slot=" << slot
                     << " not owned by this master and no forward target, "
                        "rejecting with SLOT_NOT_OWNED";
        results[i] = tl::make_unexpected(ErrorCode::SLOT_NOT_OWNED);
    }

    if (!local_indices.empty()) {
        std::vector<std::string> local_keys;
        local_keys.reserve(local_indices.size());
        for (size_t idx : local_indices) {
            local_keys.push_back(keys[idx]);
        }
        auto local_results =
            BatchGetReplicaListLocal(local_keys, normalized_tenant);
        for (size_t j = 0; j < local_indices.size(); ++j) {
            results[local_indices[j]] = std::move(local_results[j]);
        }
    }

    std::vector<VSegmentLayoutEntry> layouts;
#ifdef STORE_USE_ETCD
    for (const auto& [owner, indices] : forward_groups) {
        std::vector<std::string> group_keys;
        group_keys.reserve(indices.size());
        for (size_t idx : indices) {
            group_keys.push_back(keys[idx]);
        }
        auto group_result = inter_master_rpc_->BatchGetReplicaList(
            owner, group_keys, normalized_tenant.value());
        // Collect the owner-built table before moving the per-key results out.
        MergeVSegmentLayoutTablesFromResults(group_result, layouts);
        for (size_t j = 0; j < indices.size() && j < group_result.size(); ++j) {
            results[indices[j]] = std::move(group_result[j]);
        }
    }
#endif

    // 本地组：视图只由本机 vsegment service 持有，故本机解析；转发组的表
    // 已由 owner 在转发响应里带回（上面的 layouts）。两端按 vsegment_id 去重。
    std::vector<std::pair<std::string, std::string>> local_vsegments;
    for (size_t idx : local_indices) {
        const auto& result = results[idx];
        if (!result) continue;
        for (const auto& replica : result->replicas) {
            if (!replica.is_vsegment_replica()) continue;
            const auto& descriptor = replica.get_vsegment_descriptor();
            local_vsegments.emplace_back(descriptor.partition_id,
                                         descriptor.vsegment_id);
        }
    }
    if (!local_vsegments.empty()) {
        SpDiag::PerfPoint pt(PerfKey::VSEGMENT_READ_LAYOUT_BUILD,
                             SpDiag::PerfLevel::MODULE);
        pt.Start();
        auto local_layouts = BuildVSegmentLayoutTable(local_vsegments);
        pt.End(local_layouts.empty() ? -1 : 0);
        MergeVSegmentLayoutTable(layouts, local_layouts);
    }
    // 批量级别没有独立 envelope（类型码不允许），去重表挂到第一个成功项。
    AttachVSegmentLayoutTableToFirstSuccess(results, std::move(layouts));
    return results;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::BatchGetReplicaListLocal(const std::vector<std::string>& keys,
                                        const TenantId& tenant_id) {
    using GetResult = tl::expected<GetReplicaListResponse, ErrorCode>;

    assert(tenant_id.IsValid());

    std::vector<GetResult> results(
        keys.size(), tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    if (keys.empty()) {
        return results;
    }

    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    ReadFollowUps follow_ups;
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    for (size_t i = 0; i < keys.size(); ++i) {
        MasterMetricManager::instance().inc_total_get_nums();
        MetadataAccessorRO accessor(this, normalized_tenant, keys[i]);
        if (!accessor.Exists()) {
            VLOG(1) << "key=" << keys[i] << ", info=object_not_found";
            continue;
        }
        results[i] = ReadReplicaList(normalized_tenant, keys[i], accessor.Get(),
                                     follow_ups);
    }
    follow_ups.Run(*this);
    return results;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::BatchGetReplicaListForAdmin(const std::vector<std::string>& keys,
                                           const TenantId& tenant_id) {
    using GetResult = tl::expected<GetReplicaListResponse, ErrorCode>;

    assert(tenant_id.IsValid());

    std::vector<GetResult> results(
        keys.size(), tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    if (keys.empty()) {
        return results;
    }

    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    for (size_t i = 0; i < keys.size(); ++i) {
        MetadataAccessorRO accessor(this, tenant_id, keys[i]);
        if (accessor.Exists()) {
            results[i] = ReadReplicaListForAdmin(accessor.Get());
        }
    }
    return results;
}

auto MasterService::AllocateReplicas(const std::string& key,
                                     uint64_t value_length,
                                     const ReplicateConfig& config,
                                     const std::string& writer_host_id,
                                     bool* dfs_allocation_failed)
    -> tl::expected<std::vector<Replica>, ErrorCode> {
    if (dfs_allocation_failed != nullptr) {
        *dfs_allocation_failed = false;
    }
    std::vector<Replica> replicas;
    const auto write_mode = DetermineReplicaWriteMode(config);
    size_t allocated_memory_replicas = 0;
    size_t allocated_nof_replicas = 0;
    bool has_enough_memory_segments = false;
    // 方案二被动层：分配排除集 = 被动上报的坏端点快照（transport_endpoint
    // 与 segment_name 双形式，见 ReportInvalidReplicaEndpoints）。allocator
    // 锁 → invalid 锁（shared）与 MountSegment 自愈 erase 的锁序一致。
    std::set<std::string> excluded_invalid_segments;
    {
        std::shared_lock<std::shared_mutex> lock(invalid_endpoints_mutex_);
        excluded_invalid_segments.insert(invalid_replica_endpoints_.begin(),
                                         invalid_replica_endpoints_.end());
    }

    if (config.replica_num > 0) {
        std::vector<std::string> preferred_segments;
        auto append_preferred_segment = [&preferred_segments](
                                            const std::string& segment_name) {
            if (!segment_name.empty() &&
                std::find(preferred_segments.begin(), preferred_segments.end(),
                          segment_name) == preferred_segments.end()) {
                preferred_segments.push_back(segment_name);
            }
        };
        if (!config.preferred_segment.empty()) {
            append_preferred_segment(config.preferred_segment);
        } else {
            for (const auto& preferred_segment : config.preferred_segments) {
                append_preferred_segment(preferred_segment);
            }
        }
        AllocatorManager allocator_snapshot;
        {
            ScopedAllocatorAccess allocator_access =
                segment_manager_.getAllocatorAccess();
            if (!writer_host_id.empty()) {
                auto host_ordered_segments =
                    allocator_access.GetHostOrderedSegments(writer_host_id,
                                                            key);
                for (const auto& segment_name : host_ordered_segments) {
                    append_preferred_segment(segment_name);
                }
                if (!host_ordered_segments.empty()) {
                    VLOG(1) << "key=" << key
                            << ", writer_host_id=" << writer_host_id
                            << ", local_first_preferred_segments="
                            << host_ordered_segments.size();
                }
            }
            allocator_snapshot = allocator_access.SnapshotAllocatorManager();
        }

        if (StoreHcaAffinityEnabled()) {
            allocator_snapshot.SetHcaAffinity(
                config.preferred_hca_names.value_or(
                    std::vector<std::string>{}));
        }
        auto allocation_result = allocation_strategy_->Allocate(
            allocator_snapshot, value_length, config.replica_num,
            preferred_segments, excluded_invalid_segments,
            ReplicaType::MEMORY);

        // Only failed or short allocations need this eviction hint. Count on
        // the retained snapshot after releasing the segment read lock.
        if ((!allocation_result &&
             allocation_result.error() != ErrorCode::INVALID_PARAMS) ||
            (allocation_result &&
             allocation_result->size() != config.replica_num)) {
            has_enough_memory_segments =
                allocator_snapshot.getServingNames().size() >=
                config.replica_num;
        }

        if (!allocation_result.has_value()) {
            VLOG(1) << "Failed to allocate replicas for key=" << key
                    << ", error: " << allocation_result.error();
            if (allocation_result.error() == ErrorCode::INVALID_PARAMS) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
                MasterMetricManager::instance().inc_put_start_alloc_failures();
                if (has_enough_memory_segments) {
                    need_mem_eviction_ = true;
                }
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
        } else {
            allocated_memory_replicas = allocation_result->size();
            replicas = std::move(allocation_result.value());
        }
    }

#ifdef USE_NOF
    if (config.nof_replica_num > 0 &&
        nof_segment_manager_.getMountedSegmentCount() > 0) {
        ScopedAllocatorAccess allocator_access =
            nof_segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        std::vector<std::string> preferred_segments =
            config.preferred_nof_segments;

        auto allocation_result = allocation_strategy_->Allocate(
            allocator_manager, value_length, config.nof_replica_num,
            preferred_segments, excluded_invalid_segments,
            ReplicaType::NOF_SSD);

        if (!allocation_result.has_value()) {
            VLOG(1) << "Failed to allocate nof replicas for key=" << key
                    << ", error: " << allocation_result.error();
            if (allocation_result.error() == ErrorCode::INVALID_PARAMS) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
                MasterMetricManager::instance().inc_put_start_alloc_failures();
                need_nof_eviction_ = true;
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
        } else {
            allocated_nof_replicas = allocation_result->size();
            for (auto& replica : allocation_result.value()) {
                replicas.push_back(std::move(replica));
            }
        }
    }
#endif

    if (!HasExpectedReplicaAllocation(config, allocated_memory_replicas,
                                      allocated_nof_replicas,
                                      strict_replica_allocation_)) {
        if ((config.replica_num > 0 &&
             allocated_memory_replicas != config.replica_num) ||
            (config.nof_replica_num > 0 &&
             allocated_nof_replicas != config.nof_replica_num)) {
            MasterMetricManager::instance().inc_put_start_alloc_failures();
            if (config.replica_num > 0 &&
                allocated_memory_replicas != config.replica_num &&
                has_enough_memory_segments) {
                need_mem_eviction_ = true;
            }
            if (config.nof_replica_num > 0 &&
                allocated_nof_replicas != config.nof_replica_num) {
                need_nof_eviction_ = true;
            }
        }
        VLOG(1) << "Failed to satisfy replica allocation requirement for key="
                << key << ", requested_memory_replicas=" << config.replica_num
                << ", allocated_memory_replicas=" << allocated_memory_replicas
                << ", requested_nof_replicas=" << config.nof_replica_num
                << ", allocated_nof_replicas=" << allocated_nof_replicas;
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Best-effort / flexible modes may pass the check above with fewer
    // replicas than requested (see HasExpectedReplicaAllocation). Surface
    // the degradation so callers and operators can detect the reduced
    // redundancy instead of failing silently.
    if (allocated_memory_replicas < config.replica_num ||
        allocated_nof_replicas < config.nof_replica_num) {
        MasterMetricManager::instance().inc_put_start_partial_allocations();
        LOG(WARNING) << "key=" << key << ", action=put_start_partial_allocation"
                     << ", requested_memory_replicas=" << config.replica_num
                     << ", allocated_memory_replicas="
                     << allocated_memory_replicas
                     << ", requested_nof_replicas=" << config.nof_replica_num
                     << ", allocated_nof_replicas=" << allocated_nof_replicas;
    }

    if (use_disk_replica_) {
        std::string file_path =
            FileUtil::ResolvePathFromKey(key, root_fs_dir_, cluster_id_);
        replicas.emplace_back(file_path, value_length,
                              ReplicaStatus::PROCESSING);
    }

    if (config.dfs_replica_num > 0) {
        if (use_kvcs_) {
            DistributedFSDescriptor descriptor;
            descriptor.object_size = value_length;
            descriptor.aligned_size = value_length;
            descriptor.shard_idx = -1;
            replicas.emplace_back(std::move(descriptor),
                                  ReplicaStatus::PROCESSING);
        } else if (!dfs_allocator_ || !dfs_allocator_->IsInitialized()) {
            LOG(ERROR) << "Failed to allocate DFS replica for key=" << key
                       << ", error=dfs_allocator_not_initialized";
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        } else {
            auto alloc = dfs_allocator_->Allocate(key, value_length);
            if (!alloc) {
                // NO_AVAILABLE_HANDLE is a capacity failure that can repeat
                // for every PutStart and is returned to the caller, so log it
                // like memory exhaustion above.
                if (alloc.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                    VLOG(1) << "Failed to allocate DFS replica for key=" << key
                            << ", error=" << alloc.error();
                } else {
                    LOG(ERROR)
                        << "Failed to allocate DFS replica for key=" << key
                        << ", error=" << alloc.error();
                }
                if (dfs_allocation_failed != nullptr) {
                    *dfs_allocation_failed =
                        alloc.error() == ErrorCode::NO_AVAILABLE_HANDLE;
                }
                return tl::make_unexpected(alloc.error());
            }
            replicas.emplace_back(std::move(*alloc), ReplicaStatus::PROCESSING);
        }
    }

    return replicas;
}

auto MasterService::InsertMetadata(
    MetadataAccessorRW& accessor, const UUID& client_id, uint64_t value_length,
    const ReplicateConfig& config, const std::string& group_id,
    const std::string& residency_affinity_id,
    const std::chrono::system_clock::time_point& now,
    const ResolvedSoftPinRequest& soft_pin_request,
    std::vector<Replica>&& replicas,
    std::optional<std::chrono::system_clock::time_point>
        committed_soft_pin_timeout)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    assert(!accessor.IsPublished());
    const std::string& key = accessor.GetKey();
    const TenantId& tenant_id = accessor.GetTenantId();
    const auto deadline_to_index = committed_soft_pin_timeout;

    std::vector<Replica::Descriptor> replica_list;
    std::unordered_set<ReplicaID> eligible_replica_ids;
    replica_list.reserve(replicas.size());
    eligible_replica_ids.reserve(replicas.size());
    int i = 0;
    MC_VLOG(1) << "PutStart, create replicas: client_id=" << client_id
               << ", key=" << key << ", value_length=" << value_length;
    for (const auto& replica : replicas) {
        const auto desc = replica.get_descriptor();
        replica_list.emplace_back(desc);
        eligible_replica_ids.insert(replica.id());

        if (replica.is_memory_replica()) {
            const auto& mem_desc = desc.get_memory_descriptor();
            MC_VLOG(1) << "Replica #" << ++i << ": buffer_address="
                       << mem_desc.buffer_descriptor.buffer_address_
                       << ", transport_endpoint="
                       << mem_desc.buffer_descriptor.transport_endpoint_;
        } else if (replica.is_nof_replica()) {
            const auto& nof_desc = desc.get_nof_descriptor();
            VLOG(1) << "Replica #" << ++i << ": buffer_address="
                    << nof_desc.buffer_descriptor.buffer_address_
                    << ", transport_endpoint="
                    << nof_desc.buffer_descriptor.transport_endpoint_;
        } else if (replica.is_dfs_replica()) {
            const auto& dfs_desc = desc.get_dfs_descriptor();
            VLOG(1) << "Replica #" << ++i << ": dfs_file=" << dfs_desc.file_path
                    << ", offset=" << dfs_desc.offset
                    << ", shard_idx=" << dfs_desc.shard_idx;
        }
    }

    accessor.Publish(client_id, now, value_length, std::move(replicas),
                     std::move(committed_soft_pin_timeout),
                     config.with_hard_pin, config.data_type, group_id,
                     tenant_id, key, residency_affinity_id);
    auto& metadata = accessor.Get();
    metadata.BeginSessionTag(config.SessionId(), eligible_replica_ids);
    metadata.BeginSoftPinAction(soft_pin_request,
                                std::move(eligible_replica_ids));
    if (deadline_to_index) {
        soft_pin_deadline_index_.Upsert(tenant_id.MakeScopedKey(key),
                                        *deadline_to_index);
    }
    accessor.GetState().is_processing = true;

    return replica_list;
}

auto MasterService::AllocateAndInsertMetadata(
    MetadataAccessorRW& accessor, const UUID& client_id, uint64_t value_length,
    const ReplicateConfig& config, const std::string& writer_host_id,
    const std::string& group_id, const std::string& residency_affinity_id,
    const std::chrono::system_clock::time_point& now,
    const ResolvedSoftPinRequest& soft_pin_request,
    std::optional<std::chrono::system_clock::time_point>
        committed_soft_pin_timeout,
    bool* dfs_allocation_failed)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    assert(!accessor.IsPublished());
    const std::string& key = accessor.GetKey();

    auto grown =
        accessor.Grow(SaturatingMultiply(value_length, config.replica_num));
    if (!grown) {
        return tl::make_unexpected(grown.error());
    }

    auto allocation_result = AllocateReplicas(
        key, value_length, config, writer_host_id, dfs_allocation_failed);
    if (!allocation_result) {
        return tl::make_unexpected(allocation_result.error());
    }

    return InsertMetadata(accessor, client_id, value_length, config, group_id,
                          residency_affinity_id, now, soft_pin_request,
                          std::move(allocation_result.value()),
                          std::move(committed_soft_pin_timeout));
}

auto MasterService::PrepareWriteStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    uint64_t slice_length, const ReplicateConfig& config,
    std::string_view action) -> tl::expected<WriteStartPlan, ErrorCode> {
    if ((config.replica_num == 0 && config.nof_replica_num == 0 &&
         config.dfs_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", dfs_replica_num=" << config.dfs_replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.dfs_replica_num > 1) {
        LOG(ERROR) << "key=" << key
                   << ", dfs_replica_num=" << config.dfs_replica_num
                   << ", error=dfs_replica_num_must_be_0_or_1";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.dfs_replica_num > 0 && !tenant_id.IsDefault()) {
        LOG(ERROR) << "key=" << key << ", tenant_id=" << tenant_id
                   << ", error=dfs_currently_requires_default_tenant";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.dfs_replica_num > 0 &&
        (!enable_dfs_ || (!use_kvcs_ && (!dfs_allocator_ ||
                                         !dfs_allocator_->IsInitialized())))) {
        LOG(ERROR) << "key=" << key
                   << ", dfs_replica_num=" << config.dfs_replica_num
                   << ", error=dfs_service_unavailable";
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if (config.prefer_alloc_in_same_node && config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", prefer_alloc_in_same_node="
                   << config.prefer_alloc_in_same_node
                   << ", error=nof_not_supported_with_prefer_same_node";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#ifndef USE_NOF
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", error=nof_pool_disabled";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#endif

    auto soft_pin_request = ResolveSoftPinRequest(config);
    if (!soft_pin_request) {
        return tl::make_unexpected(soft_pin_request.error());
    }

    UpdateClientHostId(client_id, config.host_id);
    std::string writer_host_id;
    if ((allocation_strategy_type_ == AllocationStrategyType::LOCAL_FIRST ||
         config.prefer_alloc_in_same_node) &&
        config.replica_num == 1) {
        writer_host_id = config.host_id.empty() ? GetClientHostId(client_id)
                                                : config.host_id;
    }

    if ((memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        LOG(ERROR) << "key=" << key << ", slice_length=" << slice_length
                   << ", max_size=" << kMaxSliceSize
                   << ", error=invalid_slice_size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    VLOG(1) << "key=" << key << ", value_length=" << slice_length
            << ", config=" << config << ", action=" << action << "_begin";

    if (!config.ValidSessionIds(1)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto group_id = GetGroupIdForKey(config, 1, 0);
    if (!group_id) {
        return tl::make_unexpected(group_id.error());
    }
    // Refused before the key is taken: taking it creates the tenant's
    // namespace, which stays once created. Grow(0) under the key still has
    // the last word.
    if (!IsTenantRegistered(tenant_id)) {
        return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    auto affinity_id = GetResidencyAffinityIdForKey(config, 1, 0);
    if (!affinity_id) {
        return tl::make_unexpected(affinity_id.error());
    }
    return WriteStartPlan{.soft_pin_request = std::move(*soft_pin_request),
                          .writer_host_id = std::move(writer_host_id),
                          .group_id = std::move(*group_id),
                          .residency_affinity_id = std::move(*affinity_id)};
}

auto MasterService::CleanupStaleBeforeWrite(
    const char* why, MetadataAccessorRW& accessor,
    const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients)
    -> tl::expected<void, ErrorCode> {
    if (!accessor.IsPublished()) {
        return {};
    }
    const std::string& key = accessor.GetKey();
    const TenantId& tenant_id = accessor.GetTenantId();
    auto& metadata = accessor.Get();
    auto cleanup_plan =
        BuildStaleHandleCleanupPlan(metadata, retaining_clients);
    if (cleanup_plan.removed_ids.empty()) {
        return {};
    }
    auto persist_result = PersistStaleHandleCleanupForHA(
        why, tenant_id, key, metadata, cleanup_plan);
    if (!persist_result) {
        return tl::make_unexpected(persist_result.error());
    }
    if (enable_oplog_) {
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    if (CleanupStaleHandles(key, tenant_id, accessor.Get(), accessor.GetState(),
                            retaining_clients)) {
        // EraseMetadata gives back what the object's tasks hold before they
        // go with it.
        accessor.Erase();
    }
    return {};
}

template <typename Admit>
auto MasterService::AdmitWithDfsRecovery(const ReplicateConfig& config,
                                         const Admit& admit)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    // Set by AllocateReplicas only when DFS allocation itself failed with
    // capacity exhaustion; gates the forced bucket eviction retry below so
    // memory/NoF exhaustion cannot delete unrelated DFS data.
    bool dfs_allocation_failed = false;
    auto result = admit(&dfs_allocation_failed);
    // Eviction locks keys one by one, so recover only after admit releases
    // the key. Serialize recovery and re-admit before deciding whether this
    // request still needs to evict one bucket.
    if (!result && dfs_allocation_failed && config.dfs_replica_num > 0 &&
        bucket_allocator_ != nullptr) {
        std::lock_guard recovery_lock(dfs_bucket_recovery_mutex_);
        dfs_allocation_failed = false;
        result = admit(&dfs_allocation_failed);
        if (!result && dfs_allocation_failed &&
            TryRecoverDfsSpaceAfterAllocationFailure()) {
            dfs_allocation_failed = false;
            result = admit(&dfs_allocation_failed);
        }
    }
    return result;
}

auto MasterService::PutStart(const UUID& client_id, const std::string& key,
                             const TenantId& tenant_id,
                             const uint64_t slice_length,
                             const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    auto plan = PrepareWriteStart(client_id, key, object_id.tenant_id,
                                  slice_length, config, "put_start");
    if (!plan) {
        return tl::make_unexpected(plan.error());
    }
    std::optional<std::unique_lock<std::mutex>> weight_group_operation_lock;
    if (!plan->group_id.empty() &&
        weight_manager_.IsManagedGroup(plan->group_id)) {
        weight_group_operation_lock.emplace(
            weight_manager_.LockGroup(object_id.tenant_id, plan->group_id));
        if (!weight_manager_.AllowsGroupMemberMutation(plan->group_id)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    auto admit = [&](bool* dfs_allocation_failed)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
        auto now = std::chrono::system_clock::now();
        std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
        std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
        auto retaining_clients = GetRetainingClientIdsLocked();
        client_lock.unlock();
        // The accessor holds the key across the stale cleanup and the
        // create, so no other writer of the key lands in between.
        MetadataAccessorRW accessor(this, object_id,
                                    MetadataAccessorRW::CreateKey{});
        if (auto admitted = accessor.Grow(0); !admitted) {
            return tl::make_unexpected(admitted.error());
        }
        if (auto cleaned = CleanupStaleBeforeWrite("PutStart(stale cleanup)",
                                                   accessor, retaining_clients);
            !cleaned) {
            return tl::make_unexpected(cleaned.error());
        }
        if (accessor.IsPublished()) {
            auto& metadata = accessor.Get();
            if (metadata.HasReplica(&Replica::fn_is_completed) ||
                metadata.put_start_time + put_start_discard_timeout_sec_ >=
                    now) {
                // A put of what the store already holds is a read of it: the
                // writer just showed the object is still wanted.
                if (HasReadableReplica(metadata)) {
                    metadata.GrantReadLease(
                        std::chrono::milliseconds(default_kv_lease_ttl_));
                }
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
            if (enable_oplog_ && ordered_oplog_writer_) {
                auto err = PersistRemoveForHA("PutStart(stale cleanup REMOVE)",
                                              object_id.tenant_id, key);
                if (!err) {
                    return tl::make_unexpected(err.error());
                }
            }
            auto replicas = PopReplicasWithCacheTotalAccounting(
                metadata, &Replica::fn_is_processing);
            if (!replicas.empty()) {
                FreeDfsReplicas(key, replicas);
                std::lock_guard lock(discarded_replicas_mutex_);
                discarded_replicas_.emplace_back(
                    std::move(replicas),
                    metadata.put_start_time + put_start_release_timeout_sec_);
            }
            accessor.Erase();
        }

        return AllocateAndInsertMetadata(
            accessor, client_id, slice_length, config, plan->writer_host_id,
            plan->group_id, plan->residency_affinity_id, now,
            plan->soft_pin_request, std::nullopt, dfs_allocation_failed);
    };

    // Tenant over-quota remains a background-eviction concern. Bucket-count
    // exhaustion is different: see AdmitWithDfsRecovery.
    auto result = AdmitWithDfsRecovery(config, admit);
    if (result && weight_group_operation_lock) {
        weight_manager_.RecordImportWrite(object_id.tenant_id, plan->group_id);
    }
    return result;
}

auto MasterService::PutEndInternal(const UUID& client_id,
                                   const ObjectMeta& object_meta,
                                   const TenantId& tenant_id,
                                   ReplicaType replica_type,
                                   uint32_t write_batch_size, bool overwrite,
                                   const std::string& operation_id)
    -> tl::expected<void, ErrorCode> {
    const auto& key = object_meta.key;
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
#ifdef STORE_USE_ETCD
    if (!OwnsSlot(cvm::KeySlot(object_id.tenant_id, object_id.user_key)))
        return tl::make_unexpected(ErrorCode::SLOT_NOT_OWNED);
#endif
    SpDiag::PerfPoint pt_shard_lock_wait(
        PerfKey::MASTER_PUT_END_SHARD_LOCK_WAIT, SpDiag::PerfLevel::MODULE);
    pt_shard_lock_wait.Start();
    MetadataAccessorRW accessor(this, object_id);
    pt_shard_lock_wait.End(0);
    SpDiag::PerfPoint pt_shard_lock_hold(
        PerfKey::MASTER_PUT_END_SHARD_LOCK_HOLD, SpDiag::PerfLevel::MODULE);
    pt_shard_lock_hold.Start();
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        pt_shard_lock_hold.End(-1);
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutEnd key " << key
                   << ", was PutStart-ed by " << metadata.client_id;
        pt_shard_lock_hold.End(-1);
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    auto is_target_replica = [replica_type](const Replica& replica) {
        if (replica.is_vsegment_replica()) {
            return replica_type == ReplicaType::ALL ||
                   replica_type == ReplicaType::MEMORY ||
                   replica_type == ReplicaType::VSEGMENT;
        }
        if (replica_type == ReplicaType::ALL) {
            return (replica.is_memory_replica() &&
                    !replica.has_invalid_mem_handle()) ||
                   (replica.is_nof_replica() &&
                    !replica.has_invalid_nof_handle()) ||
                   replica.is_dfs_replica();
        }
        if (replica_type == ReplicaType::MEMORY) {
            return replica.is_memory_replica() &&
                   !replica.has_invalid_mem_handle();
        }
        if (replica_type == ReplicaType::NOF_SSD) {
            return replica.is_nof_replica() &&
                   !replica.has_invalid_nof_handle();
        }
        return replica.type() == replica_type;
    };

    // A successful End removes the processing marker. Treat a retry as a
    // no-op only when every replica targeted by that End is already COMPLETE.
    // In particular, a promotion-owned PROCESSING replica keeps this check
    // from accepting a MEMORY/ALL End and is never modified here.
    if (!accessor.InProcessing()) {
        bool has_target_replica = false;
        bool all_target_replicas_complete = true;
        for (const auto& replica : metadata.GetAllReplicas()) {
            if (!is_target_replica(replica)) {
                continue;
            }
            has_target_replica = true;
            if (!replica.is_completed()) {
                all_target_replicas_complete = false;
                break;
            }
        }
        if (has_target_replica && all_target_replicas_complete) {
            return {};
        }
        LOG(ERROR) << "key=" << key << ", error=no_primary_write_in_progress";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    // Commit each targeted logical reservation before publishing its replica
    // as COMPLETE. The descriptor carries its own operation id for batched
    // writes; the RPC argument is a fallback for older descriptors.
    vsegment::VSegmentCommitBatch vsegment_commits;
    ErrorCode vsegment_commit_error = ErrorCode::OK;
    metadata.VisitReplicas(
        [&is_target_replica](const Replica& replica) {
            return replica.is_vsegment_replica() && replica.is_processing() &&
                   is_target_replica(replica);
        },
        [&](const Replica& replica) {
            if (vsegment_commit_error != ErrorCode::OK) return;
            const auto& descriptor = replica.get_vsegment_descriptor();
            const auto& effective_id = descriptor.operation_id.empty()
                                           ? operation_id
                                           : descriptor.operation_id;
            if (effective_id.empty() || !vsegment_service_) {
                vsegment_commit_error = ErrorCode::INVALID_PARAMS;
                return;
            }
            vsegment_commit_error = vsegment_service_->CommitPutOwned(
                descriptor, effective_id,
                object_id.tenant_id.MakeScopedKey(key), &vsegment_commits);
            if (vsegment_commit_error == ErrorCode::TASK_PENDING_LIMIT_EXCEEDED &&
                vsegment_commits.Flush() == ErrorCode::OK) {
                vsegment_commit_error = vsegment_service_->CommitPutOwned(
                    descriptor, effective_id,
                    object_id.tenant_id.MakeScopedKey(key), &vsegment_commits);
            }
        });
    vsegment_commits.DiscardDurabilityWaits();
    if (vsegment_commit_error != ErrorCode::OK) {
        return tl::make_unexpected(vsegment_commit_error);
    }

    // Do not take the group lock while holding the object shard. The guard
    // arbitrates timeout admission without holding a lock across durability.
    auto import_write = weight_manager_.TrackImportWrite(object_id.tenant_id,
                                                         metadata.group_id);
    if (!import_write) {
        return tl::make_unexpected(import_write.error());
    }

    if (bucket_allocator_ != nullptr && (replica_type == ReplicaType::ALL ||
                                         replica_type == ReplicaType::DFS)) {
        std::vector<DistributedFSDescriptor> pending_descriptors;
        metadata.VisitReplicas(
            [](const Replica& replica) {
                return replica.is_dfs_replica() && replica.is_processing();
            },
            [&pending_descriptors](const Replica& replica) {
                pending_descriptors.push_back(replica.get_dfs_descriptor());
            });
        for (const auto& descriptor : pending_descriptors) {
            if (!bucket_allocator_->MarkCommitted(key, descriptor)) {
                LOG(ERROR) << "key=" << key
                           << ", error=dfs_runtime_commit_failed, bucket_id="
                           << descriptor.shard_idx;
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
            }
        }
    }

    const bool had_completed_replica =
        metadata.HasReplica(&Replica::fn_is_completed);
    const bool advanced_primary_write =
        metadata.HasReplica([&is_target_replica](const Replica& replica) {
            return replica.is_processing() && is_target_replica(replica);
        });
    bool completed_pending_replica = false;
    metadata.VisitReplicas(
        [&is_target_replica](const Replica& replica) {
            return replica.is_processing() && is_target_replica(replica);
        },
        [this, &key, &metadata, &completed_pending_replica](Replica& replica) {
            if (replica.is_processing() &&
                metadata.PendingSoftPinOwnsReplica(replica.id())) {
                completed_pending_replica = true;
            }
            metadata.CommitSessionTag(replica.id());
            replica.mark_complete();
            if (replica.is_dfs_replica() && dfs_allocator_) {
                const auto& desc = replica.get_dfs_descriptor();
                dfs_allocator_->UpdateAccess(key, desc);
            }
        });

    if (!had_completed_replica && completed_pending_replica &&
        metadata.HasReplica(&Replica::fn_is_completed)) {
        const auto soft_pin_result =
            metadata.CommitPendingSoftPin(std::chrono::system_clock::now());
        ApplySoftPinEvaluation(metadata, soft_pin_result);
    }

    if (object_meta.object_checksum.has_value() ||
        replica_type == ReplicaType::ALL ||
        replica_type == ReplicaType::MEMORY ||
        replica_type == ReplicaType::NOF_SSD) {
        metadata.MutablePart(0).SetChecksum(object_meta.object_checksum);
    }

    if (replica_type != ReplicaType::DFS && enable_offload_ &&
        !offload_on_evict_ && !metadata.HasReplica([](const Replica& replica) {
            return replica.is_dfs_replica() && replica.is_processing();
        })) {
        auto& state = accessor.GetState();
        // One marker covers every mirror pushed below, so the mirrors are
        // collected before the marker is recorded.
        std::optional<ReplicaID> source_id;
        std::vector<UUID> mirror_clients;
        metadata.VisitReplicas(
            [](const Replica& replica) {
                return replica.is_completed() && replica.is_memory_replica();
            },
            [this, &object_id, &source_id, &mirror_clients](Replica& replica) {
                auto result =
                    PushOffloadingQueue(object_id, replica, &mirror_clients);
                if (result && !source_id.has_value()) {
                    replica.inc_refcnt();
                    source_id = replica.id();
                }
            });
        if (source_id.has_value() && !state.offloading_task) {
            state.offloading_task =
                OffloadingTask{*source_id, std::chrono::system_clock::now(),
                               std::move(mirror_clients)};
        }
    }

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    SyncCacheTotalAccounting(metadata);
    // TODO: add inc_nof_cache_nums() (ranhaojia)
    metadata.GrantReadLease(std::chrono::milliseconds::zero());
    PublishKvStored(key, metadata, object_id.tenant_id);

    // TODO(P99-diag): Temporarily disable PutEnd IO-pattern recording. The
    // synchronous feedback path runs every 64 accesses and was observed to
    // hold the metadata shard write lock for up to milliseconds. Restore this
    // after moving RecordAccess/ScheduleAdmission outside the shard lock or
    // making feedback processing asynchronous.
#if 0
    if (io_pattern_runtime_) {
        io_pattern_runtime_->RecordAccess(
            key, {.object = {object_id.tenant_id, key},
                  .observed_at_ns = static_cast<uint64_t>(
                      std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::steady_clock::now().time_since_epoch())
                          .count()),
                  .block_size = metadata.size,
                  .tier = replica_type == ReplicaType::MEMORY
                              ? io_pattern::CacheTier::kL1Host
                              : (replica_type == ReplicaType::LOCAL_DISK
                                     ? io_pattern::CacheTier::kLocalDisk
                                     : io_pattern::CacheTier::kL3NofSsd),
                  .operation = io_pattern::IoOperation::kPut,
                  .is_hit = true,
                  .write_batch_size = write_batch_size,
                  .overwrite = overwrite});
        if (replica_type != ReplicaType::MEMORY) {
            io_pattern_runtime_->ScheduleAdmission(
                {object_id.tenant_id, key}, io_pattern::CacheTier::kL1Host);
        }
    }
#endif
    (void)write_batch_size;
    (void)overwrite;

    if (enable_oplog_ && ordered_oplog_writer_) {
        SpDiag::PerfPoint pt_object_oplog(PerfKey::MASTER_PUT_END_OBJECT_OPLOG,
                                          SpDiag::PerfLevel::MODULE);
        pt_object_oplog.Start();
        std::string payload = SerializeMetadataForOpLog(metadata);
        auto result = AppendOpLogVisibleBeforeDurable(
            OpType::PUT_END, object_id.tenant_id.value(), key, payload);
        pt_object_oplog.End(result ? 0 : -1);
        if (!result) {
            LOG(WARNING) << "PutEnd: OpLog queue failed for key=" << key
                         << ", err=" << static_cast<int>(result.error());
        }
    }
    if (advanced_primary_write) {
        import_write->Accept();
    }
    return {};
}

auto MasterService::AddReplica(const UUID& client_id, const std::string& key,
                               const TenantId& tenant_id, Replica& replica)
    -> tl::expected<bool, ErrorCode> {
    const auto client_liveness = FindClientRecord(client_id);
    auto retaining_guard = client_liveness
                               ? client_liveness->TryAcquireRetainingGuard()
                               : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    replica.bindClientLiveness(client_liveness);
    return AddReplicaForRetainedClient(client_id, key, tenant_id, replica);
}

auto MasterService::AddReplicaForRetainedClient(const UUID& client_id,
                                                const std::string& key,
                                                const TenantId& tenant_id,
                                                Replica& replica)
    -> tl::expected<bool, ErrorCode> {
    assert(tenant_id.IsValid());
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    // Held for the whole request, so the policy's control plane cannot revoke
    // the tenant between the admission and the write.
    std::unique_ptr<AdmissionToken> admission;
    if (policy_ != nullptr) {
        auto admitted = policy_->AdmitWrite(normalized_tenant);
        if (!admitted) {
            return tl::make_unexpected(admitted.error());
        }
        admission = std::move(admitted.value());
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    // Same admission rule as NotifyOffloadSuccess's existing-object path,
    // checked inside the same shared-lock section as the write below: a disk
    // replica may only be registered for a client whose LOCAL_DISK segment
    // entry still exists, so a registration cannot land after a concurrent
    // UnmountLocalDiskSegment's sweep and survive as a stale owner. Scoped
    // to enable_offload_, where the segment registry exists and a
    // deregistration can race; with the subsystem off both the mount and
    // unmount RPCs refuse, so there is nothing to check against and no race
    // to close.
    if (enable_offload_ && !HasMountedLocalDiskSegment(client_id)) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    const ObjectIdentity object_id{normalized_tenant, key};
    MetadataAccessorRW accessor(this, object_id,
                                MetadataAccessorRW::CreateKey{});
    if (!accessor.IsPublished()) {
        accessor.Publish(
            client_id, std::chrono::system_clock::now(),
            replica.get_descriptor().get_local_disk_descriptor().object_size,
            std::vector<Replica>{}, std::nullopt, false,
            ObjectDataType::UNKNOWN, std::string{}, object_id.tenant_id, key);
    }
    auto& metadata = accessor.Get();
    const auto previous_kv_media = KvMediaSnapshot(metadata);
    if (replica.type() != ReplicaType::LOCAL_DISK) {
        LOG(ERROR) << "Invalid replica type: " << replica.type()
                   << ". Expected ReplicaType::LOCAL_DISK.";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const bool replacing_existing =
        metadata.HasReplica(&Replica::fn_is_local_disk_replica);

    // Build OPLog payload BEFORE moving the replica, so that
    // get_descriptor() is still valid on `replica`.
    std::string oplog_payload;
    bool oplog_required = false;
    if (enable_oplog_ && ordered_oplog_writer_) {
        std::vector<Replica::Descriptor> post;
        for (const auto& existing : metadata.GetAllReplicas()) {
            if (existing.status() != ReplicaStatus::COMPLETE) continue;
            if (replacing_existing &&
                existing.type() == ReplicaType::LOCAL_DISK &&
                existing.get_descriptor()
                        .get_local_disk_descriptor()
                        .client_id == client_id) {
                // Substitute with the updated descriptor.
                Replica::Descriptor updated = existing.get_descriptor();
                updated.get_local_disk_descriptor().transport_endpoint =
                    replica.get_descriptor()
                        .get_local_disk_descriptor()
                        .transport_endpoint;
                updated.get_local_disk_descriptor().object_size =
                    replica.get_descriptor()
                        .get_local_disk_descriptor()
                        .object_size;
                post.push_back(std::move(updated));
            } else {
                post.push_back(existing.get_descriptor());
            }
        }
        if (!replacing_existing) {
            // The new LOCAL_DISK replica is COMPLETE upon AddReplica.
            post.push_back(replica.get_descriptor());
        }

        auto persist_result = AppendOpLogVisibleBeforeDurable(
            OpType::PUT_END, object_id.tenant_id.value(), key,
            SerializeMetadataForOpLogFromReplicaDescriptors(metadata, post));
        if (!persist_result) {
            return tl::make_unexpected(persist_result.error());
        }
    }

    // Step 1: Update metadata first for LOCAL_DISK replicas.
    // This ensures the replica is registered immediately, even when the
    // OPLog writer is backed up (e.g. during standby promotion recovery).
    // LOCAL_DISK is a secondary replica type: losing its OPLog entry is
    // recoverable because the client will re-register on remount.
    if (!replacing_existing) {
        std::vector<Replica> replicas;
        replicas.emplace_back(std::move(replica));
        metadata.AddReplicas(std::move(replicas));
        SyncCacheTotalAccounting(metadata);
        SyncKvObjectState(key, metadata, object_id.tenant_id,
                          previous_kv_media);
        return true;
    }

    // Replace-existing path: update the existing LOCAL_DISK replica.
    // Record every LOCAL_DISK replica per owning client. First try to refresh
    // an existing replica owned by THIS client (idempotent re-offload or
    // restart re-registration only changes the endpoint/size). If this client
    // has no LOCAL_DISK replica yet, append a new one so that a key offloaded
    // by multiple nodes keeps one LOCAL_DISK replica per node.
    size_t updated = metadata.VisitReplicas(
        [client_id](const Replica& rep) {
            return rep.type() == ReplicaType::LOCAL_DISK &&
                   rep.get_descriptor().get_local_disk_descriptor().client_id ==
                       client_id;
        },
        [&replica](Replica& rep) {
            const auto desc =
                replica.get_descriptor().get_local_disk_descriptor();
            rep.update_local_disk_location(desc.transport_endpoint,
                                           desc.object_size);
        });

    if (updated == 0) {
        std::vector<Replica> replicas;
        replicas.emplace_back(std::move(replica));
        metadata.AddReplicas(std::move(replicas));
    }

    // Best-effort OPLog write for replace-existing path.
    if (oplog_required) {
        auto persist_result = AppendOpLogVisibleBeforeDurable(
            OpType::PUT_END, object_id.tenant_id.value(), key, oplog_payload);
        if (!persist_result) {
            LOG(WARNING) << "AddReplica: OpLog skipped for local_disk"
                         << " (metadata already updated), key=" << key
                         << ", err="
                         << static_cast<int>(persist_result.error());
        }
    }
    return false;
}

auto MasterService::PutRevoke(const UUID& client_id, const std::string& key,
                              const TenantId& tenant_id,
                              ReplicaType replica_type,
                              const std::string& operation_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
#ifdef STORE_USE_ETCD
    if (!OwnsSlot(cvm::KeySlot(object_id.tenant_id, object_id.user_key)))
        return tl::make_unexpected(ErrorCode::SLOT_NOT_OWNED);
#endif
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        MC_LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    const auto previous_kv_media = KvMediaSnapshot(metadata);
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutRevoke key "
                   << key << ", was PutStart-ed by " << metadata.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }
    ErrorCode vsegment_abort_error = ErrorCode::OK;
    metadata.VisitReplicas(
        &Replica::fn_is_vsegment_replica, [&](const Replica& replica) {
            if (vsegment_abort_error != ErrorCode::OK) return;
            const auto& descriptor = replica.get_vsegment_descriptor();
            const std::string effective_operation_id =
                descriptor.operation_id.empty() ? operation_id
                                                : descriptor.operation_id;
            if (effective_operation_id.empty() || !vsegment_service_) {
                vsegment_abort_error = ErrorCode::INVALID_PARAMS;
                return;
            }
            vsegment_abort_error = vsegment_service_->AbortPutOwned(
                descriptor.partition_id, descriptor.vsegment_id,
                effective_operation_id);
            // Same-size Upsert reuses a committed vsegment allocation and
            // marks its replica PROCESSING without creating a new logical
            // reservation. Abort therefore returns INVALID_WRITE. Let the
            // normal replica-erasure path below release that committed
            // allocation instead of leaving the key stuck in PROCESSING.
            if (vsegment_abort_error == ErrorCode::INVALID_WRITE &&
                replica.is_processing()) {
                vsegment_abort_error = ErrorCode::OK;
            }
        });
    if (vsegment_abort_error != ErrorCode::OK)
        return tl::make_unexpected(vsegment_abort_error);

    if (!accessor.InProcessing()) {
        LOG(ERROR) << "key=" << key << ", error=no_primary_write_in_progress";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    auto processing_rep =
        metadata.GetFirstReplica([replica_type](const Replica& replica) {
            if (replica.is_vsegment_replica() &&
                (replica_type == ReplicaType::ALL ||
                 replica_type == ReplicaType::MEMORY ||
                 replica_type == ReplicaType::VSEGMENT)) {
                return !replica.is_processing();
            }
            if (replica_type == ReplicaType::ALL) {
                return (replica.is_memory_replica() ||
                        replica.is_nof_replica() || replica.is_dfs_replica()) &&
                       !replica.is_processing();
            }
            return replica.type() == replica_type && !replica.is_processing();
        });
    if (processing_rep != nullptr) {
        LOG(ERROR) << "key=" << key << ", status=" << processing_rep->status()
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    auto target_pred = [replica_type](const Replica& r) {
        if (!r.is_processing()) {
            return false;
        }
        if (r.is_vsegment_replica()) {
            return replica_type == ReplicaType::ALL ||
                   replica_type == ReplicaType::MEMORY ||
                   replica_type == ReplicaType::VSEGMENT;
        }
        if (replica_type == ReplicaType::ALL) {
            return r.is_memory_replica() || r.is_nof_replica() ||
                   r.is_dfs_replica();
        }
        return r.type() == replica_type;
    };

    if (enable_oplog_ && ordered_oplog_writer_) {
        auto remaining =
            BuildRemainingReplicaDescriptors(metadata, target_pred);
        std::vector<ReplicaID> removed_ids;
        auto reservation = ReserveBatchOpLogSlot();
        if (!reservation) {
            return tl::make_unexpected(reservation.error());
        }
        metadata.VisitReplicas(target_pred, [&removed_ids](Replica& r) {
            removed_ids.push_back(r.id());
            r.mark_removed();
        });
        metadata.ClearPendingSoftPinIfNoViableReplica();
        metadata.ClearSessionTagIfNoViableReplica();

        tl::expected<OpLogEntry, ErrorCode> persist_result;
        if (remaining.empty()) {
            persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::REMOVE,
                tenant_id.value(), key, {},
                [this, removed_ids = std::move(removed_ids)](
                    const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(durable_entry,
                                                        removed_ids);
                });
        } else {
            persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::PUT_END,
                tenant_id.value(), key,
                SerializeMetadataForOpLogFromReplicaDescriptors(metadata,
                                                                remaining),
                [this, removed_ids = std::move(removed_ids)](
                    const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(durable_entry,
                                                        removed_ids);
                });
        }
        if (!persist_result) {
            return tl::make_unexpected(persist_result.error());
        }
        return {};
    }

    EraseReplicasWithCacheTotalAccounting(metadata, target_pred);
    metadata.ClearPendingSoftPinIfNoViableReplica();
    metadata.ClearSessionTagIfNoViableReplica();
    if (!metadata.IsValid()) {
        accessor.Erase();
        return {};
    }

    SyncKvObjectState(key, metadata, object_id.tenant_id, previous_kv_media);

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    return {};
}

auto MasterService::PutEnd(const UUID& client_id, const ObjectMeta& object_meta,
                           const TenantId& tenant_id, ReplicaType replica_type,
                           const std::string& operation_id)
    -> tl::expected<void, ErrorCode> {
    return PutEndInternal(client_id, object_meta, tenant_id, replica_type,
                          /*write_batch_size=*/1, /*overwrite=*/false,
                          operation_id);
}

auto MasterService::PutEnd(const UUID& client_id, const std::string& key,
                           const TenantId& tenant_id, ReplicaType replica_type,
                           const std::string& operation_id)
    -> tl::expected<void, ErrorCode> {
    return PutEnd(client_id, ObjectMeta{key, std::nullopt}, tenant_id,
                  replica_type, operation_id);
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutEnd(
    const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
    const TenantId& tenant_id, ReplicaType replica_type) {
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_metas.size());
    for (const auto& object_meta : object_metas) {
        results.emplace_back(PutEndInternal(
            client_id, object_meta, tenant_id, replica_type,
            static_cast<uint32_t>(std::min<size_t>(
                object_metas.size(), std::numeric_limits<uint32_t>::max())),
            /*overwrite=*/false));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutRevoke(
    const UUID& client_id, const std::vector<std::string>& keys,
    const TenantId& tenant_id, ReplicaType replica_type) {
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(
            PutRevoke(client_id, key, tenant_id, replica_type));
    }
    return results;
}

// UpsertStart — insert-or-update entry point.
//
// Three-way dispatch depending on key state:
//   Case A: key does not exist  → allocate new buffers (same as PutStart)
//   Case B: key exists, same size, no readers → in-place update
//   Case C: key exists, different size, bucket DFS, or leased memory → allocate
//           new buffers
//
// Before reaching Case B/C the function runs safety checks and may preempt
// an in-progress Put/Upsert on the same key.  Preempted PROCESSING replicas
// are moved to discarded_replicas_ for delayed release (the previous writer
// may still be performing RDMA writes to those buffers).
//
// Note: during Case B the key is temporarily unreadable (all replicas are
// PROCESSING).  Readers will get REPLICA_IS_NOT_READY until UpsertEnd.
auto MasterService::UpsertStart(const UUID& client_id, const std::string& key,
                                const TenantId& tenant_id,
                                const uint64_t slice_length,
                                const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    auto plan = PrepareWriteStart(client_id, key, object_id.tenant_id,
                                  slice_length, config, "upsert_start");
    if (!plan) {
        return tl::make_unexpected(plan.error());
    }
    std::optional<std::unique_lock<std::mutex>> weight_group_operation_lock;
    if (!plan->group_id.empty() &&
        weight_manager_.IsManagedGroup(plan->group_id)) {
        weight_group_operation_lock.emplace(
            weight_manager_.LockGroup(object_id.tenant_id, plan->group_id));
        if (!weight_manager_.AllowsGroupMemberMutation(plan->group_id)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }
    const std::string& writer_host_id = plan->writer_host_id;
    const std::string& group_id = plan->group_id;
    const std::string& residency_affinity_id = plan->residency_affinity_id;
    const ResolvedSoftPinRequest& soft_pin_request = plan->soft_pin_request;

    ReplicateConfig allocation_config = config;
    std::string allocation_group_id = group_id;
    std::string allocation_session_id;
    std::string allocation_residency_affinity_id = residency_affinity_id;
    std::optional<std::chrono::system_clock::time_point>
        allocation_committed_soft_pin_timeout;
    auto admit = [&](bool* dfs_allocation_failed)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
        auto now = std::chrono::system_clock::now();
        {
            // --- Lock acquisition ---
            std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
            std::shared_lock<std::shared_mutex> shared_lock(
                master_state_mutex_);
            auto retaining_clients = GetRetainingClientIdsLocked();
            client_lock.unlock();
            // The whole read-modify-write runs under the key's lock, held
            // whether or not the key holds an object, so a replacement is
            // published under the same lock that took the old object down
            // and no other writer of the key lands in between.
            MetadataAccessorRW accessor(
                this, object_id,
                MetadataAccessorRW::CreateKey{.cleanup_invalid = false});
            if (auto admitted = accessor.Grow(0); !admitted) {
                return tl::make_unexpected(admitted.error());
            }

            // Group membership is immutable while an object exists, including
            // when stale replicas need cleanup.
            if (accessor.IsPublished() && config.group_ids.has_value() &&
                accessor.Get().group_id != group_id) {
                LOG(ERROR) << "key=" << key
                           << ", error=group_membership_is_immutable";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            if (accessor.IsPublished() && config.residency_affinity_ids &&
                accessor.Get().residency_affinity_id != residency_affinity_id) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            // Managed writes must acquire the group lock before the shard
            // lock. Require an explicit group before any stale cleanup.
            if (accessor.IsPublished() && !weight_group_operation_lock &&
                weight_manager_.IsManagedGroup(accessor.Get().group_id)) {
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
            // Cleanup follows the membership checks so rejected writes leave
            // managed objects and their replicas unchanged.
            if (accessor.IsPublished()) {
                CleanupInvalidMemoryReplicas(accessor);
            }
            // --- Step 0: stale handle cleanup ---
            if (auto cleaned = CleanupStaleBeforeWrite(
                    "UpsertStart(stale cleanup)", accessor, retaining_clients);
                !cleaned) {
                return tl::make_unexpected(cleaned.error());
            }

            // --- Step 1: safety checks and preemption (only if key exists) ---
            if (accessor.IsPublished()) {
                auto& metadata = accessor.Get();
                auto& state = accessor.GetState();

                // Reject if a Copy/Move task is actively reading this key's
                // replicas.
                if (state.replication_task) {
                    LOG(INFO) << "key=" << key
                              << ", error=object_has_replication_task";
                    return tl::make_unexpected(
                        ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                }

                if (state.promotion_task) {
                    LOG(INFO)
                        << "key=" << key << ", error=object_has_promotion_task";
                    return tl::make_unexpected(
                        ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                }

                // An in-place Upsert returns and overwrites the existing
                // buffers, so every completed Client-owned target must still
                // be serving before any queue or metadata state is changed.
                if (metadata.TotalLength() == slice_length &&
                    metadata.HasReplica([](const Replica& replica) {
                        if (!replica.is_completed() ||
                            (!replica.is_memory_replica() &&
                             !replica.is_local_disk_replica())) {
                            return false;
                        }
                        Replica::Descriptor descriptor;
                        return !replica.getDescriptorIfAvailable(descriptor);
                    })) {
                    return tl::make_unexpected(
                        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                }

                // Cancel a still-queued offload so the upsert can take the key
                // over. Once a store worker owns the task it is reading the
                // source buffer for its SSD write, so the upsert waits for
                // NotifyOffloadSuccess to clear the marker instead.
                if (!CancelQueuedOffloadTask(accessor.Get(),
                                             accessor.GetState(), object_id)) {
                    LOG(INFO) << "key=" << key
                              << ", error=object_has_offloading_task";
                    return tl::make_unexpected(
                        ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                }

                // Preempt an in-progress Put/Upsert on the same key.  The
                // previous writer's PROCESSING replicas are moved to
                // discarded_replicas_ with a TTL so they are not freed while
                // the old writer may still be doing RDMA writes.  Unlike
                // PutStart (which only preempts after a timeout), UpsertStart
                // preempts immediately.
                if (state.is_processing) {
                    auto processing_replicas =
                        metadata.PopReplicas(&Replica::fn_is_processing);
                    metadata.ClearPendingSoftPinAction();
                    metadata.pending_session_tag.reset();
                    if (!processing_replicas.empty()) {
                        FreeDfsReplicas(key, processing_replicas);
                        std::lock_guard lock(discarded_replicas_mutex_);
                        discarded_replicas_.emplace_back(
                            std::move(processing_replicas),
                            now + put_start_release_timeout_sec_);
                    }
                    state.is_processing = false;

                    // If no COMPLETE replicas survive the preemption, this key
                    // effectively does not exist — fall through to Case A.
                    if (!metadata.HasReplica(&Replica::fn_is_completed)) {
                        allocation_session_id = metadata.session_id;
                        allocation_residency_affinity_id =
                            metadata.residency_affinity_id;
                        allocation_committed_soft_pin_timeout =
                            metadata.GetCommittedSoftPinTimeout();
                        if (allocation_committed_soft_pin_timeout &&
                            *allocation_committed_soft_pin_timeout <= now) {
                            allocation_committed_soft_pin_timeout.reset();
                        }
                        accessor.Erase();
                    }
                }
            }

            // --- Case A: key does not exist (or was erased above) ---
            // Allocate fresh buffers, identical to PutStart. Objects are always
            // routed by hash(tenant, key); group_id does not affect routing.
            if (!accessor.IsPublished()) {
                VLOG(1) << "key=" << key << ", action=upsert_start_case_a";
                auto result = AllocateAndInsertMetadata(
                    accessor, client_id, slice_length, allocation_config,
                    writer_host_id, allocation_group_id,
                    allocation_residency_affinity_id, now, soft_pin_request,
                    allocation_committed_soft_pin_timeout,
                    dfs_allocation_failed);
                if (result) {
                    accessor.Get().session_id = allocation_session_id;
                }
                return result;
            } else {
                // --- Step 2: key exists with COMPLETE replicas → Case B or C
                // ---
                auto& metadata = accessor.Get();

                // Reject if any reader holds a reference (refcnt > 0).
                // Overwriting a buffer that an RDMA read is streaming from
                // would cause data corruption. The client should retry after
                // readers finish.
                if (metadata.HasReplica(&Replica::fn_is_busy)) {
                    LOG(INFO) << "key=" << key << ", error=object_replica_busy";
                    return tl::make_unexpected(ErrorCode::OBJECT_REPLICA_BUSY);
                }

                bool restore_missing_bucket_dfs = false;
                if (metadata.TotalLength() == slice_length) {
                    // Validate same-size DFS topology before changing storage.
                    const size_t existing_dfs_replicas =
                        metadata.CountReplicas(&Replica::fn_is_dfs_replica);
                    if (config.dfs_replica_num > 0 ||
                        existing_dfs_replicas > 0) {
                        const size_t existing_memory_replicas =
                            metadata.CountReplicas(
                                &Replica::fn_is_memory_replica);
                        const size_t existing_nof_replicas =
                            metadata.CountReplicas(&Replica::fn_is_nof_replica);
                        restore_missing_bucket_dfs =
                            bucket_allocator_ != nullptr &&
                            existing_memory_replicas == config.replica_num &&
                            existing_nof_replicas == config.nof_replica_num &&
                            existing_dfs_replicas == 0 &&
                            config.dfs_replica_num > 0;
                        if (existing_memory_replicas != config.replica_num ||
                            existing_nof_replicas != config.nof_replica_num ||
                            (existing_dfs_replicas != config.dfs_replica_num &&
                             !restore_missing_bucket_dfs)) {
                            LOG(ERROR)
                                << "key=" << key
                                << ", error=dfs_upsert_topology_mismatch"
                                << ", existing_memory="
                                << existing_memory_replicas
                                << ", requested_memory=" << config.replica_num
                                << ", existing_nof=" << existing_nof_replicas
                                << ", requested_nof=" << config.nof_replica_num
                                << ", existing_dfs=" << existing_dfs_replicas
                                << ", requested_dfs=" << config.dfs_replica_num;
                            return tl::make_unexpected(
                                ErrorCode::INVALID_PARAMS);
                        }
                    }
                }

                const bool has_read_lease = !metadata.IsLeaseExpired(now);
                if (has_read_lease &&
                    metadata.HasReplica([](const Replica& replica) {
                        return !replica.is_memory_replica();
                    })) {
                    // File-backed reads can resolve storage by key, so they
                    // cannot retain an old version across replacement.
                    return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
                }

                const bool has_bucket_dfs_replica =
                    bucket_allocator_ != nullptr &&
                    metadata.HasReplica(&Replica::fn_is_dfs_replica);
                if (metadata.TotalLength() == slice_length && !has_read_lease &&
                    !has_bucket_dfs_replica && !restore_missing_bucket_dfs) {
                    metadata.client_id = client_id;
                    metadata.put_start_time = now;

                    const auto previous_kv_media = KvMediaSnapshot(metadata);

                    // Mark COMPLETE → PROCESSING so readers won't see stale
                    // data mid-transfer.  The key becomes unreadable until
                    // UpsertEnd.
                    std::unordered_set<ReplicaID> eligible_replica_ids;
                    metadata.VisitReplicas(
                        &Replica::fn_is_completed,
                        [&eligible_replica_ids](Replica& replica) {
                            eligible_replica_ids.insert(replica.id());
                            replica.mark_processing();
                        });
                    metadata.BeginSessionTag(config.SessionId(),
                                             eligible_replica_ids);
                    metadata.BeginSoftPinAction(
                        soft_pin_request, std::move(eligible_replica_ids));
                    SyncCacheTotalAccounting(metadata);
                    SyncKvObjectState(key, metadata, object_id.tenant_id,
                                      previous_kv_media);

                    accessor.GetState().is_processing = true;

                    // Return the existing descriptors — same buffer addresses
                    // as before.
                    std::vector<Replica::Descriptor> replica_list;
                    const auto& all_replicas = metadata.GetAllReplicas();
                    replica_list.reserve(all_replicas.size());
                    for (const auto& replica : all_replicas) {
                        replica_list.emplace_back(replica.get_descriptor());
                    }

                    VLOG(1) << "key=" << key
                            << ", action=upsert_start_case_b_inplace";
                    return replica_list;
                }

                // --- Case C: different size, bucket DFS (including a replica
                // lost to bucket eviction), or active readers — reallocate
                // --- Old buffers cannot be reused.  Move them to
                // discarded_replicas_ for delayed release (readers may still
                // hold descriptors without refcnt), then allocate fresh buffers
                // at the new size.
                //
                // Preserve hard_pin and soft_pin from the old metadata so that
                // eviction protection survives a size-changing upsert (RFC
                // §2.2.2).
                allocation_config = config;
                allocation_config.with_hard_pin =
                    allocation_config.with_hard_pin || metadata.IsHardPinned();
                allocation_committed_soft_pin_timeout =
                    metadata.GetCommittedSoftPinTimeout();
                if (allocation_committed_soft_pin_timeout &&
                    *allocation_committed_soft_pin_timeout <= now) {
                    allocation_committed_soft_pin_timeout.reset();
                }

                allocation_group_id = metadata.group_id;
                allocation_session_id = metadata.session_id;
                allocation_residency_affinity_id =
                    metadata.residency_affinity_id;
                const auto previous_kv_media = KvMediaSnapshot(metadata);
                // A leased memory snapshot still references the old replica
                // descriptors. Allocate the replacement before removing the
                // old metadata so an allocation failure leaves the old object
                // readable instead of turning it into OBJECT_NOT_FOUND.
                std::optional<std::vector<Replica>> replacement_replicas;
                if (has_read_lease) {
                    auto grown = accessor.Grow(SaturatingMultiply(
                        slice_length, allocation_config.replica_num));
                    if (!grown) {
                        return tl::make_unexpected(grown.error());
                    }
                    auto allocation_result =
                        AllocateReplicas(key, slice_length, allocation_config,
                                         writer_host_id, dfs_allocation_failed);
                    if (!allocation_result) {
                        return tl::make_unexpected(allocation_result.error());
                    }
                    replacement_replicas.emplace(
                        std::move(allocation_result.value()));
                }

                // A query granted before this write lock can retain its
                // descriptor for a full read TTL. Keep the old allocation for
                // at least that long as well as the writer grace period.
                const auto release_at =
                    std::chrono::system_clock::now() +
                    std::max(
                        std::chrono::milliseconds(default_kv_lease_ttl_),
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            put_start_release_timeout_sec_));
                // What the old object charges stays on the key until the
                // replacement's write settles.
                accessor.BeforeReplace();
                auto old_replicas =
                    PopReplicasWithCacheTotalAccounting(metadata);
                if (!old_replicas.empty()) {
                    FreeDfsReplicas(key, old_replicas);
                    std::lock_guard lock(discarded_replicas_mutex_);
                    discarded_replicas_.emplace_back(std::move(old_replicas),
                                                     release_at);
                }
                // The replacement is published under this same lock, so no
                // reader or writer finds the key between the two objects.
                // `metadata` is not used past this point.
                accessor.Erase(EraseMode::kReplace, previous_kv_media);

                VLOG(1) << "key=" << key
                        << ", action=upsert_start_case_c_reallocate";
                tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
                    allocate_result =
                        replacement_replicas.has_value()
                            ? InsertMetadata(
                                  accessor, client_id, slice_length,
                                  allocation_config, allocation_group_id,
                                  allocation_residency_affinity_id, now,
                                  soft_pin_request,
                                  std::move(*replacement_replicas),
                                  allocation_committed_soft_pin_timeout)
                            : AllocateAndInsertMetadata(
                                  accessor, client_id, slice_length,
                                  allocation_config, writer_host_id,
                                  allocation_group_id,
                                  allocation_residency_affinity_id, now,
                                  soft_pin_request,
                                  allocation_committed_soft_pin_timeout,
                                  dfs_allocation_failed);
                if (allocate_result && accessor.IsPublished()) {
                    accessor.Get().session_id = allocation_session_id;
                }
                return allocate_result;
            }
        }
    };

    // Over-quota is rejected outright. Making room is the namespace policy's
    // maintenance pass, not the write path's: this write fails, the evictor
    // builds headroom, the next one succeeds.
    auto result = AdmitWithDfsRecovery(config, admit);
    if (result && weight_group_operation_lock) {
        weight_manager_.RecordImportWrite(object_id.tenant_id, group_id);
    }
    return result;
}

auto MasterService::UpsertEnd(const UUID& client_id,
                              const ObjectMeta& object_meta,
                              const TenantId& tenant_id,
                              ReplicaType replica_type,
                              const std::string& operation_id)
    -> tl::expected<void, ErrorCode> {
    return PutEndInternal(client_id, object_meta, tenant_id, replica_type,
                          /*write_batch_size=*/1, /*overwrite=*/true,
                          operation_id);
}

auto MasterService::UpsertEnd(const UUID& client_id, const std::string& key,
                              const TenantId& tenant_id,
                              ReplicaType replica_type,
                              const std::string& operation_id)
    -> tl::expected<void, ErrorCode> {
    return UpsertEnd(client_id, ObjectMeta{key, std::nullopt}, tenant_id,
                     replica_type, operation_id);
}

auto MasterService::UpsertRevoke(const UUID& client_id, const std::string& key,
                                 const TenantId& tenant_id,
                                 ReplicaType replica_type,
                                 const std::string& operation_id)
    -> tl::expected<void, ErrorCode> {
    return PutRevoke(client_id, key, tenant_id, replica_type, operation_id);
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterService::BatchUpsertStart(const UUID& client_id,
                                const std::vector<std::string>& keys,
                                const TenantId& tenant_id,
                                const std::vector<uint64_t>& slice_lengths,
                                const ReplicateConfig& config) {
    assert(tenant_id.IsValid());
    if (keys.size() != slice_lengths.size()) {
        LOG(ERROR) << "BatchUpsertStart: keys.size()=" << keys.size()
                   << " != slice_lengths.size()=" << slice_lengths.size();
        return std::vector<
            tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (!config.ValidSessionIds(keys.size())) {
        return {keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS)};
    }
    if (!config.HasValidKeyHints(keys.size())) {
        LOG(ERROR)
            << "BatchUpsertStart: per-key hint count does not match keys";
        return std::vector<
            tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results;
    results.reserve(keys.size());
    ReplicateConfig key_config = config;
    for (size_t i = 0; i < keys.size(); ++i) {
        key_config.SetKeyIds(config, i);
        results.emplace_back(UpsertStart(client_id, keys[i], tenant_id,
                                         slice_lengths[i], key_config));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchUpsertEnd(
    const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
    const TenantId& tenant_id) {
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_metas.size());
    const auto batch_size = static_cast<uint32_t>(std::min<size_t>(
        object_metas.size(), std::numeric_limits<uint32_t>::max()));
    for (const auto& object_meta : object_metas) {
        results.emplace_back(PutEndInternal(client_id, object_meta, tenant_id,
                                            ReplicaType::ALL, batch_size,
                                            /*overwrite=*/true));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchUpsertRevoke(
    const UUID& client_id, const std::vector<std::string>& keys,
    const TenantId& tenant_id) {
    return BatchPutRevoke(client_id, keys, tenant_id);
}

auto MasterService::EvictDiskReplicaImpl(const UUID& client_id,
                                         const std::string& key,
                                         const TenantId& tenant_id,
                                         ReplicaType replica_type,
                                         const ReplicaID* expected_replica_id)
    -> tl::expected<bool, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key
                  << ", tenant_id=" << object_id.tenant_id.value()
                  << ", info=object_not_found_for_eviction";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (weight_manager_.IsManagedGroup(metadata.group_id)) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    const auto previous_kv_media = KvMediaSnapshot(metadata);

    const bool supports_dfs_condition = replica_type == ReplicaType::DFS &&
                                        expected_replica_id != nullptr &&
                                        use_kvcs_;
    if (replica_type != ReplicaType::DISK &&
        replica_type != ReplicaType::LOCAL_DISK && !supports_dfs_condition) {
        LOG(ERROR) << "key=" << key
                   << ", error=invalid_replica_type_for_eviction";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto target_pred = [this, replica_type, &client_id,
                        expected_replica_id](const Replica& r) {
        if (replica_type == ReplicaType::DISK) {
            return r.is_disk_replica();
        } else if (replica_type == ReplicaType::LOCAL_DISK) {
            return r.is_local_disk_replica() &&
                   r.get_descriptor().get_local_disk_descriptor().client_id ==
                       client_id;
        } else if (replica_type == ReplicaType::DFS) {
            return use_kvcs_ && expected_replica_id != nullptr &&
                   r.is_dfs_replica() && r.id() == *expected_replica_id;
        }
        return false;
    };

    if (expected_replica_id != nullptr &&
        !metadata.HasReplica([&](const Replica& r) {
            return target_pred(r) && r.id() == *expected_replica_id;
        })) {
        // The replica the caller failed on is no longer the current record:
        // it was replaced or already evicted. Leave whatever is there now
        // untouched and report the precondition miss.
        LOG(INFO) << "key=" << key << ", replica_id=" << *expected_replica_id
                  << ", info=eviction_precondition_miss";
        return false;
    }

    if (enable_oplog_ && ordered_oplog_writer_) {
        auto remaining =
            BuildRemainingReplicaDescriptors(metadata, target_pred);
        auto reservation = ReserveBatchOpLogSlot();
        if (!reservation) {
            return tl::make_unexpected(reservation.error());
        }
        std::vector<ReplicaID> removed_ids;
        metadata.VisitReplicas(target_pred, [&removed_ids](Replica& replica) {
            removed_ids.push_back(replica.id());
            replica.mark_removed();
        });

        tl::expected<OpLogEntry, ErrorCode> persist_result;
        if (remaining.empty()) {
            persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::REMOVE,
                metadata.tenant_id.value(), key, {},
                [this, removed_ids = std::move(removed_ids)](
                    const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(durable_entry,
                                                        removed_ids);
                });
        } else {
            persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::PUT_END,
                metadata.tenant_id.value(), key,
                SerializeMetadataForOpLogFromReplicaDescriptors(metadata,
                                                                remaining),
                [this, removed_ids = std::move(removed_ids)](
                    const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(durable_entry,
                                                        removed_ids);
                });
        }
        if (!persist_result) {
            return tl::make_unexpected(persist_result.error());
        }
        return true;
    }

    if (replica_type == ReplicaType::DISK || replica_type == ReplicaType::DFS) {
        EraseReplicasWithCacheTotalAccounting(metadata, target_pred);
    } else if (replica_type == ReplicaType::LOCAL_DISK) {
        EraseReplicasWithCacheTotalAccounting(metadata, target_pred);
    }

    SyncKvObjectState(key, metadata, object_id.tenant_id, previous_kv_media);
    if (!metadata.IsValid()) {
        accessor.Erase();
    }
    return true;
}

auto MasterService::EvictDiskReplica(const UUID& client_id,
                                     const std::string& key,
                                     const TenantId& tenant_id,
                                     ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    auto result =
        EvictDiskReplicaImpl(client_id, key, tenant_id, replica_type, nullptr);
    if (!result) {
        return tl::make_unexpected(result.error());
    }
    return {};
}

auto MasterService::EvictDiskReplicaIfCurrent(const UUID& client_id,
                                              const std::string& key,
                                              const TenantId& tenant_id,
                                              ReplicaType replica_type,
                                              ReplicaID expected_replica_id)
    -> tl::expected<bool, ErrorCode> {
    return EvictDiskReplicaImpl(client_id, key, tenant_id, replica_type,
                                &expected_replica_id);
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchEvictDiskReplica(
    const UUID& client_id, const std::vector<std::string>& keys,
    const TenantId& tenant_id, ReplicaType replica_type) {
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.push_back(
            EvictDiskReplica(client_id, key, tenant_id, replica_type));
    }
    return results;
}

tl::expected<CopyStartResponse, ErrorCode> MasterService::CopyStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    const bool dynamic_copy = dynamic_replication_lease_id != UUID{};
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    if (!dynamic_copy) {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& tgt_segment : tgt_segments) {
            if (!segment_access.ExistsSegmentName(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_found";
                return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
            }
            if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_allocatable";
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
        }
    }
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto& state = accessor.GetState();

    size_t new_replica_count = 0;
    for (const auto& tgt_segment : tgt_segments) {
        if (metadata.GetReplicaBySegmentName(tgt_segment) == nullptr) {
            ++new_replica_count;
        }
    }

    auto pending_validation = ValidateDynamicReplicaPendingForCopyStart(
        state, object_id.tenant_id, key, dynamic_replication_lease_id,
        client_id, src_segment,
        DynamicReplicationController::VersionEpoch(metadata),
        dynamic_replication_version_epoch, tgt_segments);
    if (!pending_validation) {
        return tl::make_unexpected(pending_validation.error());
    }
    // Every failure from here on gives up the pending task a dynamic copy was
    // admitted for.
    DynamicReplicationController::PendingCopyGuard pending_guard(
        dynamic_replication_, object_id.tenant_id, key, state, dynamic_copy);
    if (new_replica_count == 0 && dynamic_copy) {
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    if (dynamic_copy) {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& tgt_segment : tgt_segments) {
            if (!segment_access.ExistsSegmentName(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_found";
                return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
            }
            if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_allocatable";
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
        }
    }

    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not valid";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }
    Replica::Descriptor source_descriptor;
    if (!TryGetReadableReplicaDescriptor(*source, source_descriptor)) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    if (auto grown = accessor.Grow(
            SaturatingMultiply(static_cast<uint64_t>(metadata.TotalLength()),
                               static_cast<uint64_t>(new_replica_count)));
        !grown) {
        return tl::make_unexpected(grown.error());
    }

    std::vector<Replica> replicas;
    replicas.reserve(new_replica_count);
    std::vector<std::string> new_target_segments;
    new_target_segments.reserve(new_replica_count);
    // Target allocation acquires the target owner's guard. Do not retain the
    // executor's guard across it: source/target Clients may be identical, and
    // two opposite copy operations must not hold their source guards while
    // waiting for each other's target guard. This CopyStart was already
    // admitted while the source guard was held; the key remains
    // locked, so terminal cleanup cannot pass this operation before its
    // metadata changes become visible.
    serving_guard.reset();
    AllocatorManager allocator_snapshot;
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        allocator_snapshot = allocator_access.SnapshotAllocatorManager();
    }

    for (auto& tgt_segment : tgt_segments) {
        if (metadata.GetReplicaBySegmentName(tgt_segment) != nullptr) {
            // Skip used segments.
            continue;
        }

        auto replica = allocation_strategy_->AllocateFrom(
            allocator_snapshot, metadata.TotalLength(), tgt_segment);
        if (!replica.has_value()) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", failed to allocate replica";
            return tl::make_unexpected(replica.error());
        }
        replicas.push_back(std::move(*replica));
        new_target_segments.push_back(tgt_segment);
    }

    CopyStartResponse response;
    response.targets.reserve(replicas.size());
    std::vector<ReplicaID> replica_ids;
    replica_ids.reserve(replicas.size());

    response.source = std::move(source_descriptor);
    for (const auto& replica : replicas) {
        replica_ids.push_back(replica.id());
        Replica::Descriptor descriptor;
        if (!replica.getDescriptorIfAvailable(descriptor)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        response.targets.push_back(std::move(descriptor));
    }
    // Hard C: the client reads the copy source with the table carried here;
    // it has no GetVSegmentView/GetPSegmentEndpoint fallback.
    {
        std::vector<std::pair<std::string, std::string>> vsegments;
        auto collect = [&vsegments](const Replica::Descriptor& replica) {
            if (!replica.is_vsegment_replica()) return;
            const auto& descriptor = replica.get_vsegment_descriptor();
            vsegments.emplace_back(descriptor.partition_id,
                                   descriptor.vsegment_id);
        };
        collect(response.source);
        for (const auto& target : response.targets) collect(target);
        if (!vsegments.empty()) {
            auto layouts = BuildVSegmentLayoutTable(vsegments);
            if (!layouts.empty()) {
                response.vsegment_layouts = std::move(layouts);
            }
        }
    }

    // Create replication task for tracking.
    auto& replication_task = state.replication_task;
    if (replication_task) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }
    replication_task.emplace(
        client_id, std::chrono::system_clock::now(),
        ReplicationTask::Type::COPY, source->id(), std::move(replica_ids),
        dynamic_replication_lease_id, dynamic_replication_version_epoch);

    dynamic_replication_.RegisterCopyStartLocked(
        metadata, state, src_segment,
        DynamicReplicationController::VersionEpoch(metadata),
        new_target_segments, replication_task->replica_ids);
    pending_guard.Release();

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> MasterService::CopyEnd(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRW accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyEnd key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (task.durable_cleanup_pending) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (task.dynamic_replication_lease_id != dynamic_replication_lease_id ||
        task.dynamic_replication_version_epoch !=
            dynamic_replication_version_epoch) {
        LOG(ERROR) << "key=" << key
                   << ", error=dynamic_replication_token_mismatch";
        return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    }

    auto& metadata = accessor.Get();
    const auto previous_kv_media = KvMediaSnapshot(metadata);
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", copy source becomes invalid during data transfer";
        // Release the refcnt taken in CopyStart. The success path below does
        // this once the copy completes; this error path must do it too, or the
        // source replica stays pinned and can never be evicted.
        if (source != nullptr) {
            source->dec_refcnt();
        }
        // Discard target replicas and clear the replication task.
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&task](const Replica& replica) {
                return std::find(task.replica_ids.begin(),
                                 task.replica_ids.end(),
                                 replica.id()) != task.replica_ids.end();
            });
        accessor.EraseReplicationTask();
        dynamic_replication_.ClearPendingLocked(accessor.GetTenantId(), key,
                                                accessor.GetState());
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // First validate that all target replicas are still healthy. If any
    // replica is invalid we won't be able to mark it complete; this affects
    // the post-mutation descriptor list.
    bool all_complete = true;
    std::vector<ReplicaID> commit_target_ids;
    std::vector<ReplicaID> failed_target_ids;
    commit_target_ids.reserve(task.replica_ids.size());
    for (const auto& replica_id : task.replica_ids) {
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << replica_id
                << ", copy target becomes invalid during data transfer";
            all_complete = false;
            failed_target_ids.push_back(replica_id);
        } else {
            commit_target_ids.push_back(replica_id);
        }
    }

    std::optional<OrderedOpLogWriter::Reservation> batch_reservation;
    if (enable_ha_ && enable_oplog_) {
        auto reservation = ReserveBatchOpLogSlot();
        if (!reservation) {
            return tl::make_unexpected(reservation.error());
        }
        batch_reservation = std::move(reservation.value());
    }

    source->dec_refcnt();
    for (const auto& replica_id : commit_target_ids) {
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica != nullptr) {
            replica->mark_complete();
        }
    }

    metadata.MarkDynamicReplicasComplete(commit_target_ids);
    metadata.ForgetDynamicReplicas(failed_target_ids);
    if (!failed_target_ids.empty()) {
        std::unordered_set<ReplicaID> failed_ids(failed_target_ids.begin(),
                                                 failed_target_ids.end());
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&failed_ids](const Replica& replica) {
                return failed_ids.contains(replica.id());
            });
    }

    dynamic_replication_.ClearPendingLocked(accessor.GetTenantId(), key,
                                            accessor.GetState());

    if (enable_oplog_ && ordered_oplog_writer_) {
        std::vector<Replica::Descriptor> post;
        metadata.VisitReplicas(&Replica::fn_is_completed,
                               [&post](const Replica& replica) {
                                   post.push_back(replica.get_descriptor());
                               });
        auto payload =
            SerializeMetadataForOpLogFromReplicaDescriptors(metadata, post);
        if (batch_reservation) {
            auto persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(*batch_reservation), OpType::PUT_END,
                tenant_id.value(), key, payload, nullptr);
            if (!persist_result) {
                LOG(WARNING)
                    << "CopyEnd: PUT_END persist failed for key=" << key
                    << ", err=" << static_cast<int>(persist_result.error());
            }
        } else {
            auto persist_result = AppendOpLogVisibleBeforeDurable(
                OpType::PUT_END, tenant_id.value(), key, payload);
            if (!persist_result) {
                LOG(WARNING)
                    << "CopyEnd: PUT_END persist failed for key=" << key
                    << ", err=" << static_cast<int>(persist_result.error());
            }
        }
    }

    SyncCacheTotalAccounting(metadata);

    accessor.EraseReplicationTask();

    SyncKvObjectState(key, metadata, tenant_id, previous_kv_media);

    return all_complete ? tl::expected<void, ErrorCode>()
                        : tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
}

tl::expected<void, ErrorCode> MasterService::CopyRevoke(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRW accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyRevoke key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (task.durable_cleanup_pending) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (task.dynamic_replication_lease_id != dynamic_replication_lease_id ||
        task.dynamic_replication_version_epoch !=
            dynamic_replication_version_epoch) {
        LOG(ERROR) << "key=" << key
                   << ", error=dynamic_replication_token_mismatch";
        return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    }

    auto& metadata = accessor.Get();
    const auto source_id = task.source_id;
    const auto replica_ids = task.replica_ids;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", copy source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids
    for (const auto& replica_id : replica_ids) {
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&replica_id](const Replica& replica) {
                return replica.id() == replica_id;
            });
    }

    accessor.EraseReplicationTask();
    dynamic_replication_.ClearPendingLocked(accessor.GetTenantId(), key,
                                            accessor.GetState());

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

tl::expected<MoveStartResponse, ErrorCode> MasterService::MoveStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const std::string& src_segment, const std::string& tgt_segment) {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    if (src_segment == tgt_segment) {
        LOG(ERROR) << "key=" << key << ", move_tgt=" << tgt_segment
                   << " cannot be the same as move_src=" << src_segment;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        if (!segment_access.ExistsSegmentName(tgt_segment)) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", error=target_segment_not_found";
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", error=target_segment_not_allocatable";
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not completed";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }
    Replica::Descriptor source_descriptor;
    if (!TryGetReadableReplicaDescriptor(*source, source_descriptor)) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    std::vector<Replica> replicas;
    // See CopyStart: target allocation owns its own Client guard and must not
    // be nested under the source executor's guard. The key's lock keeps
    // terminal cleanup ordered after this already-admitted MoveStart.
    serving_guard.reset();
    const bool needs_target =
        metadata.GetReplicaBySegmentName(tgt_segment) == nullptr;
    if (auto grown = accessor.Grow(
            needs_target ? static_cast<uint64_t>(metadata.TotalLength()) : 0);
        !grown) {
        return tl::make_unexpected(grown.error());
    }
    if (needs_target) {
        AllocatorManager allocator_snapshot;
        {
            ScopedAllocatorAccess allocator_access =
                segment_manager_.getAllocatorAccess();
            allocator_snapshot = allocator_access.SnapshotAllocatorManager();
        }
        auto replica = allocation_strategy_->AllocateFrom(
            allocator_snapshot, metadata.TotalLength(), tgt_segment);
        if (!replica.has_value()) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", failed to allocate replica";
            return tl::make_unexpected(replica.error());
        }
        replicas.push_back(std::move(*replica));
    }

    MoveStartResponse response;
    std::vector<ReplicaID> replica_ids;

    response.source = std::move(source_descriptor);
    if (!replicas.empty()) {
        replica_ids.push_back(replicas[0].id());
        Replica::Descriptor descriptor;
        if (!replicas[0].getDescriptorIfAvailable(descriptor)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        response.target = std::move(descriptor);
    } else {
        response.target = std::nullopt;
    }
    // Hard C: the client transfers the move source with the table carried
    // here; it has no GetVSegmentView/GetPSegmentEndpoint fallback.
    {
        std::vector<std::pair<std::string, std::string>> vsegments;
        auto collect = [&vsegments](const Replica::Descriptor& replica) {
            if (!replica.is_vsegment_replica()) return;
            const auto& descriptor = replica.get_vsegment_descriptor();
            vsegments.emplace_back(descriptor.partition_id,
                                   descriptor.vsegment_id);
        };
        collect(response.source);
        if (response.target.has_value()) collect(*response.target);
        if (!vsegments.empty()) {
            auto layouts = BuildVSegmentLayoutTable(vsegments);
            if (!layouts.empty()) {
                response.vsegment_layouts = std::move(layouts);
            }
        }
    }

    // Create replication task for tracking.
    auto& replication_task = accessor.GetState().replication_task;
    if (replication_task) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }
    replication_task.emplace(client_id, std::chrono::system_clock::now(),
                             ReplicationTask::Type::MOVE, source->id(),
                             std::move(replica_ids), UUID{}, 0);

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> MasterService::MoveEnd(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id) {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRW accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveEnd key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    const auto previous_kv_media = KvMediaSnapshot(metadata);
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", move source becomes invalid during data transfer";
        // Release the refcnt taken in MoveStart. The success path below does
        // this once the move completes; this error path must do it too, or the
        // source replica stays pinned and can never be evicted.
        if (source != nullptr) {
            source->dec_refcnt();
        }
        // Discard target replica and clear the replication task.
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&task](const Replica& replica) {
                return std::find(task.replica_ids.begin(),
                                 task.replica_ids.end(),
                                 replica.id()) != task.replica_ids.end();
            });
        accessor.EraseReplicationTask();
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // Validate the target replica before any mutation. Source dec_refcnt
    // and target mark_complete are deferred until after persist.
    bool has_target = !task.replica_ids.empty();
    ReplicaID target_id = has_target ? task.replica_ids[0] : ReplicaID{};
    if (has_target) {
        auto replica = metadata.GetReplicaByID(target_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << target_id
                << ", move target becomes invalid during data transfer";
            // Release the refcnt taken in MoveStart. The success path below
            // does this once the move completes; this error path must do it
            // too, or the source replica stays pinned.
            source->dec_refcnt();
            // Discard target replica and clear the replication task.
            EraseReplicasWithCacheTotalAccounting(
                metadata, [&task](const Replica& replica) {
                    return std::find(task.replica_ids.begin(),
                                     task.replica_ids.end(),
                                     replica.id()) != task.replica_ids.end();
                });
            accessor.EraseReplicationTask();
            return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
        }
    }

    if (enable_oplog_ && ordered_oplog_writer_) {
        // Build post-mutation descriptors:
        //   - existing COMPLETE replicas, except the source (about to be
        //   popped)
        //   - target (if any) flipped to COMPLETE
        std::vector<Replica::Descriptor> post;
        for (const auto& rep : metadata.GetAllReplicas()) {
            if (rep.id() == source_id) continue;
            if (rep.status() == ReplicaStatus::COMPLETE) {
                post.push_back(rep.get_descriptor());
                continue;
            }
            if (has_target && rep.id() == target_id) {
                Replica::Descriptor desc = rep.get_descriptor();
                desc.status = ReplicaStatus::COMPLETE;
                post.push_back(std::move(desc));
            }
        }

        tl::expected<OpLogEntry, ErrorCode> persist_result;
        auto reservation = ReserveBatchOpLogSlot();
        if (!reservation) {
            return tl::make_unexpected(reservation.error());
        }
        source->mark_removed();
        persist_result = AppendReservedOpLogWithDurableFinalize(
            std::move(reservation.value()), OpType::PUT_END,
            metadata.tenant_id.value(), key,
            SerializeMetadataForOpLogFromReplicaDescriptors(metadata, post),
            [this, removed_ids = std::vector<ReplicaID>{source_id}](
                const OpLogEntry& durable_entry) {
                FinalizeRemovedReplicasAfterDurable(durable_entry, removed_ids);
            });
        if (!persist_result) {
            return tl::make_unexpected(persist_result.error());
        }
    }

    // Persist OK — apply local commit.
    source->dec_refcnt();
    if (has_target) {
        auto replica = metadata.GetReplicaByID(target_id);
        if (replica != nullptr) {
            replica->mark_complete();
        }
    }
    if (!(enable_ha_ && enable_oplog_)) {
        // Remove the source replica and release its space later.
        auto source_replica = PopReplicasWithCacheTotalAccounting(
            metadata, [&source_id](const Replica& replica) {
                return replica.id() == source_id;
            });
        if (!source_replica.empty()) {
            FreeDfsReplicas(key, source_replica);
            std::lock_guard lock(discarded_replicas_mutex_);
            discarded_replicas_.emplace_back(
                std::move(source_replica), std::chrono::system_clock::now() +
                                               put_start_release_timeout_sec_);
        }
    }

    accessor.EraseReplicationTask();

    SyncKvObjectState(key, metadata, tenant_id, previous_kv_media);

    return {};
}

tl::expected<void, ErrorCode> MasterService::MoveRevoke(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id) {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRW accessor(this,
                                MakeObjectIdentityForRequest(key, tenant_id));
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveRevoke key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", move source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids (in MOVE operation, there should be at most one)
    for (const auto& replica_id : task.replica_ids) {
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&replica_id](const Replica& replica) {
                return replica.id() == replica_id;
            });
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

// How a single-key remove reports a refusal: a held lease is routine, while
// the other refusals mean a write or a copy is still on the object.
static void LogRemoveRefused(const std::string& key, ErrorCode error) {
    if (error == ErrorCode::OBJECT_HAS_LEASE) {
        VLOG(1) << "key=" << key << ", error=object_has_lease";
    } else if (error == ErrorCode::REPLICA_IS_NOT_READY) {
        LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
    } else {
        LOG(ERROR) << "key=" << key << ", error=object_has_replication_task";
    }
}

auto MasterService::Remove(const std::string& key, const TenantId& tenant_id,
                           bool force) -> tl::expected<void, ErrorCode> {
    return RemoveObject(key, tenant_id, force, false);
}

auto MasterService::RemoveObject(const std::string& key,
                                 const TenantId& tenant_id, bool force,
                                 bool allow_managed_weight)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (!allow_managed_weight &&
        weight_manager_.IsManagedGroup(metadata.group_id)) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    auto removable = CheckRemovable(metadata, accessor.GetState(), force,
                                    std::chrono::system_clock::now());
    if (!removable) {
        LogRemoveRefused(key, removable.error());
        return removable;
    }

    if (enable_ha_ && enable_oplog_) {
        return MarkRemovedAndPersist(object_id.tenant_id, key, metadata);
    }
    const auto erase_result = accessor.Erase();
    if (erase_result != ErrorCode::OK) {
        return tl::make_unexpected(erase_result);
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::MarkRemovedAndPersist(
    const TenantId& tenant_id, const std::string& key,
    ObjectMetadata& metadata) {
    auto reservation = ReserveBatchOpLogSlot();
    if (!reservation) {
        return tl::make_unexpected(reservation.error());
    }
    std::vector<ReplicaID> removed_ids;
    metadata.VisitReplicas(&Replica::fn_is_completed,
                           [&removed_ids](const Replica& replica) {
                               removed_ids.push_back(replica.id());
                           });
    auto persist_result = AppendReservedOpLogWithDurableFinalize(
        std::move(reservation.value()), OpType::REMOVE, tenant_id.value(), key,
        {},
        [this, removed_ids =
                   std::move(removed_ids)](const OpLogEntry& durable_entry) {
            FinalizeRemovedReplicasAfterDurable(durable_entry, removed_ids);
        });
    if (!persist_result) {
        return tl::make_unexpected(persist_result.error());
    }
    // Marked only once the REMOVE is queued, so a failed append leaves the
    // object as it was. The durable finalizer takes this object's key lock,
    // which the caller holds, so it cannot run before the marks land.
    metadata.VisitReplicas(&Replica::fn_is_completed,
                           [](Replica& replica) { replica.mark_removed(); });
    return {};
}

tl::expected<void, ErrorCode> MasterService::CheckRemovable(
    const ObjectMetadata& metadata, const route::ObjectState& state, bool force,
    std::chrono::system_clock::time_point now) {
    if (!force && !metadata.IsLeaseExpired(now)) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }
    /**
     * The reason the force operation here does not bypass the replica
     * check is that put operations (which could also be copy or move)
     * and remove operations might be happening concurrently, making it
     * extremely dangerous to perform a direct removal at this point.
     */
    if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    if (state.replication_task) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }
    return {};
}

MasterService::RemoveResult MasterService::RemoveTenantObjects(
    const TenantId& tenant_id, bool force,
    std::chrono::system_clock::time_point now,
    const std::function<bool(const std::string& key)>& matches_key) {
    RemoveResult result;
    const metadata::Namespace* ns = namespaces_.Lookup(tenant_id);
    if (ns == nullptr) {
        return result;
    }
    const auto removable = [&](const std::string& key,
                               const ObjectMetadata& metadata,
                               const route::ObjectState& state) {
        if (weight_manager_.IsManagedGroup(metadata.group_id)) {
            return false;
        }
        auto checked = CheckRemovable(metadata, state, force, now);
        if (!checked) {
            VLOG(1) << "key=" << key << ", action=remove_skipped"
                    << ", error=" << toString(checked.error());
        }
        return checked.has_value();
    };
    // The walk only names the objects that look removable: each is judged
    // again under its own lock below, and one torn down or published again
    // meanwhile is left alone.
    std::vector<route::ObjectRef> candidates;
    for (auto object : ns->objects.ReadCursor()) {
        if (matches_key && !matches_key(object.key())) {
            continue;
        }
        result.saw_object = true;
        if (removable(object.key(), object.metadata(), object.state())) {
            candidates.push_back(object.ref());
        } else {
            result.skipped_object = true;
        }
    }
    for (const auto& candidate : candidates) {
        MetadataAccessorRW accessor(this, tenant_id, candidate);
        if (!accessor.IsPublished()) {
            continue;
        }
        auto& metadata = accessor.Get();
        if (!removable(candidate.key, metadata, accessor.GetState())) {
            result.skipped_object = true;
            continue;
        }
        const int64_t freed =
            metadata.TotalLength() *
            metadata.CountReplicas(&Replica::fn_is_memory_replica);
        if (enable_ha_ && enable_oplog_) {
            if (!MarkRemovedAndPersist(tenant_id, candidate.key, metadata)) {
                result.skipped_object = true;
                continue;
            }
        } else if (accessor.Erase() != ErrorCode::OK) {
            result.skipped_object = true;
            continue;
        }
        result.freed_bytes += freed;
        result.removed_count++;
    }
    return result;
}

auto MasterService::RemoveByRegex(const std::string& regex_pattern,
                                  const TenantId& tenant_id, bool force)
    -> tl::expected<long, ErrorCode> {
    assert(tenant_id.IsValid());
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const RemoveResult result = RemoveTenantObjects(
        ResolveRequestTenantId(tenant_id), force,
        std::chrono::system_clock::now(), [&pattern](const std::string& key) {
            return std::regex_search(key, pattern);
        });

    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << result.removed_count;
    return result.removed_count;
}

long MasterService::RemoveAll(bool force) {
    long removed_count = 0;
    int64_t total_freed_size = 0;
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    auto now = std::chrono::system_clock::now();
    // Tracking which tenants ended up empty costs a hash insert per visited
    // tenant, so it is skipped entirely when nobody is listening for `cleared`.
    const bool track_cleared_tenants =
        KvEventsEnabled() || kv_track_tenant_epochs_;
    // Baselines must be read before the scan starts, not at first sight of each
    // tenant: a commit can land in a tenant the walk already passed while that
    // tenant is still unseen, and a per-tenant read would then fold that commit
    // into its own baseline and wrongly conclude nothing raced.
    const auto epoch_baseline = SnapshotKvTenantEpochs(track_cleared_tenants);
    std::unordered_set<std::string> tenants_seen;
    std::unordered_set<std::string> tenants_with_remaining_objects;

    // Remove per object so managed weights and concurrent imports survive.
    VisitNamespaces(
        [&](const TenantId& tenant_id, metadata::Namespace& /*ns*/) {
            const RemoveResult result =
                RemoveTenantObjects(tenant_id, force, now, nullptr);
            removed_count += result.removed_count;
            total_freed_size += result.freed_bytes;
            // Record the tenant only once the walk actually reached an object.
            // A tenant row can outlive its objects, and recording it
            // unconditionally made every empty-but-present tenant look like a
            // fresh wipe and emit a spurious `cleared`. Under HA+oplog the
            // erase is deferred to the durable callback, so only a skipped
            // object means the tenant is not empty.
            if (track_cleared_tenants && result.saw_object) {
                tenants_seen.insert(tenant_id.value());
                if (result.skipped_object) {
                    tenants_with_remaining_objects.insert(tenant_id.value());
                }
            }
            if (kv_remove_all_tenant_hook_) {
                kv_remove_all_tenant_hook_(tenant_id);
            }
        });

    // tenants_seen holds only tenants that actually held an object, so this
    // difference is "held objects and lost all of them" — a real wipe. Both
    // sets stay empty unless track_cleared_tenants was set.
    if (track_cleared_tenants) {
        for (const auto& tenant : tenants_seen) {
            if (!tenants_with_remaining_objects.contains(tenant)) {
                PublishKvClearedIfEpochUnchanged(
                    TenantId(tenant),
                    epoch_baseline[KvTenantEpochSlot(tenant)]);
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

long MasterService::RemoveAll(const TenantId& tenant_id, bool force) {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    auto now = std::chrono::system_clock::now();
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    // This overload knows its tenant up front, so the epoch can be read before
    // the scan starts rather than at first sight of an object.
    const uint64_t entry_epoch = ReadKvTenantEpoch(normalized_tenant.value());

    // A `cleared` event means "this tenant is now empty". Deciding that from
    // the tenant's remaining objects is wrong in both directions: a tenant
    // that never existed looks empty, and under HA+oplog the erase is deferred
    // to the durable callback so a full wipe still looks non-empty. Count what
    // the walk actually did instead.
    const RemoveResult result =
        RemoveTenantObjects(normalized_tenant, force, now, nullptr);
    if (kv_remove_all_tenant_hook_) {
        kv_remove_all_tenant_hook_(normalized_tenant);
    }

    if (result.saw_object && !result.skipped_object) {
        PublishKvClearedIfEpochUnchanged(normalized_tenant, entry_epoch);
    }

    VLOG(1) << "action=remove_all_objects"
            << ", tenant_id=" << normalized_tenant.value()
            << ", removed_count=" << result.removed_count
            << ", total_freed_size=" << result.freed_bytes;
    return result.removed_count;
}

auto MasterService::BatchRemove(const std::vector<std::string>& keys,
                                const TenantId& tenant_id, bool force)
    -> std::vector<tl::expected<void, ErrorCode>> {
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);

    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();

    const auto now = std::chrono::system_clock::now();
    for (size_t original_idx = 0; original_idx < keys.size(); ++original_idx) {
        const std::string& key = keys[original_idx];
        // Every check and the erase run under the key's lock, so a
        // concurrent write on this key cannot slip between them.
        MetadataAccessorRW accessor(this,
                                    MakeObjectIdentity(key, normalized_tenant));
        if (!accessor.IsPublished()) {
            VLOG(1) << "key=" << key << ", error=object_not_found";
            results[original_idx] =
                tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            continue;
        }

        if (weight_manager_.IsManagedGroup(accessor.Get().group_id)) {
            results[original_idx] =
                tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            continue;
        }

        // Clean up stale replica handles (consistent with single Remove).
        auto cleanup_plan =
            BuildStaleHandleCleanupPlan(accessor.Get(), retaining_clients);
        if (!cleanup_plan.removed_ids.empty()) {
            auto persist_result = PersistStaleHandleCleanupForHA(
                "BatchRemove(stale cleanup)", normalized_tenant, key,
                accessor.Get(), cleanup_plan);
            if (!persist_result) {
                results[original_idx] =
                    tl::make_unexpected(persist_result.error());
                continue;
            }
            if (enable_oplog_) {
                results[original_idx] =
                    tl::make_unexpected(cleanup_plan.would_invalidate
                                            ? ErrorCode::OBJECT_NOT_FOUND
                                            : ErrorCode::OBJECT_ALREADY_EXISTS);
                continue;
            } else if (CleanupStaleHandles(key, normalized_tenant,
                                           accessor.Get(), accessor.GetState(),
                                           retaining_clients)) {
                accessor.Erase();
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }
        }
        if (!accessor.Exists()) {
            results[original_idx] =
                tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            continue;
        }

        auto& metadata = accessor.Get();
        auto removable =
            CheckRemovable(metadata, accessor.GetState(), force, now);
        if (!removable) {
            LogRemoveRefused(key, removable.error());
            results[original_idx] = removable;
            continue;
        }

        if (enable_ha_ && enable_oplog_) {
            results[original_idx] =
                MarkRemovedAndPersist(normalized_tenant, key, metadata);
            continue;
        }
        accessor.Erase();
        results[original_idx] = {};  // Success
    }

    return results;
}

void MasterService::CancelPromotionTaskForRemovedReplicas(
    ObjectMetadata& metadata, route::ObjectState& state,
    const std::vector<ReplicaID>& removed_replica_ids) {
    if (removed_replica_ids.empty()) {
        return;
    }

    const auto& task = state.promotion_task;
    if (!task || task->alloc_id == 0 ||
        std::find(removed_replica_ids.begin(), removed_replica_ids.end(),
                  task->alloc_id) == removed_replica_ids.end()) {
        return;
    }

    if (auto* source = metadata.GetReplicaByID(task->source_id);
        source != nullptr) {
        source->dec_refcnt();
    }
    const UUID holder_id = task->holder_id;
    ErasePromotionTaskIfPresent(state);

    // Best-effort cleanup of a task that may still be queued on the holder.
    local_ssd_manager_.RemovePromotion(holder_id, metadata.tenant_id,
                                       metadata.user_key);
}

bool MasterService::CleanupStaleHandles(
    const std::string& key, const TenantId& tenant_id, ObjectMetadata& metadata,
    route::ObjectState& state,
    const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients) {
    // Removes replicas with invalid allocators (memory replicas on unmounted
    // segments) and local_disk replicas whose owner no longer retains
    // resources.
    // Kept as a thin wrapper over the predicate form so the owner-targeted
    // LOCAL_DISK sweep (ClearLocalDiskHandlesOwnedBy) shares this accounting
    // rather than duplicating it.
    return CleanupStaleHandles(
        key, tenant_id, metadata, state,
        [&retaining_clients](const Replica& replica) {
            return (replica.has_invalid_mem_handle() ||
                    replica.has_invalid_nof_handle() ||
                    replica.has_stale_local_disk_client(retaining_clients)) &&
                   replica.is_completed();
        });
}

bool MasterService::CleanupStaleHandles(
    const std::string& key, const TenantId& tenant_id, ObjectMetadata& metadata,
    route::ObjectState& state,
    const std::function<bool(const Replica&)>& is_stale) {
    const auto previous_kv_media = KvMediaSnapshot(metadata);
    std::vector<ReplicaID> removed_replica_ids;
    EraseReplicasWithCacheTotalAccounting(metadata, is_stale,
                                          &removed_replica_ids);
    CancelPromotionTaskForRemovedReplicas(metadata, state, removed_replica_ids);
    SyncKvObjectState(key, metadata, tenant_id, previous_kv_media);

    // Return true if no valid replicas remain after cleanup
    return !metadata.IsValid();
}

void MasterService::FreeDfsReplicas(const std::string& key,
                                    std::span<const Replica> replicas) {
    if (!dfs_allocator_) return;
    for (const auto& replica : replicas) {
        if (!replica.is_dfs_replica()) continue;
        const auto& desc = replica.get_dfs_descriptor();
        dfs_allocator_->Free(key, desc);
    }
}

void MasterService::RunBucketDfsEviction() {
    (void)RunBucketDfsEvictionInternal(false);
}

bool MasterService::TryRecoverDfsSpaceAfterAllocationFailure() {
    if (bucket_allocator_ == nullptr ||
        !bucket_allocator_->IsEvictionEnabled()) {
        return false;
    }
    LOG(WARNING) << "DFS bucket allocation exhausted; forcing one validated "
                    "whole-bucket eviction";
    return RunBucketDfsEvictionInternal(true);
}

bool MasterService::RunBucketDfsEvictionInternal(bool force_one) {
    if (bucket_allocator_ == nullptr) return false;
    const size_t retried = bucket_allocator_->RetryFailedEvictions();
    bool evicted = retried > 0;
    if (force_one && retried > 0) return evicted;

    const TenantId tenant_id = TenantId::Default();
    std::set<int64_t> attempted;
    while (true) {
        auto pending =
            force_one ? bucket_allocator_->PrepareEvictionForAllocationFailure()
                      : bucket_allocator_->PrepareEviction();
        if (pending.Empty()) return evicted;
        if (!attempted.insert(pending.bucket_id()).second) {
            bucket_allocator_->AbortEviction(std::move(pending));
            return evicted;
        }

        const auto& candidates = pending.Candidates();

        auto matches_candidate =
            [](const Replica& replica,
               const DfsAllocatorInterface::EvictionCandidate& candidate) {
                if (!replica.is_dfs_replica()) return false;
                const auto& descriptor = replica.get_dfs_descriptor();
                return descriptor.shard_idx == candidate.descriptor.shard_idx &&
                       descriptor.offset == candidate.descriptor.offset &&
                       descriptor.object_size ==
                           candidate.descriptor.object_size &&
                       descriptor.aligned_size ==
                           candidate.descriptor.aligned_size &&
                       descriptor.file_path == candidate.descriptor.file_path;
            };

        // The bucket goes as a whole: every member is validated before the
        // allocator commits the bucket, and one member that may not be evicted
        // aborts the attempt. A prepared eviction keeps the bucket's extents
        // out of allocation until it is committed or aborted, so no member
        // gains a replica in it meanwhile, and each member is validated under
        // its own key rather than all of them under one lock. A member read
        // after it was validated still loses its replica once the bucket is
        // committed, but its reader holds a lease: the file is kept until the
        // last lease seen on a member that drops a replica has run out.
        bool all_accepted = true;
        ImmutableBucketAllocator::EvictedBucket evicted_bucket;
        bool logical_committed = false;
        std::chrono::system_clock::time_point keep_until{};
        {
            std::shared_lock<std::shared_mutex> snapshot_lock(
                master_state_mutex_);
            auto now = std::chrono::system_clock::now();
            for (const auto& candidate : candidates) {
                MetadataAccessorRO accessor(
                    this, ObjectIdentity{tenant_id, candidate.key});
                if (!accessor.IsPublished()) continue;
                const auto& metadata = accessor.Get();
                // A key without the candidate replica has already released
                // the extent from the master's side.
                if (!metadata.HasReplica([&](const Replica& replica) {
                        return matches_candidate(replica, candidate);
                    })) {
                    continue;
                }

                const bool processing =
                    metadata.HasReplica([&](const Replica& replica) {
                        return matches_candidate(replica, candidate) &&
                               replica.is_processing();
                    });
                if (processing || accessor.InProcessing() ||
                    metadata.IsHardPinned() || !metadata.IsLeaseExpired(now) ||
                    (IsSoftPinActive(metadata, now) &&
                     !allow_evict_soft_pinned_objects_)) {
                    all_accepted = false;
                    break;
                }
            }

            if (all_accepted) {
                auto logical = bucket_allocator_->CommitEvictionLogical(
                    std::move(pending));
                if (logical) {
                    evicted_bucket = std::move(*logical);
                    logical_committed = true;
                } else {
                    LOG(ERROR) << "DFS bucket eviction logical commit failed, "
                                  "error="
                               << logical.error();
                }
            }

            // The bucket is committed, so the members that still carry one of
            // its replicas drop it now. Each key is resolved again: a member
            // may have been republished since it was validated, and the
            // replica is matched by its descriptor, not by the publication.
            if (logical_committed) {
                for (const auto& candidate : candidates) {
                    MetadataAccessorRW accessor(
                        this, ObjectIdentity{tenant_id, candidate.key},
                        MetadataAccessorRW::AsIs{});
                    if (!accessor.IsPublished()) continue;
                    auto& metadata = accessor.Get();
                    const auto lease_deadline = metadata.EvictionDeadline();
                    const size_t erased =
                        metadata.EraseReplicas([&](const Replica& replica) {
                            return matches_candidate(replica, candidate);
                        });
                    if (erased > 0) {
                        keep_until = std::max(keep_until, lease_deadline);
                        PublishKvRemovedAfterEvict(candidate.key, erased,
                                                   "disk", metadata, tenant_id);
                        if (!metadata.IsValid()) {
                            accessor.Erase();
                        }
                    }
                }
            }
        }

        if (!logical_committed) {
            bucket_allocator_->AbortEviction(std::move(pending));
            continue;
        }
        // The bucket's space comes back only once the file is deleted, so a
        // forced eviction goes on to another bucket.
        if (keep_until > std::chrono::system_clock::now()) {
            VLOG(1) << "DFS bucket eviction deferred until a member's lease "
                       "runs out, bucket_id="
                    << evicted_bucket.bucket_id();
            bucket_allocator_->DeferDeletion(std::move(evicted_bucket),
                                             keep_until);
            continue;
        }

        auto deleted =
            bucket_allocator_->DeleteEvictedBucket(std::move(evicted_bucket));
        if (!deleted) {
            LOG(ERROR) << "DFS bucket data deletion failed; capacity remains "
                          "reserved for runtime retry, error="
                       << deleted.error();
            return evicted;
        } else {
            evicted = true;
            if (force_one) return true;
        }
    }
}

void MasterService::RunDfsEviction() {
    if (!dfs_allocator_) return;
    if (bucket_allocator_ != nullptr) {
        RunBucketDfsEviction();
        return;
    }
    RunShardDfsEviction();
}

void MasterService::RefreshKvcsDfsUsage() {
    if (!dfs_kvcs_capacity_provider_) return;

    auto capacity = dfs_kvcs_capacity_provider_->GetCapacity();
    if (capacity) {
        {
            std::lock_guard lock(dfs_kvcs_usage_mutex_);
            dfs_kvcs_usage_ = *capacity;
        }
        RunKvcsDfsEviction(*capacity);
    } else {
        // Do not let a stale success drive a later watermark decision.
        std::lock_guard lock(dfs_kvcs_usage_mutex_);
        dfs_kvcs_usage_ = {};
        VLOG(1) << "KVCS DFS capacity is unknown: "
                << toString(capacity.error());
    }
}

void MasterService::InitKvcsDfsEviction() {
    if (!kvcs_dfs_eviction_) {
        kvcs_dfs_eviction_ = std::make_unique<KvcsDfsEviction>(
            *this, allow_evict_soft_pinned_objects_);
    }
}

void MasterService::RunKvcsDfsEviction(const DfsUsageSnapshot& usage) {
    if (!dfs_kvcs_capacity_provider_ || !kvcs_dfs_eviction_ ||
        !dfs_kvcs_eviction_enabled_ || !usage.enabled ||
        usage.capacity_bytes == 0) {
        return;
    }
    if (enable_oplog_) {
        VLOG(1) << "KVCS DFS watermark eviction is disabled with OpLog";
        return;
    }

    const long double high_bytes =
        static_cast<long double>(usage.capacity_bytes) *
        dfs_kvcs_eviction_high_watermark_;
    if (static_cast<long double>(usage.used_bytes) <= high_bytes) return;

    const uint64_t low_bytes =
        static_cast<uint64_t>(static_cast<long double>(usage.capacity_bytes) *
                              dfs_kvcs_eviction_low_watermark_);
    const uint64_t target_bytes = usage.used_bytes - low_bytes;
    eviction::Report report;
    {
        std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
        report = kvcs_dfs_eviction_->evictor.Run(
            TenantId::Default(),
            eviction::ByteGoal{.target_bytes = target_bytes});
    }

    const size_t failed = kvcs_dfs_eviction_->reclaimer.TakeFailures();

    auto& metrics = MasterMetricManager::instance();
    if (report.MadeProgress()) {
        metrics.inc_eviction_success(
            static_cast<int64_t>(report.evicted_objects),
            static_cast<int64_t>(std::min<uint64_t>(
                report.released_bytes,
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))));
    } else if (report.filed > 0) {
        metrics.inc_eviction_fail();
    }
    LOG(INFO) << "action=kvcs_dfs_watermark_eviction"
              << ", used_bytes=" << usage.used_bytes
              << ", capacity_bytes=" << usage.capacity_bytes
              << ", target_bytes=" << target_bytes
              << ", evicted_objects=" << report.evicted_objects
              << ", released_bytes=" << report.released_bytes
              << ", provider_delete_failures=" << failed;
}

void MasterService::RunShardDfsEviction() {
    if (!shard_allocator_) return;

    const TenantId tenant_id = TenantId::Default();
    using CandidateIdentity = std::tuple<std::string, int, uint64_t>;
    std::set<CandidateIdentity> attempted;

    while (true) {
        auto pending = shard_allocator_->PrepareEviction();
        if (pending.Empty()) return;

        const auto candidates = pending.Candidates();
        std::vector<bool> accepted(candidates.size(), false);
        std::vector<bool> considered(candidates.size(), false);
        bool saw_repeated_candidate = false;

        // Prepared allocator extents stay unavailable until
        // ResolvePreparedEviction(), so each candidate is validated and removed
        // under its own key rather than in one transaction across keys.
        std::vector<size_t> candidate_indexes;
        candidate_indexes.reserve(candidates.size());
        for (size_t i = 0; i < candidates.size(); ++i) {
            const auto& candidate = candidates[i];
            if (!attempted
                     .emplace(candidate.key, candidate.shard_idx,
                              candidate.offset)
                     .second) {
                saw_repeated_candidate = true;
                continue;
            }
            considered[i] = true;
            candidate_indexes.push_back(i);
        }

        auto matches_candidate = [](const Replica& replica,
                                    const auto& candidate) {
            return replica.is_dfs_replica() &&
                   replica.get_dfs_descriptor().shard_idx ==
                       candidate.shard_idx &&
                   replica.get_dfs_descriptor().offset == candidate.offset;
        };

        auto now = std::chrono::system_clock::now();
        {
            std::shared_lock<std::shared_mutex> snapshot_lock(
                master_state_mutex_);

            // Validate and remove each candidate under the same key lock. Once
            // a candidate has been seen in this cycle, it is excluded above;
            // encountering it again means the LRU scan has wrapped.
            for (const size_t i : candidate_indexes) {
                const auto& candidate = candidates[i];
                MetadataAccessorRW accessor(
                    this, ObjectIdentity{tenant_id, candidate.key},
                    MetadataAccessorRW::AsIs{});
                if (!accessor.IsPublished()) {
                    accepted[i] = true;
                    continue;
                }

                auto& metadata = accessor.Get();
                const bool has_candidate =
                    metadata.HasReplica([&](const Replica& replica) {
                        return matches_candidate(replica, candidate);
                    });
                if (!has_candidate) {
                    accepted[i] = true;
                    continue;
                }

                const bool candidate_is_processing =
                    metadata.HasReplica([&](const Replica& replica) {
                        return matches_candidate(replica, candidate) &&
                               replica.is_processing();
                    });
                const bool managed_weight =
                    weight_manager_.IsManagedGroup(metadata.group_id);
                accepted[i] = !managed_weight && !candidate_is_processing &&
                              !accessor.InProcessing() &&
                              !metadata.IsHardPinned() &&
                              metadata.IsLeaseExpired(now) &&
                              (!IsSoftPinActive(metadata, now) ||
                               allow_evict_soft_pinned_objects_);
                if (!accepted[i]) continue;

                // A missing descriptor was accepted above and is already
                // evicted from the master's point of view. Otherwise remove
                // the accepted descriptor before its allocator extent can be
                // committed and reused.
                const size_t erased =
                    metadata.EraseReplicas([&](const Replica& replica) {
                        return matches_candidate(replica, candidate) &&
                               !replica.is_processing();
                    });
                if (erased > 0) {
                    PublishKvRemovedAfterEvict(candidate.key, erased, "disk",
                                               metadata, tenant_id);
                    if (!metadata.IsValid()) {
                        accessor.Erase();
                    }
                }
            }
        }

        shard_allocator_->ResolvePreparedEviction(std::move(pending), accepted);

        // A protected allocation stays live, but moving it to the MRU side
        // lets this cycle inspect colder candidates behind it. The attempted
        // set bounds the scan when every remaining allocation is protected.
        for (size_t i = 0; i < candidates.size(); ++i) {
            if (considered[i] && !accepted[i]) {
                shard_allocator_->UpdateAccess(candidates[i].key,
                                               candidates[i].shard_idx,
                                               candidates[i].offset);
            }
        }

        if (saw_repeated_candidate) {
            return;
        }
    }
}

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    VisitNamespaces(
        [&total](const TenantId& /*tenant_id*/, metadata::Namespace& ns) {
            total += ns.objects.ObjectCount();
        });
    return total;
}

auto MasterService::Ping(const UUID& client_id)
    -> tl::expected<PingResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    const auto record_it = client_liveness_records_.find(client_id);
    bool observation_accepted = false;
    if (record_it != client_liveness_records_.end()) {
        const auto observation =
            record_it->second->Observe(ClientLivenessRecord::Clock::now());
        observation_accepted =
            observation != ClientLivenessObservation::REJECTED_OFFLINE;
        if (observation == ClientLivenessObservation::RECOVERED_ACTIVE) {
            MasterMetricManager::instance().client_liveness_recovered();
            LOG(INFO) << "client_id=" << client_id
                      << ", action=client_liveness_recovered, signal=ping";
        }
    }
    const ClientStatus client_status =
        observation_accepted && ok_client_.contains(client_id)
            ? ClientStatus::OK
            : ClientStatus::NEED_REMOUNT;
    return PingResponse(view_version_, client_status);
}

tl::expected<std::string, ErrorCode> MasterService::GetFsdir() const {
    if (root_fs_dir_.empty() || cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return std::string();
    }
    return root_fs_dir_ + "/" + cluster_id_;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
MasterService::GetStorageConfig() const {
    if (root_fs_dir_.empty() || cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return GetStorageConfigResponse("", enable_disk_eviction_,
                                        quota_bytes_);
    }
    std::string fsdir = root_fs_dir_ + "/" + cluster_id_;
    return GetStorageConfigResponse(fsdir, enable_disk_eviction_, quota_bytes_);
}

auto MasterService::MountLocalDiskSegment(const UUID& client_id,
                                          bool enable_offloading)
    -> tl::expected<void, ErrorCode> {
    if (!enable_offload_) {
        LOG(ERROR) << "	The offload functionality is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOAD);
    }
    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    auto [record_it, inserted] = client_liveness_records_.try_emplace(
        client_id, std::make_shared<ClientLivenessRecord>(
                       ClientLivenessRecord::Clock::now()));
    const auto record = record_it->second;
    if (inserted) {
        MasterMetricManager::instance().client_liveness_record_created();
    }
    ErrorCode err = ErrorCode::INTERNAL_ERROR;
    const auto observation =
        record->ObserveAndRun(ClientLivenessRecord::Clock::now(), [&] {
            std::shared_lock<std::shared_mutex> snapshot_lock(
                master_state_mutex_);
            err =
                local_ssd_manager_.RegisterClient(client_id, enable_offloading);
            if (err == ErrorCode::OK ||
                err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
                // Memory may have mounted before LOCAL_DISK registration.
                // Preserve the co-tenant name filter in either mount order.
                std::vector<Segment> segments;
                auto segment_access = segment_manager_.getSegmentAccess();
                if (segment_access.GetClientSegments(client_id, segments) ==
                        ErrorCode::OK &&
                    !segments.empty()) {
                    local_ssd_manager_.SetClientSegmentName(client_id,
                                                            segments.front().name);
                }
            }
            return err == ErrorCode::OK ||
                   err == ErrorCode::SEGMENT_ALREADY_EXISTS;
        });
    if (observation == ClientLivenessObservation::REJECTED_OFFLINE) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (err != ErrorCode::OK && err != ErrorCode::SEGMENT_ALREADY_EXISTS) {
        if (inserted) {
            client_liveness_records_.erase(client_id);
            MasterMetricManager::instance().on_client_liveness_record_removed(
                ClientLivenessState::ACTIVE);
        }
        return tl::make_unexpected(err);
    }
    if (observation == ClientLivenessObservation::RECOVERED_ACTIVE) {
        MasterMetricManager::instance().client_liveness_recovered();
        LOG(INFO) << "client_id=" << client_id
                  << ", action=client_liveness_recovered, "
                     "signal=local_disk_mount";
    }

    return {};
}

auto MasterService::UnmountLocalDiskSegment(const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    if (!enable_offload_) {
        LOG(ERROR) << "The offload functionality is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOAD);
    }

    // Drop the client's LOCAL_DISK registration first: from here on
    // OffloadObjectHeartbeat answers SEGMENT_NOT_FOUND, so the master hands
    // this client no further offload work. Then sweep the replicas it still
    // owns, so no reader can be given a replica whose owner is about to stop
    // serving. The sweep walks every namespace, so it must run without
    // the registry lock held -- the same order and the same reason as the
    // expiry branch of ClientMonitorFunc.
    //
    // The deregistration takes master_state_mutex_ exclusively.
    // NotifyOffloadSuccess admits a disk-replica registration by checking
    // this client's registration inside one shared-lock section together with
    // the metadata write, so that section lands entirely before this
    // deregistration (the sweep below erases the replica) or entirely after
    // (the check refuses it). Without the exclusive lock a registration
    // admitted against the old one could land on a key the sweep had
    // already passed and survive as a stale owner.
    std::optional<int64_t> reported_capacity;
    {
        std::unique_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
        reported_capacity = local_ssd_manager_.UnregisterClient(client_id);
    }
    if (!reported_capacity) {
        // Idempotent, the same way MountLocalDiskSegment treats an
        // already-mounted segment as success.
        return {};
    }
    if (*reported_capacity > 0) {
        MasterMetricManager::instance().dec_total_file_capacity(
            *reported_capacity);
    }

    // Sweep exactly this owner's replicas. Deliberately not a
    // liveness-complement sweep (ClearInvalidHandles with a staying set):
    // that classifies by absence from a point-in-time snapshot, so an owner
    // that mounts and registers after the snapshot but before the sweep
    // reaches its key would have its replicas classified stale and
    // erased -- and when that disk replica was the key's only one, the key
    // itself. An owner-id predicate cannot misclassify a concurrent mount,
    // whatever the interleaving.
    ClearLocalDiskHandlesOwnedBy(client_id);

    LOG(INFO) << "client_id=" << client_id
              << ", action=unmount_local_disk_segment_by_request";
    return {};
}

bool MasterService::HasMountedLocalDiskSegment(const UUID& client_id) {
    return local_ssd_manager_.GetUsage(client_id).has_value();
}

auto MasterService::ReportInvalidReplicaEndpoints(
    const std::vector<std::string>& endpoints)
    -> tl::expected<void, ErrorCode> {
    if (endpoints.empty()) return {};

    std::vector<std::string> names;
    std::vector<std::string> segment_ids;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        std::vector<std::pair<Segment, UUID>> all_segments;
        (void)segment_access.GetAllSegments(all_segments);
        names.reserve(endpoints.size());
        segment_ids.reserve(endpoints.size());
        for (const auto& [segment, client_id] : all_segments) {
            (void)client_id;
            if (std::find(endpoints.begin(), endpoints.end(),
                          segment.te_endpoint) != endpoints.end()) {
                names.push_back(segment.name);
                segment_ids.push_back(UuidToString(segment.id));
            }
        }
    }
#ifdef STORE_USE_ETCD
    {
        std::set<std::string> resolved(endpoints.begin(), endpoints.end());
        for (const auto& name : names) resolved.insert(name);
        std::vector<cvm::SegmentDescriptor> descriptors;
        ViewVersionId revision = 0;
        if (cvm::EtcdViewStore::LoadAllSegmentDescriptors(
                cluster_id_, descriptors, revision) == ErrorCode::OK) {
            for (const auto& desc : descriptors) {
                const bool by_endpoint = !desc.te_endpoint.empty() &&
                                         resolved.count(desc.te_endpoint) > 0;
                const bool by_name = !desc.segment_name.empty() &&
                                     resolved.count(desc.segment_name) > 0;
                if (by_endpoint || by_name) {
                    if (!desc.segment_id.empty())
                        segment_ids.push_back(desc.segment_id);
                    if (by_endpoint && !desc.segment_name.empty())
                        names.push_back(desc.segment_name);
                }
            }
        }
    }
#endif

    size_t before = 0;
    size_t after = 0;
    {
        std::lock_guard<std::shared_mutex> lock(invalid_endpoints_mutex_);
        before = invalid_replica_endpoints_.size();
        for (const auto& endpoint : endpoints) {
            if (!endpoint.empty()) invalid_replica_endpoints_.insert(endpoint);
        }
        for (const auto& name : names) invalid_replica_endpoints_.insert(name);
        for (const auto& segment_id : segment_ids)
            invalid_replica_endpoints_.insert(segment_id);
        after = invalid_replica_endpoints_.size();
    }
    LOG(WARNING) << "reported invalid replica endpoints: count="
                 << endpoints.size()
                 << ", resolved_segment_names=" << names.size()
                 << ", resolved_segment_ids=" << segment_ids.size()
                 << ", invalid_endpoints_total=" << before << "->" << after;
    return {};
}

auto MasterService::OffloadObjectHeartbeat(const UUID& client_id,
                                           bool enable_offloading)
    -> tl::expected<std::vector<OffloadTaskItem>, ErrorCode> {
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    auto pending = local_ssd_manager_.SetOffloadingAndTakePending(
        client_id, enable_offloading);
    if (!pending) {
        LOG(ERROR) << "Local disk segment not found with client id = "
                   << client_id;
        return tl::make_unexpected(pending.error());
    }
    if (enable_offloading) {
        return std::move(*pending);
    }
    // Offloading is disabled: clear the pending queue to prevent unbounded
    // growth that would trigger KEYS_ULTRA_LIMIT in PushOffloadingQueue. We
    // must also clean up the corresponding offloading tasks and decrement
    // source replica refcounts to avoid resource leaks and blocked writes
    // (OBJECT_HAS_REPLICATION_TASK).
    for (auto& task : *pending) {
        const auto object_id =
            MakeObjectIdentity(task.key, TenantId(task.tenant_id));
        MetadataAccessorRW accessor(this, object_id);
        if (accessor.Exists()) {
            auto& offloading_task = accessor.GetState().offloading_task;
            if (offloading_task) {
                auto source =
                    accessor.Get().GetReplicaByID(offloading_task->source_id);
                if (source) {
                    source->dec_refcnt();
                }
                offloading_task.reset();
            }
        }
    }
    return std::vector<OffloadTaskItem>{};
}

auto MasterService::PollRemoveAll(const UUID& client_id)
    -> tl::expected<bool, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    return local_ssd_manager_.ConsumeRemoveAll(client_id);
}

auto MasterService::RemoveObjectHeartbeat(const UUID& client_id)
    -> tl::expected<std::vector<RemoveTaskItem>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    return local_ssd_manager_.TakeRemovedKeys(client_id);
}

void MasterService::EnqueueRemoveTasks(const std::vector<UUID>& holder_ids,
                                       const RemoveTaskItem& task) {
    if (holder_ids.empty()) return;
    for (const auto& holder_id : holder_ids) {
        // SEGMENT_NOT_FOUND: the holder deregistered (expiry or generational
        // unmount) before the durable erase finished; its SSD files are
        // reclaimed by that path, so nothing is left to notify.
        const auto err = local_ssd_manager_.EnqueueRemoved(holder_id, task);
        if (err != ErrorCode::OK && err != ErrorCode::SEGMENT_NOT_FOUND &&
            err != ErrorCode::OBJECT_ALREADY_EXISTS) {
            LOG(ERROR) << "client_id=" << holder_id
                       << ", key=" << task.key
                       << ", action=enqueue_remove_task_failed"
                       << ", error=" << toString(err);
        }
    }
}

auto MasterService::AckRemoveObjectHeartbeat(
    const UUID& client_id, const std::vector<RemoveTaskItem>& tasks)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    return local_ssd_manager_.AckRemovedKeys(client_id, tasks);
}

auto MasterService::ReportSsdCapacity(const UUID& client_id,
                                      int64_t ssd_total_capacity_bytes)
    -> tl::expected<void, ErrorCode> {
    if (ssd_total_capacity_bytes < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    auto capacity =
        local_ssd_manager_.ReportCapacity(client_id, ssd_total_capacity_bytes);
    if (!capacity) {
        if (capacity.error() == ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(ERROR) << "Local disk segment not found with client id = "
                       << client_id;
        }
        return tl::make_unexpected(capacity.error());
    }
    int64_t old_capacity = capacity->previous_bytes;
    if (ssd_total_capacity_bytes != old_capacity) {
        if (old_capacity > 0) {
            MasterMetricManager::instance().dec_total_file_capacity(
                old_capacity);
        }
        if (ssd_total_capacity_bytes > 0) {
            MasterMetricManager::instance().inc_total_file_capacity(
                ssd_total_capacity_bytes);
        }
    }
    return {};
}

auto MasterService::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas)
    -> tl::expected<void, ErrorCode> {
    if (tasks.size() != metadatas.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    // Set when an entry's replica registration is refused because the client
    // no longer has a LOCAL_DISK segment entry (see the per-entry checks
    // below). NACK cleanups still run for the rest of the batch; the caller
    // gets SEGMENT_NOT_FOUND so a rescan stops re-registering.
    bool refused_unmounted = false;

    for (size_t i = 0; i < tasks.size(); ++i) {
        const auto& task = tasks[i];
        const auto& metadata = metadatas[i];
        const TenantId task_tenant = enable_multi_tenants_
                                         ? TenantId(task.tenant_id)
                                         : TenantId::Default();
        const auto request_object_id =
            MakeObjectIdentityForRequest(task.key, task_tenant);

        // Slot 归属校验：多 submaster 下，offload 完成通知可能因路由切换
        // 被发到非 owner 的 master。本机不拥有该 slot 时拒绝处理（不建
        // LOCAL_DISK 副本、不动 offloading_task），由 worker 按最新路由
        // 重发到 owner。单 master 模式 owned_slots_ready_==false 直接
        // 放行，行为不变。校验同时覆盖成功与 NACK（data_size<0）两条路径。
        const uint16_t slot = cvm::KeySlot(request_object_id.tenant_id,
                                           request_object_id.user_key);
        const ErrorCode svc = CheckSlotServiceability(slot);
        if (svc != ErrorCode::OK) {
            // 双投清理：slot 已不归本机，但若该 task 由本机派发（本机存在
            // 该对象的 offloading_task 状态），worker 会把通知额外投回本机
            // ——此时只清理任务状态（dec 源副本 refcnt + reset 任务），
            // 跳过全部元数据操作与 ssd_used_bytes 记账（副本注册由新
            // owner 的那条通知路径完成，本机从未累加过该对象的用量）。
            // 找不到任务说明是纯错投或迁移清空后的迟到通知，WARNING 即可。
            bool cleaned = false;
            {
                std::shared_lock<std::shared_mutex> shared_lock(
                    master_state_mutex_);
                MetadataAccessorRW accessor(this, request_object_id);
                if (accessor.Exists()) {
                    auto& offloading_task = accessor.GetState().offloading_task;
                    if (offloading_task) {
                        auto source = accessor.Get().GetReplicaByID(
                            offloading_task->source_id);
                        if (source != nullptr) {
                            source->dec_refcnt();
                        }
                        offloading_task.reset();
                        cleaned = true;
                    }
                }
            }
            if (cleaned) {
                LOG(INFO) << "NotifyOffloadSuccess: cleaned offloading task"
                          << " for migrated slot"
                          << " key=" << task.key << " slot=" << (int)slot
                          << " error=" << svc << " client=" << client_id;
            } else {
                LOG(WARNING) << "NotifyOffloadSuccess: rejected non-owned slot"
                             << " key=" << task.key << " slot=" << (int)slot
                             << " error=" << svc << " client=" << client_id;
            }
            continue;
        }

        // NACK sentinel: offload failed on worker. Clean up the
        // offloading_task + dec_refcnt but skip AddReplica.
        if (metadata.data_size < 0) {
            std::shared_lock<std::shared_mutex> shared_lock(
                master_state_mutex_);
            MetadataAccessorRW accessor(this, request_object_id);
            if (accessor.Exists()) {
                auto& offloading_task = accessor.GetState().offloading_task;
                if (offloading_task) {
                    auto source = accessor.Get().GetReplicaByID(
                        offloading_task->source_id);
                    if (source != nullptr) {
                        source->dec_refcnt();
                    }
                    offloading_task.reset();
                    MasterMetricManager::instance().inc_offload_failed(
                        UuidToString(client_id));
                }
            }
            continue;
        }

        Replica replica(client_id, metadata.data_size,
                        metadata.transport_endpoint, ReplicaStatus::COMPLETE,
                        record);
        bool handled_existing_object = false;
        bool added_new_local_disk_replica = false;
        {
            std::shared_lock<std::shared_mutex> shared_lock(
                master_state_mutex_);
            // A disk replica may only be registered for a client whose
            // LOCAL_DISK segment entry still exists. Checked inside this
            // shared-lock section, before the key's lock, so the check and
            // the write below cannot straddle UnmountLocalDiskSegment's
            // removal (which holds master_state_mutex_ exclusively): either the
            // replica lands first and its sweep erases it, or the check here
            // sees the segment gone and refuses. Without this, a
            // registration racing a deregistration -- an in-flight rescan
            // batch, or an offload completion from a heartbeat that was past
            // its own drain check -- could land after the sweep and leave
            // the master advertising a departed owner. Scoped to
            // enable_offload_, where the segment registry exists and a
            // deregistration can race; with the subsystem off both the mount
            // and unmount RPCs refuse, so there is nothing to check against
            // and no race to close.
            const bool segment_mounted =
                !enable_offload_ || HasMountedLocalDiskSegment(client_id);
            MetadataAccessorRW accessor(this, request_object_id);
            if (accessor.Exists()) {
                auto& obj_metadata = accessor.Get();
                const auto previous_kv_media = KvMediaSnapshot(obj_metadata);
                auto& offloading_task = accessor.GetState().offloading_task;
                if (offloading_task &&
                    replica.type() != ReplicaType::LOCAL_DISK) {
                    LOG(ERROR) << "Invalid replica type: " << replica.type()
                               << ". Expected ReplicaType::LOCAL_DISK.";
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }

                // Existing orphan objects can only bypass tenant registration
                // for a master-admitted offload completion. Without this task
                // marker, fall through to the regular registration check.
                if (offloading_task) {
                    auto source =
                        obj_metadata.GetReplicaByID(offloading_task->source_id);
                    if (source != nullptr) {
                        source->dec_refcnt();
                    }
                    offloading_task.reset();

                    if (!segment_mounted) {
                        // The offload bookkeeping above still ran; only the
                        // registration is refused.
                        refused_unmounted = true;
                    } else if (!obj_metadata.HasReplica(
                                   &Replica::fn_is_local_disk_replica)) {
                        std::vector<Replica> replicas;
                        replicas.emplace_back(std::move(replica));
                        obj_metadata.AddReplicas(std::move(replicas));
                        SyncCacheTotalAccounting(obj_metadata);
                        MasterMetricManager::instance().inc_offload_completed(
                            UuidToString(client_id));
                        added_new_local_disk_replica = true;
                    }
                    handled_existing_object = true;
                    SyncKvObjectState(request_object_id.user_key, obj_metadata,
                                      request_object_id.tenant_id,
                                      previous_kv_media);
                }
                handled_existing_object = true;
            }
        }

        if (!handled_existing_object) {
            auto normalized_tenant_result =
                ResolveTenantIdForWrite(request_object_id.tenant_id);
            if (!normalized_tenant_result) {
                return tl::make_unexpected(normalized_tenant_result.error());
            }
            const ObjectIdentity object_id{
                std::move(normalized_tenant_result.value()),
                request_object_id.user_key};

            auto res = AddReplicaForRetainedClient(
                client_id, object_id.user_key, object_id.tenant_id, replica);
            if (!res) {
                if (res.error() == ErrorCode::OBJECT_NOT_FOUND) {
                    continue;
                }
                if (res.error() == ErrorCode::SEGMENT_NOT_FOUND) {
                    // AddReplica's own mounted-segment check refused it (the
                    // segment entry vanished under a deregistration). Finish
                    // the batch so remaining NACK cleanups still run.
                    refused_unmounted = true;
                    continue;
                }
                LOG(ERROR) << "Failed to add replica: error=" << res.error()
                           << ", client_id=" << client_id
                           << ", tenant_id=" << object_id.tenant_id.value()
                           << ", key=" << object_id.user_key;
                return tl::make_unexpected(res.error());
            }
            added_new_local_disk_replica = res.value();
        }
        if (metadata.data_size > 0 && added_new_local_disk_replica) {
            local_ssd_manager_.AdjustUsedBytes(client_id, metadata.data_size);
        }
        if (added_new_local_disk_replica && io_pattern_runtime_) {
            // The object now holds a durable lower-tier replica, so record it:
            // once its MEMORY replica is reclaimed the key must be promotable
            // again instead of looking like it has no replica at all.
            // LOCAL_DISK is reported under kL3NofSsd until CacheTier grows a
            // dedicated member for it. The per-object identity resolved inside
            // the add branch is scoped to that branch, so use the loop-level
            // request identity here.
            io_pattern_runtime_->RecordTierEvent(io_pattern::CacheEvent{
                .type = io_pattern::CacheEventType::kInserted,
                .object = {request_object_id.tenant_id,
                           request_object_id.user_key},
                .target_tier = io_pattern::CacheTier::kL3NofSsd});
        }
    }

    if (refused_unmounted) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", action=notify_offload_success_refused"
                     << ", error=no_local_disk_segment";
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::PushOffloadingQueue(
    const ObjectIdentity& object_id, Replica& replica,
    std::vector<UUID>* mirror_clients) {
    const auto& segment_names = replica.get_segment_names();
    // No source segment names means there is no usable source segment to
    // offload from. Returning a silent {} here caused the caller to record an
    // OffloadingTask + inc_refcnt for work that was never enqueued, leaking the
    // refcount until TTL expiry (issue #2997). Return an explicit
    // UNABLE_OFFLOADING so callers treat this as a no-op rather than a success.
    if (segment_names.empty() ||
        std::none_of(segment_names.begin(), segment_names.end(),
                     [](const auto& name) { return name.has_value(); })) {
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
    }
    Replica::Descriptor source_descriptor;
    if (!TryGetReadableReplicaDescriptor(replica, source_descriptor) ||
        !source_descriptor.is_memory_replica()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    bool any_enqueued = false;
    std::string client_id_str;
    const auto liveness = replica.getClientLiveness();
    if (!liveness) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::vector<UUID> queued_clients;
    queued_clients.reserve(segment_names.size());
    for (const auto& segment_name_it : segment_names) {
        if (!segment_name_it.has_value()) {
            continue;
        }
        std::optional<UUID> client_id;
        {
            auto allocator_access = segment_manager_.getAllocatorAccess();
            client_id =
                allocator_access.GetOwnerClientId(segment_name_it.value());
        }
        if (!client_id) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        // Callers hold the source object's key. This serving observation is
        // the admission point; terminal cleanup must acquire the same key
        // before it can remove the source or unregister the destination.
        if (!liveness->IsServing()) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        auto allocator_access = segment_manager_.getAllocatorAccess();
        const auto current_owner =
            allocator_access.GetOwnerClientId(segment_name_it.value());
        if (!current_owner || *current_owner != *client_id ||
            !TryGetReadableReplicaDescriptor(replica, source_descriptor)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        const int64_t size =
            source_descriptor.get_memory_descriptor().buffer_descriptor.size_;
        auto err = local_ssd_manager_.EnqueueOffload(
            *client_id,
            OffloadTaskItem{.tenant_id = object_id.tenant_id.value(),
                            .key = object_id.user_key,
                            .size = size},
            offloading_queue_limit_);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
        }
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::KEYS_ULTRA_LIMIT) {
                MasterMetricManager::instance().inc_offload_enqueue_rejected(
                    UuidToString(*client_id));
            }
            return tl::make_unexpected(err);
        }
        if (mirror_clients != nullptr) {
            mirror_clients->push_back(*client_id);
        }
        client_id_str = UuidToString(*client_id);
        any_enqueued = true;
    }
    // Every segment name was nullopt (or EnqueueOffload found no usable
    // segment), so nothing was enqueued. Same invariant as the empty case:
    // never report success when no task was actually submitted.
    if (!any_enqueued) {
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
    }
    MasterMetricManager::instance().inc_offload_enqueued(client_id_str);
    return {};
}

bool MasterService::CancelQueuedOffloadTask(ObjectMetadata& metadata,
                                            route::ObjectState& state,
                                            const ObjectIdentity& object_id) {
    auto& offloading_task = state.offloading_task;
    if (!offloading_task) {
        return true;
    }
    const auto& mirror_clients = offloading_task->mirror_clients;
    if (mirror_clients.empty()) {
        return false;
    }

    if (!local_ssd_manager_.CancelOffloadsIfAllPending(
            mirror_clients, object_id.tenant_id, object_id.user_key)) {
        return false;
    }

    auto source = metadata.GetReplicaByID(offloading_task->source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    MasterMetricManager::instance().inc_offload_cancelled(
        UuidToString(mirror_clients[0]));
    offloading_task.reset();
    return true;
}

// Promotion-on-hit

// Push a key into the holder client's promotion mailbox. Resolve the holder
// via the LOCAL_DISK replica's embedded client_id rather than via the
// segment-name reverse lookup.
tl::expected<void, ErrorCode> MasterService::PushPromotionQueue(
    const ObjectIdentity& object_id, Replica& source_replica) {
    auto holder_id = source_replica.get_local_disk_client_id();
    if (!holder_id.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto liveness = source_replica.getClientLiveness();
    if (!liveness) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    // TryPushPromotionQueue holds the source object's key, which orders a
    // concurrently accepted enqueue before terminal cleanup.
    if (!liveness->IsServing()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    auto err = local_ssd_manager_.EnqueuePromotion(
        *holder_id, PromotionTaskItem{.tenant_id = object_id.tenant_id.value(),
                                      .key = object_id.user_key,
                                      .size = static_cast<int64_t>(
                                          source_replica.get_descriptor()
                                              .get_local_disk_descriptor()
                                              .object_size)});
    if (err == ErrorCode::SEGMENT_NOT_FOUND) {
        // Holder client expired or never registered LocalSSD;
        // the LOCAL_DISK replica will be cleaned up by ClientMonitorFunc on
        // its own schedule.
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

// --- Promotion retry candidate helpers ---

void MasterService::EraseCandidate(const ObjectIdentity& object_id) {
    MetadataAccessorRW accessor(this, object_id, MetadataAccessorRW::AsIs{});
    if (!accessor.IsPublished()) {
        promotion_candidates_.ForgetLocked(object_id.tenant_id,
                                           object_id.user_key);
        return;
    }
    promotion_candidates_.EraseLocked(object_id.tenant_id, object_id.user_key,
                                      accessor.GetState());
}

void MasterService::BackoffCandidate(const ObjectIdentity& object_id,
                                     PromotionQueueResult result) {
    MetadataAccessorRW accessor(this, object_id, MetadataAccessorRW::AsIs{});
    if (!accessor.IsPublished()) {
        promotion_candidates_.ForgetLocked(object_id.tenant_id,
                                           object_id.user_key);
        return;
    }
    promotion_candidates_.BackoffLocked(object_id.tenant_id, object_id.user_key,
                                        accessor.GetState(), result);
}

size_t MasterService::RunPromotionCandidateRetry() {
    if (!promotion_on_hit_ || promotion_candidates_.Empty()) {
        return 0;
    }

    const auto now = std::chrono::steady_clock::now();
    std::vector<ObjectIdentity> due_candidates;
    due_candidates.reserve(kPromotionRetryBatchSize);

    // The tracker files every candidate at its retry time, so a round reads
    // only the due ones, the longest overdue first, and the batch size bounds
    // how much one round touches.
    {
        std::shared_lock<std::shared_mutex> snap_lock(master_state_mutex_);
        for (auto& [tenant_id, key] :
             promotion_candidates_.Due(now, kPromotionRetryBatchSize)) {
            ObjectIdentity object_id{.tenant_id = std::move(tenant_id),
                                     .user_key = std::move(key)};
            MetadataAccessorRW accessor(this, object_id,
                                        MetadataAccessorRW::AsIs{});
            if (!accessor.IsPublished()) {
                promotion_candidates_.ForgetLocked(object_id.tenant_id,
                                                   object_id.user_key);
                continue;
            }
            auto& metadata = accessor.Get();
            auto& state = accessor.GetState();
            if (!promotion_candidates_.DueLocked(
                    object_id.tenant_id, object_id.user_key, state, now)) {
                continue;
            }

            // Quick pre-filter under the key's lock to avoid adding
            // candidates that are obviously ineligible.
            if (!metadata.IsValid() || state.is_processing ||
                state.promotion_task ||
                metadata.HasReplica(&Replica::fn_is_memory_replica) ||
                !metadata.HasReplica(&Replica::fn_is_local_disk_replica)) {
                promotion_candidates_.EraseLocked(object_id.tenant_id,
                                                  object_id.user_key, state);
                continue;
            }

            due_candidates.push_back(std::move(object_id));
        }
    }

    size_t queued = 0;
    {
        std::shared_lock<std::shared_mutex> snap_lock(master_state_mutex_);
        for (const auto& object_id : due_candidates) {
            const auto result =
                TryPushPromotionQueue(object_id, /*record_candidate=*/false);
            if (result == PromotionQueueResult::kQueued) {
                queued++;
                MasterMetricManager::instance()
                    .inc_promotion_candidate_admitted();
            } else if (PromotionCandidateTracker::IsTransient(result)) {
                MasterMetricManager::instance()
                    .inc_promotion_candidate_admission_rejected();
                BackoffCandidate(object_id, result);
            } else {
                EraseCandidate(object_id);
            }
        }
    }

    return queued;
}

void MasterService::TrySubmitDynamicReplicaProposal(
    const ObjectIdentity& object_id) {
    if (!dynamic_replication_.Enforcing() ||
        !dynamic_replication_.HeatAdmitted(object_id.tenant_id,
                                           object_id.user_key)) {
        return;
    }

    auto lease =
        SubmitReplicaActionProposalLocked(dynamic_replication_.MakeAutoProposal(
            object_id.tenant_id, object_id.user_key));
    if (!lease.has_value()) {
        VLOG(1) << "dynamic_replication_auto_proposal_rejected key="
                << object_id.user_key << ", error_code=" << lease.error();
    }
}

void MasterService::CleanupExpiredDynamicReplicationState() {
    if (!dynamic_replication_.Enabled()) {
        return;
    }
    const int64_t now_ms = DynamicReplicationController::NowMs();
    // Only the keys a pending task was issued for are looked at, not every
    // object. Each is resolved again under its own lock: since it was indexed,
    // its task may have started or been dropped, or its object may be gone.
    for (const auto& [tenant_id, key] : dynamic_replication_.PendingKeys()) {
        MetadataAccessorRW accessor(this, ObjectIdentity{tenant_id, key},
                                    MetadataAccessorRW::AsIs{});
        if (!accessor.IsPublished() ||
            !accessor.GetState().dynamic_replication_pending) {
            dynamic_replication_.UnindexPendingLocked(tenant_id, key);
            continue;
        }
        auto& state = accessor.GetState();
        if (!dynamic_replication_.PendingExpiredLocked(state, now_ms)) {
            continue;
        }
        task_manager_.get_write_access().fail_task_if_pending(
            state.dynamic_replication_pending->task_id,
            "dynamic replica lease expired");
        dynamic_replication_.ClearPendingLocked(tenant_id, key, state);
    }
    dynamic_replication_.EraseExpiredLeases(std::chrono::system_clock::now());
}

tl::expected<ReplicaActionLease, ErrorCode>
MasterService::SubmitReplicaActionProposalLocked(
    const ReplicaActionProposal& proposal) {
    const int64_t now_ms = DynamicReplicationController::NowMs();
    if (auto valid = dynamic_replication_.ValidateProposal(proposal, now_ms);
        !valid) {
        return tl::make_unexpected(valid.error());
    }

    auto normalized_tenant_result =
        ResolveTenantIdForWrite(TenantId(proposal.tenant_id));
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                             proposal.key};

    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& state = accessor.GetState();
    auto existing_lease = dynamic_replication_.FindReusableLease(
        object_id.tenant_id, object_id.user_key, proposal, now_ms);
    if (!existing_lease) {
        return tl::make_unexpected(existing_lease.error());
    }
    if (existing_lease->has_value()) {
        return **existing_lease;
    }

    if (!dynamic_replication_.HeatAdmitted(object_id.tenant_id,
                                           object_id.user_key)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (auto cooled = dynamic_replication_.CheckCooldownLocked(state);
        !cooled) {
        return tl::make_unexpected(cooled.error());
    }

    if (accessor.InProcessing() || accessor.HasReplicationTask() ||
        dynamic_replication_.HasPendingLocked(object_id.tenant_id,
                                              object_id.user_key, state)) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    const uint64_t version_epoch =
        DynamicReplicationController::VersionEpoch(metadata);
    if (proposal.observed_version_epoch != 0 &&
        proposal.observed_version_epoch != version_epoch) {
        return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    }
    if (proposal.object_size_bytes != 0 &&
        proposal.object_size_bytes != metadata.TotalLength()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (metadata.DynamicReplicationRecreateBlocked(
            std::chrono::steady_clock::now())) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    std::optional<DynamicReplicationController::Plan> plan;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        plan = dynamic_replication_.SelectPlan(
            segment_access, metadata,
            [this](const Replica& replica) {
                return IsReplicaReadable(replica);
            },
            proposal.preferred_target_segment, proposal.target_domain);
    }
    if (!plan.has_value()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    auto lease = dynamic_replication_.IssueLeaseLocked(
        state, object_id.tenant_id, object_id.user_key, proposal, *plan,
        version_epoch, now_ms);

    auto task = SubmitDynamicReplicaCopyTask(object_id, *plan, lease.lease_id,
                                             version_epoch);
    if (!task.has_value()) {
        dynamic_replication_.ClearPendingLocked(object_id.tenant_id,
                                                object_id.user_key, state);
        return tl::make_unexpected(task.error());
    }

    lease.task_id = task.value();
    dynamic_replication_.CommitLeaseLocked(state, object_id.tenant_id,
                                           object_id.user_key, lease);
    return lease;
}

tl::expected<UUID, ErrorCode> MasterService::SubmitDynamicReplicaCopyTask(
    const ObjectIdentity& object_id,
    const DynamicReplicationController::Plan& plan, const UUID& lease_id,
    uint64_t version_epoch) {
    UUID source_client;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        ErrorCode error = segment_access.GetClientIdBySegmentName(
            plan.source_segment, source_client);
        if (error != ErrorCode::OK) {
            return tl::make_unexpected(error);
        }
    }
    const auto& liveness = plan.source_liveness;
    if (!liveness) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    // The proposal caller holds the object's key. Observe serving
    // eligibility without blocking on the Client transition mutex while that
    // key is held; offboarding cleanup is ordered after this key section.
    if (!liveness->IsServing()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    UUID current_source_client;
    if (segment_access.GetClientIdBySegmentName(
            plan.source_segment, current_source_client) != ErrorCode::OK ||
        current_source_client != source_client) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_COPY>(
            source_client,
            {.tenant_id = object_id.tenant_id.value(),
             .key = object_id.user_key,
             .source = plan.source_segment,
             .targets = {plan.target_segment},
             .dynamic_replication_lease_id_high = lease_id.first,
             .dynamic_replication_lease_id_low = lease_id.second,
             .dynamic_replication_version_epoch = version_epoch});
}

PromotionQueueResult MasterService::TryPushPromotionQueue(
    const ObjectIdentity& object_id, bool record_candidate, bool force) {
    if (!force && (!promotion_on_hit_ || !promotion_sketch_)) {
        return PromotionQueueResult::kDisabled;
    }
    const auto& key = object_id.user_key;
    const auto admission_key = object_id.tenant_id.MakeScopedKey(key);

    // Frequency gate: bump and compare against the threshold. The sketch
    // returns uint8_t (saturating at 255); promotion_admission_threshold_
    // is clamped into [1, 255] at config parse time (see master.cpp), so
    // direct comparison is well-defined and threshold=0 (which would
    // bypass the gate entirely since freq is uint8_t) cannot reach here.
    const uint8_t freq = force ? std::numeric_limits<uint8_t>::max()
                               : promotion_sketch_->increment(admission_key);
    if (!force && freq < promotion_admission_threshold_) {
        MasterMetricManager::instance().inc_promotion_rejected_frequency();
        return PromotionQueueResult::kFrequencyRejected;
    }

    // Watermark gate: don't promote if DRAM is already under eviction
    // pressure. The check is best-effort (state can change between this
    // sample and the actual allocation in PromotionAllocStart).
    const double used_ratio = segment_manager_.GetMemoryUsage().used_ratio();
    if (used_ratio >= eviction_high_watermark_ratio_) {
        MasterMetricManager::instance().inc_promotion_rejected_watermark();
        if (record_candidate && !force) {
            MetadataAccessorRW accessor(this, object_id);
            if (accessor.Exists()) {
                promotion_candidates_.RecordLocked(
                    object_id.tenant_id, key, accessor.GetState(), freq,
                    PromotionCandidateReason::kWatermark, ErrorCode::OK);
            }
        }
        return PromotionQueueResult::kWatermarkRejected;
    }

    // Acquire a fresh RW accessor for dedup, refcnt-pin, and task
    // record. Safe to call here because GetReplicaList has already released
    // its RO accessor.
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        VLOG(1) << "promotion_rejected key=" << key
                << " reason=not_found force=" << force;
        return PromotionQueueResult::kNotFound;
    }
    auto& metadata = accessor.Get();
    auto& state = accessor.GetState();

    // A primary Put/Upsert owns all PROCESSING replicas while the object is
    // processing. Promotion must not establish a second owner.
    if (accessor.InProcessing()) {
        promotion_candidates_.EraseLocked(object_id.tenant_id, key, state);
        VLOG(1) << "promotion_rejected key=" << key
                << " reason=primary_write_in_flight force=" << force;
        return PromotionQueueResult::kAlreadyInFlight;
    }

    // Dedup: don't queue twice if a promotion is already in flight or if a
    // MEMORY replica has appeared since GetReplicaList observed only-disk.
    auto& promotion_task = state.promotion_task;
    if (promotion_task) {
        // A read hit an already in-flight promotion: re-mark the queued
        // entry's recency so the next heartbeat delivers it ahead of stale
        // admissions. No-op if the heartbeat already took the entry (the
        // promotion is executing) or the holder's mailbox is gone.
        local_ssd_manager_.TouchPromotion(promotion_task->holder_id,
                                          object_id.tenant_id, key);
        promotion_candidates_.EraseLocked(object_id.tenant_id, key, state);
        VLOG(1) << "promotion_rejected key=" << key
                << " reason=promotion_in_flight force=" << force;
        return PromotionQueueResult::kAlreadyInFlight;
    }
    if (metadata.HasReplica(&Replica::fn_is_memory_replica)) {
        promotion_candidates_.EraseLocked(object_id.tenant_id, key, state);
        return PromotionQueueResult::kMemoryReplicaPresent;
    }

    // Find the LOCAL_DISK source replica.
    Replica* source = nullptr;
    metadata.VisitReplicas(&Replica::fn_is_local_disk_replica,
                           [&source](Replica& r) {
                               if (source == nullptr) source = &r;
                           });
    if (source == nullptr) {
        promotion_candidates_.EraseLocked(object_id.tenant_id, key, state);
        return PromotionQueueResult::kNoLocalDiskSource;
    }

    // Cap gate: read the cluster-wide in-flight count. Soft cap — a
    // benign TOCTOU race between this load and the emplace below can let
    // a few extra tasks slip in, but the key's lock already
    // serializes inserts on one key and the dedup gate above prevents
    // duplicate work, so the worst case is N concurrent inserters across
    // distinct keys each admitting one extra task. Atomic load is
    // relaxed because the value is purely advisory.
    if (promotion_in_flight_.load(std::memory_order_relaxed) >=
        promotion_queue_limit_) {
        MasterMetricManager::instance().inc_promotion_rejected_cap();
        if (record_candidate && !force) {
            promotion_candidates_.RecordLocked(
                object_id.tenant_id, key, state, freq,
                PromotionCandidateReason::kQueueCap, ErrorCode::OK);
        }
        return PromotionQueueResult::kQueueCapRejected;
    }

    // Pin the source replica.
    source->inc_refcnt();
    const uint64_t object_size =
        source->get_descriptor().get_local_disk_descriptor().object_size;

    // Try to enqueue on the holder client. On failure, drop the refcnt back.
    auto push_result = PushPromotionQueue(object_id, *source);
    if (!push_result) {
        source->dec_refcnt();
        VLOG(1) << "promotion_push_failed key=" << key
                << " error=" << push_result.error();
        if (push_result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
            promotion_candidates_.EraseLocked(object_id.tenant_id, key, state);
            return PromotionQueueResult::kAlreadyInFlight;
        }
        if (push_result.error() == ErrorCode::SEGMENT_NOT_FOUND ||
            push_result.error() == ErrorCode::INVALID_PARAMS) {
            promotion_candidates_.EraseLocked(object_id.tenant_id, key, state);
            return PromotionQueueResult::kNoLocalDiskSource;
        }
        if (record_candidate && !force) {
            promotion_candidates_.RecordLocked(
                object_id.tenant_id, key, state, freq,
                PromotionCandidateReason::kPushFailed, push_result.error());
        }
        return PromotionQueueResult::kPushFailed;
    }

    // Capture the holder client_id so NotifyPromotionSuccess can reject
    // calls from other clients. PushPromotionQueue already validated
    // get_local_disk_client_id() returns a value, so .value() is safe.
    const UUID holder_id = source->get_local_disk_client_id().value();

    // Record the in-flight task. alloc_id is filled in by
    // PromotionAllocStart once the new MEMORY replica is staged.
    // Propagate the execution-failure count across the candidate's
    // consumption so NotifyPromotionFailure can bound self-sustaining
    // execution-failure cycles; an absent candidate means a fresh chain (0).
    const uint32_t execution_failures =
        promotion_candidates_.ConsumeLocked(object_id.tenant_id, key, state);
    promotion_task =
        PromotionTask{.source_id = source->id(),
                      .alloc_id = 0,
                      .object_size = object_size,
                      .start_time = std::chrono::system_clock::now(),
                      .holder_id = holder_id,
                      .execution_failures = execution_failures};
    promotion_in_flight_.fetch_add(1, std::memory_order_relaxed);
    MasterMetricManager::instance().inc_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_admitted();
    VLOG(1) << "promotion_queued key=" << key << " size=" << object_size;
    return PromotionQueueResult::kQueued;
}

auto MasterService::RegisterPrefetchTask(const UUID& client_id,
                                         const std::string& key,
                                         const TenantId& tenant_id)
    -> tl::expected<void, ErrorCode> {
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();

    // A primary Put/Upsert owns all PROCESSING replicas while the object is
    // processing. Prefetch promotion must not establish a second owner.
    if (accessor.InProcessing()) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Already resident or already in flight (either a prefetch task or a
    // promotion-on-hit task): a normal outcome for best-effort prefetch,
    // reported distinctly so the caller can skip silently instead of
    // running the promotion chain into a guaranteed failure.
    auto& promotion_task = accessor.GetState().promotion_task;
    if (metadata.HasReplica(&Replica::fn_is_memory_replica) ||
        promotion_task.has_value()) {
        return tl::make_unexpected(ErrorCode::PROMOTION_ALREADY_EXISTS);
    }

    // Find a COMPLETE LOCAL_DISK source replica. A still-PROCESSING disk
    // replica (offload in flight) is not readable yet.
    Replica* source = nullptr;
    metadata.VisitReplicas(&Replica::fn_is_local_disk_replica,
                           [&source](Replica& r) {
                               if (source == nullptr && r.is_completed()) {
                                   source = &r;
                               }
                           });
    if (source == nullptr) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Holder-only gate, mirroring PromotionAllocStart: the client executing
    // the promotion must own the source LOCAL_DISK replica.
    auto holder_id = source->get_local_disk_client_id();
    if (!holder_id.has_value() || holder_id.value() != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Shared cap with promotion-on-hit (advisory TOCTOU, same as the
    // admission path's soft cap).
    if (promotion_in_flight_.load(std::memory_order_relaxed) >=
        promotion_queue_limit_) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }

    // Pin the source replica, then record the in-flight task. alloc_id is
    // filled in by PromotionAllocStart once the new MEMORY replica is
    // staged. No promotion mailbox push: the prefetch caller drives the
    // execution chain directly.
    //
    // Propagate the execution-failure count across the candidate's
    // consumption, same contract as TryPushPromotionQueue: without it a
    // persistently failing hot key would cycle prefetch -> fail -> on-hit
    // retry -> re-record -> prefetch with the breaker reset to zero every
    // time a probe re-triggers.
    source->inc_refcnt();
    const uint64_t object_size =
        source->get_descriptor().get_local_disk_descriptor().object_size;
    const uint32_t execution_failures = promotion_candidates_.ConsumeLocked(
        object_id.tenant_id, object_id.user_key, accessor.GetState());
    promotion_task =
        PromotionTask{.source_id = source->id(),
                      .alloc_id = 0,
                      .object_size = object_size,
                      .start_time = std::chrono::system_clock::now(),
                      .holder_id = holder_id.value(),
                      .execution_failures = execution_failures,
                      .from_prefetch = true};
    promotion_in_flight_.fetch_add(1, std::memory_order_relaxed);
    MasterMetricManager::instance().inc_promotion_in_flight();
    VLOG(1) << "prefetch_task_registered key=" << object_id.user_key
            << " size=" << object_size;
    return {};
}

auto MasterService::PromotionObjectHeartbeat(const UUID& client_id)
    -> tl::expected<std::vector<PromotionTaskItem>, ErrorCode> {
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    // Return at most promotion_max_per_heartbeat_ tasks. Each task does
    // a synchronous SSD read + RDMA write on the client side; allowing
    // more than one per heartbeat risks blocking past the client-
    // liveness window and the master marking the client dead. The rest
    // stay queued in the mailbox for subsequent heartbeats. The
    // cap must live here (server side) rather than on the client so
    // leftover work isn't silently dropped.
    return local_ssd_manager_.TakePromotions(client_id,
                                             promotion_max_per_heartbeat_);
}

auto MasterService::PromotionAllocStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    uint64_t size, const std::vector<std::string>& preferred_segments)
    -> tl::expected<PromotionAllocStartResponse, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();

    if (accessor.InProcessing()) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Verify the in-flight task still exists before allocating. The
    // reaper can sweep it between the holder's heartbeat and this
    // AllocStart call (a hung client, GC pause, or HA failover can
    // stall AllocStart past put_start_release_timeout_sec_). If we
    // allocated and AddReplicas'd anyway, the staged PROCESSING MEMORY
    // replica would have no PromotionTask pointing at it: the generic
    // PROCESSING reaper only reclaims objects marked processing (which
    // promotion never sets) and the promotion-task reaper would have
    // nothing left to reclaim, leaking the buffer until the object is
    // removed or evicted. The key's lock is held for the rest of this
    // function, so the task stays in place across the allocation step.
    auto& promotion_task = accessor.GetState().promotion_task;
    if (!promotion_task) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (promotion_task->holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Defensive size check: must match the source LOCAL_DISK
    // descriptor's object_size captured at admission. A mismatch would
    // let a buggy caller request a wrong-sized allocation — smaller
    // risks RDMA overflow, larger wastes DRAM pinned until reaper TTL.
    if (promotion_task->object_size != size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (metadata.HasReplica(&Replica::fn_is_memory_replica)) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    if (promotion_task->alloc_id != 0) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    if (auto grown = accessor.Grow(size); !grown) {
        return tl::make_unexpected(grown.error());
    }

    // Allocate a single MEMORY replica via the existing strategy, biased to
    // the holder's mem segment when possible.
    ReplicateConfig config;
    config.replica_num = 1;
    if (!preferred_segments.empty()) {
        config.preferred_segments = preferred_segments;
    }

    std::vector<Replica> staged_replicas;
    // Allocation is guarded by the selected memory Segment owner. Release the
    // holder guard first so a co-located holder cannot self-deadlock and two
    // Clients cannot form a source/target lock cycle. The holder operation was
    // already admitted and its key stays locked until the staged
    // replica and task state are committed or rolled back.
    serving_guard.reset();
    AllocatorManager allocator_snapshot;
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        allocator_snapshot = allocator_access.SnapshotAllocatorManager();
    }
    auto allocation_result = allocation_strategy_->Allocate(
        allocator_snapshot, size, config.replica_num, preferred_segments);
    if (!allocation_result) {
        return tl::make_unexpected(allocation_result.error());
    }
    staged_replicas = std::move(allocation_result.value());
    if (staged_replicas.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Append the new PROCESSING MEMORY replica to the existing object's
    // metadata. Visible only after NotifyPromotionSuccess flips it COMPLETE.
    Replica::Descriptor desc;
    if (!staged_replicas[0].getDescriptorIfAvailable(desc)) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    const ReplicaID new_id = staged_replicas[0].id();
    std::vector<Replica> to_add;
    to_add.push_back(std::move(staged_replicas[0]));
    metadata.AddReplicas(std::move(to_add));

    // Record the new replica's ID on the in-flight PromotionTask so
    // NotifyPromotionSuccess knows exactly which replica to commit. A
    // concurrent Put on this key may stage other PROCESSING MEMORY
    // replicas; using alloc_id avoids the "first PROCESSING memory"
    // ambiguity.
    //
    // Also reset start_time so the reaper TTL covers the active-
    // transfer phase (AllocStart -> SSD read -> RDMA write -> Notify)
    // measured from when a master-allocated buffer becomes vulnerable,
    // rather than being consumed by queue-waiting. Without the reset,
    // a backlogged task could enter active transfer with little TTL
    // remaining and the reaper could erase the staged replica by its
    // allocation id mid-RDMA-write. The queue-waiting phase
    // (alloc_id == 0) is bounded by its own original start_time window
    // during which the reaper's erase-by-allocation branch is a no-op.
    promotion_task->alloc_id = new_id;
    promotion_task->start_time = std::chrono::system_clock::now();
    return PromotionAllocStartResponse{std::move(desc)};
}

auto MasterService::NotifyPromotionSuccess(const UUID& client_id,
                                           const std::string& key,
                                           const TenantId& tenant_id)
    -> tl::expected<void, ErrorCode> {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);

    // Slot 归属校验（与 PromotionAllocStart 对称）：staged 副本与
    // promotion_tasks 条目只存在于 owner master，非 owner 收到 Success
    // 通知时不应触碰任何状态。单 master 模式放行，行为不变。
    {
        const uint16_t slot =
            cvm::KeySlot(object_id.tenant_id, object_id.user_key);
        const ErrorCode svc = CheckSlotServiceability(slot);
        if (svc != ErrorCode::OK) {
            LOG(WARNING) << "NotifyPromotionSuccess: rejected non-owned slot"
                         << " key=" << key << " slot=" << (int)slot
                         << " error=" << svc << " client=" << client_id;
            return tl::make_unexpected(svc);
        }
    }

    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();
    const auto previous_kv_media = KvMediaSnapshot(metadata);

    // Look up the in-flight task to find the exact replica we staged. A
    // concurrent Put on this key may have created other PROCESSING MEMORY
    // replicas, so we must not just "mark first PROCESSING memory
    // complete" — that would risk committing someone else's half-written
    // replica.
    auto& promotion_task = accessor.GetState().promotion_task;
    if (!promotion_task || promotion_task->alloc_id == 0) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (promotion_task->holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const bool from_prefetch = promotion_task->from_prefetch;

    bool committed = false;
    Replica* staged = metadata.GetReplicaByID(promotion_task->alloc_id);
    if (staged != nullptr && staged->is_memory_replica() &&
        staged->is_processing()) {
        std::optional<OrderedOpLogWriter::Reservation> batch_reservation;
        if (enable_ha_ && enable_oplog_) {
            auto reservation = ReserveBatchOpLogSlot();
            if (!reservation) {
                return tl::make_unexpected(reservation.error());
            }
            batch_reservation = std::move(reservation.value());
        }
        staged->mark_complete();
        committed = true;
        if (enable_oplog_ && ordered_oplog_writer_) {
            std::vector<Replica::Descriptor> post;
            metadata.VisitReplicas(&Replica::fn_is_completed,
                                   [&post](const Replica& replica) {
                                       post.push_back(replica.get_descriptor());
                                   });

            const auto payload =
                SerializeMetadataForOpLogFromReplicaDescriptors(metadata, post);
            if (batch_reservation) {
                auto persist_result = AppendReservedOpLogWithDurableFinalize(
                    std::move(*batch_reservation), OpType::PUT_END,
                    tenant_id.value(), key, payload, nullptr);
                if (!persist_result) {
                    LOG(WARNING)
                        << "NotifyPromotionSuccess: PUT_END persist failed "
                        << "for key=" << key
                        << ", err=" << static_cast<int>(persist_result.error());
                }
            } else {
                auto persist_result = AppendOpLogVisibleBeforeDurable(
                    OpType::PUT_END, tenant_id.value(), key, payload);
                if (!persist_result) {
                    LOG(WARNING)
                        << "NotifyPromotionSuccess: PUT_END persist failed "
                        << "for key=" << key
                        << ", err=" << static_cast<int>(persist_result.error());
                }
            }
        }
    }

    // Drop the source LOCAL_DISK replica's refcnt and erase the task.
    auto* source = metadata.GetReplicaByID(promotion_task->source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    const uint64_t completed_bytes = promotion_task->object_size;
    promotion_task.reset();
    promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
    MasterMetricManager::instance().dec_promotion_in_flight();
    if (committed) {
        SyncCacheTotalAccounting(metadata);
        MasterMetricManager::instance().inc_promotion_completed();
        MasterMetricManager::instance().inc_promotion_completed_bytes(
            static_cast<int64_t>(completed_bytes));
    } else {
        MasterMetricManager::instance().inc_promotion_cancelled();
    }

    // Erase the per-client promotion mailbox entry (best-effort; the
    // heartbeat may have already drained it).
    local_ssd_manager_.RemovePromotion(client_id, object_id.tenant_id,
                                       object_id.user_key);

    if (!committed) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    SyncKvObjectState(key, metadata, object_id.tenant_id, previous_kv_media);
    if (from_prefetch) {
        // Prefetch-promoted keys get the same read lease as exist/get so the
        // DRAM replica survives until the follow-up get() arrives.
        metadata.GrantReadLease(
            std::chrono::milliseconds(default_kv_lease_ttl_));
    }
    return {};
}

auto MasterService::NotifyPromotionFailure(const UUID& client_id,
                                           const std::string& key,
                                           const TenantId& tenant_id)
    -> tl::expected<void, ErrorCode> {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);

    // Slot 归属校验（与 PromotionAllocStart 对称）。注意 Failure 通知的
    // 语义是幂等释放：非 owner 上本就没有任务条目，直接拒绝（而非 OK）
    // 以暴露路由错投；worker 按 source master 定向重试。
    // 单 master 模式放行，行为不变。
    {
        const uint16_t slot =
            cvm::KeySlot(object_id.tenant_id, object_id.user_key);
        const ErrorCode svc = CheckSlotServiceability(slot);
        if (svc != ErrorCode::OK) {
            LOG(WARNING) << "NotifyPromotionFailure: rejected non-owned slot"
                         << " key=" << key << " slot=" << (int)slot
                         << " error=" << svc << " client=" << client_id;
            return tl::make_unexpected(svc);
        }
    }

    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();

    auto& promotion_task = accessor.GetState().promotion_task;
    if (!promotion_task) {
        // No task to release. Either the reaper already swept it, or the
        // client never had a task here. Return OK to keep this RPC
        // idempotent — repeated failure notifications on the same key
        // should be safe.
        return {};
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (promotion_task->holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Mirror the reaper's expiry path; see DiscardExpiredProcessingReplicas
    // Part 4 for the full rationale on each step.
    auto* source = metadata.GetReplicaByID(promotion_task->source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    if (promotion_task->alloc_id != 0) {
        const ReplicaID alloc_id = promotion_task->alloc_id;
        EraseReplicasWithCacheTotalAccounting(
            metadata, [alloc_id](const Replica& replica) {
                return replica.id() == alloc_id;
            });
    }
    // Capture the chain's execution-failure count BEFORE erasing the task
    // (the iterator is invalidated by the erase).
    const uint32_t prior_failures = promotion_task->execution_failures;
    const bool from_prefetch = promotion_task->from_prefetch;
    promotion_task.reset();
    promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
    MasterMetricManager::instance().dec_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_failed();

    // A transient execution failure (DRAM pressure at AllocStart, a TE write
    // flake, SSD throttling) must not silently kill the promotion: re-record
    // a retry candidate so the eviction-thread retry loop re-queues it with
    // backoff. Bounded by PromotionCandidateTracker::kMaxRetries /
    // kTtl, and the retry path erases the candidate on
    // permanent conditions (not-found, memory-present, no-local-disk-source),
    // so this cannot spin. Record the current estimate via count()
    // (read-only), NOT increment(): an executor-side failure is not demand
    // signal, and bumping the sketch here would pollute the frequency signal
    // the admission gate relies on.
    //
    // The self-sustaining cycle is additionally bounded by
    // kMaxPromotionExecutionFailures: without it, a persistently-failing key
    // (e.g. a broken SSD file that still has a LOCAL_DISK replica) would
    // re-record -> re-admit -> fail -> re-record forever, monopolizing
    // delivery slots with no read demand. Once the bound is hit we stop
    // re-recording — a genuine read can still re-admit the key (fresh chain).
    // from_prefetch tasks skip this re-record: prefetch retries are driven
    // by the client-side throttle (probe re-trigger after its backoff), not
    // by the on-hit retry pipeline. With promotion_on_hit off the sweeper
    // never runs, so a recorded candidate would leak (and pin the tenant
    // state) permanently.
    if (!from_prefetch &&
        !metadata.HasReplica(&Replica::fn_is_memory_replica) &&
        metadata.HasReplica(&Replica::fn_is_local_disk_replica)) {
        if (prior_failures >= kMaxPromotionExecutionFailures) {
            LOG(WARNING) << "promotion_execution_gave_up key="
                         << object_id.user_key
                         << " failures=" << prior_failures;
            MasterMetricManager::instance().inc_promotion_execution_gave_up();
        } else {
            const auto admission_key =
                object_id.tenant_id.MakeScopedKey(object_id.user_key);
            const uint8_t freq =
                promotion_sketch_ ? promotion_sketch_->count(admission_key) : 0;
            promotion_candidates_.RecordLocked(
                object_id.tenant_id, object_id.user_key, accessor.GetState(),
                freq, PromotionCandidateReason::kExecutionFailed, ErrorCode::OK,
                prior_failures + 1);
        }
    }

    // Clear the holder's per-client promotion mailbox entry. Same
    // best-effort cleanup pattern as NotifyPromotionSuccess — the
    // heartbeat may have already drained it.
    local_ssd_manager_.RemovePromotion(client_id, object_id.tenant_id,
                                       object_id.user_key);

    return {};
}

void MasterService::EvictionThreadFunc() {
    VLOG(1) << "action=eviction_thread_started";

    auto last_discard_time = std::chrono::system_clock::now();
    auto next_dfs_eviction_time = std::chrono::steady_clock::now();
    while (eviction_running_) {
        // RFC #4700 slice 1: eviction, tenant watermark eviction, DFS/NoF
        // eviction, expired-processing discard and promotion retry all
        // mutate business metadata, so none of them runs while a promotion
        // restore is in flight.
        if (!AdmitsBusinessWorkers()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kEvictionThreadSleepMs));
            continue;
        }

        const auto now = std::chrono::system_clock::now();
        double used_ratio = segment_manager_.GetMemoryUsage().used_ratio();
        if (used_ratio > eviction_high_watermark_ratio_ ||
            (need_mem_eviction_ && eviction_ratio_ > 0.0)) {
            LOG(INFO) << "[EVICT-TRIGGER] memory_ratio=" << used_ratio
                      << " high_watermark=" << eviction_high_watermark_ratio_
                      << " need_mem_eviction=" << need_mem_eviction_
                      << " eviction_ratio=" << eviction_ratio_;
            double evict_ratio_target = std::max(
                eviction_ratio_,
                used_ratio - eviction_high_watermark_ratio_ + eviction_ratio_);
            double evict_ratio_lowerbound =
                std::max(evict_ratio_target * 0.5,
                         used_ratio - eviction_high_watermark_ratio_);
            if (io_pattern_runtime_) {
                io_pattern_runtime_->RecordStorageMetric(
                    {.source_id = "master-memory",
                     .tier = io_pattern::CacheTier::kL1Host,
                     .memory_used_ratio = static_cast<float>(used_ratio)});
                const auto capacity = std::max<int64_t>(
                    0,
                    MasterMetricManager::instance().get_total_mem_capacity());
                const auto status = io_pattern_runtime_->Execute(
                    io_pattern::CacheTier::kL1Host,
                    static_cast<uint64_t>(evict_ratio_target * capacity), {});
                // A shortfall is already handled inside the runtime's eviction
                // handler (RunLegacyEvictionFallback), which also covers the
                // report-driven worker; running BatchEvict again here would
                // evict twice for one watermark breach.
                (void)status;
            } else {
                BatchEvict(evict_ratio_target, evict_ratio_lowerbound);
            }
            LOG(INFO) << "[EVICT-DONE] eviction execution completed.";
            last_discard_time = now;
        } else if (now - last_discard_time > put_start_release_timeout_sec_) {
            // Try discarding expired processing keys and ongoing replication
            // tasks if we have not done this for a long time.
            {
                SpDiag::PerfPoint pt_discard(PerfKey::MASTER_BG_DISCARD_EXPIRED,
                                             SpDiag::PerfLevel::MODULE);
                pt_discard.Start();
                std::shared_lock<std::shared_mutex> shared_lock(
                    master_state_mutex_);
                VisitNamespaces([&](const TenantId& /*tenant_id*/,
                                    metadata::Namespace& ns) {
                    DiscardExpiredProcessingReplicas(ns, now);
                });
                ReleaseExpiredDiscardedReplicas(now);
                pt_discard.End(0);
            }
            last_discard_time = now;
        }

        // The namespace policy's own round, such as a tenant over its OWN
        // watermark while the pool is still under the pool-wide one, so it is
        // not gated on the pool-level condition having fired.
        if (policy_ != nullptr) {
            policy_->OnMaintenance();
        }

#ifdef USE_NOF
        double nof_used_ratio = nof_segment_manager_.GetUsage().used_ratio();
        if (nof_used_ratio > nof_eviction_high_watermark_ratio_ ||
            (need_nof_eviction_ && nof_eviction_ratio_ > 0.0)) {
            double nof_evict_ratio_target =
                std::max(nof_eviction_ratio_,
                         nof_used_ratio - nof_eviction_high_watermark_ratio_ +
                             nof_eviction_ratio_);
            double nof_evict_ratio_lowerbound =
                std::max(nof_evict_ratio_target * 0.5,
                         nof_used_ratio - nof_eviction_high_watermark_ratio_);
            NoFBatchEvict(nof_evict_ratio_target, nof_evict_ratio_lowerbound);
        }
#endif

        if ((dfs_allocator_ && dfs_allocator_->IsEvictionEnabled()) ||
            dfs_kvcs_capacity_provider_) {
            const auto steady_now = std::chrono::steady_clock::now();
            if (steady_now >= next_dfs_eviction_time) {
                const auto interval =
                    dfs_kvcs_capacity_provider_
                        ? dfs_kvcs_capacity_check_interval_
                        : dfs_allocator_->GetEvictionCheckInterval();
                if (dfs_kvcs_capacity_provider_) {
                    kvcs_capacity_refresh_worker_.Schedule();
                } else {
                    RunDfsEviction();
                }
                next_dfs_eviction_time =
                    std::chrono::steady_clock::now() + interval;
            }
        }

        if (!promotion_candidates_.Empty()) {
            RunPromotionCandidateRetry();
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kEvictionThreadSleepMs));
    }

    VLOG(1) << "action=eviction_thread_stopped";
}

void MasterService::DiscardExpiredProcessingReplicas(
    const metadata::Namespace& ns,
    const std::chrono::system_clock::time_point& now) {
    std::list<DiscardedReplicas> discarded_replicas;
    const TenantId& tenant_id = ns.id();

    // Every task of the tenant lives on its own object, and the namespace
    // lists the keys whose object carries one, so each pass below walks that
    // list and holds one key at a time. Each key is resolved again as it is
    // held: its work may have finished, or its object been torn down or
    // replaced, since the list was taken, and a pass acts on the object it
    // holds now.
    const auto keys = ns.InFlightKeys();

    for (const auto& key : keys) {
        MetadataAccessorRW accessor(this, ObjectIdentity{tenant_id, key},
                                    MetadataAccessorRW::AsIs{});
        if (!accessor.InProcessing()) {
            continue;
        }

        auto& metadata = accessor.Get();
        if (!metadata.IsValid() ||
            metadata.AllReplicas(&Replica::fn_is_completed)) {
            metadata.ClearPendingSoftPinIfNoViableReplica();
            metadata.ClearSessionTagIfNoViableReplica();
            if (!metadata.IsValid()) {
                accessor.Erase();
            } else {
                accessor.EraseFromProcessing();
            }
            continue;
        }

        const auto ttl =
            metadata.put_start_time + put_start_release_timeout_sec_;
        if (ttl < now) {
            const route::ObjectRef object = accessor.GetRef();
            const bool had_complete_replica =
                metadata.HasReplica(&Replica::fn_is_completed);
            // Predict post-discard descriptors WITHOUT mutating: drop
            // PROCESSING replicas; keep COMPLETE replicas.
            auto post_descriptors = BuildRemainingReplicaDescriptors(
                metadata, &Replica::fn_is_processing);
            const bool would_invalidate = post_descriptors.empty();

            if (had_complete_replica && enable_oplog_ &&
                ordered_oplog_writer_) {
                // The discard is applied once it is durable.
                auto persist_result = AppendOpLogWithDurableFinalize(
                    would_invalidate ? OpType::REMOVE : OpType::PUT_END,
                    tenant_id.value(), key,
                    would_invalidate
                        ? std::string{}
                        : SerializeMetadataForOpLogFromReplicaDescriptors(
                              metadata, post_descriptors),
                    [this, object, ttl](const OpLogEntry& durable_entry) {
                        FinalizeExpiredProcessingReplicasAfterDurable(
                            object, durable_entry, ttl);
                    });
                if (!persist_result) {
                    LOG(WARNING)
                        << "DiscardExpiredProcessingReplicas: "
                           "OpLog persist failed for key="
                        << key
                        << ", err=" << static_cast<int>(persist_result.error())
                        << ", deferring discard";
                }
                continue;
            }

            // HA disabled or never published — apply.
            auto replicas = metadata.PopReplicas(&Replica::fn_is_processing);
            metadata.ClearPendingSoftPinIfNoViableReplica();
            metadata.ClearSessionTagIfNoViableReplica();
            if (!replicas.empty()) {
                FreeDfsReplicas(key, replicas);
                discarded_replicas.emplace_back(std::move(replicas), ttl);
            }
            if (!metadata.IsValid()) {
                accessor.Erase();
            } else {
                accessor.EraseFromProcessing();
            }
        }
    }

    for (const auto& key : keys) {
        MetadataAccessorRW accessor(this, ObjectIdentity{tenant_id, key},
                                    MetadataAccessorRW::AsIs{});
        if (!accessor.HasReplicationTask()) {
            continue;
        }
        auto& task = accessor.GetState().replication_task;

        const auto ttl = task->start_time + put_start_release_timeout_sec_;
        if (ttl > now || task->durable_cleanup_pending) {
            continue;
        }

        auto& metadata = accessor.Get();

        const bool had_complete_replica =
            metadata.HasReplica(&Replica::fn_is_completed);
        auto& replica_ids = task->replica_ids;

        const auto target_pred = [&replica_ids](const Replica& r) {
            return std::find(replica_ids.begin(), replica_ids.end(), r.id()) !=
                   replica_ids.end();
        };
        // Predict post-discard descriptor list WITHOUT mutating: drop
        // task target replicas; keep the rest of the COMPLETE replicas.
        auto post_descriptors =
            BuildRemainingReplicaDescriptors(metadata, target_pred);
        const bool would_invalidate = post_descriptors.empty();

        if (had_complete_replica && enable_oplog_ && ordered_oplog_writer_) {
            // The discard is applied once it is durable.
            task->durable_cleanup_pending = true;
            auto persist_result = AppendOpLogWithDurableFinalize(
                would_invalidate ? OpType::REMOVE : OpType::PUT_END,
                tenant_id.value(), key,
                would_invalidate
                    ? std::string{}
                    : SerializeMetadataForOpLogFromReplicaDescriptors(
                          metadata, post_descriptors),
                [this, source_id = task->source_id, target_ids = replica_ids,
                 dynamic_lease_id = task->dynamic_replication_lease_id,
                 dynamic_version_epoch =
                     task->dynamic_replication_version_epoch,
                 ttl](const OpLogEntry& durable_entry) {
                    FinalizeExpiredReplicationTaskAfterDurable(
                        durable_entry, source_id, target_ids, dynamic_lease_id,
                        dynamic_version_epoch, ttl);
                });
            if (!persist_result) {
                LOG(WARNING)
                    << "DiscardExpiredProcessingReplicas: OpLog persist "
                       "failed for replication task key="
                    << key
                    << ", err=" << static_cast<int>(persist_result.error())
                    << ", deferring discard";
                task->durable_cleanup_pending = false;
            }
            continue;
        }

        auto source = metadata.GetReplicaByID(task->source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }

        auto replicas =
            PopReplicasWithCacheTotalAccounting(metadata, target_pred);
        std::vector<ReplicaID> erased_replica_ids;
        erased_replica_ids.reserve(replicas.size());
        for (const auto& replica : replicas) {
            erased_replica_ids.push_back(replica.id());
        }
        const bool dynamic_task =
            task->dynamic_replication_lease_id != UUID{} ||
            task->dynamic_replication_version_epoch != 0;
        dynamic_replication_.RecordReplicaRemoval(metadata, erased_replica_ids);
        if (!replicas.empty()) {
            FreeDfsReplicas(key, replicas);
            discarded_replicas.emplace_back(std::move(replicas), ttl);
        }
        if (dynamic_task) {
            dynamic_replication_.ClearPendingLocked(tenant_id, key,
                                                    accessor.GetState());
        }
        if (!metadata.IsValid()) {
            accessor.Erase();
        } else {
            task.reset();
        }
    }

    for (const auto& key : keys) {
        MetadataAccessorRW accessor(this, ObjectIdentity{tenant_id, key},
                                    MetadataAccessorRW::AsIs{});
        if (!accessor.IsPublished()) {
            continue;
        }
        auto& task = accessor.GetState().offloading_task;
        if (!task) {
            continue;
        }
        const auto ttl = task->start_time + put_start_release_timeout_sec_;
        if (ttl > now) {
            continue;
        }
        auto source = accessor.Get().GetReplicaByID(task->source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }
        LOG(WARNING) << "Offloading task expired for key: " << key
                     << " tenant=" << tenant_id;
        task.reset();
    }

    for (const auto& key : keys) {
        MetadataAccessorRW accessor(this, ObjectIdentity{tenant_id, key},
                                    MetadataAccessorRW::AsIs{});
        if (!accessor.IsPublished()) {
            continue;
        }
        auto& metadata = accessor.Get();
        auto& task = accessor.GetState().promotion_task;
        if (!task) {
            continue;
        }
        const auto ttl = task->start_time + put_start_release_timeout_sec_;
        if (ttl > now) {
            continue;
        }
        auto source = metadata.GetReplicaByID(task->source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }
        if (task->alloc_id != 0) {
            const ReplicaID alloc_id = task->alloc_id;
            EraseReplicasWithCacheTotalAccounting(
                metadata, [alloc_id](const Replica& replica) {
                    return replica.id() == alloc_id;
                });
        }
        LOG(WARNING) << "Promotion task expired for key: " << key;
        task.reset();
        promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
        MasterMetricManager::instance().dec_promotion_in_flight();
        MasterMetricManager::instance().inc_promotion_expired();
    }

    if (!discarded_replicas.empty()) {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.splice(discarded_replicas_.end(),
                                   std::move(discarded_replicas));
    }
}

uint64_t MasterService::ReleaseExpiredDiscardedReplicas(
    const std::chrono::system_clock::time_point& now) {
    uint64_t released_cnt = 0;
    std::lock_guard lock(discarded_replicas_mutex_);
    discarded_replicas_.remove_if(
        [&now, &released_cnt](const DiscardedReplicas& item) {
            const bool expired = item.isExpired(now);
            if (expired && item.memSize() > 0) {
                released_cnt++;
            }
            return expired;
        });
    return released_cnt;
}

MasterService::NamespaceEvictionResult MasterService::EvictNamespaceMemory(
    const TenantId& tenant_id, uint64_t target_bytes) {
    if (target_bytes == 0) {
        return {};
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const eviction::Report report = memory_eviction_->evictor.Run(
        tenant_id, eviction::ByteGoal{.target_bytes = target_bytes});
    return {.freed_bytes = report.released_bytes,
            .evicted_objects = report.evicted_objects};
}

void MasterService::BatchEvict(double evict_ratio_target,
                               double evict_ratio_lowerbound) {
    // Consume the allocation-pressure signal before this round. A new failure
    // during the round remains visible to the next iteration.
    const bool allocation_pressure =
        need_mem_eviction_.exchange(false, std::memory_order_relaxed);

    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    // Writes that expired before they finished, and replicas whose release
    // was deferred, go first, so the round judges what is left.
    const auto now = std::chrono::system_clock::now();
    VisitNamespaces(
        [&](const TenantId& /*tenant_id*/, metadata::Namespace& ns) {
            DiscardExpiredProcessingReplicas(ns, now);
        });
    const uint64_t released_discarded = ReleaseExpiredDiscardedReplicas(now);
    const eviction::Report report = memory_eviction_->evictor.Run(
        eviction::Scope::All(),
        eviction::ObjectGoal{.target_ratio = evict_ratio_target,
                             .min_ratio = evict_ratio_lowerbound});
    const bool made_progress = report.MadeProgress() || released_discarded > 0;
    if (allocation_pressure && !report.stopped && !made_progress &&
        report.filed > 0) {
        need_mem_eviction_.store(true, std::memory_order_relaxed);
    }

    // Counted here, not by the evictor's observers: releasing discarded
    // replicas is progress too, and only the round sees it. A round with
    // nothing filed and nothing released had nothing to succeed or fail at.
    auto& metrics = MasterMetricManager::instance();
    if (made_progress) {
        const auto objects = static_cast<int64_t>(report.evicted_objects);
        const auto bytes = static_cast<int64_t>(report.released_bytes);
        metrics.inc_eviction_success(objects, bytes);
        metrics.inc_mem_eviction_success(objects, bytes);
    } else if (report.filed > 0) {
        metrics.inc_eviction_fail();
        metrics.inc_mem_eviction_fail();
    }

    }

void MasterService::NoFBatchEvict(double evict_ratio_target,
                                  double evict_ratio_lowerbound) {
    SpDiag::PerfPoint pt_nof_evict(PerfKey::MASTER_BG_NOF_BATCH_EVICT,
                                   SpDiag::PerfLevel::KEY_MODULE);
    pt_nof_evict.Start();

    if (evict_ratio_target < evict_ratio_lowerbound) {
        MC_LOG(ERROR) << "nof_evict_ratio_target=" << evict_ratio_target
                      << ", nof_evict_ratio_lowerbound="
                      << evict_ratio_lowerbound << ", error=invalid_params";
        evict_ratio_lowerbound = evict_ratio_target;
    }

    auto now = std::chrono::system_clock::now();
    long evicted_count = 0;
    long object_count = 0;
    uint64_t total_freed_size = 0;
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);

    auto is_evictable_nof_replica = [](const Replica& replica) {
        return replica.is_nof_replica() && replica.is_completed() &&
               replica.get_refcnt() == 0;
    };
    auto may_evict = [&](const ObjectMetadata& metadata) {
        return !weight_manager_.IsManagedGroup(metadata.group_id) &&
               !metadata.IsHardPinned() && metadata.IsLeaseExpired(now) &&
               !IsSoftPinActive(metadata, now) &&
               metadata.HasReplica(is_evictable_nof_replica);
    };

    // The target is a share of the whole population, so every tenant is
    // counted before any object is evicted. The walk only names the objects
    // to consider; each is checked again under its own lock below. The list
    // is rotated to a random start, so that the objects a walk reaches first
    // are not the ones every round prefers.
    std::vector<std::pair<TenantId, route::ObjectRef>> candidates;
    VisitNamespaces([&](const TenantId& tenant_id, metadata::Namespace& ns) {
        DiscardExpiredProcessingReplicas(ns, now);
        for (auto object : ns.objects.ReadCursor()) {
            object_count++;
            if (may_evict(object.metadata())) {
                candidates.emplace_back(tenant_id, object.ref());
            }
        }
    });
    const long ideal_evict_num =
        static_cast<long>(std::ceil(object_count * evict_ratio_target));
    if (!candidates.empty()) {
        std::rotate(candidates.begin(),
                    candidates.begin() + randomIndex(candidates.size()),
                    candidates.end());
    }

    for (const auto& [tenant_id, object] : candidates) {
        if (evicted_count >= ideal_evict_num) {
            break;
        }
        const std::string& key = object.key;
        MetadataAccessorRW accessor(this, tenant_id, object);
        if (!accessor.IsPublished()) {
            continue;
        }
        auto& metadata = accessor.Get();
        if (!may_evict(metadata)) {
            continue;
        }

        // HA strong consistency: persist BEFORE erasing NoF replicas.
        // Skip the key on persist failure.
        if (enable_oplog_ && ordered_oplog_writer_) {
            auto remaining = BuildRemainingReplicaDescriptors(
                metadata, is_evictable_nof_replica);
            auto reservation = ReserveBatchOpLogSlot();
            if (!reservation) {
                LOG(WARNING)
                    << "NoFBatchEvict: OpLog reservation failed "
                       "for key="
                    << key << ", err=" << static_cast<int>(reservation.error())
                    << ", skipping eviction";
                continue;
            }
            std::vector<ReplicaID> removed_ids;
            metadata.VisitReplicas(is_evictable_nof_replica,
                                   [&removed_ids](Replica& replica) {
                                       removed_ids.push_back(replica.id());
                                       replica.mark_removed();
                                   });
            const size_t removed_count = removed_ids.size();
            tl::expected<OpLogEntry, ErrorCode> persist_result;
            if (remaining.empty()) {
                persist_result = AppendReservedOpLogWithDurableFinalize(
                    std::move(reservation.value()), OpType::REMOVE,
                    tenant_id.value(), key, {},
                    [this, removed_ids = std::move(removed_ids)](
                        const OpLogEntry& durable_entry) {
                        FinalizeRemovedReplicasAfterDurable(durable_entry,
                                                            removed_ids);
                    });
            } else {
                persist_result = AppendReservedOpLogWithDurableFinalize(
                    std::move(reservation.value()), OpType::PUT_END,
                    tenant_id.value(), key,
                    SerializeMetadataForOpLogFromReplicaDescriptors(metadata,
                                                                    remaining),
                    [this, removed_ids = std::move(removed_ids)](
                        const OpLogEntry& durable_entry) {
                        FinalizeRemovedReplicasAfterDurable(durable_entry,
                                                            removed_ids);
                    });
            }
            if (!persist_result) {
                LOG(WARNING)
                    << "NoFBatchEvict: OpLog persist failed for key=" << key
                    << ", err=" << static_cast<int>(persist_result.error())
                    << ", skipping eviction";
                continue;
            }
            total_freed_size += metadata.TotalLength() * removed_count;
            evicted_count++;
            continue;
        }

        const size_t erased = metadata.EraseReplicas(is_evictable_nof_replica);
        if (erased == 0) {
            continue;
        }

        total_freed_size += metadata.TotalLength() * erased;
        evicted_count++;
        PublishKvRemovedAfterEvict(key, erased, "disk", metadata, tenant_id);
        if (!metadata.IsValid()) {
            accessor.Erase();
        }
    }

    if (evicted_count > 0) {
        need_nof_eviction_ = false;
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
        MasterMetricManager::instance().inc_nof_eviction_success(
            evicted_count, total_freed_size);
    } else {
        if (object_count == 0) {
            need_nof_eviction_ = false;
        }
        MasterMetricManager::instance().inc_eviction_fail();
        MasterMetricManager::instance().inc_nof_eviction_fail();
    }

    VLOG(1) << "action=evict_nof_replicas"
            << ", evicted_count=" << evicted_count
            << ", total_freed_size=" << total_freed_size;

    pt_nof_evict.End(0);
}

void MasterService::ClientMonitorFunc() {
    while (client_monitor_running_) {
        // RFC #4700 slice 1: liveness transitions retire clients and drive
        // offboarding, so the monitor stays out of the restore window too.
        if (!AdmitsBusinessWorkers()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kClientMonitorSleepMs));
            continue;
        }

        const auto now = ClientLivenessRecord::Clock::now();
        std::vector<std::pair<UUID, std::shared_ptr<ClientLivenessRecord>>>
            clients;
        {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            clients.reserve(client_liveness_records_.size());
            for (const auto& entry : client_liveness_records_) {
                clients.push_back(entry);
            }
        }

        for (const auto& [client_id, record] : clients) {
            const auto transition = record->EvaluateAndRetire(
                now, std::chrono::seconds(client_active_ttl_sec_),
                std::chrono::seconds(client_suspicion_ttl_sec_),
                [&] { client_offboarding_worker_.ReserveJob(); },
                [&] {
                    {
                        std::unique_lock<std::shared_mutex> client_lock(
                            client_mutex_);
                        const auto current =
                            client_liveness_records_.find(client_id);
                        CHECK(current != client_liveness_records_.end());
                        CHECK(current->second == record);
                        if (ok_client_.erase(client_id) != 0) {
                            MasterMetricManager::instance()
                                .dec_active_clients();
                        }
                        client_host_id_.erase(client_id);
                    }
                    graceful_unmount_scheduler_.RemoveIf(
                        [&client_id](
                            const GracefulUnmountDeadlineRecord& pending) {
                            return pending.client_id == client_id;
                        });

                    ClientOffboardingJob job;
                    job.client_id = client_id;
                    job.liveness = record;
                    std::shared_lock<std::shared_mutex> snapshot_lock(
                        master_state_mutex_);
                    {
                        ScopedSegmentAccess segment_access =
                            segment_manager_.getSegmentAccess();
                        std::vector<Segment> segments;
                        const auto get_result =
                            segment_access.GetClientSegments(client_id,
                                                             segments);
                        if (get_result == ErrorCode::OK) {
                            for (const auto& segment : segments) {
                                SegmentStatus status = SegmentStatus::UNDEFINED;
                                const auto status_result =
                                    segment_access.GetSegmentStatusById(
                                        segment.id, status);
                                if (status_result ==
                                    ErrorCode::SEGMENT_NOT_FOUND) {
                                    continue;
                                }
                                if (status_result == ErrorCode::OK &&
                                    status == SegmentStatus::UNMOUNTING) {
                                    job.pending_prepare_segments.push_back(
                                        {.segment_id = segment.id,
                                         .segment_name = segment.name,
                                         .transport_endpoint =
                                             segment.te_endpoint});
                                    continue;
                                }

                                size_t metrics_dec_capacity = 0;
                                const auto prepare_result =
                                    segment_access.PrepareUnmountSegment(
                                        segment.id, metrics_dec_capacity);
                                if (prepare_result == ErrorCode::OK) {
                                    job.prepared_segments.push_back(
                                        {.segment_id = segment.id,
                                         .segment_name = segment.name,
                                         .transport_endpoint =
                                             segment.te_endpoint,
                                         .metrics_dec_capacity =
                                             metrics_dec_capacity});
                                } else if (prepare_result !=
                                           ErrorCode::SEGMENT_NOT_FOUND) {
                                    job.pending_prepare_segments.push_back(
                                        {.segment_id = segment.id,
                                         .segment_name = segment.name,
                                         .transport_endpoint =
                                             segment.te_endpoint});
                                    LOG(ERROR)
                                        << "client_id=" << client_id
                                        << ", segment_name=" << segment.name
                                        << ", action=prepare_client_offboarding"
                                        << ", error="
                                        << toString(prepare_result);
                                }
                            }
                        }
                    }

                    MasterMetricManager::instance()
                        .client_liveness_became_offline();
                    client_offboarding_worker_.ScheduleReserved(std::move(job));
                });

            if (transition == ClientLivenessTransition::BECAME_SUSPECTED) {
                MasterMetricManager::instance()
                    .client_liveness_became_suspected();
                LOG(INFO) << "client_id=" << client_id
                          << ", action=client_liveness_suspected";
            } else if (transition == ClientLivenessTransition::BECAME_OFFLINE) {
                LOG(INFO) << "client_id=" << client_id
                          << ", action=client_liveness_offline";
            }
        }

                std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));
    }
}

bool MasterService::ProbeNoFSegment(const std::string& te_endpoint,
                                    std::string* error_reason) {
#ifndef USE_NOF
    if (error_reason) {
        *error_reason = "nof_pool_disabled";
    }
    return false;
#else
    NoFProbeFn probe_fn;
    {
        std::lock_guard<std::mutex> lock(nof_probe_fn_mutex_);
        probe_fn = nof_probe_fn_;
    }
    if (!probe_fn) {
        if (error_reason) {
            *error_reason = "probe_not_configured";
        }
        return false;
    }
    return probe_fn(
        te_endpoint,
        static_cast<uint32_t>(nof_heartbeat_probe_timeout_ms_.count()),
        error_reason);
#endif
}

bool MasterService::TryUnmountNoFSegmentByHeartbeat(
    const MountedNoFSegmentSnapshot& snapshot,
    const std::string& error_reason) {
    size_t metrics_dec_capacity = 0;
    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();
    {
        auto nof_segment_access = nof_segment_manager_.getNoFSegmentAccess();
        ErrorCode err = nof_segment_access.PrepareUnmountSegment(
            snapshot.segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND ||
            err == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS) {
            std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
            nof_heartbeat_states_.erase(snapshot.segment_id);
            VLOG(1) << "segment_id=" << snapshot.segment_id
                    << ", action=skip_nof_heartbeat_unmount"
                    << ", reason=" << toString(err);
            return false;
        }
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "segment_id=" << snapshot.segment_id
                       << ", segment_name=" << snapshot.segment.name
                       << ", error=prepare_unmount_nof_segment_by_"
                          "heartbeat_failed"
                       << ", reason=" << err;
            return false;
        }
    }

    ClearInvalidHandles(retaining_clients);

    {
        auto nof_segment_access = nof_segment_manager_.getNoFSegmentAccess();
        ErrorCode err = nof_segment_access.CommitUnmountSegment(
            snapshot.segment_id, snapshot.client_id, metrics_dec_capacity);
        if (err != ErrorCode::OK && err != ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(ERROR) << "segment_id=" << snapshot.segment_id
                       << ", segment_name=" << snapshot.segment.name
                       << ", error=commit_unmount_nof_segment_by_"
                          "heartbeat_failed"
                       << ", reason=" << err;
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
        nof_heartbeat_states_.erase(snapshot.segment_id);
    }
    MasterMetricManager::instance()
        .inc_nof_segments_unmounted_by_heartbeat_total();
    LOG(INFO) << "segment_id=" << snapshot.segment_id
              << ", client_id=" << snapshot.client_id
              << ", segment_name=" << snapshot.segment.name
              << ", endpoint=" << snapshot.segment.te_endpoint
              << ", action=unmount_nof_segment_by_heartbeat"
              << ", last_error_reason=" << error_reason;
    return true;
}

void MasterService::NofHeartbeatThreadFunc() {
    size_t next_probe_index = 0;
    while (nof_heartbeat_running_) {
        // RFC #4700 slice 1: heartbeat-driven NoF unmounts mutate segment
        // and replica state.
        if (!AdmitsBusinessWorkers()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kNoFHeartbeatThreadSleepMs));
            continue;
        }

        auto now = std::chrono::steady_clock::now();
        std::vector<MountedNoFSegmentSnapshot> mounted_segments;
        nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);

        std::vector<MountedNoFSegmentSnapshot> ok_segments;
        ok_segments.reserve(mounted_segments.size());
        for (const auto& snapshot : mounted_segments) {
            if (snapshot.status == SegmentStatus::OK) {
                ok_segments.push_back(snapshot);
            }
        }

        std::optional<MountedNoFSegmentSnapshot> probe_target;
        {
            std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
            std::unordered_set<UUID, boost::hash<UUID>> live_segment_ids;
            live_segment_ids.reserve(ok_segments.size());

            const auto interval_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    nof_heartbeat_interval_sec_);
            for (size_t i = 0; i < ok_segments.size(); ++i) {
                const auto& snapshot = ok_segments[i];
                live_segment_ids.insert(snapshot.segment_id);
                auto [it, inserted] =
                    nof_heartbeat_states_.try_emplace(snapshot.segment_id);
                auto& state = it->second;
                if (inserted) {
                    int64_t spread_ms = 0;
                    if (!ok_segments.empty()) {
                        spread_ms = static_cast<int64_t>(
                            (interval_ms.count() * i) / ok_segments.size());
                    }
                    state.last_success_at = now;
                    state.next_probe_at = now + nof_heartbeat_interval_sec_ +
                                          std::chrono::milliseconds(spread_ms);
                }
            }

            for (auto it = nof_heartbeat_states_.begin();
                 it != nof_heartbeat_states_.end();) {
                if (!live_segment_ids.contains(it->first)) {
                    it = nof_heartbeat_states_.erase(it);
                } else {
                    ++it;
                }
            }

            if (!ok_segments.empty()) {
                next_probe_index %= ok_segments.size();
                for (size_t offset = 0; offset < ok_segments.size(); ++offset) {
                    const auto& candidate =
                        ok_segments[(next_probe_index + offset) %
                                    ok_segments.size()];
                    auto state_it =
                        nof_heartbeat_states_.find(candidate.segment_id);
                    if (state_it == nof_heartbeat_states_.end()) {
                        continue;
                    }
                    if (state_it->second.next_probe_at <= now) {
                        probe_target = candidate;
                        next_probe_index = (next_probe_index + offset + 1) %
                                           ok_segments.size();
                        break;
                    }
                }
            }
        }

        if (!probe_target.has_value()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kNoFHeartbeatThreadSleepMs));
            continue;
        }

        auto probe_start = std::chrono::steady_clock::now();
        std::string error_reason;
        bool probe_success =
            ProbeNoFSegment(probe_target->segment.te_endpoint, &error_reason);
        auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - probe_start)
                              .count();
        MasterMetricManager::instance().observe_nof_heartbeat_probe_latency_ms(
            latency_ms);

        if (probe_success) {
            MasterMetricManager::instance().inc_nof_heartbeat_success_total();
            auto success_time = std::chrono::steady_clock::now();
            {
                std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
                auto it = nof_heartbeat_states_.find(probe_target->segment_id);
                if (it != nof_heartbeat_states_.end()) {
                    it->second.consecutive_failures = 0;
                    it->second.last_success_at = success_time;
                    it->second.next_probe_at =
                        success_time + nof_heartbeat_interval_sec_;
                }
            }
            VLOG(1) << "segment_id=" << probe_target->segment_id
                    << ", segment_name=" << probe_target->segment.name
                    << ", endpoint=" << probe_target->segment.te_endpoint
                    << ", action=nof_heartbeat_success"
                    << ", latency_ms=" << latency_ms;
            continue;
        }

        MasterMetricManager::instance().inc_nof_heartbeat_failure_total();
        if (error_reason == "completion_timeout") {
            MasterMetricManager::instance().inc_nof_heartbeat_timeout_total();
        }

        bool should_unmount = false;
        uint32_t failure_count = 0;
        auto failure_time = std::chrono::steady_clock::now();
        auto alive_timeout =
            nof_heartbeat_interval_sec_ *
            static_cast<int64_t>(nof_heartbeat_failures_threshold_);
        {
            std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
            auto it = nof_heartbeat_states_.find(probe_target->segment_id);
            if (it != nof_heartbeat_states_.end()) {
                it->second.consecutive_failures++;
                failure_count = it->second.consecutive_failures;
                it->second.next_probe_at =
                    failure_time + nof_heartbeat_interval_sec_;
                should_unmount =
                    failure_time - it->second.last_success_at >= alive_timeout;
            }
        }

        LOG(WARNING) << "segment_id=" << probe_target->segment_id
                     << ", segment_name=" << probe_target->segment.name
                     << ", endpoint=" << probe_target->segment.te_endpoint
                     << ", action=nof_heartbeat_failure"
                     << ", failure_count=" << failure_count
                     << ", latency_ms=" << latency_ms
                     << ", reason=" << error_reason;

        if (should_unmount) {
            TryUnmountNoFSegmentByHeartbeat(*probe_target, error_reason);
        }
    }
}

tl::expected<void, ErrorCode>
MasterService::ValidateDynamicReplicaPendingForCopyStart(
    route::ObjectState& state, const TenantId& tenant_id,
    const std::string& key, const UUID& dynamic_replication_lease_id,
    const UUID& client_id, const std::string& source_segment,
    uint64_t current_version_epoch, uint64_t dynamic_replication_version_epoch,
    const std::vector<std::string>& target_segments) {
    auto valid = dynamic_replication_.ValidateCopyStartLocked(
        state, tenant_id, key, dynamic_replication_lease_id, source_segment,
        current_version_epoch, dynamic_replication_version_epoch,
        target_segments);
    if (!valid || dynamic_replication_lease_id == UUID{}) {
        return valid;
    }
    // The pending task names this source segment; only its owner may copy.
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    UUID source_client;
    auto err =
        segment_access.GetClientIdBySegmentName(source_segment, source_client);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    if (source_client != client_id) {
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }
    return {};
}

tl::expected<UUID, ErrorCode> MasterService::CreateCopyTask(
    const std::string& key, const TenantId& tenant_id,
    const std::vector<std::string>& targets) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    if (targets.empty()) {
        LOG(ERROR) << "key=" << key << ", error=empty_targets";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<
        std::tuple<std::string, UUID, std::shared_ptr<ClientLivenessRecord>>>
        serving_sources;
    {
        std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
        MetadataAccessorRO accessor(this, object_id);
        if (!accessor.Exists()) {
            VLOG(1) << "key=" << key << ", info=object_not_found";
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }

        const auto& metadata = accessor.Get();
        ScopedSegmentAccess segment_accessor =
            segment_manager_.getSegmentAccess();
        for (const auto& target : targets) {
            if (!segment_accessor.ExistsSegmentName(target)) {
                LOG(ERROR) << "key=" << key << ", target_segment=" << target
                           << ", error=target_segment_not_mounted";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (!segment_accessor.IsSegmentAllocatable(target)) {
                LOG(ERROR) << "key=" << key << ", target_segment=" << target
                           << ", error=target_segment_not_allocatable";
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
        }

        metadata.VisitReplicas(
            [this](const Replica& replica) {
                return IsReplicaReadable(replica);
            },
            [&](const Replica& replica) {
                for (const auto& segment_name : replica.get_segment_names()) {
                    if (!segment_name) {
                        continue;
                    }
                    UUID owner;
                    if (segment_accessor.GetClientIdBySegmentName(
                            *segment_name, owner) == ErrorCode::OK) {
                        const auto liveness = replica.getClientLiveness();
                        if (liveness) {
                            serving_sources.emplace_back(*segment_name, owner,
                                                         std::move(liveness));
                        }
                    }
                }
            });
    }
    if (serving_sources.empty()) {
        LOG(ERROR) << "key=" << key << ", error=no_valid_source_replicas";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    const auto& [selected_source_segment, select_client, liveness] =
        serving_sources[randomIndex(serving_sources.size())];
    auto serving_guard = liveness->TryAcquireServingGuard();
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    // Completion paths acquire the Client guard before the object's key.
    // Reacquire and validate in that same order after discovery.
    std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    MetadataAccessorRO submission_metadata(this, object_id);
    if (!submission_metadata.Exists()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    const auto& metadata = submission_metadata.Get();
    ScopedSegmentAccess submission_access = segment_manager_.getSegmentAccess();
    UUID current_source_client;
    if (submission_access.GetClientIdBySegmentName(
            selected_source_segment, current_source_client) != ErrorCode::OK ||
        current_source_client != select_client ||
        !metadata.HasReplica(
            [this, &selected_source_segment](const Replica& replica) {
                if (!IsReplicaReadable(replica)) {
                    return false;
                }
                const auto names = replica.get_segment_names();
                return std::any_of(
                    names.begin(), names.end(),
                    [&selected_source_segment](const auto& name) {
                        return name && *name == selected_source_segment;
                    });
            })) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    for (const auto& target : targets) {
        if (!submission_access.IsSegmentAllocatable(target)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }
    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_COPY>(
            select_client, {.tenant_id = object_id.tenant_id.value(),
                            .key = object_id.user_key,
                            .source = selected_source_segment,
                            .targets = targets});
}

tl::expected<UUID, ErrorCode> MasterService::CreateMoveTask(
    const std::string& key, const TenantId& tenant_id,
    const std::string& source, const std::string& target) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    if (source == target) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", target_segment=" << target
                   << ", error=source_target_segments_are_same";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    bool source_exists = false;
    bool source_is_serving = false;
    UUID select_client;
    std::shared_ptr<ClientLivenessRecord> source_liveness;
    {
        std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
        MetadataAccessorRO accessor(this, object_id);
        if (!accessor.Exists()) {
            VLOG(1) << "key=" << key << ", info=object_not_found";
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }

        const auto& metadata = accessor.Get();
        ScopedSegmentAccess segment_accessor =
            segment_manager_.getSegmentAccess();
        if (!segment_accessor.ExistsSegmentName(target)) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_mounted";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!segment_accessor.IsSegmentAllocatable(target)) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_allocatable";
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }

        metadata.VisitReplicas(
            [](const Replica&) { return true; },
            [this, &source, &source_exists, &source_is_serving,
             &source_liveness](const Replica& replica) {
                const auto names = replica.get_segment_names();
                const bool matches = std::any_of(
                    names.begin(), names.end(), [&source](const auto& name) {
                        return name && *name == source;
                    });
                if (matches) {
                    source_exists = true;
                    if (IsReplicaReadable(replica)) {
                        source_is_serving = true;
                        source_liveness = replica.getClientLiveness();
                    }
                }
            });
        if (!source_exists) {
            LOG(ERROR) << "key=" << key << ", source_segment=" << source
                       << ", error=source_replica_not_found";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!source_is_serving) {
            LOG(ERROR) << "key=" << key << ", source_segment=" << source
                       << ", error=source_segment_not_serving";
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        ErrorCode error =
            segment_accessor.GetClientIdBySegmentName(source, select_client);
        if (error != ErrorCode::OK) {
            LOG(ERROR) << "key=" << key << ", segment_name=" << source
                       << ", error=client_id_not_found";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    if (!source_liveness) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    auto serving_guard = source_liveness->TryAcquireServingGuard();
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    // Do not hold a key while acquiring the Client guard: MoveEnd
    // takes those locks in the opposite order.
    std::shared_lock<std::shared_mutex> snapshot_lock(master_state_mutex_);
    MetadataAccessorRO submission_metadata(this, object_id);
    if (!submission_metadata.Exists()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    const auto& metadata = submission_metadata.Get();
    ScopedSegmentAccess submission_access = segment_manager_.getSegmentAccess();
    UUID current_source_client;
    if (submission_access.GetClientIdBySegmentName(
            source, current_source_client) != ErrorCode::OK ||
        current_source_client != select_client ||
        !submission_access.IsSegmentAllocatable(target) ||
        !metadata.HasReplica([this, &source](const Replica& replica) {
            if (!IsReplicaReadable(replica)) {
                return false;
            }
            const auto names = replica.get_segment_names();
            return std::any_of(names.begin(), names.end(),
                               [&source](const auto& name) {
                                   return name && *name == source;
                               });
        })) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_MOVE>(
            select_client, {.tenant_id = object_id.tenant_id.value(),
                            .key = object_id.user_key,
                            .source = source,
                            .target = target});
}

tl::expected<QueryTaskResponse, ErrorCode> MasterService::QueryTask(
    const UUID& task_id) {
    const auto& task_option =
        task_manager_.get_read_access().find_task_by_id(task_id);
    if (!task_option.has_value()) {
        LOG(ERROR) << "task_id=" << task_id << ", error=task_not_found";
        return tl::make_unexpected(ErrorCode::TASK_NOT_FOUND);
    }
    return QueryTaskResponse(task_option.value());
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> MasterService::FetchTasks(
    const UUID& client_id, size_t batch_size) {
    const auto liveness = FindClientRecord(client_id);
    if (!liveness) {
        return std::vector<TaskAssignment>{};
    }
    auto serving_guard = liveness->TryAcquireServingGuard();
    if (!serving_guard) {
        return std::vector<TaskAssignment>{};
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    const auto& tasks =
        task_manager_.get_write_access().pop_tasks(client_id, batch_size);
    std::vector<TaskAssignment> assignments;
    for (const auto& task : tasks) {
        assignments.emplace_back(task);
    }
    return assignments;
}

tl::expected<void, ErrorCode> MasterService::MarkTaskToComplete(
    const UUID& client_id, const TaskCompleteRequest& request) {
    const auto liveness = FindClientRecord(client_id);
    if (!liveness) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    auto retaining_guard = liveness->TryAcquireRetainingGuard();
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    auto write_access = task_manager_.get_write_access();
    ErrorCode err = write_access.complete_task(client_id, request.id,
                                               request.status, request.message);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "task_id=" << request.id
                   << ", error=complete_task_failed";
        return tl::make_unexpected(err);
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::ValidateDrainRequestLocked(
    ScopedSegmentAccess& segment_access, const CreateDrainJobRequest& request) {
    if (request.segments.empty() || request.max_concurrency == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::unordered_set<std::string> unique_segments(request.segments.begin(),
                                                    request.segments.end());
    if (unique_segments.size() != request.segments.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (const auto& segment_name : request.segments) {
        if (!segment_access.ExistsSegmentName(segment_name)) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        SegmentStatus status = SegmentStatus::UNDEFINED;
        auto err = segment_access.GetSegmentStatusByName(segment_name, status);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
        if (status != SegmentStatus::OK) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    for (const auto& target_segment : request.target_segments) {
        if (unique_segments.contains(target_segment)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!segment_access.ExistsSegmentName(target_segment)) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        if (!segment_access.IsSegmentAllocatable(target_segment)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }
    return {};
}

tl::expected<UUID, ErrorCode> MasterService::CreateDrainJob(
    const CreateDrainJobRequest& request) {
    // Held until the job is registered so SetSegmentStatus never sees a
    // segment this job has marked DRAINING without also seeing the job.
    std::lock_guard<std::mutex> lock(job_mutex_);
    std::vector<std::string> draining_segments;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        auto valid = ValidateDrainRequestLocked(segment_access, request);
        if (!valid.has_value()) {
            return tl::make_unexpected(valid.error());
        }

        draining_segments.reserve(request.segments.size());
        for (const auto& segment_name : request.segments) {
            auto err = segment_access.SetSegmentStatusByName(
                segment_name, SegmentStatus::DRAINING);
            if (err != ErrorCode::OK) {
                for (const auto& updated_segment : draining_segments) {
                    (void)segment_access.SetSegmentStatusByName(
                        updated_segment, SegmentStatus::OK);
                }
                return tl::make_unexpected(err);
            }
            draining_segments.push_back(segment_name);
        }
    }

    auto job = std::make_shared<DrainJob>();
    job->id = generate_uuid();
    job->request = request;
    job->created_at = std::chrono::system_clock::now();
    job->last_updated_at = job->created_at;
    job->status = JobStatus::CREATED;
    job->message = "Drain job created";
    drain_jobs_.emplace(job->id, job);
    return job->id;
}

tl::expected<QueryJobResponse, ErrorCode> MasterService::QueryDrainJob(
    const UUID& job_id) {
    std::shared_ptr<DrainJob> job;
    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        auto it = drain_jobs_.find(job_id);
        if (it == drain_jobs_.end()) {
            return tl::make_unexpected(ErrorCode::JOB_NOT_FOUND);
        }
        job = it->second;
    }

    std::lock_guard<std::mutex> job_lock(job->mutex);
    QueryJobResponse response;
    response.id = job->id;
    response.type = job->type;
    response.status = job->status;
    response.created_at_ms_epoch = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            job->created_at.time_since_epoch())
            .count());
    response.last_updated_at_ms_epoch = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            job->last_updated_at.time_since_epoch())
            .count());
    response.segments = job->request.segments;
    response.succeeded_units = job->succeeded_units;
    response.failed_units = job->failed_units;
    response.blocked_units = job->blocked_units;
    response.active_units = static_cast<uint64_t>(job->active_tasks.size());
    response.migrated_bytes = job->migrated_bytes;
    response.message = job->message;
    return response;
}

tl::expected<void, ErrorCode> MasterService::CancelDrainJob(
    const UUID& job_id) {
    std::shared_ptr<DrainJob> job;
    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        auto it = drain_jobs_.find(job_id);
        if (it == drain_jobs_.end()) {
            return tl::make_unexpected(ErrorCode::JOB_NOT_FOUND);
        }
        job = it->second;
    }

    std::vector<std::string> segments_to_restore;
    {
        std::lock_guard<std::mutex> job_lock(job->mutex);
        if (job->status == JobStatus::SUCCEEDED ||
            job->status == JobStatus::FAILED ||
            job->status == JobStatus::CANCELED || !job->active_tasks.empty()) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }

        job->status = JobStatus::CANCELED;
        job->last_updated_at = std::chrono::system_clock::now();
        job->message = "Drain job canceled";
        segments_to_restore = job->request.segments;
    }

    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    for (const auto& segment_name : segments_to_restore) {
        SegmentStatus status = SegmentStatus::UNDEFINED;
        if (segment_access.GetSegmentStatusByName(segment_name, status) ==
                ErrorCode::OK &&
            status != SegmentStatus::UNMOUNTING) {
            (void)segment_access.SetSegmentStatusByName(segment_name,
                                                        SegmentStatus::OK);
        }
    }
    return {};
}

std::string MasterService::MakeDrainUnitKey(
    const TenantId& tenant_id, const std::string& key,
    const std::string& source_segment) const {
    return std::to_string(tenant_id.value().size()) + ":" + tenant_id.value() +
           ":" + std::to_string(key.size()) + ":" + key + ":" + source_segment;
}

std::optional<std::string> MasterService::SelectDrainTargetForKey(
    const ObjectMetadata& metadata, const std::string& source_segment,
    const std::vector<std::string>& requested_targets) {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::string> candidate_segments = requested_targets;
    if (candidate_segments.empty()) {
        auto err = segment_access.GetAllSegments(candidate_segments);
        if (err != ErrorCode::OK) {
            return std::nullopt;
        }
    }

    const auto existing_segments = metadata.GetReplicaSegmentNames();
    double best_util = std::numeric_limits<double>::max();
    std::optional<std::string> best_target;
    for (const auto& candidate : candidate_segments) {
        if (candidate == source_segment) {
            continue;
        }
        if (std::find(existing_segments.begin(), existing_segments.end(),
                      candidate) != existing_segments.end()) {
            continue;
        }
        if (!segment_access.IsSegmentAllocatable(candidate)) {
            continue;
        }
        size_t used = 0, capacity = 0;
        if (segment_access.QuerySegments(candidate, used, capacity) !=
                ErrorCode::OK ||
            capacity == 0) {
            continue;
        }
        const double util =
            static_cast<double>(used) / static_cast<double>(capacity);
        if (util < best_util) {
            best_util = util;
            best_target = candidate;
        }
    }
    return best_target;
}

void MasterService::RefreshDrainJobTasks(DrainJob& job) {
    auto read_access = task_manager_.get_read_access();
    std::vector<UUID> finished_task_ids;
    finished_task_ids.reserve(job.active_tasks.size());

    for (const auto& [task_id, active_task] : job.active_tasks) {
        auto task_opt = read_access.find_task_by_id(task_id);
        if (!task_opt.has_value()) {
            finished_task_ids.push_back(task_id);
            job.failed_units++;
            job.terminal_failed_unit_keys.insert(active_task.unit_key);
            continue;
        }
        if (!task_opt->is_finished()) {
            continue;
        }

        finished_task_ids.push_back(task_id);
        if (task_opt->status == TaskStatus::SUCCESS) {
            job.succeeded_units++;
            job.migrated_bytes += active_task.bytes;
            job.completed_unit_keys.insert(active_task.unit_key);
        } else {
            job.failed_units++;
            auto& retry_count = job.retry_counts[active_task.unit_key];
            retry_count++;
            if (retry_count >= kMaxDrainUnitRetries) {
                job.terminal_failed_unit_keys.insert(active_task.unit_key);
            }
        }
    }

    for (const auto& task_id : finished_task_ids) {
        job.active_tasks.erase(task_id);
    }
}

void MasterService::ScheduleDrainJobTasks(DrainJob& job) {
    if (job.status == JobStatus::CREATED) {
        job.status = JobStatus::PLANNING;
    }

    const uint32_t max_concurrency =
        std::max<uint32_t>(1, job.request.max_concurrency);
    if (job.active_tasks.size() >= max_concurrency) {
        job.status = JobStatus::RUNNING;
        return;
    }

    struct DrainPlan {
        TenantId tenant_id;
        std::string key;
        std::string source_segment;
        std::string target_segment;
        size_t bytes;
        std::string unit_key;
    };

    const size_t slots = max_concurrency - job.active_tasks.size();
    std::vector<DrainPlan> plans;
    plans.reserve(slots);
    std::unordered_set<std::string> active_unit_keys;
    for (const auto& [_, task] : job.active_tasks) {
        active_unit_keys.insert(task.unit_key);
    }

    std::unordered_set<std::string> blocked_unit_keys;
    {
        std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
        VisitNamespaces([&](const TenantId& tenant_id,
                            metadata::Namespace& ns) {
            // The walk only narrows the keys to those with a replica on a
            // source segment: target selection waits on the segment lock,
            // which the walk body must not do while it holds the cursor, so
            // each key is resolved again below and judged on what it holds
            // then.
            std::vector<std::string> keys_on_source;
            for (auto object : ns.objects.ReadCursor()) {
                const auto replica_segments =
                    object.metadata().GetReplicaSegmentNames();
                if (std::any_of(job.request.segments.begin(),
                                job.request.segments.end(),
                                [&](const std::string& source_segment) {
                                    return std::find(replica_segments.begin(),
                                                     replica_segments.end(),
                                                     source_segment) !=
                                           replica_segments.end();
                                })) {
                    keys_on_source.push_back(object.key());
                }
            }
            for (const std::string& key : keys_on_source) {
                MetadataAccessorRO accessor(this,
                                            ObjectIdentity{tenant_id, key});
                if (!accessor.IsPublished()) {
                    continue;
                }
                const auto& metadata = accessor.Get();
                for (const auto& source_segment : job.request.segments) {
                    const auto unit_key =
                        MakeDrainUnitKey(tenant_id, key, source_segment);
                    if (job.completed_unit_keys.contains(unit_key) ||
                        active_unit_keys.contains(unit_key) ||
                        job.terminal_failed_unit_keys.contains(unit_key)) {
                        continue;
                    }

                    const auto replica_segments =
                        metadata.GetReplicaSegmentNames();
                    if (std::find(replica_segments.begin(),
                                  replica_segments.end(),
                                  source_segment) == replica_segments.end()) {
                        continue;
                    }

                    bool source_is_serving = false;
                    metadata.VisitReplicas(
                        [this, &source_segment](const Replica& replica) {
                            if (!IsReplicaReadable(replica)) {
                                return false;
                            }
                            const auto names = replica.get_segment_names();
                            return std::any_of(
                                names.begin(), names.end(),
                                [&source_segment](const auto& name) {
                                    return name && *name == source_segment;
                                });
                        },
                        [&source_is_serving](const Replica&) {
                            source_is_serving = true;
                        });
                    if (!source_is_serving) {
                        blocked_unit_keys.insert(unit_key);
                        continue;
                    }

                    if (metadata.IsHardPinned() || !metadata.IsLeaseExpired() ||
                        !metadata.AllReplicas(&Replica::fn_is_completed) ||
                        accessor.GetState().replication_task) {
                        blocked_unit_keys.insert(unit_key);
                        continue;
                    }

                    auto target = SelectDrainTargetForKey(
                        metadata, source_segment, job.request.target_segments);
                    if (!target.has_value()) {
                        blocked_unit_keys.insert(unit_key);
                        continue;
                    }

                    if (plans.size() < slots) {
                        plans.push_back({tenant_id, key, source_segment,
                                         *target, metadata.TotalLength(),
                                         unit_key});
                    }
                }
            }
        });
    }

    job.blocked_units = blocked_unit_keys.size();

    for (const auto& plan : plans) {
        auto task_id = CreateMoveTask(plan.key, plan.tenant_id,
                                      plan.source_segment, plan.target_segment);
        if (task_id.has_value()) {
            ActiveDrainTask active_task;
            active_task.task_id = task_id.value();
            active_task.tenant_id = plan.tenant_id;
            active_task.key = plan.key;
            active_task.source_segment = plan.source_segment;
            active_task.target_segment = plan.target_segment;
            active_task.bytes = plan.bytes;
            active_task.unit_key = plan.unit_key;
            job.active_tasks.emplace(task_id.value(), std::move(active_task));
        } else if (task_id.error() == ErrorCode::NO_AVAILABLE_HANDLE ||
                   task_id.error() ==
                       ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS ||
                   task_id.error() == ErrorCode::OBJECT_HAS_REPLICATION_TASK) {
            job.blocked_units++;
        } else {
            job.failed_units++;
            auto& retry_count = job.retry_counts[plan.unit_key];
            retry_count++;
            if (retry_count >= kMaxDrainUnitRetries) {
                job.terminal_failed_unit_keys.insert(plan.unit_key);
            }
        }
    }

    job.status = JobStatus::RUNNING;
    job.last_updated_at = std::chrono::system_clock::now();
    job.message = "Drain job running";
}

bool MasterService::MaybeCompleteDrainJob(DrainJob& job) {
    if (!job.active_tasks.empty()) {
        return false;
    }

    std::unordered_set<std::string> remaining_segments;
    std::unordered_set<std::string> remaining_unit_keys;
    {
        std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
        VisitNamespaces([&](const TenantId& tenant_id,
                            metadata::Namespace& ns) {
            for (auto object : ns.objects.ReadCursor()) {
                const auto replica_segments =
                    object.metadata().GetReplicaSegmentNames();
                for (const auto& source_segment : job.request.segments) {
                    if (std::find(replica_segments.begin(),
                                  replica_segments.end(),
                                  source_segment) != replica_segments.end()) {
                        remaining_segments.insert(source_segment);
                        remaining_unit_keys.insert(MakeDrainUnitKey(
                            tenant_id, object.key(), source_segment));
                    }
                }
            }
        });
    }

    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& segment_name : job.request.segments) {
            if (!remaining_segments.contains(segment_name)) {
                (void)segment_access.SetSegmentStatusByName(
                    segment_name, SegmentStatus::DRAINED);
            }
        }
    }

    if (remaining_segments.empty()) {
        job.status = JobStatus::SUCCEEDED;
        job.last_updated_at = std::chrono::system_clock::now();
        job.message = "Drain job finished successfully";
        return true;
    }

    bool all_remaining_terminal_failed = !remaining_unit_keys.empty();
    for (const auto& unit_key : remaining_unit_keys) {
        if (!job.terminal_failed_unit_keys.contains(unit_key)) {
            all_remaining_terminal_failed = false;
            break;
        }
    }
    if (!all_remaining_terminal_failed) {
        return false;
    }

    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& segment_name : job.request.segments) {
            SegmentStatus status = SegmentStatus::UNDEFINED;
            if (segment_access.GetSegmentStatusByName(segment_name, status) ==
                    ErrorCode::OK &&
                status != SegmentStatus::UNMOUNTING) {
                (void)segment_access.SetSegmentStatusByName(segment_name,
                                                            SegmentStatus::OK);
            }
        }
    }

    job.status = JobStatus::FAILED;
    job.last_updated_at = std::chrono::system_clock::now();
    job.message = "Drain job failed: unrecoverable units remain";
    return true;
}

void MasterService::ProcessDrainJobs() {
    std::vector<std::shared_ptr<DrainJob>> jobs;
    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        jobs.reserve(drain_jobs_.size());
        for (const auto& [_, job] : drain_jobs_) {
            jobs.push_back(job);
        }
    }

    for (const auto& job : jobs) {
        if (!job) {
            continue;
        }
        std::lock_guard<std::mutex> job_lock(job->mutex);
        if (job->status == JobStatus::SUCCEEDED ||
            job->status == JobStatus::FAILED ||
            job->status == JobStatus::CANCELED) {
            continue;
        }
        RefreshDrainJobTasks(*job);
        if (MaybeCompleteDrainJob(*job)) {
            continue;
        }
        ScheduleDrainJobTasks(*job);
    }
}

void MasterService::JobDispatchThreadFunc() {
    while (job_dispatch_running_) {
        // RFC #4700 slice 1: drain jobs mutate business metadata.
        if (!AdmitsBusinessWorkers()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kJobDispatchThreadSleepMs));
            continue;
        }

        ProcessDrainJobs();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kJobDispatchThreadSleepMs));
    }
}

KvEventConfig MasterService::BuildKvEventConfig(
    const MasterServiceConfig& config) {
    KvEventConfig kv_config;
    kv_config.enabled = config.enable_kv_events;
    kv_config.bind_endpoint = config.kv_events_bind_endpoint;
    kv_config.model_name = config.kv_events_model_name;
    kv_config.backend_id = config.kv_events_backend_id;
    kv_config.tenant_id = config.kv_events_tenant_id;
    kv_config.additional_salt = config.kv_events_additional_salt;
    kv_config.lora_name = config.kv_events_lora_name;
    kv_config.block_size = config.kv_events_block_size;
    kv_config.dp_rank = config.kv_events_dp_rank;
    kv_config.emit_legacy_compat_fields = config.kv_events_emit_legacy_compat;
    kv_config.emit_object_key = config.kv_events_emit_object_key;
    kv_config.queue_capacity = config.kv_events_queue_capacity;
    return kv_config;
}

// Every non-memory replica type, including DFS, is normalized to the "disk"
// medium so subscribers see one logical tier per storage class.
std::vector<std::string> MasterService::KvMediaForMetadata(
    const ObjectMetadata& metadata) {
    bool has_cpu = false;
    bool has_disk = false;
    metadata.VisitReplicas(
        [](const Replica& replica) { return replica.is_completed(); },
        [&](const Replica& replica) {
            if (replica.is_memory_replica() &&
                !replica.has_invalid_mem_handle()) {
                has_cpu = true;
            } else if (replica.is_nof_replica() &&
                       !replica.has_invalid_nof_handle()) {
                has_disk = true;
            } else if (replica.is_disk_replica() ||
                       replica.is_local_disk_replica() ||
                       replica.is_dfs_replica()) {
                has_disk = true;
            }
        });
    std::vector<std::string> media;
    if (has_cpu) {
        media.emplace_back("cpu");
    }
    if (has_disk) {
        media.emplace_back("disk");
    }
    return media;
}

std::vector<std::string> MasterService::KvMediaForRemoval(
    const ObjectMetadata& metadata) {
    auto media = KvMediaForMetadata(metadata);
    bool has_cpu = std::find(media.begin(), media.end(), "cpu") != media.end();
    bool has_disk =
        std::find(media.begin(), media.end(), "disk") != media.end();
    // KvMediaForMetadata above only counts completed replicas. A removal path
    // can run after a replica has moved to PROCESSING or REMOVED, and both
    // states still had their medium announced to subscribers, so both must
    // appear in the removal delta or the tier is never retracted.
    metadata.VisitReplicas(
        [](const Replica& replica) {
            return replica.status() == ReplicaStatus::REMOVED ||
                   replica.status() == ReplicaStatus::PROCESSING;
        },
        [&](const Replica& replica) {
            if (replica.is_memory_replica()) {
                has_cpu = true;
            } else if (replica.is_nof_replica() || replica.is_disk_replica() ||
                       replica.is_local_disk_replica() ||
                       replica.is_dfs_replica()) {
                has_disk = true;
            }
        });
    if (has_cpu &&
        std::find(media.begin(), media.end(), "cpu") == media.end()) {
        media.emplace_back("cpu");
    }
    if (has_disk &&
        std::find(media.begin(), media.end(), "disk") == media.end()) {
        media.emplace_back("disk");
    }
    return media;
}

void MasterService::PublishKvStored(const std::string& key,
                                    const ObjectMetadata& metadata,
                                    const TenantId& tenant_id) {
    // The epoch is bumped even when no publisher is listening so that the
    // ordering rule is testable, and always before the enqueue below: a
    // concurrent RemoveAll must either see the new epoch and withhold its
    // `cleared`, or have already published it ahead of this `stored`.
    BumpKvTenantEpoch(tenant_id.value());
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    kv_event_publisher_->PublishCommitted(key, KvMediaForMetadata(metadata),
                                          tenant_id.value(), metadata.group_id,
                                          metadata.session_id);
}

void MasterService::SyncKvObjectState(
    const std::string& key, const ObjectMetadata& metadata,
    const TenantId& tenant_id,
    const std::vector<std::string>& previous_media_hint) {
    const bool publishing =
        kv_event_publisher_ && kv_event_publisher_->enabled();
    if (!publishing && !kv_track_tenant_epochs_) {
        return;
    }
    auto current_media = KvMediaForMetadata(metadata);
    // Only an object that still has a medium can contradict a `cleared`. Sync
    // calls that end with no medium left are retractions, which a clear may
    // safely follow.
    if (!current_media.empty()) {
        BumpKvTenantEpoch(tenant_id.value());
    }
    if (!publishing) {
        return;
    }
    kv_event_publisher_->SyncObjectState(key, current_media, tenant_id.value(),
                                         metadata.group_id, previous_media_hint,
                                         metadata.session_id);
}

void MasterService::PublishKvRemoved(
    const std::string& key, const ObjectMetadata& metadata,
    const TenantId& tenant_id,
    const std::vector<std::string>& previous_media_hint) {
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    auto previous_media = KvRemovalSnapshot(metadata);
    previous_media.insert(previous_media.end(), previous_media_hint.begin(),
                          previous_media_hint.end());
    kv_event_publisher_->PublishObjectRemoved(
        key, tenant_id.value(), metadata.group_id, previous_media);
}

void MasterService::PublishKvRemovedAfterEvict(const std::string& key,
                                               size_t evicted_replica_count,
                                               const std::string& medium,
                                               const ObjectMetadata& metadata,
                                               const TenantId& tenant_id) {
    if (evicted_replica_count == 0) {
        return;
    }
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    if (!metadata.IsValid()) {
        // EraseMetadata can only retract media still represented in metadata.
        // The last replica may already have been detached; announce its
        // medium here unless the pending erase will do so itself.
        const auto remaining_media = KvRemovalSnapshot(metadata);
        if (std::find(remaining_media.begin(), remaining_media.end(), medium) ==
            remaining_media.end()) {
            kv_event_publisher_->PublishRemoved(key, medium, tenant_id,
                                                metadata.group_id);
        }
        return;
    }
    // The object survives. Feeding the evicted medium in as part of the
    // previous set lets the delta drop it, unless another replica still lives
    // on it.
    auto previous_media = KvMediaForMetadata(metadata);
    if (std::find(previous_media.begin(), previous_media.end(), medium) ==
        previous_media.end()) {
        previous_media.push_back(medium);
    }
    SyncKvObjectState(key, metadata, tenant_id, previous_media);
}

void MasterService::PublishKvCleared(const TenantId& tenant_id) {
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    kv_event_publisher_->PublishCleared(tenant_id.value());
}

void MasterService::PublishKvClearedIfEpochUnchanged(const TenantId& tenant_id,
                                                     uint64_t expected_epoch) {
    if (!kv_track_tenant_epochs_) {
        PublishKvCleared(tenant_id);
        return;
    }
    // Holding the epoch lock across the publish is what makes this a barrier
    // rather than a racy re-check: a commit that wants to announce this tenant
    // has to wait for the lock, and will then find the clear already enqueued.
    std::lock_guard<std::mutex> lock(kv_tenant_epoch_mutex_);
    if (kv_tenant_epochs_[KvTenantEpochSlot(tenant_id.value())] !=
        expected_epoch) {
        // A commit landed in an already-scanned stripe. Its `stored` is on the
        // wire, so a clear here would retract a live object.
        kv_cleared_suppressed_by_epoch_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    kv_cleared_published_.fetch_add(1, std::memory_order_relaxed);
    PublishKvCleared(tenant_id);
}

bool MasterService::KvEventsEnabled() const {
    return kv_event_publisher_ && kv_event_publisher_->enabled();
}

KvEventPublisher::Stats MasterService::GetKvEventStats() const {
    if (!kv_event_publisher_) {
        return {};
    }
    return kv_event_publisher_->GetStats();
}

void MasterService::setHttpMetadataServer(HttpMetadataServer* server) {
    http_metadata_server_ = server;
    if (server) {
        LOG(INFO) << "HTTP metadata cleanup on client timeout: enabled "
                     "(co-located metadata server)";
    }
}

void MasterService::setHttpMetadataRemoteUrl(
    const std::string& metadata_connstring) {
    // The HTTP client is built without TLS, so only plain http:// metadata
    // servers support remote cleanup.
    if (metadata_connstring.rfind("http://", 0) != 0) {
        LOG(WARNING) << "enable_metadata_cleanup_on_timeout is set but the "
                        "configured metadata server '"
                     << metadata_connstring
                     << "' is not an http:// endpoint; remote cleanup "
                        "currently supports only plain HTTP. Metadata cleanup "
                        "on timeout disabled.";
        return;
    }
    http_metadata_remote_url_ = metadata_connstring;
    LOG(INFO) << "HTTP metadata cleanup on client timeout: enabled "
                 "(remote metadata server "
              << metadata_connstring << ")";
    // Start async cleanup worker now that http_metadata_remote_url_ is set.
    http_metadata_cleanup_running_ = true;
    http_metadata_cleanup_thread_ =
        std::thread(&MasterService::HttpMetadataCleanupThreadFunc, this);
    LOG(INFO) << "HTTP metadata cleanup worker thread started";
}

void MasterService::StopHttpMetadataCleanup() {
    if (!http_metadata_cleanup_thread_.joinable()) {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(http_metadata_cleanup_mutex_);
        http_metadata_cleanup_running_ = false;
        http_metadata_cleanup_queue_.clear();
    }
    http_metadata_cleanup_cv_.notify_all();
    http_metadata_cleanup_thread_.join();
}

void MasterService::cleanupHttpMetadata(const std::string& segment_name) {
    // Co-located: remove in-process, safe to run inline (no network I/O).
    if (http_metadata_server_) {
        const std::string ram_key =
            http_metadata_prefix_ + "ram/" + segment_name;
        const std::string rpc_key =
            http_metadata_prefix_ + "rpc_meta/" + segment_name;
        bool ram_removed = http_metadata_server_->removeKey(ram_key);
        bool rpc_removed = http_metadata_server_->removeKey(rpc_key);
        LOG(INFO) << "Cleaned up HTTP metadata for segment: " << segment_name
                  << ", ram_key_removed=" << ram_removed
                  << ", rpc_key_removed=" << rpc_removed;
        return;
    }

    // Separately-deployed: enqueue for async cleanup so a slow/unreachable
    // server never blocks the client monitor thread.
    if (!http_metadata_remote_url_.empty()) {
        {
            std::lock_guard<std::mutex> lk(http_metadata_cleanup_mutex_);
            // A stopped worker never drains the queue, and a late DELETE
            // could land after the lease is released.
            if (!http_metadata_cleanup_running_) {
                return;
            }
            http_metadata_cleanup_queue_.push_back(segment_name);
        }
        http_metadata_cleanup_cv_.notify_one();
        return;
    }

    // Neither configured: cleanup is disabled, nothing to do.
}

bool MasterService::removeRemoteHttpMetadataKey(const std::string& key) const {
    // Matches the timeout the Transfer Engine HTTP metadata plugin used.
    constexpr std::chrono::milliseconds kTimeout{3000};
    auto result = httpDelete(
        http_metadata_remote_url_ + "?key=" + code_utils::url_encode(key),
        kTimeout);
    if (!result) {
        LOG(WARNING) << "Remote HTTP metadata cleanup failed for key: " << key
                     << ": " << result.error();
        return false;
    }
    return true;
}

void MasterService::HttpMetadataCleanupThreadFunc() {
    LOG(INFO) << "HTTP metadata cleanup worker started";
    while (http_metadata_cleanup_running_) {
        std::vector<std::string> batch;
        {
            std::unique_lock<std::mutex> lk(http_metadata_cleanup_mutex_);
            http_metadata_cleanup_cv_.wait(lk, [&] {
                return !http_metadata_cleanup_queue_.empty() ||
                       !http_metadata_cleanup_running_.load();
            });
            if (!http_metadata_cleanup_running_ &&
                http_metadata_cleanup_queue_.empty()) {
                break;
            }
            batch.swap(http_metadata_cleanup_queue_);
        }

        for (const auto& segment_name : batch) {
            // Stopping skips the rest of the batch.
            if (!http_metadata_cleanup_running_) {
                break;
            }
            const std::string ram_key =
                http_metadata_prefix_ + "ram/" + segment_name;
            const std::string rpc_key =
                http_metadata_prefix_ + "rpc_meta/" + segment_name;

            // Each key attempted independently so one failure does not
            // prevent cleanup of the other.
            const bool ram_removed = removeRemoteHttpMetadataKey(ram_key);
            // Stopping also skips the second key, so at most one DELETE is
            // in flight when the stop request arrives.
            const bool rpc_removed = http_metadata_cleanup_running_ &&
                                     removeRemoteHttpMetadataKey(rpc_key);
            LOG(INFO) << "Cleaned up remote HTTP metadata for segment: "
                      << segment_name << ", ram_key_removed=" << ram_removed
                      << ", rpc_key_removed=" << rpc_removed;
        }
    }
    LOG(INFO) << "HTTP metadata cleanup worker stopped";
}

std::string MasterService::SerializeMetadataForOpLog(
    const ObjectMetadata& metadata) const {
    const auto all_replicas = metadata.GetAllReplicas();
    std::vector<Replica::Descriptor> replicas;
    replicas.reserve(all_replicas.size());
    for (const auto& replica : all_replicas) {
        replicas.push_back(replica.get_descriptor());
    }
    return SerializeMetadataForOpLogFromReplicaDescriptors(metadata, replicas);
}

std::string MasterService::SerializeMetadataForOpLogFromReplicaDescriptors(
    const ObjectMetadata& metadata,
    const std::vector<Replica::Descriptor>& replicas) const {
    // Leases are not serialized: a standby does not evict, and a promoted
    // primary grants fresh leases instead of restoring old ones.
    MetadataPayload payload;
    payload.client_id = metadata.client_id;
    payload.size = metadata.TotalLength();
    payload.replicas = replicas;
    payload.group_id = metadata.group_id;
    payload.session_id = metadata.session_id;
    payload.data_type = metadata.data_type;
    payload.hard_pinned = metadata.IsHardPinned();
    payload.object_checksum = metadata.GetPart(0).checksum();
    payload.residency_affinity_id = metadata.residency_affinity_id;
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

ErrorCode MasterService::InitializeBatchOpLogWriter(
    std::shared_ptr<HaKvBackend> backend, bool require_fenced_writer) {
    if (!backend || !backend->SupportsTxn()) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (require_fenced_writer && view_version_ == 0) {
        LOG(ERROR) << "Fenced batch OpLog writer requires a non-zero acquired "
                      "producer view";
        return ErrorCode::INVALID_PARAMS;
    }

    // Single-leader HA keeps the unpartitioned stream (batch snapshots,
    // compaction, failover); only multi-submaster clusters write per-master
    // streams. Standbys derive the same namespace (ha::OpLogSourceId).
    auto storage = std::make_unique<OpLogBatchStorage>(
        cluster_id_, *backend, ha::OpLogSourceId(master_id_, submaster_count_));
    DurablePrefix durable_prefix;
    ErrorCode err = storage->InitDurablePrefix(durable_prefix);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (require_fenced_writer) {
        err = storage->ClaimProducerView(view_version_);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to claim producer view for batch OpLog "
                       << "writer: " << toString(err);
            return err;
        }
    }

    OrderedOpLogWriterConfig writer_config;
    writer_config.max_entries_per_batch = oplog_batch_max_entries_;
    writer_config.initial_durable_prefix = durable_prefix;
    OpLogBatchStorage* storage_ptr = storage.get();
    const ViewVersionId producer_view_version = view_version_;
    OrderedOpLogWriter::WriteBatchFn write_batch =
        [storage_ptr, require_fenced_writer, producer_view_version](
            const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            if (require_fenced_writer) {
                return storage_ptr->WriteBatchAndAdvancePrefix(
                    batch, expected_prefix, producer_view_version);
            }
            return storage_ptr->WriteBatchAndAdvancePrefix(batch,
                                                           expected_prefix);
        };
    auto writer =
        batch_oplog_writer_factory_(writer_config, std::move(write_batch));
    if (!writer) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (!writer->IsAccepting()) {
        return writer->LastError();
    }
    writer->Start();

    if (ordered_oplog_writer_) {
        ordered_oplog_writer_->Stop();
    }
    batch_oplog_kv_backend_ = std::move(backend);
    batch_oplog_storage_ = std::move(storage);
    ordered_oplog_writer_ = std::move(writer);
    ordered_oplog_writer_->ActivateRuntimeMetrics();
    return ErrorCode::OK;
}

// 前置条件（调用方契约，见 TrySelfRecoverFromOpLog /
// TryRecoverFromDeadOwnerOpLog）：
//   1. 仅限 serve 前或 slot 导入屏障期调用。supervisor 路径在
//      RegisterRpcService 之前（serve 前）；ImportSlotMetadata 路径在
//      kMigrating 写屏障 / slot 未 ready 期间。若未来需要在 serve 期调用，
//      须先解决重放条目与在途客户端写的覆盖竞态（当前由
//      oplog_replay_finalized_sources_ 定局登记 + P4 owned-slot 过滤收窄，
//      合并后不再有 StandbyMetadataStore 单调 seq 防护（main 已删除
//      last_sequence_id），竞态未完全消除）。
//   2. 本函数自身不获取 master 全局锁（snapshot/client/segment/shard），
//      仅持有局部 vsegment_mutex；全局锁交互全部发生在调用方随后的
//      RestoreFromStandbySnapshot 内（锁序 snapshot → client → segment
//      → shard，与既有路径一致）。oplog_replay_mutex_ 由外层调用方持有，
//      不得在其内再获取上述全局锁（当前实现满足）。
//   3. 返回瞬态错误（etcd 抖动）时不定局：调用方可下轮重试。
ErrorCode MasterService::RecoverStateFromOpLogStream(
    const std::string& source_id, bool& stream_present,
    std::vector<StandbyObjectEntry>& objects,
    std::vector<StandbySegmentInfo>& segments,
    std::vector<vsegment::PartitionVSegmentSnapshot>& vsegment_partitions) {
    stream_present = false;
    objects.clear();
    segments.clear();
    vsegment_partitions.clear();
    if (source_id.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (!enable_oplog_ || !batch_oplog_kv_backend_) {
        // OpLog 未启用（或 etcd 后端未就绪）：无可重放数据。
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    const auto replay_t0 = std::chrono::steady_clock::now();
    uint64_t total_applied_entries = 0;

    // Standby 侧原语复用：applier 应用到独立 StandbyMetadataStore，
    // reader 以 source_id 为流主拉取（流主已消亡，或本机 serve 前无新写）。
    // main moved the standby store out of HotStandbyService. Its PutMetadata
    // overwrites (no per-key sequence guard: main dropped
    // StandbyObjectMetadata::last_sequence_id), so replay idempotency rests on
    // oplog_replay_finalized_sources_ and the owned-slot filter.
    auto metadata_store = std::make_unique<StandbyMetadataStore>();
    OpLogApplier applier(metadata_store.get(), cluster_id_);

    // vsegment 状态捕获：镜像 HotStandbyService::ApplyVSegmentState 的
    // revision 单调校验（防乱序/重复覆盖），按 partition 独立积累。
    std::mutex vsegment_mutex;
    std::unordered_map<std::string, vsegment::PartitionVSegmentSnapshot>
        vsegment_map;
    applier.SetVSegmentStateHandler([&](const OpLogEntry& entry) {
        std::lock_guard<std::mutex> lock(vsegment_mutex);
        std::string detail;
        const auto error =
            vsegment::ApplyVSegmentStateEntry(entry, vsegment_map, &detail);
        if (error != ErrorCode::OK)
            LOG(ERROR) << "OpLog replay: vsegment state rejected: " << detail;
        return error == ErrorCode::OK;
    });

    OpLogBatchStandbyReader reader(cluster_id_, *batch_oplog_kv_backend_,
                                   applier, source_id);
    // 追平判定：流主已死 / 本机未开写，轮询在有限轮内收敛——
    // durable prefix 已见且本轮无新应用即达流尾；prefix 未见且无进展
    // = 流不存在（新集群首启，正常空态）。RETRYABLE（etcd 抖动）小睡
    // 重试，轮次上限兜底防活流无限轮询。
    constexpr int kMaxPollRounds = 4096;
    // 墙钟预算：RETRYABLE（etcd 抖动）重试可能长期不收敛（如 etcd 持续
    // 不可达），4096 轮 × 50ms ≈ 204s 上限过高，且重放期间持有调用方的
    // 串行锁（oplog_replay_mutex_）会阻塞后续 slot 导入——30s 预算超限
    // 按越窗同语义返回（不定局），调用方回退旧语义并下轮 heartbeat
    // 自然重试。
    constexpr auto kMaxReplayWallClock = std::chrono::seconds(30);
    const auto replay_deadline =
        std::chrono::steady_clock::now() + kMaxReplayWallClock;
    bool prefix_seen = false;
    for (int round = 0; round < kMaxPollRounds; ++round) {
        auto poll = reader.PollOnce();
        if (poll.disposition == OpLogBatchStandbyPollDisposition::FATAL) {
            LOG(WARNING) << "OpLog replay: fatal poll error, source="
                         << source_id << ", err=" << toString(poll.error)
                         << " (stream may be compacted beyond retention)";
            return poll.error;
        }
        if (poll.disposition == OpLogBatchStandbyPollDisposition::RETRYABLE) {
            if (std::chrono::steady_clock::now() > replay_deadline) {
                LOG(WARNING) << "OpLog replay: wall-clock budget exhausted ("
                             << kMaxReplayWallClock.count()
                             << "s), source=" << source_id;
                return ErrorCode::INCOMPLETE_OPLOG_CATCH_UP;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }
        if (poll.durable_prefix_present) {
            prefix_seen = true;
        }
        total_applied_entries += poll.applied_entries;
        if (poll.applied_entries == 0 && prefix_seen) {
            break;  // 流尾
        }
        if (poll.applied_entries == 0 && !poll.durable_prefix_present) {
            break;  // 流不存在
        }
    }

    metadata_store->Snapshot(objects);
    segments = applier.GetSegmentRegistry().GetAllSegments();
    vsegment_partitions.reserve(vsegment_map.size());
    for (auto& [partition_id, state] : vsegment_map) {
        (void)partition_id;
        vsegment_partitions.push_back(std::move(state));
    }

    // 成功路径汇总（可观测性）：恢复规模 / 重放条目数 / 耗时一次打齐，
    // 失败路径已由各 return 点单独告警。
    LOG(INFO) << "oplog replay: source=" << source_id
              << ", applied_entries=" << total_applied_entries
              << ", objects=" << objects.size()
              << ", segments=" << segments.size()
              << ", vsegment_partitions=" << vsegment_partitions.size()
              << ", elapsed_ms="
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - replay_t0)
                     .count();
    stream_present = prefix_seen;
    return ErrorCode::OK;
}

// 前置条件：仅限 serve 前调用（supervisor 在 RegisterRpcService 之前
// 一次性触发；本进程内 oplog_replay_finalized_sources_ 定局登记保证
// 不会二次重放）。瞬态失败有界重试——rank 未迁移的快重启场景下这是
// 唯一自愈点（ImportSlotMetadata 不会重触），放弃重试即空态 serve。
tl::expected<bool, ErrorCode> MasterService::TrySelfRecoverFromOpLog() {
    if (!enable_oplog_) {
        return false;
    }
    // Self-recovery replays this master's own per-master stream, which only
    // exists in multi-submaster clusters. Single-leader HA writes the
    // unpartitioned stream and recovers through standby promotion instead.
    if (ha::OpLogSourceId(master_id_, submaster_count_).empty()) {
        return false;
    }
    std::vector<StandbyObjectEntry> objects;
    std::vector<StandbySegmentInfo> segments;
    std::vector<vsegment::PartitionVSegmentSnapshot> vsegment_partitions;
    bool stream_present = false;
    ErrorCode err = ErrorCode::OK;
    constexpr int kMaxSelfRecoverAttempts = 3;
    for (int attempt = 1; attempt <= kMaxSelfRecoverAttempts; ++attempt) {
        objects.clear();
        segments.clear();
        vsegment_partitions.clear();
        err = RecoverStateFromOpLogStream(master_id_, stream_present, objects,
                                          segments, vsegment_partitions);
        if (err == ErrorCode::OK) {
            break;
        }
        LOG(WARNING) << "self oplog replay attempt=" << attempt
                     << " failed, master_id=" << master_id_
                     << ", err=" << toString(err);
        if (attempt < kMaxSelfRecoverAttempts) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "self oplog replay failed after "
                   << kMaxSelfRecoverAttempts << " attempts:"
                   << " master_id=" << master_id_ << ", err=" << toString(err);
        return tl::make_unexpected(err);
    }
    if (stream_present &&
        (!objects.empty() || !segments.empty() || !vsegment_partitions.empty())) {
        LOG(INFO) << "self oplog replay: restoring recovered state, master_id="
                  << master_id_ << ", objects=" << objects.size()
                  << ", segments=" << segments.size()
                  << ", vsegment_partitions=" << vsegment_partitions.size();
        const auto materialized =
            MaterializeReplayedState(objects, segments, vsegment_partitions);
        if (!materialized) {
            LOG(ERROR) << "self oplog replay materialization failed, master_id="
                       << master_id_ << ", err="
                       << toString(materialized.error());
            return tl::make_unexpected(materialized.error());
        }
    }
    // A source must only be finalized after its recovered state is applied.
    // Otherwise an unsuccessful materialization prevents a subsequent retry.
    {
        std::lock_guard<std::mutex> lock(oplog_replay_mutex_);
        oplog_replay_finalized_sources_.insert(master_id_);
    }
    return stream_present &&
           (!objects.empty() || !segments.empty() || !vsegment_partitions.empty());
}

// 前置条件：仅限 slot 导入屏障期调用（ImportSlotMetadata 在
// kMigrating 写屏障 / slot 未 ready 期间触发；本机其余 slot 的 serve
// 不受影响——P4 owned-slot 过滤只物化本机接管的 slot）。瞬态失败
// 不定局，由下轮 heartbeat 的 ImportSlotMetadata 重试自然兜底。
tl::expected<bool, ErrorCode> MasterService::TryRecoverFromDeadOwnerOpLog(
    const std::string& source_id) {
    if (!enable_oplog_ || source_id.empty()) {
        return false;
    }
    // 串行化重放并做 source 定局登记：定局后不再重放（成功时元数据已
    // 落，流不存在时避免每 slot 重复空扫）；瞬态失败不登记，允许下轮
    // heartbeat 周期重试。
    std::lock_guard<std::mutex> lock(oplog_replay_mutex_);
    if (oplog_replay_finalized_sources_.count(source_id)) {
        return false;
    }
    std::vector<StandbyObjectEntry> objects;
    std::vector<StandbySegmentInfo> segments;
    std::vector<vsegment::PartitionVSegmentSnapshot> vsegment_partitions;
    bool stream_present = false;
    const ErrorCode err = RecoverStateFromOpLogStream(
        source_id, stream_present, objects, segments, vsegment_partitions);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "cold takeover oplog replay failed (will retry):"
                     << " source=" << source_id << ", err=" << toString(err);
        return tl::make_unexpected(err);
    }
    const bool has_recovered_state =
        stream_present &&
        (!objects.empty() || !segments.empty() || !vsegment_partitions.empty());
    if (has_recovered_state) {
        LOG(INFO) << "cold takeover oplog replay: restoring dead owner state,"
                  << " source=" << source_id << ", objects=" << objects.size()
                  << ", segments=" << segments.size()
                  << ", vsegment_partitions=" << vsegment_partitions.size();
        const auto materialized =
            MaterializeReplayedState(objects, segments, vsegment_partitions);
        if (!materialized) {
            LOG(WARNING) << "cold takeover materialization failed (will retry):"
                         << " source=" << source_id << ", err="
                         << toString(materialized.error());
            return tl::make_unexpected(materialized.error());
        }
    }
    oplog_replay_finalized_sources_.insert(source_id);
    return has_recovered_state;
}

ErrorCode MasterService::AdoptMaterializeFromDeadOwner(
    const std::string& source_id, bool& stream_present) {
    // 互备 1.4 ②：与 TryRecoverFromDeadOwnerOpLog 同通道的物化原语，
    // 供 supervisor RingStandbyRuntime 在 ring-adopt staging（kMigrating
    // 写屏障）期间编排调用（契约满足 RecoverStateFromOpLogStream 第 1
    // 条：领养 rank 处于写屏障，重放条目与在途写无覆盖竞态）。区别仅在
    // 错误传达：bool（回退判空语义，心跳路径自闭环）升级为 ErrorCode
    // 三态（OK=可收尾 / INCOMPLETE=瞬态重试 / FATAL=不可恢复可收尾），
    // 收尾决策权移交编排方（见头文件注释）。锁与定局登记语义同
    // TryRecoverFromDeadOwnerOpLog：oplog_replay_mutex_ 串行化重放，
    // 成功才定局，瞬态失败不登记。
    stream_present = false;
    if (!enable_oplog_ || source_id.empty()) {
        // OpLog 未启用：无可恢复数据（与既有晋升空态语义一致，调用方
        // 告警后按空态收尾）。
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }
    std::lock_guard<std::mutex> lock(oplog_replay_mutex_);
    if (oplog_replay_finalized_sources_.count(source_id)) {
        // 已定局（心跳死 owner 分支 / 先前周期已物化）：幂等短路。
        stream_present = true;
        return ErrorCode::OK;
    }
    std::vector<StandbyObjectEntry> objects;
    std::vector<StandbySegmentInfo> segments;
    std::vector<vsegment::PartitionVSegmentSnapshot> vsegment_partitions;
    const ErrorCode err = RecoverStateFromOpLogStream(
        source_id, stream_present, objects, segments, vsegment_partitions);
    if (err != ErrorCode::OK) {
        // INCOMPLETE_OPLOG_CATCH_UP（预算耗尽）/ FATAL（越窗、prefix
        // 缺失）：均不定局，三态交编排方决策（重试或告警收尾）。
        return err;
    }
    if (stream_present && (!objects.empty() || !segments.empty() ||
                           !vsegment_partitions.empty())) {
        LOG(INFO) << "ring adoption replay: materializing dead owner state,"
                  << " source=" << source_id << ", objects=" << objects.size()
                  << ", segments=" << segments.size()
                  << ", vsegment_partitions=" << vsegment_partitions.size();
        const auto materialized =
            MaterializeReplayedState(objects, segments, vsegment_partitions);
        if (!materialized) {
            return materialized.error();
        }
    }
    oplog_replay_finalized_sources_.insert(source_id);
    return ErrorCode::OK;
}

tl::expected<uint64_t, ErrorCode>
MasterService::AppendOpLogVisibleBeforeDurable(OpType type,
                                               const std::string& tenant_id,
                                               const std::string& key,
                                               const std::string& payload) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!ordered_oplog_writer_) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const TenantId resolved_tenant(enable_multi_tenants_
                                       ? tenant_id
                                       : std::string(TenantId::kDefaultValue));
    if (!resolved_tenant.IsValid()) {
        return tl::unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }

    auto reservation = ordered_oplog_writer_->Reserve();
    if (!reservation) {
        return tl::unexpected(reservation.error());
    }
    OpLogEntry entry;
    entry.op_type = type;
    entry.tenant_id = resolved_tenant.value();
    entry.object_key = key;
    entry.payload = payload;
    auto pending = ordered_oplog_writer_->Commit(std::move(reservation.value()),
                                                 std::move(entry), nullptr);
    if (!pending) {
        return tl::unexpected(pending.error());
    }
    return pending.value().sequence_id();
}

tl::expected<OpLogEntry, ErrorCode>
MasterService::AppendOpLogWithDurableFinalize(
    OpType type, const std::string& tenant_id, const std::string& key,
    const std::string& payload, DurableFinalizeCallback callback) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto reservation = ReserveBatchOpLogSlot();
    if (!reservation) {
        return tl::unexpected(reservation.error());
    }
    return AppendReservedOpLogWithDurableFinalize(
        std::move(reservation.value()), type, tenant_id, key, payload,
        std::move(callback));
}

tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
MasterService::ReserveBatchOpLogSlot() {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!ordered_oplog_writer_) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return ordered_oplog_writer_->Reserve();
}

tl::expected<uint64_t, ErrorCode>
MasterService::AppendOpLogBatchWithDurableFinalize(
    std::vector<OpLogEntry> entries, DurableFinalizeCallback callback) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!ordered_oplog_writer_) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (entries.empty()) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (auto& entry : entries) {
        const TenantId resolved_tenant(
            enable_multi_tenants_ ? entry.tenant_id
                                  : std::string(TenantId::kDefaultValue));
        if (!resolved_tenant.IsValid()) {
            return tl::unexpected(ErrorCode::TENANT_NOT_REGISTERED);
        }
        entry.tenant_id = resolved_tenant.value();
    }
    auto reservation = ordered_oplog_writer_->ReserveBatch(entries.size());
    if (!reservation) {
        return tl::unexpected(reservation.error());
    }
    auto pending = ordered_oplog_writer_->CommitBatch(
        std::move(reservation.value()), std::move(entries),
        std::move(callback));
    if (!pending) {
        return tl::unexpected(pending.error());
    }
    return pending.value().back().sequence_id();
}

tl::expected<OpLogEntry, ErrorCode>
MasterService::AppendReservedOpLogWithDurableFinalize(
    OrderedOpLogWriter::Reservation&& reservation, OpType type,
    const std::string& tenant_id, const std::string& key,
    const std::string& payload, DurableFinalizeCallback callback) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const TenantId resolved_tenant(enable_multi_tenants_
                                       ? tenant_id
                                       : std::string(TenantId::kDefaultValue));
    if (!resolved_tenant.IsValid()) {
        return tl::unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    OpLogEntry entry;
    entry.op_type = type;
    entry.tenant_id = resolved_tenant.value();
    entry.object_key = key;
    entry.payload = payload;
    auto pending = ordered_oplog_writer_->Commit(std::move(reservation), entry,
                                                 std::move(callback));
    if (!pending) {
        return tl::unexpected(pending.error());
    }
    entry.sequence_id = pending.value().sequence_id();
    return entry;
}

tl::expected<void, ErrorCode> MasterService::PersistRemoveForHA(
    const char* why, const TenantId& tenant_id, const std::string& key) {
    auto result = AppendOpLogWithDurableFinalize(
        OpType::REMOVE, tenant_id.value(), key, {}, nullptr);
    if (!result) {
        LOG(WARNING) << why << ": REMOVE persist failed for key=" << key
                     << ", err=" << static_cast<int>(result.error());
        return tl::unexpected(result.error());
    }
    return {};
}

void MasterService::PersistSegmentOpForHA(const char* why, OpType type,
                                          const std::string& key,
                                          const std::string& payload) {
    auto result = AppendOpLogVisibleBeforeDurable(
        type, TenantId::Default().value(), key, payload);
    if (!result) {
        LOG(WARNING) << why << ": segment OpLog queue failed for key=" << key
                     << ", type=" << static_cast<int>(type)
                     << ", err=" << static_cast<int>(result.error());
    }
}

std::vector<Replica::Descriptor>
MasterService::BuildRemainingReplicaDescriptors(
    const ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& should_remove) const {
    std::vector<Replica::Descriptor> remaining;
    for (const auto& replica : metadata.GetAllReplicas()) {
        if (!should_remove(replica) &&
            replica.status() == ReplicaStatus::COMPLETE) {
            remaining.push_back(replica.get_descriptor());
        }
    }
    return remaining;
}

}  // namespace mooncake
