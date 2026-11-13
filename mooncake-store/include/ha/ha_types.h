#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "types.h"

namespace mooncake {
namespace ha {

using ClusterNamespace = std::string;
using OwnerToken = std::string;
using OpLogSequenceId = uint64_t;

enum class HABackendType {
    UNKNOWN = 0,
    ETCD = 1,
    REDIS = 2,
    K8S = 3,
};

inline std::string HABackendTypeToString(HABackendType type) {
    switch (type) {
        case HABackendType::UNKNOWN:
            return "unknown";
        case HABackendType::ETCD:
            return "etcd";
        case HABackendType::REDIS:
            return "redis";
        case HABackendType::K8S:
            return "k8s";
    }
    return "unknown";
}

inline bool HaBackendSupportsOpLog(HABackendType type) {
    switch (type) {
        case HABackendType::ETCD:
        case HABackendType::REDIS:
            return true;
        case HABackendType::UNKNOWN:
        case HABackendType::K8S:
            return false;
    }
    return false;
}

inline std::optional<HABackendType> ParseHABackendType(std::string_view type) {
    if (type == "etcd") {
        return HABackendType::ETCD;
    }
    if (type == "redis") {
        return HABackendType::REDIS;
    }
    if (type == "k8s") {
        return HABackendType::K8S;
    }
    return std::nullopt;
}

inline bool HaBackendSupportsOpLog(std::string_view type) {
    const auto parsed = ParseHABackendType(type);
    return parsed.has_value() && HaBackendSupportsOpLog(parsed.value());
}

inline ErrorCode ValidateHABackendAvailability(HABackendType type) {
    switch (type) {
        case HABackendType::UNKNOWN:
            return ErrorCode::INVALID_PARAMS;
        case HABackendType::ETCD:
#ifdef STORE_USE_ETCD
            return ErrorCode::OK;
#else
            return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
        case HABackendType::REDIS:
#ifdef STORE_USE_REDIS
            return ErrorCode::OK;
#else
            return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
        case HABackendType::K8S:
#ifdef STORE_USE_K8S_LEASE
            return ErrorCode::OK;
#else
            return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
#endif
    }

    return ErrorCode::INVALID_PARAMS;
}

struct HABackendSpec {
    HABackendType type = HABackendType::UNKNOWN;
    std::string connstring;
    ClusterNamespace cluster_namespace;
    // Leadership lease TTL in seconds. Currently honored by the etcd backend.
    int64_t leader_lease_ttl_sec = DEFAULT_MASTER_VIEW_LEASE_TTL_SEC;
    // TTL in seconds for the master-view lease (redis/k8s backends). Controls
    // how fast a standby detects a dead primary. Alias of
    // leader_lease_ttl_sec: the supervisor fills both from
    // --ha_leader_lease_ttl_sec unless --master_view_lease_ttl_sec is set.
    int64_t master_view_lease_ttl_sec = DEFAULT_MASTER_VIEW_LEASE_TTL_SEC;
};

struct MasterView {
    std::string leader_address;
    ViewVersionId view_version = 0;
};

// A single submaster replay source for a standby. master_id doubles as the
// per-source OpLog namespace (see ha/oplog/oplog_batch_types.h).
struct MasterSource {
    std::string master_id;
    std::string address;
};

using MasterSources = std::vector<MasterSource>;

// OpLog source namespace written by a producer. Only multi-submaster (CVM)
// clusters partition the OpLog per master; single-leader HA keeps the
// unpartitioned layout so batch snapshots, compaction and leader failover
// all continue on one stream. Producer and standby must both derive the
// namespace through this function.
inline std::string OpLogSourceId(const std::string& master_id,
                                 uint32_t submaster_count) {
    return submaster_count > 1 ? master_id : std::string();
}

// Replay source set for a single-leader standby: the observed leader on the
// unpartitioned stream. Empty when no leader is known.
inline MasterSources SingleLeaderSources(
    const std::optional<MasterView>& leader_view) {
    if (!leader_view || leader_view->leader_address.empty()) {
        return {};
    }
    return {MasterSource{std::string(), leader_view->leader_address}};
}

// True for the single-leader layout (one unpartitioned source). Batch
// OpLog snapshots and compaction are only supported in this mode; multi-
// source standbys replay every source from the beginning.
inline bool IsUnpartitionedSingleSource(const MasterSources& sources) {
    return sources.size() == 1 && sources.front().master_id.empty();
}

struct LeadershipSession {
    MasterView view;
    // Backend-issued opaque ownership token. Only the backend that created
    // this session is responsible for minting and interpreting it.
    OwnerToken owner_token;
    std::chrono::milliseconds lease_ttl{0};
};

enum class AcquireLeadershipStatus {
    ACQUIRED,
    CONTENDED,
};

struct AcquireLeadershipResult {
    AcquireLeadershipStatus status = AcquireLeadershipStatus::CONTENDED;
    std::optional<LeadershipSession> session;
    std::optional<MasterView> observed_view;
};

struct ViewChangeResult {
    bool changed = false;
    bool timed_out = false;
    std::optional<MasterView> current_view;
};

enum class MasterRuntimeState {
    kStarting,
    kStandby,
    kCandidate,
    kRecovering,
    kCatchingUp,
    kLeaderWarmup,
    kServing,
};

inline const char* MasterRuntimeStateToString(MasterRuntimeState state) {
    switch (state) {
        case MasterRuntimeState::kStarting:
            return "starting";
        case MasterRuntimeState::kStandby:
            return "standby";
        case MasterRuntimeState::kCandidate:
            return "candidate";
        case MasterRuntimeState::kRecovering:
            return "recovering";
        case MasterRuntimeState::kCatchingUp:
            return "catching_up";
        case MasterRuntimeState::kLeaderWarmup:
            return "leader_warmup";
        case MasterRuntimeState::kServing:
            return "serving";
    }
    return "unknown";
}

inline const char* MasterRuntimeRoleToString(MasterRuntimeState state) {
    switch (state) {
        case MasterRuntimeState::kLeaderWarmup:
        case MasterRuntimeState::kServing:
            return "leader";
        case MasterRuntimeState::kStarting:
        case MasterRuntimeState::kStandby:
        case MasterRuntimeState::kCandidate:
        case MasterRuntimeState::kRecovering:
        case MasterRuntimeState::kCatchingUp:
            return "standby";
    }
    return "unknown";
}

enum class LeadershipLossReason {
    kRenewError,
    kLostLeadership,
};

inline const char* LeadershipLossReasonToString(LeadershipLossReason reason) {
    switch (reason) {
        case LeadershipLossReason::kRenewError:
            return "renew_error";
        case LeadershipLossReason::kLostLeadership:
            return "lost_leadership";
    }
    return "unknown";
}

using LeadershipLostCallback = std::function<void(LeadershipLossReason reason)>;

struct OpLogRecord {
    OpLogSequenceId seq = 0;
    ViewVersionId producer_view_version = 0;
    std::string payload;
};

struct OpLogAppendRequest {
    OpLogSequenceId expected_next_seq = 0;
    ViewVersionId producer_view_version = 0;
    std::string payload;
};

struct OpLogPollResult {
    std::vector<OpLogRecord> records;
    OpLogSequenceId next_seq = 0;
    bool timed_out = false;
};

}  // namespace ha
}  // namespace mooncake
