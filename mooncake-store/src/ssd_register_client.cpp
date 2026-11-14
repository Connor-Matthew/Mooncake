#include "ssd_register_client.h"

#include "config/nof_register_config.h"

namespace mooncake {

namespace {

// Modified By Yida (v3): trtype 解析规则——
//   显式传入空串  -> 读 MC_NOF_TRTYPE，缺省 RDMA（旧行为，向后兼容）；
//   显式传入值    -> 仅接受 RDMA/TCP/URMA，非法值直接报错，绝不静默回退。
// te_endpoint 是 master 上 segment 的唯一身份（含 trtype），静默改写 trtype
// 会让 register/unregister 两端产生两个不同的 segment 名，故障难以排查。
bool NormalizeTrType(const std::string &raw_trtype, std::string &trtype) {
    if (raw_trtype.empty()) {
        const char *trtype_env = std::getenv("MC_NOF_TRTYPE");
        trtype = trtype_env ? trtype_env : "RDMA";
    } else {
        trtype = raw_trtype;
    }
    std::transform(
        trtype.begin(), trtype.end(), trtype.begin(),
        [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    if (trtype != "RDMA" && trtype != "TCP" && trtype != "URMA") {
        LOG(ERROR) << "Unsupported NoF trtype=" << trtype
                   << " (allowed: RDMA, TCP, URMA)";
        return false;
    }
    return true;
}

std::string BuildTeEndpoint(const std::string &traddr, size_t trsvcid,
                            const std::string &nqn,
                            const std::string &trtype, size_t nsid) {
    return "traddr:" + traddr + " trsvcid:" + std::to_string(trsvcid) +
           " subnqn:" + nqn + " trtype:" + trtype + " adrfam:IPv4 ns:" +
           std::to_string(nsid);
}

}  // namespace

NoFRegisterClient::NoFRegisterClient()
    : master_client_(generate_uuid(), nullptr) {}

NoFRegisterClient::~NoFRegisterClient() = default;

int NoFRegisterClient::set_register(const std::string &nqn, size_t nsid,
                                    const std::string &traddr, size_t trsvcid,
                                    const std::string &trtype, uintptr_t base,
                                    size_t size,
                                    const std::string &master_server_addr) {
    std::string normalized_trtype;
    if (!NormalizeTrType(trtype, normalized_trtype)) {
        return OPERATION_FAILED;
    }
    LOG(INFO) << "Registering SSD: nqn=" << nqn << ",nsid=" << nsid
              << ",traddr=" << traddr << ",trsvcid=" << trsvcid
              << ",trtype=" << normalized_trtype
              << ",master=" << master_server_addr << ",base=" << base
              << ",size=" << size;

    auto err = master_client_.Connect(master_server_addr);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to master";
        return OPERATION_FAILED;
    }

    std::string te_endpoint = BuildTeEndpoint(traddr, trsvcid, nqn,
                                              normalized_trtype, nsid);

    NoFSegment segment;
    segment.base = base;
    segment.size = size;
    segment.id = generate_uuid();
    segment.name = te_endpoint;
    segment.te_endpoint = te_endpoint;
    auto mount_result = master_client_.MountNoFSegment(segment);
    if (!mount_result) {
        LOG(ERROR) << "mount_segment_to_master_failed ";
        return OPERATION_FAILED;
    }

    return OPERATION_OK;
}

int NoFRegisterClient::set_unregister_by_endpoint(
    const std::string &nqn, size_t nsid, const std::string &traddr,
    size_t trsvcid, const std::string &trtype,
    const std::string &master_server_addr) {
    std::string normalized_trtype;
    if (!NormalizeTrType(trtype, normalized_trtype)) {
        return OPERATION_FAILED;
    }
    LOG(INFO) << "Unregistering SSD by endpoint: nqn=" << nqn
              << ",nsid=" << nsid << ",traddr=" << traddr
              << ",trsvcid=" << trsvcid << ",trtype=" << normalized_trtype
              << ",master=" << master_server_addr;

    // Connect to master server
    auto err = master_client_.Connect(master_server_addr);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to master: " << static_cast<int>(err);
        return OPERATION_FAILED;
    }

    // Build the te_endpoint string to match registered segments
    std::string te_endpoint = BuildTeEndpoint(traddr, trsvcid, nqn,
                                              normalized_trtype, nsid);

    LOG(INFO) << "Built te_endpoint: " << te_endpoint;

    auto matching_segments_result =
        master_client_.GetNoFSegmentsByName(te_endpoint);
    if (!matching_segments_result) {
        LOG(ERROR) << "Failed to get NoF segments by name: "
                   << static_cast<int>(matching_segments_result.error());
        return OPERATION_FAILED;
    }

    std::vector<NoFSegmentOwnerInfo> matching_segments =
        matching_segments_result.value();
    LOG(INFO) << "Retrieved " << matching_segments.size()
              << " mounted NoF segments for te_endpoint";
    if (matching_segments.empty()) {
        LOG(ERROR) << "No segment found for te_endpoint: " << te_endpoint;
        return OPERATION_FAILED;
    }

    // Unmount all matching segments
    bool all_unmounted = true;
    for (const auto &segment : matching_segments) {
        LOG(INFO) << "Found matching segment: id=" << segment.segment_id
                  << ", owner_client_id=" << segment.client_id;
        MasterClient owner_master_client(segment.client_id, nullptr);
        err = owner_master_client.Connect(master_server_addr);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect owner master client for segment "
                       << segment.segment_id << ": " << static_cast<int>(err);
            all_unmounted = false;
            continue;
        }

        auto unmount_result =
            owner_master_client.UnmountNoFSegment(segment.segment_id);
        if (!unmount_result) {
            LOG(ERROR) << "Failed to unmount segment " << segment.segment_id
                       << ": " << static_cast<int>(unmount_result.error());
            all_unmounted = false;
        } else {
            LOG(INFO) << "Successfully unmounted segment " << segment.segment_id
                      << ", owner_client_id=" << segment.client_id;
        }
    }

    return all_unmounted ? OPERATION_OK : OPERATION_FAILED;
}

}  // namespace mooncake
