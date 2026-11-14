#include "cvm/cvm_http_server.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>

#if __has_include(<jsoncpp/json/json.h>)
#include <jsoncpp/json/json.h>
#else
#include <json/json.h>
#endif

#include "cvm/etcd_view_store.h"

namespace mooncake {
namespace cvm {

namespace {

// SlotState 数值 → 稳定字符串（视图 JSON 用，防前端耦合枚举值）。
const char* SlotStateName(int32_t state) {
    switch (static_cast<SlotState>(state)) {
        case SlotState::kStable:
            return "kStable";
        case SlotState::kMigrating:
            return "kMigrating";
        default:
            return "unknown";
    }
}

// 纯数字解析 string_view → uint32（拒负号/溢出/空），HTTP 查询参数安全。
bool ParseUintParam(const std::string_view& sv, uint32_t& out) {
    if (sv.empty() || sv.size() > 10) {
        return false;
    }
    uint64_t value = 0;
    for (const char c : sv) {
        if (c < '0' || c > '9') {
            return false;
        }
        value = value * 10 + static_cast<uint64_t>(c - '0');
    }
    if (value > UINT32_MAX) {
        return false;
    }
    out = static_cast<uint32_t>(value);
    return true;
}

// 同上，uint64 变体（配额参数 stripe_size/member_extent_size 等用）。
bool ParseUint64Param(const std::string_view& sv, uint64_t& out) {
    if (sv.empty() || sv.size() > 20) {
        return false;
    }
    uint64_t value = 0;
    for (const char c : sv) {
        if (c < '0' || c > '9') {
            return false;
        }
        value = value * 10 + static_cast<uint64_t>(c - '0');
    }
    out = value;
    return true;
}

// 小数十进制解析 string_view → double（[0,1) 比率参数用，拒多余字符）。
bool ParseDoubleParam(const std::string_view& sv, double& out) {
    if (sv.empty() || sv.size() > 31) {
        return false;
    }
    bool seen_dot = false, seen_digit = false;
    for (const char c : sv) {
        if (c >= '0' && c <= '9') {
            seen_digit = true;
        } else if (c == '.' && !seen_dot) {
            seen_dot = true;
        } else {
            return false;
        }
    }
    if (!seen_digit) return false;
    out = std::strtod(std::string(sv).c_str(), nullptr);
    return true;
}

// JSON 字符串值最小转义（引号/反斜杠/控制字符），错误 reason 内嵌库
// detail 时防止破坏响应体。
std::string JsonEscape(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    for (const char c : in) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
            case '\r':
            case '\t':
                out += ' ';
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    out += ' ';
                } else {
                    out += c;
                }
                break;
        }
    }
    return out;
}

}  // namespace

CvmHttpServer::CvmHttpServer(Config config)
    : config_(std::move(config)),
      server_(std::make_unique<coro_http::coro_http_server>(4, config_.port)) {
    InitRoutes();
}

CvmHttpServer::~CvmHttpServer() { Stop(); }

void CvmHttpServer::InitRoutes() {
    using namespace coro_http;

    server_->set_http_handler<GET>(
        "/segment_view",
        [this](coro_http_request& req, coro_http_response& resp) {
            (void)req;
            std::string json = GetSegmentViewJson();
            if (json.empty()) {
                resp.set_status_and_content(
                    status_type::not_found, "segment view not found");
                return;
            }
            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, json);
        });

    server_->set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            (void)req;
            resp.set_status_and_content(status_type::ok, "OK");
        });

    // §16.14.6 观测：槽位组归属视图（G / 每 rank 归属 / 迁移意图）。
    server_->set_http_handler<GET>(
        "/ring_slots",
        [this](coro_http_request& req, coro_http_response& resp) {
            (void)req;
            std::string json = GetRingSlotsViewJson();
            if (json.empty()) {
                resp.set_status_and_content(
                    status_type::internal_server_error,
                    "ring_slots view unavailable (etcd read failed)");
                return;
            }
            resp.add_header("Content-Type", "application/json");
            resp.set_status_and_content(status_type::ok, json);
        });

    // §16.16.7 管理员 reshard 入口：
    //   POST /reshard?rank=<0..G-1>&target=<master_id>
    // 源自动取当前 owner，后续由目标侧 driver 断点续传；幂等重发安全。
    server_->set_http_handler<POST>(
        "/reshard",
        [this](coro_http_request& req, coro_http_response& resp) {
            uint32_t rank = 0;
            const auto rank_sv = req.get_query_value("rank");
            const auto target_sv = req.get_query_value("target");
            std::string json;
            ErrorCode err = ErrorCode::INVALID_PARAMS;
            if (ParseUintParam(rank_sv, rank)) {
                err = TriggerReshard(rank, std::string(target_sv), json);
            } else {
                json = R"({"status":"error","reason":"invalid or missing rank"})";
            }
            resp.add_header("Content-Type", "application/json");
            switch (err) {
                case ErrorCode::OK:
                    resp.set_status_and_content(status_type::ok, json);
                    break;
                case ErrorCode::INVALID_PARAMS:
                    resp.set_status_and_content(status_type::bad_request,
                                                json);
                    break;
                case ErrorCode::ETCD_KEY_NOT_EXIST:
                    resp.set_status_and_content(status_type::not_found, json);
                    break;
                case ErrorCode::ETCD_TRANSACTION_FAIL:
                case ErrorCode::STALE_ROUTE:
                    resp.set_status_and_content(status_type::conflict, json);
                    break;
                default:
                    resp.set_status_and_content(
                        status_type::internal_server_error, json);
                    break;
            }
        });

    // 管理员在线规划并发布 vsegment 配额（等价 planner CLI 的自动发现 +
    // --publish；etcd/namespace 用本 master 配置，无需调用方传）：
    //   POST /vsegment_quota?member_count=2&stripe_size=65536
    //                      &member_extent_size=1048576
    // 可选：reserved_ratio / initial_vsegment_count / required_medium /
    //       io_alignment / max_etcd_value_bytes（默认 32MiB，16384 partition
    //       的快照实测 ~4.6MB）。快照已存在 → 409（重复发布属预期）。
    server_->set_http_handler<POST>(
        "/vsegment_quota",
        [this](coro_http_request& req, coro_http_response& resp) {
            vsegment::VSegmentUserPolicy policy;
            uint64_t max_bytes = vsegment::kDefaultQuotaMaxEtcdValueBytes;
            std::string json;
            ErrorCode err = ErrorCode::INVALID_PARAMS;
            const auto mc_sv = req.get_query_value("member_count");
            const auto ss_sv = req.get_query_value("stripe_size");
            const auto mes_sv = req.get_query_value("member_extent_size");
            const bool required_ok =
                ParseUintParam(mc_sv, policy.member_count) &&
                ParseUint64Param(ss_sv, policy.stripe_size) &&
                ParseUint64Param(mes_sv, policy.member_extent_size);
            bool optional_ok = true;
            const auto rr_sv = req.get_query_value("reserved_ratio");
            if (!rr_sv.empty() &&
                !ParseDoubleParam(rr_sv, policy.reserved_ratio)) {
                optional_ok = false;
            }
            const auto ivc_sv = req.get_query_value("initial_vsegment_count");
            if (!ivc_sv.empty() &&
                !ParseUintParam(ivc_sv, policy.initial_vsegment_count)) {
                optional_ok = false;
            }
            const auto rm_sv = req.get_query_value("required_medium");
            if (!rm_sv.empty()) {
                policy.required_medium = std::string(rm_sv);
            }
            const auto ia_sv = req.get_query_value("io_alignment");
            if (!ia_sv.empty() &&
                !ParseUint64Param(ia_sv, policy.io_alignment)) {
                optional_ok = false;
            }
            const auto mb_sv = req.get_query_value("max_etcd_value_bytes");
            if (!mb_sv.empty() && !ParseUint64Param(mb_sv, max_bytes)) {
                optional_ok = false;
            }
            if (required_ok && optional_ok) {
                err = TriggerVSegmentQuota(policy, max_bytes, json);
            } else {
                json = required_ok
                           ? R"({"status":"error","reason":"invalid optional parameter"})"
                           : R"({"status":"error","reason":"member_count, stripe_size and member_extent_size are required"})";
            }
            resp.add_header("Content-Type", "application/json");
            switch (err) {
                case ErrorCode::OK:
                    resp.set_status_and_content(status_type::ok, json);
                    break;
                case ErrorCode::INVALID_PARAMS:
                    resp.set_status_and_content(status_type::bad_request,
                                                json);
                    break;
                case ErrorCode::ETCD_TRANSACTION_FAIL:
                    resp.set_status_and_content(status_type::conflict, json);
                    break;
                default:
                    resp.set_status_and_content(
                        status_type::internal_server_error, json);
                    break;
            }
        });
}

ErrorCode CvmHttpServer::Start() {
    if (running_.load()) {
        return ErrorCode::OK;
    }

    // async_start() binds synchronously and returns a future that is already
    // resolved (hasResult()) when the bind failed. Mirrors
    // HttpMetadataServer::start().
    auto ec = server_->async_start();
    if (ec.hasResult()) {
        LOG(ERROR) << "CvmHttpServer failed to start on " << config_.host << ":"
                   << config_.port;
        return ErrorCode::RPC_FAIL;
    }
    running_.store(true);
    LOG(INFO) << "CvmHttpServer started on " << config_.host << ":"
              << config_.port;
    return ErrorCode::OK;
}

void CvmHttpServer::Stop() {
    if (!running_.exchange(false)) {
        return;
    }
    server_->stop();
    LOG(INFO) << "CvmHttpServer stopped";
}

std::string CvmHttpServer::GetSegmentViewJson() const {
    std::vector<SegmentDescriptor> descriptors;
    ViewVersionId desc_version = 0;
    ErrorCode err = EtcdViewStore::LoadAllSegmentDescriptors(
        config_.cluster_namespace, descriptors, desc_version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmHttpServer load segment descriptors failed: "
                     << err;
        return "";
    }

    std::vector<std::pair<std::string, MountEntry>> mounts;
    ViewVersionId mount_version = 0;
    err = EtcdViewStore::LoadAllMountEntries(config_.cluster_namespace, mounts,
                                             mount_version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmHttpServer load segment mounts failed: " << err;
        return "";
    }

    // 按照 segment_id 聚合：每个 segment 输出其描述符 + 挂载它的 master 列表。
    std::map<std::string, const SegmentDescriptor*> desc_by_id;
    for (const auto& desc : descriptors) {
        desc_by_id[desc.segment_id] = &desc;
    }

    std::map<std::string, std::vector<std::pair<std::string, const MountEntry*>>>
        mounts_by_seg;
    for (const auto& mount : mounts) {
        mounts_by_seg[mount.second.segment_id].emplace_back(mount.first,
                                                            &mount.second);
    }

    // 收集所有 segment_id（描述符 ∪ 挂载记录）并去重，保持稳定排序输出。
    std::vector<std::string> segment_ids;
    segment_ids.reserve(desc_by_id.size() + mounts_by_seg.size());
    for (const auto& kv : desc_by_id) {
        segment_ids.push_back(kv.first);
    }
    for (const auto& kv : mounts_by_seg) {
        segment_ids.push_back(kv.first);
    }
    std::sort(segment_ids.begin(), segment_ids.end());
    segment_ids.erase(std::unique(segment_ids.begin(), segment_ids.end()),
                      segment_ids.end());

    Json::Value root(Json::objectValue);
    Json::Value segments(Json::arrayValue);
    for (const auto& segment_id : segment_ids) {
        Json::Value seg(Json::objectValue);
        seg["segment_id"] = segment_id;

        auto desc_it = desc_by_id.find(segment_id);
        if (desc_it != desc_by_id.end()) {
            const SegmentDescriptor& desc = *desc_it->second;
            seg["segment_name"] = desc.segment_name;
            seg["capacity"] = static_cast<Json::Value::UInt64>(desc.capacity);
            seg["te_endpoint"] = desc.te_endpoint;
            seg["protocol"] = desc.protocol;
            seg["host_id"] = desc.host_id;
            // 资源事实（供 vsegment 自动发现 + 运维查看）
            seg["medium"] = desc.medium;
            seg["io_alignment"] =
                static_cast<Json::Value::UInt64>(desc.io_alignment);
            seg["supports_unaligned_io"] = desc.supports_unaligned_io;
            seg["failure_domain"] = desc.failure_domain;
            seg["vsegment_exclusive"] = desc.vsegment_exclusive;
        }

        Json::Value masters(Json::arrayValue);
        auto mount_it = mounts_by_seg.find(segment_id);
        if (mount_it != mounts_by_seg.end()) {
            for (const auto& m : mount_it->second) {
                Json::Value mount(Json::objectValue);
                mount["master_id"] = m.first;
                mount["mounted_at_ms"] =
                    static_cast<Json::Value::Int64>(m.second->mounted_at_ms);
                Json::Value slot_starts(Json::arrayValue);
                for (uint16_t s : m.second->partition_slot_starts) {
                    slot_starts.append(s);
                }
                mount["partition_slot_starts"] = slot_starts;
                masters.append(mount);
            }
        }
        seg["masters"] = masters;

        segments.append(seg);
    }
    root["segments"] = segments;

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

std::string CvmHttpServer::GetRingSlotsViewJson() const {
    // §16.14.6 观测视图：G + 存活成员 + 每 rank 归属（primary/standbys/
    // state/epoch）+ 进行中的迁移意图。全部只读 etcd，可打向任意 master。
    RingMeta meta;
    ViewVersionId version = 0;
    ErrorCode err = EtcdViewStore::LoadClusterMeta(
        config_.cluster_namespace, meta, version);
    if (err != ErrorCode::OK) {
        LOG(WARNING) << "CvmHttpServer load cluster meta failed: " << err;
        return "";
    }

    std::vector<RingSlotAssign> assigns;
    if (EtcdViewStore::LoadAllRingSlotAssigns(config_.cluster_namespace,
                                              assigns,
                                              version) != ErrorCode::OK) {
        LOG(WARNING) << "CvmHttpServer load ring slots failed";
        return "";
    }

    std::vector<ReshardIntent> intents;
    if (EtcdViewStore::LoadAllReshardIntents(config_.cluster_namespace,
                                              intents,
                                              version) != ErrorCode::OK) {
        LOG(WARNING) << "CvmHttpServer load reshard intents failed";
        return "";
    }

    std::vector<MasterRegistration> masters;
    if (EtcdViewStore::LoadAllMasters(config_.cluster_namespace, masters,
                                      version) != ErrorCode::OK) {
        LOG(WARNING) << "CvmHttpServer load masters failed";
        return "";
    }

    Json::Value root(Json::objectValue);
    root["slot_group_count"] =
        static_cast<Json::Value::UInt>(meta.slot_group_count);
    root["submaster_count"] =
        static_cast<Json::Value::UInt>(meta.submaster_count);
    root["slot_count"] = static_cast<Json::Value::UInt>(meta.slot_count);
    // 与 CvmController::HasCachedRingSlotAssigns 同语义：存在归属记录 =
    // 模型启用；false = 旧集群灰度回退中（ranks 为空数组）。
    root["model_active"] = !assigns.empty();

    Json::Value masters_arr(Json::arrayValue);
    for (const auto& m : masters) {
        Json::Value entry(Json::objectValue);
        entry["master_id"] = m.master_id;
        entry["address"] = m.address;
        entry["role"] = static_cast<Json::Value::Int>(m.role);
        masters_arr.append(entry);
    }
    root["masters"] = masters_arr;

    Json::Value ranks_arr(Json::arrayValue);
    for (const auto& a : assigns) {
        Json::Value entry(Json::objectValue);
        entry["rank"] = static_cast<Json::Value::UInt>(a.rank);
        entry["primary"] = a.primary_id;
        Json::Value standbys(Json::arrayValue);
        for (const auto& sid : a.standby_ids) {
            standbys.append(sid);
        }
        entry["standbys"] = standbys;
        entry["state"] = SlotStateName(a.state);
        entry["migrating_to"] = a.migrating_to_id;
        entry["epoch"] = static_cast<Json::Value::UInt64>(a.epoch);
        ranks_arr.append(entry);
    }
    root["ranks"] = ranks_arr;

    Json::Value intents_arr(Json::arrayValue);
    for (const auto& i : intents) {
        Json::Value entry(Json::objectValue);
        entry["rank"] = static_cast<Json::Value::UInt>(i.rank);
        entry["source_primary_id"] = i.source_primary_id;
        entry["target_primary_id"] = i.target_primary_id;
        intents_arr.append(entry);
    }
    root["reshard_intents"] = intents_arr;

    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

ErrorCode CvmHttpServer::TriggerReshard(uint32_t rank,
                                       const std::string& target_master_id,
                                       std::string& out_json) {
    // §16.16.7 管理员 reshard 入口。只做「写 intent + CAS kMigrating」
    // 两个动作（源不动、数据不动），后续阶段由目标侧 driver 断点续传——
    // 管理员入口不感知迁移进度，天然幂等可重发。
    out_json.clear();
    if (target_master_id.empty()) {
        out_json = R"({"status":"error","reason":"missing target"})";
        return ErrorCode::INVALID_PARAMS;
    }

    RingMeta meta;
    ViewVersionId version = 0;
    ErrorCode err = EtcdViewStore::LoadClusterMeta(
        config_.cluster_namespace, meta, version);
    if (err != ErrorCode::OK) {
        out_json = R"({"status":"error","reason":"etcd unavailable"})";
        return err;
    }
    if (meta.slot_group_count == 0 || rank >= meta.slot_group_count) {
        out_json = R"({"status":"error","reason":"rank out of range"})";
        return ErrorCode::INVALID_PARAMS;
    }

    RingSlotAssign assign;
    err = EtcdViewStore::LoadRingSlotAssign(config_.cluster_namespace, rank,
                                            assign, version);
    if (err == ErrorCode::ETCD_KEY_NOT_EXIST) {
        out_json = R"({"status":"error","reason":"rank not assigned"})";
        return ErrorCode::ETCD_KEY_NOT_EXIST;
    }
    if (err != ErrorCode::OK) {
        out_json = R"({"status":"error","reason":"etcd unavailable"})";
        return err;
    }

    if (assign.primary_id == target_master_id) {
        // 目标已是 owner：幂等完成态（残留 intent 由目标 driver 自愈清理）。
        out_json =
            R"({"status":"ok","reason":"target already owns this rank"})";
        return ErrorCode::OK;
    }
    if (static_cast<SlotState>(assign.state) == SlotState::kMigrating) {
        if (assign.migrating_to_id == target_master_id) {
            // 同一迁移已在进行：幂等成功。
            out_json = R"({"status":"ok","reason":"migration in progress"})";
            return ErrorCode::OK;
        }
        out_json = R"({"status":"error","reason":"another migration in)"
                   R"( progress on this rank"})";
        return ErrorCode::ETCD_TRANSACTION_FAIL;
    }

    // 成员校验：target 与源都必须存活（源死亡属故障恢复路径，不归管理员
    // reshard 处理，避免与晋升/接管竞争）。
    std::vector<MasterRegistration> masters;
    if (EtcdViewStore::LoadAllMasters(config_.cluster_namespace, masters,
                                      version) != ErrorCode::OK) {
        out_json = R"({"status":"error","reason":"etcd unavailable"})";
        return ErrorCode::RPC_FAIL;
    }
    const auto is_alive = [&masters](const std::string& id) {
        return std::any_of(masters.begin(), masters.end(),
                           [&](const MasterRegistration& m) {
                               return m.master_id == id;
                           });
    };
    if (!is_alive(target_master_id)) {
        out_json = R"({"status":"error","reason":"target not a live)"
                   R"( master"})";
        return ErrorCode::INVALID_PARAMS;
    }
    if (!is_alive(assign.primary_id)) {
        // 自定义定界符 R"j(...)j"：reason 文本含 "(...)"，其 `)"` 与默认
        // raw string 终结符冲突（默认定界符无法表达，GCC 报 missing
        // terminating " character）。
        out_json =
            R"j({"status":"error","reason":"source primary not alive (handled by recovery paths)"})j";
        return ErrorCode::INVALID_PARAMS;
    }

    // 写意图（源取当前 owner）。已存在：同 (rank, source, target) 视为重发
    // （断点续传），否则拒绝。
    ReshardIntent intent;
    intent.rank = rank;
    intent.source_primary_id = assign.primary_id;
    intent.target_primary_id = target_master_id;
    err = EtcdViewStore::CreateReshardIntent(config_.cluster_namespace, intent);
    if (err == ErrorCode::ETCD_TRANSACTION_FAIL) {
        bool confirmed = false;
        std::vector<ReshardIntent> existing;
        if (EtcdViewStore::LoadAllReshardIntents(config_.cluster_namespace,
                                                 existing,
                                                 version) == ErrorCode::OK) {
            for (const auto& i : existing) {
                if (i.rank != rank) {
                    continue;
                }
                if (i.source_primary_id == intent.source_primary_id &&
                    i.target_primary_id == target_master_id) {
                    confirmed = true;  // 重发，继续
                }
                break;
            }
        }
        if (!confirmed) {
            // 冲突意图被并发删除（极窄竞态）→ 重写一次；仍失败即真冲突。
            err = EtcdViewStore::CreateReshardIntent(config_.cluster_namespace,
                                                     intent);
            if (err != ErrorCode::OK) {
                out_json = R"({"status":"error","reason":"conflicting)"
                           R"( intent exists"})";
                return ErrorCode::ETCD_TRANSACTION_FAIL;
            }
        }
    } else if (err != ErrorCode::OK) {
        out_json = R"({"status":"error","reason":"etcd unavailable"})";
        return err;
    }

    // CAS kMigrating（源 primary 不动，migrating_to=target）：源 watch 即时
    // 冻结该段写，读继续由源服务；目标 ComputeDesiredRole 认 migrating_to
    // 后进入 serve 阶段拉起 driver 续传。
    RingSlotAssign out_assign;
    err = EtcdViewStore::AdoptRankViaCAS(
        config_.cluster_namespace, rank, assign.epoch, assign.primary_id,
        SlotState::kMigrating, target_master_id, out_assign, nullptr,
        "admin reshard", assign.primary_id, /*success_as_warning=*/false,
        /*clear_intent=*/false);
    if (err != ErrorCode::OK) {
        // 回滚本机刚写的意图（幂等删除）；并发改判（STALE）返回冲突。
        (void)EtcdViewStore::DeleteReshardIntent(config_.cluster_namespace,
                                                  rank);
        out_json = err == ErrorCode::STALE_ROUTE
                       ? R"({"status":"error","reason":"concurrent)"
                         R"( modification, retry"})"
                       : R"({"status":"error","reason":"etcd unavailable"})";
        return err == ErrorCode::STALE_ROUTE ? ErrorCode::STALE_ROUTE : err;
    }

    out_json = R"({"status":"accepted","rank":)" + std::to_string(rank) +
               R"(,"source":")" + assign.primary_id + R"(","target":")" +
               target_master_id + R"("})";
    return ErrorCode::OK;
}

ErrorCode CvmHttpServer::TriggerVSegmentQuota(
    const vsegment::VSegmentUserPolicy& policy,
    uint64_t max_etcd_value_bytes, std::string& out_json) {
    out_json.clear();
    std::string detail;
    const ErrorCode err = vsegment::PlanAndPublishDiscoveredQuota(
        config_.cluster_namespace, policy,
        static_cast<size_t>(max_etcd_value_bytes), &detail, &out_json);
    if (err == ErrorCode::OK) {
        return ErrorCode::OK;  // out_json 已是摘要（PlanAndPublish 填好）
    }
    // 失败：详细 detail 进服务端日志，响应体给转义后的短语（409 = 快照
    // 已存在，重复发布同一代属预期，调用方可视为已发布）。
    LOG(ERROR) << "TriggerVSegmentQuota failed: err=" << toString(err)
               << ", detail=" << detail;
    const std::string reason = detail.empty() ? toString(err) : detail;
    out_json = "{\"status\":\"error\",\"reason\":\"" + JsonEscape(reason) +
               "\"}";
    return err;
}

}  // namespace cvm
}  // namespace mooncake
