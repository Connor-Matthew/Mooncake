#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include <ylt/coro_http/coro_http_server.hpp>

#include "types.h"
#include "vsegment/partition_quota_planner.h"

namespace mooncake {
namespace cvm {

// CVM 三模块之一：外部 HTTP 接口（cvmhttpserver）。
//
// 提供只读 HTTP API，直接聚合 etcd 中的 segment 视图并返回 JSON，供网页 /
// 外部客户端展示。segment 视图由「中立描述符 segments/{seg_id}」与「每 master
// 挂载记录 snapshot/{master_id}/segments/{seg_id}」聚合而来（见 cvm_keys.h）。
class CvmHttpServer {
   public:
    struct Config {
        std::string host = "0.0.0.0";
        uint16_t port = 0;
        std::string cluster_namespace;
    };

    explicit CvmHttpServer(Config config);
    ~CvmHttpServer();

    CvmHttpServer(const CvmHttpServer&) = delete;
    CvmHttpServer& operator=(const CvmHttpServer&) = delete;

    ErrorCode Start();
    void Stop();

    // 聚合 snapshot/*/segments/ 的挂载记录与 segments/* 的描述符，返回
    // segment 视图 JSON；读取失败时返回空串（成功时返回含 `segments` 数组的
    // JSON，可能为空数组）。
    std::string GetSegmentViewJson() const;

    // 聚合 cluster_meta（G）+ ring_slots/{rank} + reshard_intent/{rank} +
    // 存活 masters，返回槽位组归属视图 JSON（排查「谁持有哪个 rank /
    // 迁移卡在哪个阶段」的一站式观测点）。model_active=false 表示
    // ring_slots 模型未启用（旧集群灰度回退中）。读取失败返回空串。
    std::string GetRingSlotsViewJson() const;

    // 管理员发起 reshard（§16.16.7）：target 必须是存活成员且非当前
    // owner；写 reshard_intent（源自动取当前 owner）+ CAS kMigrating
    // 冻结源写。后续阶段（切 owner / 拉快照 / ack）由目标侧 driver 断点
    // 续传。幂等：同一 (rank, target) 重复调用返回 OK。成功时 out_json
    // 为响应体；校验失败 INVALID_PARAMS、rank 未分配 ETCD_KEY_NOT_EXIST、
    // 并发冲突 ETCD_TRANSACTION_FAIL。
    ErrorCode TriggerReshard(uint32_t rank, const std::string& target_master_id,
                             std::string& out_json);

    // 管理员在线规划并发布 vsegment 配额快照（等价 vsegment_quota_planner
    // 的自动发现 + --publish 路径；etcd_endpoints / cluster_namespace 取本
    // master 配置，无需调用方传入）。策略三核心参数必填（member_count /
    // stripe_size / member_extent_size），其余字段用 VSegmentUserPolicy 默认
    // 值。发布走 etcd 原子首写：快照已存在返回 ETCD_TRANSACTION_FAIL（重复
    // 发布同一代属预期，调用方可视为成功）。成功时 out_json 为摘要 JSON；
    // 校验失败 INVALID_PARAMS。
    ErrorCode TriggerVSegmentQuota(
        const vsegment::VSegmentUserPolicy& policy,
        uint64_t max_etcd_value_bytes, std::string& out_json);

   private:
    void InitRoutes();

    Config config_;
    std::unique_ptr<coro_http::coro_http_server> server_;

    std::atomic<bool> running_{false};
};

}  // namespace cvm
}  // namespace mooncake