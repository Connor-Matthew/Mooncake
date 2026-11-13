#pragma once

#include <csignal>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <string>
#include <thread>

#include <ylt/coro_http/coro_http_server.hpp>

#include "ha/ha_types.h"

namespace mooncake {

extern const uint64_t kMetricReportIntervalSeconds;

class WrappedMasterService;

class MasterAdminServer {
   public:
    // A zero interval uses kMetricReportIntervalSeconds.
    MasterAdminServer(uint16_t http_port, bool enable_metric_reporting,
                      std::string http_host = "0.0.0.0",
                      uint32_t metrics_log_interval_sec = 0);
    MasterAdminServer(uint16_t http_port, bool enable_metric_reporting,
                      uint32_t metrics_log_interval_sec)
        : MasterAdminServer(http_port, enable_metric_reporting, "0.0.0.0",
                            metrics_log_interval_sec) {}

    ~MasterAdminServer();

    uint16_t port() const { return http_server_.port(); }

    bool Start();

    void Stop();

    void SetRuntimeState(ha::MasterRuntimeState state);

    void SetObservedLeader(const std::optional<ha::MasterView>& leader_view);

    void SetServiceDelegate(std::shared_ptr<WrappedMasterService> service);

    void SetServiceAvailable(bool available);

    // HA standby 覆盖率低频可观测（§16.18.2 满配缺口）：由 supervisor 注入
    // 拉取式 provider（读 CvmController 当前视图），admin metrics 线程按
    // kMetricReportIntervalSeconds 周期调用并拼入 Master Admin Metrics
    // 日志（置于 owned_slots 之前，行尾槽位约定不变）。provider 为空时
    // 显示 n/a；调用方须保证 provider 闭包的捕获生命周期安全（supervisor
    // 侧以 RAII guard 摘除，见 master_service_supervisor.cpp）。
    void SetStandbyCoverageProvider(std::function<std::string()> provider);

   private:
    struct RuntimeSnapshot {
        ha::MasterRuntimeState state = ha::MasterRuntimeState::kStarting;
        std::optional<ha::MasterView> leader_view;
        std::shared_ptr<WrappedMasterService> service;
        bool service_available = false;
        std::function<std::string()> standby_coverage_provider;
    };

    RuntimeSnapshot SnapshotState() const;

    std::string BuildMetricsText() const;

    std::string BuildTenantQuotaMetricsText() const;

    std::string BuildMetricsSummaryText() const;

    std::shared_ptr<WrappedMasterService> GetActiveService() const;

    template <typename Handler>
    void WithActiveService(coro_http::coro_http_response& resp,
                           Handler&& handler) const;

    void HandleMetrics(coro_http::coro_http_request& req,
                       coro_http::coro_http_response& resp);
    void HandleMetricsSummary(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleHealth(coro_http::coro_http_request& req,
                      coro_http::coro_http_response& resp);
    void HandleVersion(coro_http::coro_http_request& req,
                       coro_http::coro_http_response& resp);
    void HandleRole(coro_http::coro_http_request& req,
                    coro_http::coro_http_response& resp);
    void HandleHaStatus(coro_http::coro_http_request& req,
                        coro_http::coro_http_response& resp);
    void HandleLeader(coro_http::coro_http_request& req,
                      coro_http::coro_http_response& resp);
    void HandleQueryKey(coro_http::coro_http_request& req,
                        coro_http::coro_http_response& resp);
    void HandleGetAllKeys(coro_http::coro_http_request& req,
                          coro_http::coro_http_response& resp);
    void HandleGetAllSegments(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleGetSegmentsDetail(coro_http::coro_http_request& req,
                                 coro_http::coro_http_response& resp);
    void HandleQuerySegment(coro_http::coro_http_request& req,
                            coro_http::coro_http_response& resp);
    void HandleGetDfsShardCount(coro_http::coro_http_request& req,
                                coro_http::coro_http_response& resp);
    async_simple::coro::Lazy<void> HandleExpandDfsShards(
        coro_http::coro_http_request& req, coro_http::coro_http_response& resp);
    void HandleCreateDrainJob(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleQueryDrainJob(coro_http::coro_http_request& req,
                             coro_http::coro_http_response& resp);
    void HandleCancelDrainJob(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleSegmentStatus(coro_http::coro_http_request& req,
                             coro_http::coro_http_response& resp);
    void HandleSetSegmentStatus(coro_http::coro_http_request& req,
                                coro_http::coro_http_response& resp);
    void HandleBatchQueryKeys(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleKvEventsStatus(coro_http::coro_http_request& req,
                              coro_http::coro_http_response& resp);
    void HandleGetTenantQuotas(coro_http::coro_http_request& req,
                               coro_http::coro_http_response& resp);
    void HandleUpsertTenantQuota(coro_http::coro_http_request& req,
                                 coro_http::coro_http_response& resp);
    void HandleDeleteTenantQuota(coro_http::coro_http_request& req,
                                 coro_http::coro_http_response& resp);
    void HandleRemoveAll(coro_http::coro_http_request& req,
                         coro_http::coro_http_response& resp);

    void HandleSoftPin(coro_http::coro_http_request& req,
                       coro_http::coro_http_response& resp, bool unpin);

    void RegisterHandler();
    void RefreshStorageMetrics() const;

    uint16_t http_port_;
    std::string http_host_;
    bool enable_metric_reporting_ = false;
    uint64_t metrics_log_interval_sec_ = kMetricReportIntervalSeconds;
    coro_http::coro_http_server http_server_;
    std::thread metric_report_thread_;
    std::atomic<bool> metric_report_running_{false};
    std::binary_semaphore metric_report_stop_sem_{0};
    std::atomic<bool> started_{false};
    // Retained by each handler across its blocking operation and completion.
    // Only one admin expansion may be pending, including disconnected requests.
    std::shared_ptr<std::atomic<bool>> dfs_expansion_running_{
        std::make_shared<std::atomic<bool>>(false)};
    // Serializes storage projection with service-plane handoff so a refresh
    // that sampled the old leader cannot publish after it becomes unavailable.
    mutable std::mutex storage_metrics_refresh_mutex_;
    mutable std::mutex state_mutex_;
    ha::MasterRuntimeState state_{ha::MasterRuntimeState::kStarting};
    std::optional<ha::MasterView> leader_view_;
    std::shared_ptr<WrappedMasterService> service_;
    bool service_available_ = false;
    std::function<std::string()> standby_coverage_provider_;
};

}  // namespace mooncake
