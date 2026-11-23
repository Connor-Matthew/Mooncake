#include "basic.h"
#include "config.h"
#include "http_transport.h"
#include "maintenance.h"
#include "metrics.h"
#include "notify_codec.h"
#include "notify_rpc_transport.h"
#include "notify_sender.h"
#include "operations.h"
#include "pipeline.h"
#include "real_client.h"
#include "real_store_adapter.h"

#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake::kvtest {
namespace {

std::atomic<bool> *g_running = nullptr;

void SignalHandler(int) {
    if (g_running) g_running->store(false, std::memory_order_release);
}

std::string BasicSummaryExtra(const Config &config,
                              const BasicRunner::Stats &stats) {
    std::ostringstream out;
    out << "\"mode\":\"basic\""
        << ",\"role\":\"" << config.role << "\""
        << ",\"operation\":\"" << config.operation << "\""
        << ",\"threads\":" << config.num_threads
        << ",\"key_count\":" << config.key_count
        << ",\"key_start\":" << config.key_start
        << ",\"key_end\":" << config.EffectiveKeyEnd()
        << ",\"data_size\":"
        << (config.data_sizes.empty() ? 0 : config.data_sizes.front())
        << ",\"batch_size\":" << config.BatchSize()
        << ",\"put_ratio\":" << config.put_ratio
        << ",\"target_qps\":" << config.target_qps
        << ",\"duration\":" << config.duration
        << ",\"warmup_duration\":" << config.warmup_duration
        << ",\"warmup_ops\":" << stats.warmup_ops
        << ",\"measured_ops\":" << stats.measured_ops
        << ",\"measured_failures\":" << stats.measured_failures;
    return out.str();
}

std::string PipelineSummaryExtra(const Config &config,
                                 const PipelineWriter::Stats *writer,
                                 const PipelineReader::Stats *reader,
                                 const NotifySender::Stats *notify,
                                 const NotifyHttpClient *http,
                                 size_t reader_queue,
                                 uint64_t reader_dropped) {
    std::ostringstream out;
    out << "\"mode\":\"pipeline\""
        << ",\"role\":\"" << config.role << "\""
        << ",\"traffic_mode\":\"" << config.traffic_mode << "\""
        << ",\"target_qps\":" << config.target_qps
        << ",\"batch_keys\":" << config.batch_keys_count
        << ",\"key_pool_size\":" << config.key_pool_size
        << ",\"notify_count\":" << config.notify_count
        << ",\"notify_transport\":\"" << config.notify_transport << "\""
        << ",\"notify_async\":"
        << (config.notify_async ? "true" : "false")
        << ",\"warmup_duration\":" << config.warmup_duration
        << ",\"duration\":" << config.duration
        << ",\"reader_queue\":" << reader_queue
        << ",\"reader_queue_dropped\":" << reader_dropped;
    if (writer != nullptr) {
        out << ",\"phase\":\"" << writer->phase << "\""
            << ",\"measured_ops\":" << writer->measured_ops
            << ",\"measured_failures\":" << writer->measured_failures
            << ",\"warmup_ops\":" << writer->warmup_ops
            << ",\"maintenance_ops\":" << writer->maintenance_ops
            << ",\"notify_submitted\":" << writer->notifications
            << ",\"notify_dispatch_failures\":" << writer->notify_failures;
    }
    if (reader != nullptr) {
        out << ",\"reader_batches\":" << reader->batches
            << ",\"reader_warmup_batches\":" << reader->warmup_batches
            << ",\"reader_precheck_misses\":" << reader->precheck_misses;
    }
    if (notify != nullptr) {
        const uint64_t attempts = notify->ok + notify->fail;
        const double avg_ms =
            attempts == 0 ? 0.0
                          : notify->elapsed_sum_ms /
                                static_cast<double>(attempts);
        const double wait_avg_ms =
            attempts == 0 ? 0.0
                          : notify->queue_wait_sum_ms /
                                static_cast<double>(attempts);
        out << ",\"notify_http_submitted\":" << notify->submitted
            << ",\"notify_http_ok\":" << notify->ok
            << ",\"notify_http_fail\":" << notify->fail
            << ",\"notify_dropped\":" << notify->dropped
            << ",\"notify_inflight_peak\":" << notify->inflight_peak
            << ",\"notify_queue_peak\":" << notify->queue_peak
            << ",\"notify_backpressure_ms\":" << notify->backpressure_ms
            << ",\"notify_queue_wait_avg_ms\":" << wait_avg_ms
            << ",\"notify_queue_wait_max_ms\":" << notify->queue_wait_max_ms
            << ",\"notify_drain_ms\":" << notify->drain_ms
            << ",\"notify_avg_ms\":" << avg_ms
            << ",\"notify_max_ms\":" << notify->elapsed_max_ms;
    }
    if (http != nullptr) {
        out << ",\"notify_connections_created\":" << http->Created()
            << ",\"notify_connections_reused\":" << http->Reused();
    }
    return out.str();
}

}  // namespace
}  // namespace mooncake::kvtest

int main(int argc, char **argv) {
    using namespace mooncake;
    using namespace mooncake::kvtest;

    if (argc < 2 || argc > 3) {
        std::cerr << "Usage: " << argv[0]
                  << " <config.json> [--check-config]\n";
        return 2;
    }
    const bool check_only = argc == 3 && std::string(argv[2]) == "--check-config";
    Config config;
    std::string error;
    if (!LoadConfig(argv[1], &config, &error)) {
        std::cerr << "Config error: " << error << '\n';
        return 2;
    }
    if (config.IsPipeline()) {
        if (!ValidateOpList(config.pipeline, OpSide::kAny, true, &error) ||
            !ValidateOpList(config.notify_pipeline, OpSide::kRead, true,
                            &error)) {
            std::cerr << "Config error: " << error << '\n';
            return 2;
        }
    } else if (config.IsBasic() &&
               !ValidateBasicOperation(config, &error)) {
        std::cerr << "Config error: " << error << '\n';
        return 2;
    }
    if (check_only) {
        std::cout << "Config OK: mode=" << config.mode
                  << " role=" << config.role
                  << " write_threads=" << config.num_threads
                  << " read_threads=" << config.NumReadThreads()
                  << " traffic=" << config.traffic_mode
                  << " target_qps=" << config.target_qps
                  << " batch_keys=" << config.batch_keys_count;
        if (config.IsBasic()) std::cout << " operation=" << config.operation;
        std::cout << '\n';
        return 0;
    }
    if (!ApplyCpuAffinity(config.cpu_affinity, &error)) {
        std::cerr << "CPU affinity error: " << error << '\n';
        return 2;
    }

    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    std::filesystem::create_directories(config.output_dir);

    auto client = RealClient::create();
    const int setup_rc = client->setup_real(
        config.store.local_hostname, config.store.metadata_server,
        config.store.global_segment_size, config.store.local_buffer_size,
        config.store.protocol, config.store.rdma_devices,
        config.store.master_server_address, nullptr, "", false, "",
        config.store.tenant_id);
    if (setup_rc != 0) {
        LOG(ERROR) << "Mooncake client setup failed: " << setup_rc;
        google::ShutdownGoogleLogging();
        return 1;
    }

    RealStoreAdapter store_adapter(client, config.store.replica_num);
    if (config.IsOneShot()) {
        const int rc = config.mode == "prefill"
                           ? RunPrefill(config, store_adapter, &error)
                           : RunCleanup(config, store_adapter, &error);
        if (rc != 0) {
            std::cerr << "kvtest " << config.mode << " failed: " << error
                      << '\n';
        }
        client->tearDownAll();
        google::ShutdownGoogleLogging();
        return rc;
    }

    std::atomic<bool> running{true};
    g_running = &running;
    std::signal(SIGTERM, SignalHandler);
    std::signal(SIGINT, SignalHandler);
#ifdef SIGPIPE
    std::signal(SIGPIPE, SIG_IGN);
#endif

    MetricsCollector metrics(config.instance_id, config.metrics_interval_ms,
                             config.output_dir, config.samples_max);
    metrics.Start();

    std::unique_ptr<PipelineWriter> writer;
    std::unique_ptr<PipelineReader> reader;
    std::unique_ptr<PipelineDispatcher> dispatcher;
    std::unique_ptr<NotifySender> notify_sender;
    std::unique_ptr<NotifyHttpClient> http_client;
    std::unique_ptr<NotifyRpcClient> rpc_client;
    std::unique_ptr<NotifyRpcServer> rpc_server;
    std::unique_ptr<BasicRunner> basic_runner;

    auto startup_failed = [&](const std::string &message) {
        LOG(ERROR) << message;
        metrics.Stop();
        client->tearDownAll();
        google::ShutdownGoogleLogging();
        return 1;
    };

    auto prepare_writer = [&]() -> bool {
        writer = std::make_unique<PipelineWriter>(
            config, store_adapter, metrics, notify_sender.get());
        if (!writer->Prepare(&error)) {
            LOG(ERROR) << "writer prepare failed: " << error;
            return false;
        }
        return true;
    };
    auto prepare_basic = [&]() -> bool {
        basic_runner =
            std::make_unique<BasicRunner>(config, store_adapter, metrics);
        if (!basic_runner->Prepare(&error)) {
            LOG(ERROR) << "basic prepare failed: " << error;
            return false;
        }
        return true;
    };

    if (config.IsPipeline()) {
        if (config.role == "reader" || config.role == "dual") {
            reader = std::make_unique<PipelineReader>(config, store_adapter,
                                                      metrics);
            if (!reader->Prepare(&error)) {
                return startup_failed("reader prepare failed: " + error);
            }
            dispatcher = std::make_unique<PipelineDispatcher>(
                config, *reader, metrics);
        }
        if (config.role == "writer" || config.role == "dual") {
            http_client = std::make_unique<NotifyHttpClient>(3);
            NotifyHttpClient *http = http_client.get();
            if (config.notify_transport == "rpc") {
                rpc_client = std::make_unique<NotifyRpcClient>(
                    config.notify_rpc_connections_per_peer,
                    config.notify_rpc_reuse_limit,
                    config.notify_rpc_timeout_ms);
            }
            NotifyRpcClient *rpc = rpc_client.get();
            const auto send_fn = [http, rpc, &config, &metrics](
                                     const std::string &peer,
                                     const NotifyBatch &batch,
                                     double *latency_ms) -> bool {
                const auto start = std::chrono::steady_clock::now();
                bool ok = false;
                std::string send_error;
                if (config.notify_transport == "rpc") {
                    NotifyRpcRequest request;
                    request.keys = batch.keys;
                    request.size = batch.size;
                    request.sender = batch.sender;
                    request.warmup = batch.warmup;
                    NotifyRpcTiming timing;
                    ok = rpc->Notify(peer, request, &timing, &send_error);
                    metrics.Record("notify_rpc_pool_lookup",
                                   timing.pool_lookup_ms,
                                   timing.pool_found ? RecordStatus::kOk
                                                     : RecordStatus::kError,
                                   0);
                    metrics.Record("notify_rpc_pool_dispatch",
                                   timing.pool_dispatch_ms,
                                   timing.dispatched ? RecordStatus::kOk
                                                     : RecordStatus::kError,
                                   0);
                    metrics.Record("notify_rpc_await_response",
                                   timing.await_response_ms,
                                   timing.responded ? RecordStatus::kOk
                                                    : RecordStatus::kError,
                                   0);
                } else {
                    std::string method = "POST";
                    std::string path = "/notify";
                    std::string body;
                    if (config.notify_format == "query") {
                        method = "GET";
                        path = EncodeNotifyQuery(batch);
                    } else {
                        body = EncodeNotifyJson(batch);
                    }
                    ok = http->Send(peer, method, path, body, &send_error);
                }
                if (!ok) {
                    static std::atomic<int> warned{0};
                    if (warned.fetch_add(1, std::memory_order_relaxed) < 3) {
                        LOG(WARNING) << "notify send failed for " << peer
                                     << " (" << config.notify_transport
                                     << "): " << send_error;
                    }
                }
                if (latency_ms != nullptr) {
                    *latency_ms =
                        std::chrono::duration<double, std::milli>(
                            std::chrono::steady_clock::now() - start)
                            .count();
                }
                return ok;
            };
            const auto record_fn = [&metrics, &config](bool ok,
                                                       double latency_ms) {
                metrics.Record(config.notify_transport == "rpc" ? "notify_rpc"
                                                                : "notify_http",
                               latency_ms,
                               ok ? RecordStatus::kOk : RecordStatus::kError,
                               0);
            };
            const NotifySender::Overflow overflow =
                config.notify_overflow == "block"
                    ? NotifySender::Overflow::kBlock
                    : NotifySender::Overflow::kDrop;
            notify_sender = std::make_unique<NotifySender>(
                static_cast<size_t>(config.notify_threads),
                config.notify_queue_max, overflow, config.notify_async,
                send_fn, record_fn);
            if (!prepare_writer()) {
                return startup_failed("writer prepare failed: " + error);
            }
        }
        metrics.SetSummaryExtra(PipelineSummaryExtra(
            config, nullptr, nullptr, nullptr, http_client.get(), 0, 0));
    } else {
        if (!prepare_basic()) {
            return startup_failed("basic prepare failed: " + error);
        }
        metrics.SetSummaryExtra(
            BasicSummaryExtra(config, BasicRunner::Stats{}));
    }

    if (config.IsPipeline() && config.notify_transport == "rpc" &&
        (config.role == "reader" || config.role == "dual")) {
        rpc_server = std::make_unique<NotifyRpcServer>(
            config.notify_rpc_port, config.notify_rpc_threads,
            [&](NotifyRpcRequest request) {
                if (!dispatcher || !running.load(std::memory_order_acquire)) {
                    return false;
                }
                NotifyBatch batch;
                batch.keys = std::move(request.keys);
                batch.sender = request.sender;
                batch.size = request.size;
                batch.warmup = request.warmup;
                return dispatcher->Dispatch(std::move(batch));
            });
    }

    std::mutex workload_mutex;
    bool workload_started = false;
    bool workload_paused = false;
    bool measurement_armed = false;
    std::thread workload_thread;
    std::atomic<bool> workload_failed{false};

    auto start_workload = [&](std::string *start_error) -> bool {
        if (workload_thread.joinable()) return true;
        if (basic_runner) {
            workload_thread = std::thread([&] {
                std::string run_error;
                if (!basic_runner->Start(&run_error)) {
                    workload_failed.store(true, std::memory_order_release);
                    LOG(ERROR) << "basic workload failed: " << run_error;
                }
            });
            return true;
        }
        if (writer) {
            workload_thread = std::thread([&] {
                std::string run_error;
                if (!writer->Start(&run_error)) {
                    workload_failed.store(true, std::memory_order_release);
                    LOG(ERROR) << "pipeline writer failed: " << run_error;
                }
            });
            return true;
        }
        (void)start_error;
        return true;  // pure reader
    };

    HttpServer server(
        config.listen_port, 4,
        [&](const std::string &method, const std::string &path,
            const std::string &body) -> HttpResponse {
            const auto query = path.find('?');
            const std::string route =
                query == std::string::npos ? path : path.substr(0, query);
            if ((method == "POST" || method == "GET") &&
                route == "/notify") {
                if (!dispatcher) {
                    return {404, "text/plain", "not a reader"};
                }
                if (!running.load(std::memory_order_acquire)) {
                    return {503, "text/plain", "stopping"};
                }
                NotifyBatch batch;
                std::string decode_error;
                if (!DecodeNotify(path, body, config.notify_read_format, &batch,
                                  &decode_error)) {
                    return {400, "text/plain", "invalid notify"};
                }
                return dispatcher->Dispatch(std::move(batch))
                           ? HttpResponse{200, "text/plain", "ok"}
                           : HttpResponse{503, "text/plain", "queue full"};
            }
            if (method == "GET" && route == "/stats") {
                if (config.IsBasic()) {
                    metrics.SetSummaryExtra(BasicSummaryExtra(
                        config, basic_runner != nullptr
                                    ? basic_runner->GetStats()
                                    : BasicRunner::Stats{}));
                    return {200, "application/json", metrics.StatsJson()};
                }
                PipelineWriter::Stats writer_stats;
                PipelineReader::Stats reader_stats;
                NotifySender::Stats sender_stats;
                if (writer) writer_stats = writer->GetStats();
                if (reader) reader_stats = reader->GetStats();
                if (notify_sender) sender_stats = notify_sender->GetStats();
                metrics.SetSummaryExtra(PipelineSummaryExtra(
                    config, writer ? &writer_stats : nullptr,
                    reader ? &reader_stats : nullptr,
                    notify_sender ? &sender_stats : nullptr,
                    http_client.get(),
                    dispatcher ? dispatcher->QueueSize() : 0,
                    dispatcher ? dispatcher->Dropped() : 0));
                return {200, "application/json", metrics.StatsJson()};
            }
            if (method == "GET" && route == "/health") {
                return {200, "text/plain", "ok"};
            }
            if (method == "POST" && route == "/start") {
                std::lock_guard<std::mutex> lock(workload_mutex);
                if (!running.load(std::memory_order_acquire)) {
                    return {409, "text/plain", "stopping"};
                }
                if (workload_started) {
                    return {200, "text/plain", "already started"};
                }
                if (!basic_runner && !writer) metrics.Reset();
                std::string start_error;
                if (!start_workload(&start_error)) {
                    return {500, "text/plain", start_error};
                }
                workload_started = true;
                workload_paused = false;
                measurement_armed = false;
                return {200, "text/plain", "started"};
            }
            if (method == "POST" && route == "/pause") {
                std::lock_guard<std::mutex> lock(workload_mutex);
                if (!workload_started || workload_paused) {
                    return {409, "text/plain", "not running or already paused"};
                }
                metrics.Freeze();
                if (writer) writer->RequestStop();
                if (basic_runner) basic_runner->RequestStop();
                if (workload_thread.joinable()) workload_thread.join();
                if (notify_sender) notify_sender->Drain();
                workload_paused = true;
                measurement_armed = false;
                return {200, "text/plain", "paused"};
            }
            if (method == "POST" && route == "/drain") {
                if (dispatcher) dispatcher->Drain(std::chrono::seconds(5));
                return {200, "text/plain", "drained"};
            }
            if (method == "POST" && route == "/reset") {
                std::lock_guard<std::mutex> lock(workload_mutex);
                if (!workload_paused) {
                    return {409, "text/plain", "workload is not paused"};
                }
                metrics.ResetPaused();
                measurement_armed = false;
                return {200, "text/plain", "reset"};
            }
            if (method == "POST" && route == "/arm") {
                std::lock_guard<std::mutex> lock(workload_mutex);
                if (!workload_paused) {
                    return {409, "text/plain", "workload is not paused"};
                }
                if (!measurement_armed) {
                    metrics.BeginMeasurement();
                    measurement_armed = true;
                }
                return {200, "text/plain", "armed"};
            }
            if (method == "POST" && route == "/resume") {
                std::lock_guard<std::mutex> lock(workload_mutex);
                if (!workload_paused || !measurement_armed) {
                    return {409, "text/plain", "pause/arm first"};
                }
                if (config.IsPipeline() &&
                    (config.role == "writer" || config.role == "dual")) {
                    writer.reset();
                    if (!prepare_writer()) {
                        return {500, "text/plain", error};
                    }
                }
                if (config.IsBasic()) {
                    basic_runner.reset();
                    if (!prepare_basic()) {
                        return {500, "text/plain", error};
                    }
                }
                workload_failed.store(false, std::memory_order_release);
                std::string resume_error;
                if (!start_workload(&resume_error)) {
                    return {500, "text/plain", resume_error};
                }
                workload_paused = false;
                return {200, "text/plain", "resumed"};
            }
            if (method == "POST" && route == "/summary") {
                metrics.WriteSummary();
                return {200, "text/plain", "ok"};
            }
            if (method == "POST" &&
                (route == "/stop" || route == "/shutdown")) {
                std::lock_guard<std::mutex> lock(workload_mutex);
                metrics.Freeze();
                running.store(false, std::memory_order_release);
                if (writer) writer->RequestStop();
                if (basic_runner) basic_runner->RequestStop();
                return {200, "text/plain", "stopping"};
            }
            return {404, "text/plain", "not found"};
        });
    int exit_code = 0;
    if (!server.Start(&error)) {
        LOG(ERROR) << "HTTP server failed: " << error;
        exit_code = 1;
        running.store(false, std::memory_order_release);
    }
    if (exit_code == 0 && rpc_server && !rpc_server->Start(&error)) {
        LOG(ERROR) << "RPC notify server failed: " << error;
        exit_code = 1;
        running.store(false, std::memory_order_release);
    }
    if (running.load(std::memory_order_acquire) && !config.start_paused) {
        std::lock_guard<std::mutex> lock(workload_mutex);
        if (!basic_runner && !writer) metrics.Reset();
        std::string start_error;
        if (!start_workload(&start_error)) {
            LOG(ERROR) << start_error;
            exit_code = 1;
            running.store(false, std::memory_order_release);
        } else {
            workload_started = true;
        }
    }

    LOG(INFO) << "mooncake_kvtest started mode=" << config.mode
              << " role=" << config.role
              << " instance=" << config.instance_id
              << " operation=" << config.operation
              << " write_threads=" << config.num_threads
              << " read_threads=" << config.NumReadThreads()
              << " traffic_mode=" << config.traffic_mode
              << " target_qps=" << config.target_qps
              << " batch_keys=" << config.batch_keys_count
              << " notify_async=" << config.notify_async
              << " warmup=" << config.warmup_duration
              << " duration=" << config.duration
              << " listen_port=" << config.listen_port
              << " start_paused=" << config.start_paused;

    std::vector<MetricSnapshot> previous;
    auto last_report = std::chrono::steady_clock::now();
    while (running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        if (workload_failed.load(std::memory_order_acquire)) {
            exit_code = 1;
            running.store(false, std::memory_order_release);
            continue;
        }
        const auto now = std::chrono::steady_clock::now();
        const double seconds =
            std::chrono::duration<double>(now - last_report).count();
        if (seconds < 3.0) continue;
        last_report = now;
        const auto current = metrics.SnapshotCounts();
        std::ostringstream rates;
        for (const auto &metric : current) {
            auto it = std::find_if(previous.begin(), previous.end(),
                                   [&](const auto &old) {
                                       return old.name == metric.name;
                                   });
            const uint64_t old_total = it == previous.end() ? 0 : it->total;
            if (rates.tellp() > 0) rates << ", ";
            rates << metric.name << '='
                  << static_cast<uint64_t>((metric.total - old_total) / seconds)
                  << "/s";
        }
        previous = current;
        const std::string phase =
            writer ? writer->GetStats().phase : std::string("-");
        LOG(INFO) << '[' << rates.str() << "] [phase=" << phase
                  << ", reader_q="
                  << (dispatcher ? dispatcher->QueueSize() : 0)
                  << ", reader_q_dropped="
                  << (dispatcher ? dispatcher->Dropped() : 0)
                  << ", notify_q="
                  << (notify_sender ? notify_sender->GetStats().queue_peak : 0)
                  << ']';
    }

    LOG(INFO) << "mooncake_kvtest shutdown: draining workload";
    metrics.Freeze();
    {
        std::lock_guard<std::mutex> lock(workload_mutex);
        if (writer) writer->RequestStop();
        if (basic_runner) basic_runner->RequestStop();
    }
    LOG(INFO) << "mooncake_kvtest shutdown: joining workload thread";
    if (workload_thread.joinable()) workload_thread.join();
    if (workload_failed.load(std::memory_order_acquire)) exit_code = 1;
    LOG(INFO) << "mooncake_kvtest shutdown: stopping notify sender";
    if (notify_sender) notify_sender->Stop();
    LOG(INFO) << "mooncake_kvtest shutdown: draining reader dispatcher";
    if (dispatcher) {
        dispatcher->Drain(std::chrono::seconds(5));
        dispatcher->StopNow();
        dispatcher->Stop();
    }
    LOG(INFO) << "mooncake_kvtest shutdown: stopping basic runner";
    if (basic_runner) basic_runner->Stop();

    PipelineWriter::Stats writer_stats;
    PipelineReader::Stats reader_stats;
    NotifySender::Stats sender_stats;
    if (writer) writer_stats = writer->GetStats();
    if (reader) reader_stats = reader->GetStats();
    if (notify_sender) sender_stats = notify_sender->GetStats();
    if (config.IsBasic()) {
        metrics.SetSummaryExtra(
            BasicSummaryExtra(config, basic_runner->GetStats()));
    } else {
        metrics.SetSummaryExtra(PipelineSummaryExtra(
            config, writer ? &writer_stats : nullptr,
            reader ? &reader_stats : nullptr,
            notify_sender ? &sender_stats : nullptr, http_client.get(),
            dispatcher ? dispatcher->QueueSize() : 0,
            dispatcher ? dispatcher->Dropped() : 0));
    }
    metrics.Stop();
    LOG(INFO) << "mooncake_kvtest shutdown: metrics persisted";
    if (rpc_server) rpc_server->Stop();
    server.Stop();
    LOG(INFO) << "mooncake_kvtest shutdown: HTTP server stopped";
    client->tearDownAll();
    g_running = nullptr;
    LOG(INFO) << "mooncake_kvtest stopped";
    google::ShutdownGoogleLogging();
    return exit_code;
}
