#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "fake_store.h"
#include "http_transport.h"
#include "metrics.h"
#include "notify_codec.h"
#include "notify_sender.h"
#include "pipeline.h"

namespace mooncake::kvtest {
namespace {

std::string MetricsDir(const std::string &name) {
    return (std::filesystem::path(::testing::TempDir()) /
            ("kvtest_http_" + name))
        .string();
}

const MetricSnapshot *FindSnapshot(const std::vector<MetricSnapshot> &snaps,
                                   const std::string &name) {
    for (const auto &snapshot : snaps) {
        if (snapshot.name == name) return &snapshot;
    }
    return nullptr;
}

}  // namespace

// End-to-end pipeline over real HTTP: writer -> NotifySender -> keep-alive
// client -> HttpServer -> reader queue -> PipelineReader -> FakeStore.
TEST(HttpPipelineTest, WriterToReaderOverHttp) {
    const int port =
        19200 + static_cast<int>(std::chrono::steady_clock::now()
                                      .time_since_epoch()
                                      .count() %
                                  500);

    FakeStore store;
    Config reader_cfg;
    reader_cfg.mode = "pipeline";
    reader_cfg.role = "reader";
    reader_cfg.instance_id = 1;
    reader_cfg.notify_pipeline = {"get_buffer"};
    reader_cfg.num_threads = 1;
    reader_cfg.num_total_threads = 2;
    reader_cfg.data_sizes = {64};
    reader_cfg.batch_keys_count = 1;
    reader_cfg.verify_level = "full";
    reader_cfg.verify_sample_bytes = 64;
    reader_cfg.verify_sample_step_bytes = 64;
    reader_cfg.output_dir = MetricsDir("reader");

    MetricsCollector reader_metrics(1, 3000, reader_cfg.output_dir);
    reader_metrics.Start();
    PipelineReader reader(reader_cfg, store, reader_metrics);
    std::string error;
    ASSERT_TRUE(reader.Prepare(&error)) << error;
    PipelineDispatcher dispatcher(reader_cfg, reader, reader_metrics);

    HttpServer server(
        port, 4,
        [&](const std::string &method, const std::string &path,
            const std::string &body) -> HttpResponse {
            const auto query = path.find('?');
            const std::string route =
                query == std::string::npos ? path : path.substr(0, query);
            if ((method == "POST" || method == "GET") && route == "/notify") {
                NotifyBatch batch;
                if (!DecodeNotify(path, body, "both", &batch, nullptr)) {
                    return {400, "text/plain", "invalid notify"};
                }
                return dispatcher.Dispatch(std::move(batch))
                           ? HttpResponse{200, "text/plain", "ok"}
                           : HttpResponse{503, "text/plain", "queue full"};
            }
            if (method == "GET" && route == "/health") {
                return {200, "text/plain", "ok"};
            }
            return {404, "text/plain", "not found"};
        });
    ASSERT_TRUE(server.Start(&error)) << error;

    Config writer_cfg;
    writer_cfg.mode = "pipeline";
    writer_cfg.role = "writer";
    writer_cfg.instance_id = 0;
    writer_cfg.pipeline = {"put"};
    writer_cfg.notify_pipeline = {"get_buffer"};
    writer_cfg.num_threads = 1;
    writer_cfg.num_total_threads = 2;
    writer_cfg.data_sizes = {64};
    writer_cfg.batch_keys_count = 1;
    writer_cfg.target_qps = 0.0;
    writer_cfg.duration = 0.15;
    writer_cfg.warmup_duration = 0.0;
    writer_cfg.notify_count = 1;
    writer_cfg.notify_threads = 2;
    writer_cfg.notify_queue_max = 128;
    writer_cfg.notify_async = true;
    writer_cfg.key_pool_size = 0;
    writer_cfg.verify_level = "full";
    writer_cfg.verify_sample_bytes = 64;
    writer_cfg.verify_sample_step_bytes = 64;
    writer_cfg.peers = {"http://127.0.0.1:" + std::to_string(port)};
    writer_cfg.output_dir = MetricsDir("writer");

    MetricsCollector writer_metrics(0, 3000, writer_cfg.output_dir);
    writer_metrics.Start();
    auto client = std::make_unique<NotifyHttpClient>(3);
    NotifyHttpClient *http = client.get();
    const auto send_fn = [http](const std::string &peer,
                                const NotifyBatch &batch,
                                double *latency_ms) -> bool {
        const std::string body = EncodeNotifyJson(batch);
        const auto start = std::chrono::steady_clock::now();
        const bool ok = http->Send(peer, "POST", "/notify", body, nullptr);
        if (latency_ms != nullptr) {
            *latency_ms = std::chrono::duration<double, std::milli>(
                              std::chrono::steady_clock::now() - start)
                              .count();
        }
        return ok;
    };
    NotifySender sender(2, 128, NotifySender::Overflow::kDrop, true, send_fn,
                        nullptr);
    PipelineWriter writer(writer_cfg, store, writer_metrics, &sender);
    ASSERT_TRUE(writer.Prepare(&error)) << error;
    ASSERT_TRUE(writer.Start(&error)) << error;
    sender.Drain();

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        const auto snaps = reader_metrics.SnapshotCounts();
        const MetricSnapshot *get = FindSnapshot(snaps, "get_buffer");
        if (get != nullptr && get->success > 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    const auto sender_stats = sender.GetStats();
    const auto reader_stats = reader.GetStats();
    const uint64_t reused_connections = client->Reused();
    const auto snaps = reader_metrics.SnapshotCounts();
    const MetricSnapshot *get = FindSnapshot(snaps, "get_buffer");

    sender.Stop();
    client.reset();
    dispatcher.Drain(std::chrono::seconds(2));
    dispatcher.StopNow();
    dispatcher.Stop();
    server.Stop();
    reader_metrics.Stop();
    writer_metrics.Stop();

    EXPECT_GT(sender_stats.ok, 0u);
    EXPECT_GT(reader_stats.batches, 0u);
    ASSERT_NE(get, nullptr);
    EXPECT_GT(get->success, 0u);
    // Keep-alive: the sender reuses pooled connections instead of opening one
    // per notification.
    EXPECT_GT(sender_stats.submitted, 1u);
    EXPECT_GT(reader_stats.batches, 1u);
    EXPECT_GT(reused_connections, 0u);
}

}  // namespace mooncake::kvtest
