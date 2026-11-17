#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "basic.h"
#include "config.h"
#include "operations.h"

namespace mooncake::kvtest {
namespace {

constexpr const char kStore[] =
    R"("store":{"local_hostname":"10.0.0.1:12345",)"
    R"("metadata_server":"http://10.0.0.10:8080/metadata",)"
    R"("master_server_address":"10.0.0.10:50051"})";

std::string WithStore(const std::string &fields) {
    return "{" + fields + "," + kStore + "}";
}

std::string WriteConfig(const std::string &name, const std::string &body) {
    const auto path = std::filesystem::path(::testing::TempDir()) / name;
    std::ofstream output(path);
    output << body;
    return path.string();
}

bool Load(const std::string &json, const std::string &name, Config *config,
          std::string *error) {
    return LoadConfig(WriteConfig(name, json), config, error);
}

std::filesystem::path ExampleConfigDir() {
#ifdef KV_TEST_SOURCE_DIR
    return std::filesystem::path(KV_TEST_SOURCE_DIR) / "config";
#else
    return std::filesystem::absolute(std::filesystem::path(__FILE__))
               .parent_path()
               .parent_path() /
           "config";
#endif
}

}  // namespace

TEST(ConfigTest, PipelineDefaultsStayCompatible) {
    Config config;
    std::string error;
    ASSERT_TRUE(Load(
        WithStore(
            R"("mode":"pipeline","role":"reader","instance_id":1,)"
            R"("listen_port":9001,"notify_pipeline":["getBuffer"],)"
            R"("num_threads":1,"num_total_threads":5,"data_sizes":["8MB"])"),
        "pipeline_defaults.json", &config, &error))
        << error;

    EXPECT_TRUE(config.IsPipeline());
    EXPECT_EQ(config.role, "reader");
    EXPECT_EQ(config.traffic_mode, "balanced");
    EXPECT_EQ(config.notify_overflow, "drop");
    EXPECT_DOUBLE_EQ(config.target_qps, 100.0);
    EXPECT_EQ(config.batch_keys_count, 1);
    EXPECT_DOUBLE_EQ(config.duration, 0.0);
    EXPECT_DOUBLE_EQ(config.warmup_duration, 0.0);
    EXPECT_EQ(config.notify_pipeline.size(), 1u);
}

TEST(ConfigTest, PulseAndAliasesAreParsed) {
    Config config;
    std::string error;
    ASSERT_TRUE(Load(
        WithStore(
            R"("mode":"pipeline","role":"writer","instance_id":0,)"
            R"("listen_port":9000,"pipeline":["put_batch"],)"
            R"("notify_pipeline":["get_buffer"],"target_qps":52.5,)"
            R"("traffic_mode":"pulse","batch_size":4,)"
            R"("notify_queue_size":4096,"warmup":{"duration":5},)"
            R"("duration":30,"num_threads":2,"num_total_threads":8,)"
            R"("nodes":[)"
            R"({"host":"10.0.0.1","port":9000,"instance_id":0,"role":"writer"},)"
            R"({"host":"10.0.0.2","port":9001,"instance_id":1,"role":"reader"}])"),
        "pipeline_pulse.json", &config, &error))
        << error;

    EXPECT_EQ(config.traffic_mode, "pulse");
    EXPECT_DOUBLE_EQ(config.target_qps, 52.5);
    EXPECT_EQ(config.batch_keys_count, 4);
    EXPECT_EQ(config.notify_queue_max, 4096u);
    EXPECT_DOUBLE_EQ(config.warmup_duration, 5.0);
    EXPECT_DOUBLE_EQ(config.duration, 30.0);
    ASSERT_EQ(config.peers.size(), 1u);
    EXPECT_EQ(config.peers.front(), "http://10.0.0.2:9001");
}

TEST(ConfigTest, IntegerTargetQpsStillWorks) {
    Config config;
    std::string error;
    ASSERT_TRUE(Load(
        WithStore(
            R"("mode":"pipeline","role":"reader","listen_port":9001,)"
            R"("target_qps":105,"notify_pipeline":["get_buffer"],)"
            R"("num_threads":1,"num_total_threads":5)"),
        "pipeline_int_qps.json", &config, &error))
        << error;
    EXPECT_DOUBLE_EQ(config.target_qps, 105.0);
}

TEST(ConfigTest, BasicModeNormalisesRoleAndKeyRange) {
    Config config;
    std::string error;
    ASSERT_TRUE(Load(
        WithStore(
            R"("mode":"basic","operation":"get_buffer","listen_port":9100,)"
            R"("key_count":1000,"key_start":100,"key_end":500,)"
            R"("data_sizes":["1MB"],"start_paused":true,"duration":10,)"
            R"("num_threads":4)"),
        "basic.json", &config, &error))
        << error;

    EXPECT_TRUE(config.IsBasic());
    EXPECT_EQ(config.role, "client");
    EXPECT_EQ(config.operation, "get_buffer");
    EXPECT_EQ(config.EffectiveKeyEnd(), 500u);
    EXPECT_TRUE(config.peers.empty());
    EXPECT_DOUBLE_EQ(config.duration, 10.0);
}

TEST(ConfigTest, CleanupScopeIsValidated) {
    Config config;
    std::string error;
    EXPECT_TRUE(Load(
        WithStore(
            R"("mode":"cleanup","scope":"all","listen_port":9100,)"
            R"("key_count":1000,"data_sizes":["1MB"])"),
        "cleanup_ok.json", &config, &error))
        << error;

    EXPECT_FALSE(Load(
        WithStore(
            R"("mode":"cleanup","scope":"everything","listen_port":9100,)"
            R"("key_count":1000,"data_sizes":["1MB"])"),
        "cleanup_bad.json", &config, &error));
    EXPECT_NE(error.find("cleanup scope"), std::string::npos);
}

TEST(ConfigTest, InvalidModesAndTrafficModesAreRejected) {
    Config config;
    std::string error;
    EXPECT_FALSE(Load(
        WithStore(R"("mode":"batch","listen_port":9000)"),
        "bad_mode.json", &config, &error));
    EXPECT_NE(error.find("mode"), std::string::npos);

    EXPECT_FALSE(Load(
        WithStore(
            R"("mode":"pipeline","role":"reader","listen_port":9000,)"
            R"("traffic_mode":"burst","num_threads":1,"num_total_threads":2)"),
        "bad_traffic.json", &config, &error));
    EXPECT_NE(error.find("traffic_mode"), std::string::npos);
}

TEST(ConfigTest, BasicModeRequiresOperation) {
    Config config;
    std::string error;
    EXPECT_FALSE(Load(
        WithStore(
            R"("mode":"basic","listen_port":9100,"key_count":10,)"
            R"("data_sizes":["1MB"])"),
        "basic_no_op.json", &config, &error));
    EXPECT_NE(error.find("operation"), std::string::npos);
}

TEST(ConfigTest, BatchAliasesMustAgree) {
    Config config;
    std::string error;
    EXPECT_FALSE(Load(
        WithStore(
            R"("mode":"pipeline","role":"reader","listen_port":9000,)"
            R"("batch_size":2,"batch_keys_count":3,"num_threads":1,)"
            R"("num_total_threads":2)"),
        "batch_conflict.json", &config, &error));
    EXPECT_NE(error.find("batch"), std::string::npos);
}

TEST(ConfigTest, ValueFileRequiresSingleDataSize) {
    Config config;
    std::string error;
    EXPECT_FALSE(Load(
        WithStore(
            R"("mode":"basic","operation":"get","listen_port":9100,)"
            R"("key_count":10,"value_file":"/tmp/value.bin",)"
            R"("data_sizes":["1MB","2MB"])"),
        "value_file_multi.json", &config, &error));
    EXPECT_NE(error.find("value_file"), std::string::npos);
}

TEST(ConfigTest, WriterWithoutPeersIsRejected) {
    Config config;
    std::string error;
    EXPECT_FALSE(Load(
        WithStore(
            R"("mode":"pipeline","role":"writer","listen_port":9000,)"
            R"("notify_count":1,"num_threads":1,"num_total_threads":2)"),
        "writer_no_peers.json", &config, &error));
    EXPECT_NE(error.find("peer"), std::string::npos);
}

TEST(ConfigTest, ExamplesLoadAndValidate) {
    const std::filesystem::path config_dir = ExampleConfigDir();
    ASSERT_TRUE(std::filesystem::is_directory(config_dir)) << config_dir;
    int checked = 0;
    for (const auto &entry :
         std::filesystem::directory_iterator(config_dir)) {
        if (!entry.is_regular_file()) continue;
        const std::string name = entry.path().filename().string();
        if (!name.ends_with(".json.example")) continue;
        Config config;
        std::string error;
        ASSERT_TRUE(LoadConfig(entry.path().string(), &config, &error))
            << name << ": " << error;
        if (config.IsPipeline()) {
            ASSERT_TRUE(ValidateOpList(config.pipeline, OpSide::kAny, true,
                                       &error))
                << name << ": " << error;
            ASSERT_TRUE(ValidateOpList(config.notify_pipeline, OpSide::kRead,
                                       true, &error))
                << name << ": " << error;
        } else if (config.IsBasic()) {
            ASSERT_TRUE(ValidateBasicOperation(config, &error))
                << name << ": " << error;
        }
        ++checked;
    }
    EXPECT_GE(checked, 7);
}

TEST(ConfigTest, PrefillAndCleanupExamplesAreOneShot) {
    for (const char *name : {"prefill.json.example", "cleanup.json.example"}) {
        Config config;
        std::string error;
        ASSERT_TRUE(LoadConfig((ExampleConfigDir() / name).string(), &config,
                               &error))
            << name << ": " << error;
        EXPECT_FALSE(config.IsPipeline());
        EXPECT_FALSE(config.IsBasic());
    }
}

TEST(ConfigTest, MetricsWindowIsOffByDefault) {
    Config config;
    EXPECT_EQ(config.metrics_interval_ms, 0);

    Config explicit_on;
    std::string error;
    ASSERT_TRUE(Load(
        WithStore(
            R"("mode":"pipeline","role":"reader","listen_port":9001,)"
            R"("notify_pipeline":["getBuffer"],"num_threads":1,)"
            R"("num_total_threads":2,"data_sizes":["8MB"],)"
            R"("metrics_interval_ms":3000)"),
        "metrics_window_on.json", &explicit_on, &error))
        << error;
    EXPECT_EQ(explicit_on.metrics_interval_ms, 3000);
}
TEST(ConfigTest, RpcTransportDerivesRpcPeers) {
    Config config;
    std::string error;
    ASSERT_TRUE(Load(
        WithStore(
            R"("mode":"pipeline","role":"writer","listen_port":9000,)"
            R"("notify_transport":"rpc","pipeline":["put"],)"
            R"("notify_pipeline":["get_buffer"],"num_threads":1,)"
            R"("num_total_threads":2,"data_sizes":["8MB"],)"
            R"("nodes":[{"host":"10.0.0.2","port":9001,)"
            R"("notify_rpc_port":19001,"instance_id":1,"role":"reader"}])"),
        "rpc_peers.json", &config, &error))
        << error;
    ASSERT_EQ(config.peers.size(), 1u);
    EXPECT_EQ(config.peers[0], "10.0.0.2:19001");
    EXPECT_EQ(config.notify_transport, "rpc");
}

TEST(ConfigTest, HttpTransportDerivesHttpPeers) {
    Config config;
    std::string error;
    ASSERT_TRUE(Load(
        WithStore(
            R"("mode":"pipeline","role":"writer","listen_port":9000,)"
            R"("pipeline":["put"],"notify_pipeline":["get_buffer"],)"
            R"("num_threads":1,"num_total_threads":2,)"
            R"("data_sizes":["8MB"],)"
            R"("nodes":[{"host":"10.0.0.2","port":9001,)"
            R"("instance_id":1,"role":"reader"}])"),
        "http_peers.json", &config, &error))
        << error;
    ASSERT_EQ(config.peers.size(), 1u);
    EXPECT_EQ(config.peers[0], "http://10.0.0.2:9001");
    EXPECT_EQ(config.notify_transport, "http");
}
}  // namespace mooncake::kvtest
