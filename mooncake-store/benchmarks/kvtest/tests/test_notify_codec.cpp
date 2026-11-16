#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "notify_codec.h"

namespace mooncake::kvtest {
namespace {

NotifyBatch SampleBatch() {
    NotifyBatch batch;
    batch.keys = {"mc_bench*basic*00000000", "mc_bench*basic*00000001"};
    batch.sender = 7;
    batch.size = 64;
    batch.warmup = true;
    return batch;
}

}  // namespace

TEST(NotifyCodecTest, JsonRoundTrip) {
    const NotifyBatch batch = SampleBatch();
    const std::string body = EncodeNotifyJson(batch);
    NotifyBatch decoded;
    std::string error;
    ASSERT_TRUE(DecodeNotifyJson(body, &decoded, &error)) << error;
    EXPECT_EQ(decoded.keys, batch.keys);
    EXPECT_EQ(decoded.sender, batch.sender);
    EXPECT_EQ(decoded.size, batch.size);
    EXPECT_TRUE(decoded.warmup);
}

TEST(NotifyCodecTest, QueryRoundTrip) {
    const NotifyBatch batch = SampleBatch();
    const std::string path = EncodeNotifyQuery(batch);
    EXPECT_NE(path.find("%2A"), std::string::npos) << path;
    NotifyBatch decoded;
    std::string error;
    ASSERT_TRUE(DecodeNotifyQuery(path, &decoded, &error)) << error;
    EXPECT_EQ(decoded.keys, batch.keys);
    EXPECT_EQ(decoded.sender, batch.sender);
    EXPECT_EQ(decoded.size, batch.size);
    EXPECT_TRUE(decoded.warmup);
}

TEST(NotifyCodecTest, BothPrefersJson) {
    NotifyBatch decoded;
    std::string error;
    ASSERT_TRUE(DecodeNotify("/notify?key=from_query",
                             EncodeNotifyJson(SampleBatch()), "both", &decoded,
                             &error))
        << error;
    EXPECT_EQ(decoded.keys.size(), 2u);
    EXPECT_EQ(decoded.sender, 7);

    ASSERT_TRUE(DecodeNotify("/notify?key=from_query&writer_id=3", "", "both",
                             &decoded, &error))
        << error;
    ASSERT_EQ(decoded.keys.size(), 1u);
    EXPECT_EQ(decoded.keys.front(), "from_query");
    EXPECT_EQ(decoded.sender, 3);
}

TEST(NotifyCodecTest, FormatsAreEnforced) {
    NotifyBatch decoded;
    std::string error;
    EXPECT_FALSE(DecodeNotify("/notify?key=k", "", "json", &decoded, &error));
    // "query" always parses the query string, even when a JSON body exists.
    EXPECT_TRUE(DecodeNotify("/notify?key=k", EncodeNotifyJson(SampleBatch()),
                             "query", &decoded, &error));
    EXPECT_FALSE(DecodeNotify("/notify", EncodeNotifyJson(SampleBatch()),
                              "query", &decoded, &error));
}

TEST(NotifyCodecTest, SingleKeyAndInvalidPayloads) {
    NotifyBatch decoded;
    std::string error;
    ASSERT_TRUE(DecodeNotifyJson(R"({"sender":1,"size":8,"key":"only"})",
                                 &decoded, &error))
        << error;
    ASSERT_EQ(decoded.keys.size(), 1u);
    EXPECT_EQ(decoded.keys.front(), "only");
    EXPECT_EQ(decoded.size, 8u);

    EXPECT_FALSE(DecodeNotifyJson("{}", &decoded, &error));
    EXPECT_FALSE(DecodeNotifyQuery("/notify", &decoded, &error));
    EXPECT_FALSE(DecodeNotifyQuery("/notify?key=%zz", &decoded, &error));
}

}  // namespace mooncake::kvtest
