#pragma once

#include <string>

#include "config.h"
#include "store_adapter.h"

namespace mooncake::kvtest {

// One-shot data preparation for read-family scenarios. Writes summary.json in
// config.output_dir and returns 0 on success, 1 on failure.
int RunPrefill(const Config &config, StoreAdapter &store, std::string *error);

// One-shot key-range or full-store cleanup. Writes summary.json and returns 0
// on success, 1 on failure.
int RunCleanup(const Config &config, StoreAdapter &store, std::string *error);

}  // namespace mooncake::kvtest
