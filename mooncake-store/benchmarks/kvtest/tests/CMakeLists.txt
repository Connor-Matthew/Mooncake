add_executable(
  mooncake_kvtest_unit_tests
  test_basic.cpp
  test_config.cpp
  test_http_pipeline.cpp
  test_maintenance.cpp
  test_metrics.cpp
  test_notify_codec.cpp
  test_notify_sender.cpp
  test_operations.cpp
  test_pipeline.cpp
  test_traffic_gate.cpp
  ../src/basic.cpp
  ../src/buffers.cpp
  ../src/config.cpp
  ../src/http_transport.cpp
  ../src/maintenance.cpp
  ../src/metrics.cpp
  ../src/notify_codec.cpp
  ../src/notify_sender.cpp
  ../src/operations.cpp
  ../src/pipeline.cpp)

target_include_directories(mooncake_kvtest_unit_tests
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
target_compile_definitions(mooncake_kvtest_unit_tests
                           PRIVATE KV_TEST_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/..")
target_link_libraries(mooncake_kvtest_unit_tests
                      PRIVATE JsonCpp::JsonCpp asio_shared gtest gtest_main
                              pthread)
add_test(NAME mooncake_kvtest_unit_tests
         COMMAND mooncake_kvtest_unit_tests)
