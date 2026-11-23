add_executable(
  mooncake_kvtest
  src/basic.cpp
  src/buffers.cpp
  src/config.cpp
  src/http_transport.cpp
  src/main.cpp
  src/maintenance.cpp
  src/metrics.cpp
  src/notify_codec.cpp
  src/notify_rpc_transport.cpp
  src/notify_sender.cpp
  src/operations.cpp
  src/pipeline.cpp
  src/real_store_adapter.cpp)

target_include_directories(mooncake_kvtest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(
  mooncake_kvtest
  PRIVATE mooncake_store transfer_engine asio_shared JsonCpp::JsonCpp
          glog::glog pthread yalantinglibs::yalantinglibs)

if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
