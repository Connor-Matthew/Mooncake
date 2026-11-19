add_executable(
  mooncake_kvtest
  src/config.cpp
  src/http_transport.cpp
  src/main.cpp
  src/metrics.cpp
  src/notify_rpc_transport.cpp)

target_include_directories(mooncake_kvtest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(
  mooncake_kvtest
  PRIVATE mooncake_store transfer_engine asio_shared JsonCpp::JsonCpp
          glog::glog pthread yalantinglibs::yalantinglibs)
