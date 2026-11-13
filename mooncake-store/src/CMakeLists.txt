find_package(OpenSSL REQUIRED COMPONENTS Crypto)

add_subdirectory(cachelib_memory_allocator)
add_subdirectory(local_ssd)

set(MOONCAKE_STORE_SHARED_SOURCES
    allocator_metric.cpp
    types.cpp
    common/network.cpp
    thread_pool.cpp
    etcd_helper.cpp
    uds_transport.cpp
    config/distributed_storage_config.cpp
    config/rpc_protocol_config.cpp
    config/replica_selection_config.cpp
    config/store_cluster_identity_config.cpp
    storage/distributed/distributed_storage_backend.cpp
    storage/distributed/object_storage_backend.cpp
    storage/distributed/posix_fs_adapter.cpp
    common/file_util.cpp
    storage/distributed/kvcs/kvcs_efc_target.cpp
    storage/distributed/kvcs/kvcs_capi_driver.cpp
    # 930: cluster view manager (CVM), partition routing and VSegment. Both
    # master_service.cpp (master objects) and client_service.cpp (client
    # objects) reference these, so they are compiled once here.
    cvm/etcd_view_store.cpp
    cvm/cvm_controller.cpp
    cvm/slot_owner_heartbeat.cpp
    cvm/slot_migrator.cpp
    cvm/cvm_http_server.cpp
    cvm/inter_master_rpc.cpp
    partition/partition_router.cpp
    vsegment/vsegment.cpp
    vsegment/partition_quota_planner.cpp
    vsegment/vsegment_runtime.cpp
    vsegment/vsegment_transfer.cpp
    vsegment/vsegment_ha.cpp
    vsegment/vsegment_service.cpp
    vsegment/vsegment_manager.cpp
    vsegment/vsegment_metrics.cpp
    nof/nof_runtime.cpp)

set(MOONCAKE_STORE_MASTER_SOURCES
    allocation_strategy.cpp
    allocator.cpp
    config/dfs_enablement_config.cpp
    config/ha_cluster_namespace_config.cpp
    config/master_metadata_config.cpp
    config/local_file_snapshot_config.cpp
    config/metrics_bootstrap_config_loader.cpp
    config/redis_connection_config.cpp
    config/s3_client_config.cpp
    segment_allocator_registration.cpp
    client_offboarding.cpp
    dynamic_replication_controller.cpp
    eviction/evictor.cpp
    eviction/offloading_reclaimer.cpp
    eviction/report_observers.cpp
    master_service.cpp
    promotion_candidate_tracker.cpp
    master_store_backend.cpp
    weight_store_manager.cpp
    master_metric_manager.cpp
    segment/region_driver.cpp
    placement/index.cpp
    placement/replica_allocator.cpp
    segment/catalog.cpp
    segment/pool.cpp
    segment/pool_mount.cpp
    segment/pool_write_access.cpp
    segment/pool_read_access.cpp
    segment.cpp
    tenant/quota.cpp
    tenant/quota_manager.cpp
    tenant/quota_policy.cpp
    tenant/quota_policy_store.cpp
    weight_metadata_store.cpp
    rpc_service.cpp
    master_admin_service.cpp
    offset_allocator.cpp
    http_metadata_server.cpp
    embedded_master.cpp
    storage/distributed/shard_allocator.cpp
    storage/distributed/immutable_bucket_allocator.cpp
    storage/distributed/kvcs/kvcs_capacity_provider.cpp
    ha/leadership/leader_coordinator_factory.cpp
    ha/leadership/backends/etcd/etcd_leader_coordinator.cpp
    ha/common/redis/redis_connection.cpp
    ha/leadership/backends/redis/redis_leader_coordinator.cpp
    ha/leadership/master_service_supervisor.cpp
    shutdown_request.cpp
    ha/master_metrics_reporter.cpp
    ha/kv/etcd_ha_kv_backend.cpp
    ha/kv/redis_ha_kv_backend.cpp
    ha/kv/ha_kv_backend_factory.cpp
    ha/standby_controller.cpp
    ha/standby_metadata_store.cpp
    ha/snapshot/batch_oplog/codec.cpp
    ha/snapshot/batch_oplog/metadata.cpp
    ha/snapshot/batch_oplog/batch_oplog_snapshot_provider.cpp
    ha/snapshot/batch_oplog/writer.cpp
    ha/snapshot/batch_oplog/batch_oplog_snapshot_publisher.cpp
    ha/snapshot/batch_oplog/batch_oplog_snapshot_coordinator.cpp
    ha/snapshot/batch_oplog/batch_oplog_snapshot_gc.cpp
    ha/snapshot/batch_oplog/batch_oplog_pruning_coordinator.cpp
    ha/snapshot/snapshot_maintenance_lease.cpp
    ha/snapshot/object/snapshot_object_store.cpp
    ha/snapshot/object/backends/local/local_file_snapshot_object_store.cpp
    ha/snapshot/object/backends/s3/s3_snapshot_object_store.cpp
    task_manager.cpp
    io_pattern/collector_impl.cpp
    io_pattern/reporter.cpp
    io_pattern/cfm_client_impl.cpp
    io_pattern/cfm_ingress.cpp
    io_pattern/cfm_service.cpp
    io_pattern/cfm_ownership_client.cpp
    io_pattern/cfm_protocol.cpp
    io_pattern/resilient_cfm_channel.cpp
    io_pattern/feedback.cpp
    io_pattern/degrading_policy_engine.cpp
    io_pattern/legacy_eviction_ops.cpp
    io_pattern/kmeans_analyzer.cpp
    io_pattern/observability.cpp
    io_pattern/sliding_window_analyzer.cpp
    io_pattern/tier_executor.cpp
    io_pattern/resilient_analyzer.cpp
    io_pattern/rpc_transport.cpp
    io_pattern/runtime.cpp
    io_pattern/threshold_analyzer.cpp
    io_pattern/policy_strategies.cpp
    ha/oplog/oplog_types.cpp
    ha/oplog/oplog_batch_codec.cpp
    ha/oplog/oplog_batch_storage.cpp
    ha/oplog/oplog_batch_standby_reader.cpp
    ha/oplog/oplog_batch_types.cpp
    ha/oplog/ordered_oplog_writer.cpp
    ha/oplog/oplog_test_failpoint.cpp
    ha/oplog/oplog_applier.cpp
    hot_standby_service.cpp
    metadata_store.cpp
    standby_state_machine.cpp
    ha_metric_manager.cpp)

set(MOONCAKE_STORE_CLIENT_SOURCES
    common/client_buffer_allocation.cpp
    mmap_arena.cpp
    config/hugepage_config.cpp
    config/mmap_arena_config.cpp
    common/mmap_buffer.cpp
    client_service.cpp
    client_metric.cpp
    master_client.cpp
    storage_backend.cpp
    storage/distributed/kvcs/kvcs_object_storage_adapter.cpp
    nvme_kv/backend.cpp
    nvme_kv/connector.cpp
    nvme_kv/config/connector_config.cpp
    nvme_kv/config/executor_config.cpp
    nvme_kv/config/io_concurrency_config.cpp
    nvme_kv/config/u32_parser.cpp
    nvme_kv/executor_io_uring.cpp
    nvme_kv/executor_ioctl.cpp
    nvme_kv/executor_util.cpp
    nvme_kv/key_codec.cpp
    nvme_kv/key_conflict_policy.cpp
    nvme_kv/object_layout.cpp
    transfer_task.cpp
    config/nof_debug_config.cpp
    config/fileread_worker_pool_config.cpp
    config/nof_worker_pool_config.cpp
    posix_file.cpp
    client_buffer.cpp
    aligned_client_buffer.cpp
    real_client.cpp
    config/registered_pinned_memory_config.cpp
    registered_pinned_memory.cpp
    dummy_client.cpp
    shm_helper.cpp
    config/shm_spdk_registration_config.cpp
    file_storage.cpp
    config/client_host_identity_config.cpp
    ssd_prefetcher.cpp
    config/client_metric_config.cpp
    config/client_object_checksum_config.cpp
    config/file_storage_config.cpp
    config/bucket_backend_config.cpp
    config/file_per_key_config.cpp
    config/client_auto_port_config.cpp
    config/client_auto_discovery_config.cpp
    config/cxl_segment_config.cpp
    config/client_numa_config.cpp
    config/local_hot_cache_config.cpp
    config/nof_register_config.cpp
    config/offset_allocator_backend_config.cpp
    config/rpc_timeout_config.cpp
    config/transfer_submitter_config.cpp
    config/egm_store_pool_config.cpp
    device/accelerator_device.cpp
    device/accelerator_registry.cpp
    device/runtime_accelerator.cpp
    device/cuda_like_accelerator_device.cpp
    device/cuda_ipc_buffer.cpp
    device/hip_accelerator_device.cpp
    device/ascend_accelerator_device.cpp
    device/sunrise_accelerator_device.cpp
    device/xpu_accelerator_device.cpp
    local_hot_cache.cpp
    store_c.cpp
    memory_alloc.cpp
    ssd_register_client.cpp)

if(BUILD_UNIT_TESTS)
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES nvme_kv/executor_stub.cpp)
endif()
set(EXTRA_LIBS "")
set(SPDK_STATIC_LIBS "")

# KVCS is optional so community builds without the vendor SDK continue to work.
# The C API driver supplies an explicit NOT_SUPPORTED stub in that case.
set(MOONCAKE_KVCS_SDK_FOUND OFF)
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(KVCS QUIET IMPORTED_TARGET "kvcs>=0.6.0")
endif()
if(TARGET PkgConfig::KVCS)
  set(MOONCAKE_KVCS_SDK_FOUND ON)
  message(STATUS "KVCS SDK 0.6.0+ found: low-level adapter enabled")
else()
  message(
    STATUS
      "KVCS SDK 0.6.0+ not found: low-level adapter builds with unsupported stub"
  )
endif()
set(MOONCAKE_KVCS_SDK_FOUND
    ${MOONCAKE_KVCS_SDK_FOUND}
    PARENT_SCOPE)

# RISC-V toolchains may implement 16-byte atomic operations in libatomic rather
# than emitting native instructions. Boost.Lockfree uses these operations in the
# Store master, so detect and propagate the runtime library in RISC-V mode.
if(USE_RISCV)
  include(CheckCXXSourceCompiles)
  check_cxx_source_compiles(
    "
alignas(16) volatile unsigned __int128 value = 0;
int main(int argc, char**) {
  __atomic_store_n(&value, static_cast<unsigned __int128>(argc),
                   __ATOMIC_SEQ_CST);
  return static_cast<int>(__atomic_load_n(&value, __ATOMIC_SEQ_CST));
}
"
    MOONCAKE_HAS_NATIVE_16BYTE_ATOMICS)
  if(NOT MOONCAKE_HAS_NATIVE_16BYTE_ATOMICS)
    set(CMAKE_REQUIRED_LIBRARIES_SAVE ${CMAKE_REQUIRED_LIBRARIES})
    set(CMAKE_REQUIRED_LIBRARIES atomic)
    check_cxx_source_compiles(
      "
alignas(16) volatile unsigned __int128 value = 0;
int main(int argc, char**) {
  __atomic_store_n(&value, static_cast<unsigned __int128>(argc),
                   __ATOMIC_SEQ_CST);
  return static_cast<int>(__atomic_load_n(&value, __ATOMIC_SEQ_CST));
}
"
      MOONCAKE_HAS_LIBATOMIC_16BYTE_ATOMICS)
    set(CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES_SAVE})
    if(MOONCAKE_HAS_LIBATOMIC_16BYTE_ATOMICS)
      list(APPEND EXTRA_LIBS atomic)
      message(STATUS "16-byte atomics: using libatomic")
    else()
      message(FATAL_ERROR "16-byte atomic operations are not supported")
    endif()
  endif()
endif()

# Alibaba Cloud OSS adapter (REST API with OSS V4 signing).
find_package(CURL QUIET)
find_package(OpenSSL QUIET)
if(CURL_FOUND AND OpenSSL_FOUND)
  message(STATUS "Alibaba Cloud OSS adapter: Enabled")
  set(MOONCAKE_OSS_ADAPTER_ENABLED
      TRUE
      CACHE INTERNAL "Whether the Alibaba Cloud OSS adapter is built")
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES config/oss_adapter_config.cpp
       storage/distributed/oss_adapter.cpp)
  list(APPEND EXTRA_LIBS CURL::libcurl OpenSSL::Crypto)
  add_definitions(-DHAVE_OSS_ADAPTER)
else()
  message(
    STATUS "Alibaba Cloud OSS adapter: Disabled (requires CURL and OpenSSL)")
  set(MOONCAKE_OSS_ADAPTER_ENABLED
      FALSE
      CACHE INTERNAL "Whether the Alibaba Cloud OSS adapter is built")
endif()

# S3-compatible adapter (REST API with AWS Signature V4). Independent of the OSS
# adapter; it needs the same CURL and OpenSSL packages found above.
if(CURL_FOUND AND OpenSSL_FOUND)
  message(STATUS "S3-compatible adapter: Enabled")
  set(MOONCAKE_S3_ADAPTER_ENABLED
      TRUE
      CACHE INTERNAL "Whether the S3-compatible adapter is built")
  list(
    APPEND
    MOONCAKE_STORE_CLIENT_SOURCES
    storage/distributed/rest_object_storage_adapter.cpp
    storage/distributed/object_storage_xml.cpp
    config/s3_adapter_config.cpp
    storage/distributed/s3_adapter.cpp)
  list(APPEND EXTRA_LIBS CURL::libcurl OpenSSL::Crypto)
  add_definitions(-DHAVE_S3_ADAPTER)
else()
  message(STATUS "S3-compatible adapter: Disabled (requires CURL and OpenSSL)")
  set(MOONCAKE_S3_ADAPTER_ENABLED
      FALSE
      CACHE INTERNAL "Whether the S3-compatible adapter is built")
endif()

# Find AWS SDK
find_package(AWSSDK QUIET COMPONENTS s3)
if(AWSSDK_FOUND)
  message(STATUS "AWS SDK found: ${AWSSDK_VERSION}")
  set(HAVE_AWS_SDK TRUE)
  # Add S3 related source files
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES common/s3_helper.cpp)
else()
  message(STATUS "AWS SDK not found, S3 functionality will be disabled")
  set(HAVE_AWS_SDK FALSE)
endif()

# If AWS SDK is found, add it to EXTRA_LIBS
if(HAVE_AWS_SDK)
  list(APPEND EXTRA_LIBS ${AWSSDK_LINK_LIBRARIES})
  add_definitions(-DHAVE_AWS_SDK)
  if(AWS_S3_LIB AND AWS_CORE_LIB)
    list(APPEND EXTRA_LIBS ${AWS_S3_LIB} ${AWS_CORE_LIB})
    add_definitions(-DHAVE_AWS_SDK)
  endif()
endif()

set(KV_EVENTS_ZMQ_FOUND FALSE)
if(ENABLE_KV_EVENTS)
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES kv_event/kv_event_publisher.cpp)
  set(KV_EVENTS_ZMQ_FOUND TRUE)
endif()

if(USE_3FS)
  add_subdirectory(hf3fs)
  list(APPEND MOONCAKE_STORE_SHARED_SOURCES ${HF3FS_SOURCES})
  list(APPEND MOONCAKE_STORE_SHARED_SOURCES
       storage/distributed/hf3fs_adapter.cpp)
  find_library(
    HF3FS_API_LIB hf3fs_api_shared
    PATHS /usr/lib
    NO_DEFAULT_PATH)
  if(NOT HF3FS_API_LIB)
    message(FATAL_ERROR "hf3fs_api_shared library not found in /usr/lib")
  endif()
  list(APPEND EXTRA_LIBS ${HF3FS_API_LIB})
endif()

set(MOONCAKE_HAVE_NVME_URING_CMD OFF)
if(TARGET Mooncake::liburing)
  list(APPEND MOONCAKE_STORE_CLIENT_SOURCES uring_file.cpp)

  include(CheckCXXSourceCompiles)
  get_target_property(MOONCAKE_LIBURING_INCLUDE_DIRS Mooncake::liburing
                      INTERFACE_INCLUDE_DIRECTORIES)
  set(CMAKE_REQUIRED_INCLUDES_SAVE ${CMAKE_REQUIRED_INCLUDES})
  set(CMAKE_REQUIRED_INCLUDES ${MOONCAKE_LIBURING_INCLUDE_DIRS})
  check_cxx_source_compiles(
    "
#include <liburing.h>
#include <linux/nvme_ioctl.h>
int main() {
    struct nvme_uring_cmd cmd{};
    io_uring_sqe sqe{};
    sqe.cmd_op = NVME_URING_CMD_IO;
    (void)sqe.cmd;
    io_uring_cqe cqe{};
    (void)cqe.big_cqe;
    unsigned flags = IORING_OP_URING_CMD | IORING_SETUP_CQE32;
    return static_cast<int>(flags) + cmd.opcode;
}
"
    MOONCAKE_NVME_URING_CMD_COMPILES)
  set(CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES_SAVE})
  if(MOONCAKE_NVME_URING_CMD_COMPILES)
    set(MOONCAKE_HAVE_NVME_URING_CMD ON)
    message(STATUS "io_uring: NVMe uring command support enabled")
  else()
    message(
      STATUS
        "io_uring: NVMe uring command support unavailable, NVMe KV io_uring executor disabled"
    )
  endif()
endif()

list(APPEND MOONCAKE_STORE_MASTER_SOURCES k8s_lease_helper.cpp)
if(STORE_USE_K8S_LEASE)
  list(APPEND MOONCAKE_STORE_MASTER_SOURCES
       ha/leadership/backends/k8s/k8s_leader_coordinator.cpp)
  list(APPEND EXTRA_LIBS ${K8S_LEASE_WRAPPER_LIB})
  set_source_files_properties(
    k8s_lease_helper.cpp
    PROPERTIES
      OBJECT_DEPENDS
      "${CMAKE_BINARY_DIR}/mooncake-common/k8s-lease/libk8s_lease_wrapper.h")
endif()

# NoF support code that is SPDK-free (page-registration bookkeeping,
# probe-context pooling, and controller env parsing). Built in every
# configuration so unit tests can exercise it without SPDK; both mooncake_store
# and mooncake_nof_spdk link it (the latter cannot link mooncake_store — that
# would be a cycle).
add_library(mooncake_nof_core STATIC nof/page_registry.cpp
                                     nof/probe_context_pool.cpp
                                     config/spdk_controller_config.cpp)
target_include_directories(mooncake_nof_core
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(mooncake_nof_core
                      PUBLIC glog::glog yalantinglibs::yalantinglibs
                             cachelib_memory_allocator mooncake_common)

if(USE_NOF)
  set(SPDK_INCLUDE_DIR
      "/usr/local/include"
      CACHE PATH "SPDK include directory")
  set(DPDK_INCLUDE_DIR
      "/usr/local/include"
      CACHE PATH "DPDK include directory")
  set(SPDK_LIB_DIR
      "/usr/local/lib"
      CACHE PATH "SPDK static library directory")
  set(DPDK_LIB_DIR
      "/usr/local/lib"
      CACHE PATH "DPDK static library directory")

  set(SPDK_ISA_L_LIB_DIR
      "${CMAKE_SOURCE_DIR}/extern/spdk/isa-l/.libs"
      CACHE PATH "isa-l static library directory")
  if(NOT EXISTS "${SPDK_ISA_L_LIB_DIR}/libisal.a")
    message(
      FATAL_ERROR
        "isa-l not found at ${SPDK_ISA_L_LIB_DIR}/libisal.a. Run "
        "`sudo bash dependencies.sh --with-spdk`, or set -DSPDK_ISA_L_LIB_DIR.")
  endif()

  # 930: version-tolerant SPDK link list (upstream v23.01 baseline / the
  # project's 26.x fork).
  # - event/nvme must exist in every version; a missing one means `make
  #   install` did not finish, so fail hard.
  # - Version-dependent archives are added only when present: 26.x splits
  #   lib/rdma into rdma_cm/rdma_provider/rdma_utils (absent without
  #   --with-rdma), folds sock_posix into libspdk_sock.a and drops blobfs;
  #   v23.01 ships a single rdma archive.
  # - Every other archive should exist in any version; a missing one warns
  #   (usually an incomplete install).
  foreach(_lib event nvme)
    if(NOT EXISTS "${SPDK_LIB_DIR}/libspdk_${_lib}.a")
      message(FATAL_ERROR
        "SPDK lib missing: ${SPDK_LIB_DIR}/libspdk_${_lib}.a — "
        "回 SPDK 目录执行 sudo make install（部署文档阶段 2）")
    endif()
  endforeach()

  set(SPDK_CORE_LIBS)
  foreach(_lib event nvme sock_posix rdma rdma_cm rdma_provider rdma_utils)
    if(EXISTS "${SPDK_LIB_DIR}/libspdk_${_lib}.a")
      list(APPEND SPDK_CORE_LIBS ${SPDK_LIB_DIR}/libspdk_${_lib}.a)
    endif()
  endforeach()

  set(SPDK_OPTIONAL_LIBS)
  foreach(_lib vfio_user dma init env_dpdk log util blob bdev conf json jsonrpc
               sock thread trace vmd rpc keyring keyring_file keyring_linux
               fuse_dispatcher)
    if(EXISTS "${SPDK_LIB_DIR}/libspdk_${_lib}.a")
      list(APPEND SPDK_OPTIONAL_LIBS ${SPDK_LIB_DIR}/libspdk_${_lib}.a)
    else()
      message(WARNING
        "SPDK lib not found (skipped): libspdk_${_lib}.a in ${SPDK_LIB_DIR}")
    endif()
  endforeach()

  include(${CMAKE_CURRENT_SOURCE_DIR}/../cmake/SpdkLinkDependencies.cmake)
  mooncake_spdk_check_core_dpdk("${DPDK_LIB_DIR}")
  mooncake_spdk_split_dpdk_libs(SPDK_SPLIT_DPDK_LIBS "${DPDK_LIB_DIR}")
  mooncake_spdk_fuse_lib(SPDK_FUSE_LIB "${SPDK_LIB_DIR}")

  set(SPDK_STATIC_LIBS
      -Wl,--whole-archive
      ${SPDK_CORE_LIBS}
      -Wl,--no-whole-archive
      ${SPDK_OPTIONAL_LIBS}
      ${DPDK_LIB_DIR}/librte_eal.a
      ${DPDK_LIB_DIR}/librte_mempool.a
      ${DPDK_LIB_DIR}/librte_ring.a
      ${DPDK_LIB_DIR}/librte_bus_pci.a
      ${DPDK_LIB_DIR}/librte_pci.a
      ${DPDK_LIB_DIR}/librte_timer.a
      ${DPDK_LIB_DIR}/librte_telemetry.a
      ${DPDK_LIB_DIR}/librte_kvargs.a
      ${DPDK_LIB_DIR}/librte_mbuf.a
      -Wl,--whole-archive
      ${DPDK_LIB_DIR}/librte_mempool_ring.a
      -Wl,--no-whole-archive
      ${DPDK_LIB_DIR}/librte_power.a
      ${DPDK_LIB_DIR}/librte_ethdev.a
      ${DPDK_LIB_DIR}/librte_net.a
      ${DPDK_LIB_DIR}/librte_vhost.a
      ${DPDK_LIB_DIR}/librte_cryptodev.a
      ${DPDK_LIB_DIR}/librte_hash.a
      ${DPDK_LIB_DIR}/librte_rcu.a
      ${SPDK_SPLIT_DPDK_LIBS}
      pthread
      dl
      numa
      rt
      m
      uuid
      crypto
      aio
      z
      -L${SPDK_ISA_L_LIB_DIR}
      isal
      elf
      ibverbs
      rdmacm
      ssl
      bsd
      ${SPDK_FUSE_LIB})

  # ---------------------------------------------------------------------------
  # 930: URMA transport (UMDK). Objects inside libspdk_nvme.a reference liburma
  # symbols, so the library has to appear after nvme in link order.
  # Use -DUMDK_ROOT=/path/to/umdk-prefix for a non-system install.
  # ---------------------------------------------------------------------------
  if(USE_NOF_URMA)
    # /usr/local may still hold an SPDK without the URMA transport (e.g. an
    # upstream v23.01 leftover). nvme_urma.h is the fork's marker header, so
    # reject that installation here instead of failing later on a missing
    # symbol or declaration.
    if(NOT EXISTS "${SPDK_INCLUDE_DIR}/spdk/nvme_urma.h")
      message(
        FATAL_ERROR
          "USE_NOF_URMA=ON but ${SPDK_INCLUDE_DIR}/spdk/nvme_urma.h not found — "
          "installed SPDK has no URMA transport. 确认 urma_modified_v6 分支产物"
          "已 make install 到 /usr/local（部署文档阶段 2），或用 -DSPDK_INCLUDE_DIR="
          "指向正确前缀")
    endif()
    set(UMDK_ROOT
        ""
        CACHE PATH "UMDK install prefix for the SPDK URMA transport")
    find_path(
      URMA_INCLUDE_DIR
      NAMES urma_api.h
      HINTS ${UMDK_ROOT}
      PATHS /usr /usr/local
      PATH_SUFFIXES include/ub/umdk/urma include/ub/umdk include)
    find_library(
      URMA_LIBRARY
      NAMES urma
      HINTS ${UMDK_ROOT}
      PATHS /usr /usr/local
      PATH_SUFFIXES lib64 lib build/lib lib)
    if(NOT URMA_INCLUDE_DIR OR NOT URMA_LIBRARY)
      message(
        FATAL_ERROR
          "USE_NOF_URMA=ON but UMDK (urma_api.h / liburma) not found; \
pass -DUMDK_ROOT=/path/to/umdk-prefix")
    endif()
    message(STATUS "NoF URMA: include=${URMA_INCLUDE_DIR} lib=${URMA_LIBRARY}")
    include(CheckCXXSourceCompiles)
    set(_NOF_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES})
    set(CMAKE_REQUIRED_INCLUDES ${SPDK_INCLUDE_DIR} ${DPDK_INCLUDE_DIR}
                                ${URMA_INCLUDE_DIR})
    check_cxx_source_compiles("
#include <spdk/nvme_urma.h>
static_assert(sizeof(decltype(&spdk_nvme_urma_register_memory_for_qpair)) > 0);
int main() { return 0; }
" HAVE_SPDK_URMA_WHOLE_POOL)
    set(CMAKE_REQUIRED_INCLUDES ${_NOF_REQUIRED_INCLUDES})
    unset(_NOF_REQUIRED_INCLUDES)
    list(APPEND SPDK_STATIC_LIBS ${URMA_LIBRARY})
  endif()

  # 930: NoF GPU Direct (memory domains) needs the CUDA driver API for pointer
  # classification and DMA-BUF export.
  if(USE_NOF_GPU_DIRECT)
    find_package(CUDAToolkit REQUIRED)
    message(STATUS "NoF GPU Direct: CUDA driver library required for provider")
    list(APPEND SPDK_STATIC_LIBS CUDA::cuda_driver)
  endif()

  # ---------------------------------------------------------------------------
  # SPDK-backed NoF initiator implementation, linked PRIVATE so SPDK/DPDK
  # static libs stop propagating as usage requirements to client/master.
  # The 930 URMA memory-domain provider is SPDK-facing, so it is compiled here
  # as well (private: no SPDK header may leak into include/).
  # ---------------------------------------------------------------------------
  add_library(mooncake_nof_spdk STATIC nof/spdk_initiator.cpp)
  if(USE_NOF_URMA)
    target_sources(mooncake_nof_spdk PRIVATE nof/nof_memory_domain.cpp)
  endif()
  target_include_directories(
    mooncake_nof_spdk
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${SPDK_INCLUDE_DIR}
            ${DPDK_INCLUDE_DIR})
  if(USE_NOF_URMA)
    target_include_directories(mooncake_nof_spdk PRIVATE ${URMA_INCLUDE_DIR})
    if(HAVE_SPDK_URMA_WHOLE_POOL)
      target_compile_definitions(mooncake_nof_spdk
                                 PRIVATE HAVE_SPDK_URMA_WHOLE_POOL=1)
    endif()
  endif()
  if(USE_NOF_GPU_DIRECT)
    target_include_directories(mooncake_nof_spdk
                               PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
  endif()
  target_link_libraries(mooncake_nof_spdk
                        PRIVATE ${SPDK_STATIC_LIBS} glog::glog
                                mooncake_nof_core yalantinglibs::yalantinglibs)

  # spdk_mem_register (SPDK >= 22.05) compile-time probe.
  # CMAKE_TRY_COMPILE_TARGET_TYPE was added in CMake 3.6 (project requires
  # 3.16), so check_cxx_source_compiles compiles to a static library and never
  # links; (void)&spdk_mem_register only needs the declaration to exist.
  set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_INCLUDES ${SPDK_INCLUDE_DIR})
  check_cxx_source_compiles(
    "#include <spdk/env.h>
     int main() { (void)&spdk_mem_register; return 0; }"
    HAVE_SPDK_MEM_REGISTER)
  unset(CMAKE_REQUIRED_INCLUDES)
  unset(CMAKE_TRY_COMPILE_TARGET_TYPE)
  if(HAVE_SPDK_MEM_REGISTER)
    target_compile_definitions(mooncake_nof_spdk
                               PRIVATE HAVE_SPDK_MEM_REGISTER=1)
  else()
    message(
      WARNING
      "spdk_mem_register not found under ${SPDK_INCLUDE_DIR} "
      "(SPDK < 22.05?). NoF-over-RDMA user-buffer registration (#3131) "
      "will remain unfixed: pure-NoF writes over RDMA will fail with "
      "'No translation for ptr' errors.")
  endif()
endif()

# Resolve target-independent configuration before defining the Store targets.
set(MOONCAKE_STORE_INCLUDE_DIRS
    ${MOONCAKE_STORE_INCLUDE_ROOT}/mooncake-store/proto
    ${MOONCAKE_STORE_INCLUDE_ROOT} ${CMAKE_CURRENT_BINARY_DIR}/../include
    ${ETCD_WRAPPER_INCLUDE})

# jemalloc replaces the process allocator, so it is linked into the executables
# rather than into libmooncake_store, which is also consumed by the Python
# extension and must not force an allocator on its host process.
#
# The results are copied into ALLOCATOR_* rather than used through the
# JEMALLOC_* names directly: pkg_check_modules writes those into the CMake
# cache, so a build directory reconfigured back to STORE_USE_JEMALLOC=OFF would
# keep linking the allocator while the compile definition is gone, leaving the
# binary running on jemalloc but reporting mooncake_jemalloc_enabled 0. These
# four are plain variables, so clearing them here takes effect every configure.
set(ALLOCATOR_DEFS)
set(ALLOCATOR_INCS)
set(ALLOCATOR_LIB_DIRS)
set(ALLOCATOR_LIBS)
if(STORE_USE_JEMALLOC)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(JEMALLOC REQUIRED jemalloc)
  set(ALLOCATOR_DEFS STORE_USE_JEMALLOC)
  set(ALLOCATOR_INCS ${JEMALLOC_INCLUDE_DIRS})
  set(ALLOCATOR_LIB_DIRS ${JEMALLOC_LIBRARY_DIRS})
  set(ALLOCATOR_LIBS ${JEMALLOC_STATIC_LIBRARIES})
endif()

include(CheckPIESupported)
check_pie_supported(LANGUAGES CXX)

string(TOUPPER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_UPPER)

# Store Client accelerator staging follows the explicitly selected build
# backend. In particular, non-CUDA wheels are built in a CUDA toolchain image,
# so SDK presence must not enable CUDA support by itself.
if(USE_CUDA)
  find_package(CUDAToolkit REQUIRED)
elseif(USE_HIP)
  find_package(hip REQUIRED)
endif()

# SpDiag instrumentation. Store sources emit perf points through
# spdiag/auto_perf.h against a shared .def file that lives next to the Python
# extension, so every object library that includes it needs both the SpDiag
# include path (mock headers or the system SDK) and that directory.
include(${CMAKE_SOURCE_DIR}/mooncake-common/FindSpDiag.cmake)
set(MOONCAKE_STORE_SPDIAG_DEF_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-integration/store)

# Sources used by both the Master and Client sides of Store.
add_library(mooncake_store_shared_objects OBJECT
            ${MOONCAKE_STORE_SHARED_SOURCES})
set_target_properties(mooncake_store_shared_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_store_shared_objects
                           PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS}
                                   ${MOONCAKE_STORE_SPDIAG_DEF_DIR})
target_link_libraries(
  mooncake_store_shared_objects
  PRIVATE cachelib_memory_allocator
          glog::glog
          gflags::gflags
          yaml-cpp
          mooncake_common
          $<$<PLATFORM_ID:Linux>:numa>
          asio_shared
          SpDiag::spdiag_lib
          yalantinglibs::yalantinglibs)
if(MOONCAKE_KVCS_SDK_FOUND)
  target_compile_definitions(mooncake_store_shared_objects
                             PRIVATE MOONCAKE_HAVE_KVCS_SDK=1)
  target_link_libraries(mooncake_store_shared_objects PRIVATE PkgConfig::KVCS)
endif()
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_shared_objects build_etcd_wrapper)
endif()
if(USE_UB)
  target_link_libraries(mooncake_store_shared_objects PRIVATE ub_allocator)
endif()

# Sources used by mooncake_master and the complete mooncake_store library.
add_library(mooncake_store_master_objects OBJECT
            ${MOONCAKE_STORE_MASTER_SOURCES})
set_target_properties(mooncake_store_master_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(
  mooncake_store_master_objects PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS}
                                        ${CMAKE_CURRENT_SOURCE_DIR}
                                        ${MOONCAKE_STORE_SPDIAG_DEF_DIR})
target_link_libraries(
  mooncake_store_master_objects
  PRIVATE mooncake_store_shared_objects
          mooncake_local_ssd
          cachelib_memory_allocator
          glog::glog
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          mooncake_common
          Mooncake::zstd
          Mooncake::xxhash
          OpenSSL::Crypto
          asio_shared
          SpDiag::spdiag_lib
          yalantinglibs::yalantinglibs)
if(KV_EVENTS_ZMQ_FOUND)
  target_compile_definitions(mooncake_store_master_objects
                             PRIVATE MOONCAKE_ENABLE_KV_EVENTS=1)
  target_link_libraries(mooncake_store_master_objects PRIVATE Mooncake::libzmq)
endif()
if(MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
  target_compile_definitions(mooncake_store_master_objects
                             PRIVATE MOONCAKE_ENABLE_OPLOG_PERF_METRICS)
endif()
if(STORE_USE_REDIS)
  target_link_libraries(mooncake_store_master_objects PRIVATE Mooncake::hiredis)
endif()
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_master_objects build_etcd_wrapper)
endif()
if(STORE_USE_K8S_LEASE)
  add_dependencies(mooncake_store_master_objects build_k8s_lease_wrapper)
endif()

# Sources used only by Store clients and the complete mooncake_store library.
add_library(mooncake_store_client_objects OBJECT
            ${MOONCAKE_STORE_CLIENT_SOURCES})
set_target_properties(mooncake_store_client_objects
                      PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(
  mooncake_store_client_objects PRIVATE ${MOONCAKE_STORE_INCLUDE_DIRS}
                                        ${CMAKE_CURRENT_SOURCE_DIR}
                                        ${MOONCAKE_STORE_SPDIAG_DEF_DIR})
target_link_libraries(
  mooncake_store_client_objects
  PRIVATE mooncake_store_shared_objects
          transfer_engine
          cachelib_memory_allocator
          glog::glog
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          Mooncake::xxhash
          mooncake_common
          $<$<PLATFORM_ID:Linux>:numa>
          asio_shared
          SpDiag::spdiag_lib
          yalantinglibs::yalantinglibs)
if(BUILD_UNIT_TESTS)
  target_compile_definitions(mooncake_store_client_objects
                             PRIVATE MOONCAKE_ENABLE_NVME_KV_TEST_STUB=1)
endif()
if(TARGET Mooncake::liburing)
  target_compile_definitions(mooncake_store_client_objects PRIVATE USE_URING)
  target_link_libraries(mooncake_store_client_objects
                        PRIVATE Mooncake::liburing)
  if(MOONCAKE_HAVE_NVME_URING_CMD)
    target_compile_definitions(mooncake_store_client_objects
                               PRIVATE MOONCAKE_HAVE_NVME_URING_CMD=1)
  endif()
endif()
if(STORE_USE_ETCD)
  add_dependencies(mooncake_store_client_objects build_etcd_wrapper)
endif()
if(USE_CUDA)
  message(STATUS "mooncake_store: CUDA enabled, enabling D2H staging")
  target_compile_definitions(mooncake_store_client_objects PRIVATE USE_CUDA)
  target_include_directories(mooncake_store_client_objects
                             PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
elseif(USE_HIP)
  message(STATUS "mooncake_store: HIP enabled, enabling D2H staging")
  target_compile_definitions(mooncake_store_client_objects PRIVATE USE_HIP)
elseif(USE_XPU)
  # USE_XPU is a global compile definition (common.cmake) and -fsycl propagates
  # from TENT's platform_xpu through transfer_engine, so the SYCL-based
  # accelerator device needs no extra include dirs or link libraries here.
  message(STATUS "mooncake_store: XPU enabled, enabling D2H staging")
endif()
if(USE_ASCEND
   OR USE_ASCEND_DIRECT
   OR USE_UBSHMEM)
  target_include_directories(mooncake_store_client_objects
                             PRIVATE $ENV{ASCEND_PATH}/include)
endif()
if(USE_TENT)
  target_compile_definitions(mooncake_store_client_objects PRIVATE USE_TENT)
  target_link_libraries(mooncake_store_client_objects PRIVATE tent)
endif()

# Reduced Store library linked by mooncake_master.
add_library(
  mooncake_store_master STATIC $<TARGET_OBJECTS:mooncake_store_shared_objects>
                               $<TARGET_OBJECTS:mooncake_store_master_objects>)
target_include_directories(mooncake_store_master
                           PUBLIC ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store_master
  PRIVATE mooncake_local_ssd
          cachelib_memory_allocator
          ${ETCD_WRAPPER_LIB}
          glog::glog
          ${EXTRA_LIBS}
          mooncake_common
          Mooncake::zstd
          asio_shared
          yalantinglibs::yalantinglibs
          gflags::gflags
          JsonCpp::JsonCpp
          yaml-cpp
          Mooncake::xxhash
          OpenSSL::Crypto
          $<$<PLATFORM_ID:Linux>:numa>)
if(KV_EVENTS_ZMQ_FOUND)
  target_link_libraries(mooncake_store_master PRIVATE Mooncake::libzmq)
endif()
if(STORE_USE_REDIS)
  target_link_libraries(mooncake_store_master PRIVATE Mooncake::hiredis)
endif()
if(USE_UB)
  target_link_libraries(mooncake_store_master PRIVATE ub_allocator)
endif()
if(MOONCAKE_KVCS_SDK_FOUND)
  target_link_libraries(mooncake_store_master PRIVATE PkgConfig::KVCS)
endif()
if(USE_NOF)
  # The master-side NoF heartbeat probe path (master_service.cpp) goes through
  # the neutral interface in nof/nof_runtime.cpp (shared sources) into the
  # SPDK-backed initiator.
  target_link_libraries(mooncake_store_master PRIVATE mooncake_nof_spdk)
endif()
# SpDiag perf points (see the object libraries above): the static library has
# to carry the link requirement so mooncake_master ends up with libspdiag.
target_link_libraries(mooncake_store_master PRIVATE SpDiag::spdiag_lib)

# Complete public Store library.
add_library(
  mooncake_store
  $<TARGET_OBJECTS:mooncake_store_shared_objects>
  $<TARGET_OBJECTS:mooncake_store_master_objects>
  $<TARGET_OBJECTS:mooncake_store_client_objects>)
target_include_directories(mooncake_store PUBLIC ${MOONCAKE_STORE_INCLUDE_DIRS})
target_link_libraries(
  mooncake_store
  PUBLIC mooncake_local_ssd
         cachelib_memory_allocator
         ${ETCD_WRAPPER_LIB}
         glog::glog
         ${EXTRA_LIBS}
         mooncake_common
         Mooncake::zstd
         asio_shared
         transfer_engine
         mooncake_nof_core
         yalantinglibs::yalantinglibs
  PRIVATE gflags::gflags JsonCpp::JsonCpp yaml-cpp Mooncake::xxhash
          OpenSSL::Crypto $<$<PLATFORM_ID:Linux>:numa>)
if(USE_NOF)
  # SPDK/DPDK stay confined to mooncake_nof_spdk (PRIVATE) instead of
  # propagating to every mooncake_store consumer.
  target_link_libraries(mooncake_store PRIVATE mooncake_nof_spdk)
endif()
if(MOONCAKE_KVCS_SDK_FOUND)
  target_link_libraries(mooncake_store PRIVATE PkgConfig::KVCS)
endif()
# SpDiag perf points (see the object libraries above).
target_link_libraries(mooncake_store PRIVATE SpDiag::spdiag_lib)
if(KV_EVENTS_ZMQ_FOUND)
  target_link_libraries(mooncake_store PRIVATE Mooncake::libzmq)
endif()
if(STORE_USE_REDIS)
  target_link_libraries(mooncake_store PRIVATE Mooncake::hiredis)
endif()
if(TARGET Mooncake::liburing)
  target_compile_definitions(mooncake_store PUBLIC USE_URING)
  target_link_libraries(mooncake_store PUBLIC Mooncake::liburing)
endif()
if(USE_CUDA)
  target_link_libraries(mooncake_store PRIVATE CUDA::cudart)
elseif(USE_HIP)
  target_link_libraries(mooncake_store PRIVATE hip::host)
endif()
if(USE_ASCEND
   OR USE_ASCEND_DIRECT
   OR USE_UBSHMEM)
  target_link_libraries(mooncake_store PRIVATE ascendcl)
endif()
if(USE_SUNRISE)
  target_link_libraries(
    mooncake_store PRIVATE ${MC_TANGRT_ROOT}/lib/libtangrt_shared.so
                           ${MC_TANGRT_ROOT}/lib/libptml_shared.so dl)
endif()

if(BUILD_UNIT_TESTS AND USE_MLU)
  # --wrap cannot intercept CNRT calls inside a shared mooncake_store. Reuse its
  # objects in a test-only archive so those calls reach the final link. Keep
  # this target here so directory-scoped dependency targets are visible.
  add_library(
    mooncake_store_mlu_test STATIC
    $<TARGET_OBJECTS:mooncake_store_shared_objects>
    $<TARGET_OBJECTS:mooncake_store_master_objects>
    $<TARGET_OBJECTS:mooncake_store_client_objects>)
  target_include_directories(
    mooncake_store_mlu_test
    PUBLIC $<TARGET_PROPERTY:mooncake_store,INTERFACE_INCLUDE_DIRECTORIES>)
  target_compile_definitions(
    mooncake_store_mlu_test
    PUBLIC $<TARGET_PROPERTY:mooncake_store,INTERFACE_COMPILE_DEFINITIONS>)
  get_target_property(mlu_test_store_dependencies mooncake_store LINK_LIBRARIES)
  target_link_libraries(mooncake_store_mlu_test
                        PUBLIC ${mlu_test_store_dependencies})
endif()

if(BUILD_SHARED_LIBS)
  set_target_properties(mooncake_store PROPERTIES INSTALL_RPATH "$ORIGIN")
  install(TARGETS mooncake_store DESTINATION lib)
endif()

# Store Master executable.
add_executable(mooncake_master master.cpp)
set_target_properties(mooncake_master PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_master PRIVATE ${ALLOCATOR_INCS})
target_compile_definitions(mooncake_master PRIVATE ${ALLOCATOR_DEFS})
target_link_directories(mooncake_master PRIVATE ${ALLOCATOR_LIB_DIRS})
target_link_libraries(
  mooncake_master
  PRIVATE mooncake_store_master
          cachelib_memory_allocator
          pthread
          $<$<BOOL:${YLT_ENABLE_IBV}>:ibverbs>
          mooncake_common
          glog::glog
          gflags::gflags
          JsonCpp::JsonCpp
          yalantinglibs::yalantinglibs
          ${ETCD_WRAPPER_LIB}
          ${ALLOCATOR_LIBS}
          asio_shared
          SpDiag::spdiag_lib)
# mooncake_store is static and propagates optional runtime dependencies to its
# consumers. The metadata-only master does not use accelerator staging, so drop
# those dependencies when they do not satisfy any symbols in the final binary.
target_link_options(mooncake_master PRIVATE "LINKER:--as-needed")
if(USE_SUNRISE)
  target_link_directories(mooncake_master PRIVATE ${MC_TANGRT_ROOT}/lib)
endif()

if(STORE_USE_ETCD)
  add_dependencies(mooncake_master build_etcd_wrapper)
endif()

if(CMAKE_BUILD_TYPE_UPPER STREQUAL "RELEASE")
  target_compile_options(mooncake_master PRIVATE -Os)
  target_link_options(mooncake_master PRIVATE -Os -s)
endif()
install(TARGETS mooncake_master DESTINATION bin)

# 930: VSegment quota planner tool.
add_executable(vsegment_quota_planner ../tools/vsegment_quota_planner.cpp)
target_link_libraries(vsegment_quota_planner PRIVATE mooncake_store glog::glog
                                                        gflags::gflags)
install(TARGETS vsegment_quota_planner DESTINATION bin)

# Store Client executable.
add_executable(mooncake_client real_client_main.cpp)
set_target_properties(mooncake_client PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(mooncake_client PRIVATE ${ALLOCATOR_INCS})
target_compile_definitions(mooncake_client PRIVATE ${ALLOCATOR_DEFS})
target_link_directories(mooncake_client PRIVATE ${ALLOCATOR_LIB_DIRS})
target_link_libraries(
  mooncake_client
  PRIVATE mooncake_store transfer_engine asio_shared gflags::gflags
          yalantinglibs::yalantinglibs ${ALLOCATOR_LIBS})
if(USE_CUDA)
  target_compile_definitions(mooncake_client PRIVATE USE_CUDA)
  target_include_directories(mooncake_client
                             PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
  target_link_libraries(mooncake_client PRIVATE CUDA::cudart)
elseif(USE_HIP)
  target_compile_definitions(mooncake_client PRIVATE USE_HIP)
  target_link_libraries(mooncake_client PRIVATE hip::host)
endif()
if(USE_ASCEND
   OR USE_ASCEND_DIRECT
   OR USE_UBSHMEM)
  target_include_directories(mooncake_client PRIVATE $ENV{ASCEND_PATH}/include)
  target_link_libraries(mooncake_client PRIVATE ascendcl)
endif()
if(USE_SUNRISE)
  target_link_libraries(
    mooncake_client PRIVATE ${MC_TANGRT_ROOT}/lib/libtangrt_shared.so
                            ${MC_TANGRT_ROOT}/lib/libptml_shared.so dl)
endif()
if(CMAKE_BUILD_TYPE_UPPER STREQUAL "RELEASE")
  target_compile_options(mooncake_client PRIVATE -Os)
  target_link_options(mooncake_client PRIVATE -Os -s)
endif()
install(TARGETS mooncake_client DESTINATION bin)

# ---------------------------------------------------------------------------
# Optional: a self-contained libmooncake_store.so exporting only the store_c.h C
# ABI, for consumers that dlopen the store (e.g. the Rust crate's `dlopen`
# feature). Whole-archives the static mooncake_store + asio_static (objects are
# -fPIC, see mooncake-common/common.cmake) and localizes everything else via the
# version script; only system/toolkit libs (and the Go etcd/k8s backends when
# enabled) stay DT_NEEDED.
# ---------------------------------------------------------------------------
if(WITH_STORE_C_SHARED)
  # ELF/GNU-ld features (--whole-archive, --as-needed, version script, $ORIGIN);
  # supported by GNU ld, gold, and lld. Linux only.
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "WITH_STORE_C_SHARED currently supports Linux/ELF only "
                        "(got CMAKE_SYSTEM_NAME=${CMAKE_SYSTEM_NAME}).")
  endif()

  # We whole-archive mooncake_store and reuse its name, so require it to be
  # static (add_library(mooncake_store) follows BUILD_SHARED_LIBS, which some
  # backends force ON).
  get_target_property(_mooncake_store_type mooncake_store TYPE)
  if(NOT _mooncake_store_type STREQUAL "STATIC_LIBRARY")
    message(
      FATAL_ERROR
        "WITH_STORE_C_SHARED requires mooncake_store to be a STATIC_LIBRARY "
        "(got ${_mooncake_store_type}); configure with BUILD_SHARED_LIBS=OFF.")
  endif()

  add_library(mooncake_store_c SHARED store_c_shared.cpp)
  set_target_properties(
    mooncake_store_c
    PROPERTIES OUTPUT_NAME mooncake_store
               # Separate dir so this .so does not shadow the static
               # libmooncake_store.a on the Rust `link` backend's -L path.
               LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/shared"
               # $ORIGIN so the installed .so finds co-installed deps (CMake
               # strips the build RUNPATH on install).
               INSTALL_RPATH "$ORIGIN"
               # Relink when the version script (a bare link flag) changes.
               LINK_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/store_c.map")
  add_dependencies(mooncake_store_c mooncake_store asio_static)
  # Whole-archive the static lib + asio_static via file paths (no interface-dep
  # propagation); re-list the mooncake_store target for its transitive deps.
  # --as-needed drops the now-redundant transitive libasio.so.
  target_link_libraries(
    mooncake_store_c
    PRIVATE -Wl,--whole-archive "$<TARGET_FILE:mooncake_store>"
            "$<TARGET_FILE:asio_static>" -Wl,--no-whole-archive -Wl,--as-needed
            mooncake_store)
  # Export only the C ABI; the version script localizes every other symbol
  # pulled from the static archives. Strip debug info in Release (it dominates
  # the .so size).
  target_link_options(
    mooncake_store_c PRIVATE
    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/store_c.map"
    "$<$<CONFIG:Release>:-Wl,-s>")
  install(TARGETS mooncake_store_c DESTINATION lib)
endif()

# NoF segment register/unregister tools (local helper binaries)
add_executable(nof_register nof_register.cpp)
target_link_libraries(
  nof_register PRIVATE mooncake_store mooncake_common pthread asio_shared
                       SpDiag::spdiag_lib)
target_link_options(nof_register PRIVATE "LINKER:--as-needed")

add_executable(nof_unregister nof_unregister.cpp)
target_link_libraries(
  nof_unregister PRIVATE mooncake_store mooncake_common pthread asio_shared
                         SpDiag::spdiag_lib)
target_link_options(nof_unregister PRIVATE "LINKER:--as-needed")
