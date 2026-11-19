set(UB_ALLOCATOR_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/ub_allocator.cpp")
file(GLOB UB_SOURCES "*.cpp" "urma/urma_endpoint.cpp")
list(REMOVE_ITEM UB_SOURCES "${UB_ALLOCATOR_SOURCE}")

add_library(ub_allocator OBJECT "${UB_ALLOCATOR_SOURCE}")
target_include_directories(
  ub_allocator PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                      $<INSTALL_INTERFACE:include>)
target_link_libraries(ub_allocator PRIVATE glog::glog numa)

# Build ub_transport with real URMA library if available
# Never include mock_urma_api.cpp in ub_transport library
# For test, we will use mock_urma_api.cpp directly
if (URMA_LIBRARY)
    add_library(ub_transport OBJECT ${UB_SOURCES})
    target_link_libraries(ub_transport
            PUBLIC
            ${URMA_LIBRARY}
    )
    message(STATUS "Using real URMA library: ${URMA_LIBRARY}")
else()
    # If liburma.so not found, we'll need to handle this differently
    # For now, just build without mock_urma_api.cpp
    list(APPEND UB_SOURCES "urma/mock_urma.cpp")
    add_library(ub_transport OBJECT ${UB_SOURCES})
    message(WARNING "Not Found liburma.so building ub_transport with Mock URMA library")
endif()
target_include_directories(ub_transport
        PUBLIC
        ${urma_INCLUDE_DIR}
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../../../../mooncake-integration/store
)

# Probe whether the URMA headers expose urma_seg_cfg_t::is_gpu_seg, required
# for the gdr-peermem GPU-Direct path. When absent (older UMDK), the build
# still succeeds; gdr-peermem is simply unavailable and the transport falls
# back to host/staging modes only.
include(CheckCXXSourceCompiles)
set(_UB_SAVED_CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES})
set(CMAKE_REQUIRED_INCLUDES ${urma_INCLUDE_DIR})
check_cxx_source_compiles("
#include <urma_api.h>
int main() {
    urma_seg_cfg_t cfg = {};
    cfg.is_gpu_seg = 1;
    return 0;
}
" HAVE_URMA_SEG_CFG_IS_GPU_SEG)
set(CMAKE_REQUIRED_INCLUDES ${_UB_SAVED_CMAKE_REQUIRED_INCLUDES})
unset(_UB_SAVED_CMAKE_REQUIRED_INCLUDES)
if(HAVE_URMA_SEG_CFG_IS_GPU_SEG)
    target_compile_definitions(ub_transport
            PRIVATE HAVE_URMA_SEG_CFG_IS_GPU_SEG=1)
    message(STATUS "URMA headers expose urma_seg_cfg_t::is_gpu_seg; gdr-peermem enabled")
else()
    message(STATUS "URMA headers lack urma_seg_cfg_t::is_gpu_seg; gdr-peermem disabled (host/staging only)")
endif()

# The k8s (optical-fabric) path exchanges user-space segment/jetty context
# blobs, so the resolved URMA headers must provide urma_get_seg_ctx and
# urma_get_rjetty. Fail at configure time with an actionable message instead
# of a cryptic compile error.
set(_UB_SAVED_CMAKE_TRY_COMPILE_TARGET_TYPE ${CMAKE_TRY_COMPILE_TARGET_TYPE})
set(_UB_SAVED_CMAKE_REQUIRED_INCLUDES ${CMAKE_REQUIRED_INCLUDES})
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)
set(CMAKE_REQUIRED_INCLUDES ${urma_INCLUDE_DIR})
check_cxx_source_compiles("
#include <urma_api.h>
int main() {
    urma_target_seg_t *tseg = nullptr;
    urma_seg_t *seg = nullptr;
    uint32_t seg_size = 0;
    (void)urma_get_seg_ctx(tseg, &seg, &seg_size);
    (void)urma_put_seg_ctx(seg);
    urma_jetty_t *jetty = nullptr;
    urma_rjetty_t *rjetty = nullptr;
    uint32_t rjetty_len = 0;
    (void)urma_get_rjetty(jetty, &rjetty, &rjetty_len);
    (void)urma_put_rjetty(rjetty);
    return 0;
}
" HAVE_URMA_SEG_CTX_API)
set(CMAKE_REQUIRED_INCLUDES ${_UB_SAVED_CMAKE_REQUIRED_INCLUDES})
set(CMAKE_TRY_COMPILE_TARGET_TYPE ${_UB_SAVED_CMAKE_TRY_COMPILE_TARGET_TYPE})
unset(_UB_SAVED_CMAKE_REQUIRED_INCLUDES)
unset(_UB_SAVED_CMAKE_TRY_COMPILE_TARGET_TYPE)
if (NOT HAVE_URMA_SEG_CTX_API)
    message(FATAL_ERROR
            "The resolved URMA headers do not provide urma_get_seg_ctx/urma_get_rjetty. "
            "Mooncake's k8s (optical-fabric) UB path requires them. Point URMA_ROOT/UMDK_ROOT "
            "at an SDK matching the deployed liburma. Resolved urma_INCLUDE_DIR: ${urma_INCLUDE_DIR}")
endif()

target_link_libraries(ub_transport
        PRIVATE
        Urma::urma
        JsonCpp::JsonCpp
        glog::glog
        pthread
        SpDiag::spdiag_lib
)