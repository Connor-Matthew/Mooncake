# Keep experimental dependency instrumentation out of the source submodule and
# the system installation. All consumers must see the same request_config_t.
option(MC_ENABLE_RPC_SUBMIT_DIAG
       "Build request-scoped coro_rpc submission diagnostics (Linux)" OFF)

if(MC_ENABLE_RPC_SUBMIT_DIAG)
  if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "MC_ENABLE_RPC_SUBMIT_DIAG requires Linux gettid")
  endif()

  get_target_property(_rpc_ylt_target yalantinglibs::yalantinglibs ALIASED_TARGET)
  if(NOT _rpc_ylt_target)
    set(_rpc_ylt_target yalantinglibs::yalantinglibs)
  endif()
  get_target_property(_rpc_ylt_dirs ${_rpc_ylt_target}
                      INTERFACE_INCLUDE_DIRECTORIES)
  find_path(
    MC_RPC_DIAG_YLT_INCLUDE_DIR ylt/coro_rpc/impl/coro_rpc_client.hpp
    HINTS ${_rpc_ylt_dirs}
    DOC "Installed yalantinglibs include root used for RPC diagnostics"
    NO_DEFAULT_PATH)
  if(NOT MC_RPC_DIAG_YLT_INCLUDE_DIR)
    message(FATAL_ERROR "Cannot locate coro_rpc headers; set MC_RPC_DIAG_YLT_INCLUDE_DIR")
  endif()
  find_program(_rpc_patch patch)
  if(NOT _rpc_patch)
    message(FATAL_ERROR "MC_ENABLE_RPC_SUBMIT_DIAG requires the patch utility")
  endif()
  set(_rpc_overlay "${CMAKE_BINARY_DIR}/rpc-submit-diag/include")
  file(MAKE_DIRECTORY "${_rpc_overlay}/ylt")
  # Copy siblings too: the client and public headers use relative includes.
  file(COPY "${MC_RPC_DIAG_YLT_INCLUDE_DIR}/ylt/coro_rpc"
       DESTINATION "${_rpc_overlay}/ylt")
  # Always reset the patched file, including on a repeated CMake configure.
  configure_file(
    "${MC_RPC_DIAG_YLT_INCLUDE_DIR}/ylt/coro_rpc/impl/coro_rpc_client.hpp"
    "${_rpc_overlay}/ylt/coro_rpc/impl/coro_rpc_client.hpp" COPYONLY)
  configure_file(
    "${CMAKE_CURRENT_LIST_DIR}/include/rpc_submit_trace.h"
    "${_rpc_overlay}/ylt/coro_rpc/impl/mooncake_submit_trace.hpp" COPYONLY)
  file(READ "${_rpc_overlay}/ylt/coro_rpc/impl/coro_rpc_client.hpp" _rpc_header)
  if(NOT _rpc_header MATCHES "mooncake_submit_trace.hpp")
    # The zero-context patch avoids whitespace-only context lines being changed
    # by format hooks. Require the exact source before applying line offsets.
    file(SHA256 "${_rpc_overlay}/ylt/coro_rpc/impl/coro_rpc_client.hpp"
         _rpc_header_sha256)
    if(NOT _rpc_header_sha256 STREQUAL
       "080d41af367c5d8e4d37792c43015cc10ff3d4b919d2206a8feb0dbdd97f792c")
      message(FATAL_ERROR
        "RPC diagnostics require the coro_rpc_client.hpp from e46cbf08356f. "
        "Installed header SHA256: ${_rpc_header_sha256}. "
        "No system files were modified; disable MC_ENABLE_RPC_SUBMIT_DIAG "
        "or use a matching yalantinglibs installation.")
    endif()
    execute_process(
      COMMAND "${_rpc_patch}" --batch --forward --fuzz=0 -p1 -i
              "${CMAKE_CURRENT_LIST_DIR}/rpc_submit_diag.patch"
      WORKING_DIRECTORY "${_rpc_overlay}"
      RESULT_VARIABLE _rpc_patch_result
      OUTPUT_VARIABLE _rpc_patch_output
      ERROR_VARIABLE _rpc_patch_error)
    if(NOT _rpc_patch_result EQUAL 0)
      message(FATAL_ERROR
        "RPC diagnostics patch does not match installed yalantinglibs. "
        "Expected client layout from e46cbf08356f; no system files were modified. "
        "${_rpc_patch_output}${_rpc_patch_error}")
    endif()
  endif()
  # Include priority is required for both public and direct impl includes.
  include_directories(BEFORE "${_rpc_overlay}")
  set_property(TARGET ${_rpc_ylt_target} PROPERTY INTERFACE_INCLUDE_DIRECTORIES
               "${_rpc_overlay};${_rpc_ylt_dirs}")
  add_compile_definitions(MOONCAKE_RPC_SUBMIT_DIAG_BUILD=1)
  message(STATUS "RPC submission diagnostics headers: ${_rpc_overlay}")
endif()
