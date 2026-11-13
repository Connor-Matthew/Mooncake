# Split DPDK libraries differ between SPDK's supported releases. Older builds
# keep some of these symbols in eal; newer builds install separate archives.
function(mooncake_spdk_check_core_dpdk directory)
  foreach(name eal mempool ring bus_pci pci timer telemetry kvargs mbuf
               mempool_ring power ethdev net vhost cryptodev hash rcu)
    if(NOT EXISTS "${directory}/librte_${name}.a")
      message(FATAL_ERROR
        "Missing required DPDK archive: ${directory}/librte_${name}.a. "
        "Install the DPDK libraries from the same SPDK build or set DPDK_LIB_DIR.")
    endif()
  endforeach()
endfunction()

function(mooncake_spdk_split_dpdk_libs output directory)
  set(libraries)
  foreach(name log argparse bus_vdev cmdline dmadev meter stack)
    if(EXISTS "${directory}/librte_${name}.a")
      list(APPEND libraries "${directory}/librte_${name}.a")
    endif()
  endforeach()
  set(${output} ${libraries} PARENT_SCOPE)
endfunction()

# FUSE is an optional SPDK feature. Requiring fuse3 for every NoF build breaks
# installations built without the dispatcher archive.
function(mooncake_spdk_fuse_lib output directory)
  if(EXISTS "${directory}/libspdk_fuse_dispatcher.a")
    find_library(MOONCAKE_SPDK_FUSE3_LIBRARY NAMES fuse3)
    if(NOT MOONCAKE_SPDK_FUSE3_LIBRARY OR
       NOT EXISTS "${MOONCAKE_SPDK_FUSE3_LIBRARY}")
      message(FATAL_ERROR
        "SPDK fuse_dispatcher is installed but libfuse3 was not found. "
        "Install the fuse3 development package or use an SPDK build without FUSE.")
    endif()
    set(${output} ${MOONCAKE_SPDK_FUSE3_LIBRARY} PARENT_SCOPE)
  else()
    set(${output} "" PARENT_SCOPE)
  endif()
endfunction()
