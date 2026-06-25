# Integration of the paimon-cpp SDK (https://github.com/alibaba/paimon-cpp /
# https://github.com/apache/paimon-cpp).
#
# Status (Phase 0 / Task 0.1):
#   The upstream SDK is still mid-migration and does NOT yet provide a top-level
#   CMake build that can produce a static/shared library. Until the SDK exposes
#   a stable build, this wrapper supports two modes:
#
#     * "stub"     (default): a tiny in-tree object library is compiled which
#                  defines just the SDK symbols ClickHouse calls into. Every
#                  stub returns `Status::NotImplemented`, which the ClickHouse
#                  wrapper turns into a `LOGICAL_ERROR` exception, so binaries
#                  link cleanly and any attempt to actually use the SDK at
#                  runtime (e.g. SET use_paimon_sdk_read = 1) produces a
#                  comprehensible error rather than a mysterious link failure.
#
#     * "prebuilt": link against a pre-built libpaimon (path supplied via the
#                  PAIMON_CPP_PREBUILT_LIBRARY cache var). Used once the SDK
#                  upstream provides a production build.
#
#   Both modes only kick in if ENABLE_PAIMON_CPP is ON and the submodule under
#   contrib/paimon-cpp is checked out (it is required for the headers).

option(ENABLE_PAIMON_CPP "Enable paimon-cpp SDK for the Paimon table engine" ${ENABLE_LIBRARIES})

if (NOT ENABLE_PAIMON_CPP)
    message(STATUS "Not using paimon-cpp")
    return()
endif()

set(PAIMON_CPP_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/paimon-cpp")
set(PAIMON_CPP_INCLUDE_DIR "${PAIMON_CPP_SOURCE_DIR}/include")

if (NOT EXISTS "${PAIMON_CPP_INCLUDE_DIR}/paimon/defs.h")
    message(WARNING
        "contrib/paimon-cpp submodule is not checked out. "
        "Run `git submodule update --init contrib/paimon-cpp`. Disabling paimon-cpp.")
    set(ENABLE_PAIMON_CPP OFF CACHE BOOL "" FORCE)
    return()
endif()

set(PAIMON_CPP_LINK_MODE "stub" CACHE STRING
    "How to obtain libpaimon at link time: 'stub' for the in-tree placeholder, 'prebuilt' to link an external library.")
set_property(CACHE PAIMON_CPP_LINK_MODE PROPERTY STRINGS stub prebuilt)

if (PAIMON_CPP_LINK_MODE STREQUAL "stub")
    add_library(_paimon_cpp_impl STATIC
        "${CMAKE_CURRENT_SOURCE_DIR}/paimon_cpp_stub.cpp")
    set_target_properties(_paimon_cpp_impl PROPERTIES POSITION_INDEPENDENT_CODE ON)
    target_include_directories(_paimon_cpp_impl SYSTEM PUBLIC "${PAIMON_CPP_INCLUDE_DIR}")
    target_compile_definitions(_paimon_cpp_impl PRIVATE PAIMON_CPP_USING_STUB=1)
    message(STATUS "paimon-cpp: using stub link mode (SDK calls return Status::NotImplemented)")
elseif (PAIMON_CPP_LINK_MODE STREQUAL "prebuilt")
    if (NOT PAIMON_CPP_PREBUILT_LIBRARY)
        message(FATAL_ERROR
            "PAIMON_CPP_LINK_MODE=prebuilt requires PAIMON_CPP_PREBUILT_LIBRARY to point at a libpaimon archive.")
    endif()
    if (NOT EXISTS "${PAIMON_CPP_PREBUILT_LIBRARY}")
        message(FATAL_ERROR "PAIMON_CPP_PREBUILT_LIBRARY=${PAIMON_CPP_PREBUILT_LIBRARY} does not exist.")
    endif()
    add_library(_paimon_cpp_impl INTERFACE)
    target_include_directories(_paimon_cpp_impl SYSTEM INTERFACE "${PAIMON_CPP_INCLUDE_DIR}")
    target_link_libraries(_paimon_cpp_impl INTERFACE "${PAIMON_CPP_PREBUILT_LIBRARY}")
    target_compile_definitions(_paimon_cpp_impl INTERFACE PAIMON_CPP_USING_STUB=0)
    message(STATUS "paimon-cpp: linking prebuilt library at ${PAIMON_CPP_PREBUILT_LIBRARY}")
else()
    message(FATAL_ERROR "Unknown PAIMON_CPP_LINK_MODE=${PAIMON_CPP_LINK_MODE}")
endif()

add_library(ch_contrib::paimon_cpp ALIAS _paimon_cpp_impl)
