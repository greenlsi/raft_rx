cmake_minimum_required(VERSION 3.20)
project(raft C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

set(RXNET_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../rxnet/c/include)
set(RXNET_LIB ${CMAKE_CURRENT_SOURCE_DIR}/../../rxnet/c/build/librxnet.a)
set(RXNET_TRACE_LIB ${CMAKE_CURRENT_SOURCE_DIR}/../../rxnet/c/build/librxnet_trace.a)

add_library(rxnet STATIC IMPORTED GLOBAL)
set_target_properties(rxnet PROPERTIES IMPORTED_LOCATION ${RXNET_LIB})
target_include_directories(rxnet INTERFACE ${RXNET_INCLUDE_DIR})

add_library(rxnet_trace STATIC IMPORTED GLOBAL)
set_target_properties(rxnet_trace PROPERTIES IMPORTED_LOCATION ${RXNET_TRACE_LIB})
target_include_directories(rxnet_trace INTERFACE ${RXNET_INCLUDE_DIR})
target_compile_definitions(rxnet_trace INTERFACE RX_TRACE_ENABLE)

set(RAFT_SOURCES
    src/raft.c
    src/raft_transport.c
    src/raft_storage.c
)

add_library(raft_rx STATIC ${RAFT_SOURCES})
set_target_properties(raft_rx PROPERTIES OUTPUT_NAME raft_rx)
target_include_directories(raft_rx PUBLIC include ${RXNET_INCLUDE_DIR})
target_link_libraries(raft_rx PUBLIC rxnet)

add_library(raft_rx_trace STATIC ${RAFT_SOURCES})
set_target_properties(raft_rx_trace PROPERTIES OUTPUT_NAME raft_rx_trace)
target_include_directories(raft_rx_trace PUBLIC include ${RXNET_INCLUDE_DIR})
target_compile_definitions(raft_rx_trace PUBLIC RX_TRACE_ENABLE)
target_link_libraries(raft_rx_trace PUBLIC rxnet_trace)

add_library(raft_kv_app STATIC src/raft_kv_app.c)
target_include_directories(raft_kv_app PUBLIC include)
target_link_libraries(raft_kv_app PUBLIC raft_rx)

add_executable(raft_kv_cluster examples/kv_cluster.c)
target_link_libraries(raft_kv_cluster PRIVATE raft_kv_app raft_rx)

add_executable(raft_kv_cluster_trace examples/kv_cluster_trace.c)
target_link_libraries(raft_kv_cluster_trace PRIVATE raft_kv_app raft_rx_trace)

add_executable(test_raft tests/test_raft.c)
target_link_libraries(test_raft PRIVATE raft_kv_app raft_rx)

enable_testing()
add_test(NAME test_raft COMMAND test_raft)
