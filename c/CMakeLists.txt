cmake_minimum_required(VERSION 3.20)
project(raft C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_library(rxnet STATIC
    ../../rxnet/c/src/runtime.c
    ../../rxnet/c/src/fsm.c
    ../../rxnet/c/src/cyclic.c
    ../../rxnet/c/src/coop.c
    ../../rxnet/c/src/thread.c
    ../../rxnet/c/src/trace.c
)
target_include_directories(rxnet PUBLIC ../../rxnet/c/include)

add_library(raft_rx STATIC
    src/raft.c
)
set_target_properties(raft_rx PROPERTIES OUTPUT_NAME raft_rx)
target_include_directories(raft_rx PUBLIC include ../../rxnet/c/include)
target_link_libraries(raft_rx PUBLIC rxnet)

add_executable(raft_kv_cluster examples/kv_cluster.c)
target_link_libraries(raft_kv_cluster PRIVATE raft_rx)

add_executable(test_raft tests/test_raft.c)
target_link_libraries(test_raft PRIVATE raft_rx)

enable_testing()
add_test(NAME test_raft COMMAND test_raft)
