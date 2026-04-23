CC ?= cc
CFLAGS ?= -std=c99 -O2 -Wall -Wextra -Iinclude -I../../rxnet/c/include
TRACE_CFLAGS ?= $(CFLAGS) -DRX_TRACE_ENABLE

RXNET_LIB := ../../rxnet/c/build/librxnet.a
RXNET_TRACE_LIB := ../../rxnet/c/build/librxnet_trace.a

all: build/libraft_rx.a build/libraft_rx_trace.a build/test_raft build/raft_kv_cluster build/raft_kv_cluster_trace

build:
	mkdir -p build

build/raft.o: build src/raft.c include/raft/raft.h
	$(CC) $(CFLAGS) -c -o $@ src/raft.c

build/raft_trace.o: build src/raft.c include/raft/raft.h
	$(CC) $(TRACE_CFLAGS) -c -o $@ src/raft.c

build/libraft_rx.a: build/raft.o
	ar rcs $@ build/raft.o

build/libraft_rx_trace.a: build/raft_trace.o
	ar rcs $@ build/raft_trace.o

build/test_raft: build tests/test_raft.c build/libraft_rx.a $(RXNET_LIB)
	$(CC) $(CFLAGS) -o $@ tests/test_raft.c build/libraft_rx.a $(RXNET_LIB)

build/raft_kv_cluster: build examples/kv_cluster.c build/libraft_rx.a $(RXNET_LIB)
	$(CC) $(CFLAGS) -o $@ examples/kv_cluster.c build/libraft_rx.a $(RXNET_LIB)

build/raft_kv_cluster_trace: build examples/kv_cluster_trace.c build/libraft_rx_trace.a $(RXNET_TRACE_LIB)
	$(CC) $(TRACE_CFLAGS) -o $@ examples/kv_cluster_trace.c build/libraft_rx_trace.a $(RXNET_TRACE_LIB)

test: build/test_raft
	./build/test_raft

clean:
	rm -rf build

.PHONY: all test clean
