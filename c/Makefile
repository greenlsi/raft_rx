CC ?= cc
CPPFLAGS ?= -D_POSIX_C_SOURCE=200809L -D_DARWIN_C_SOURCE
CFLAGS ?= -std=c99 -O2 -Wall -Wextra -Iinclude -I../../rxnet/c/include
TRACE_CFLAGS ?= $(CFLAGS) -DRX_TRACE_ENABLE

RXNET_LIB := ../../rxnet/c/build/librxnet.a
RXNET_TRACE_LIB := ../../rxnet/c/build/librxnet_trace.a

all: build/libraft_rx.a build/libraft_rx_trace.a build/test_raft build/raft_kv_cluster build/raft_kv_cluster_trace

build:
	mkdir -p build

build/raft.o: build src/raft.c include/raft/raft.h src/raft_storage_internal.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ src/raft.c

build/raft_transport.o: build src/raft_transport.c include/raft/raft.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ src/raft_transport.c

build/raft_storage.o: build src/raft_storage.c include/raft/raft.h src/raft_storage_internal.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ src/raft_storage.c

build/raft_kv_app.o: build src/raft_kv_app.c include/raft/raft_kv_app.h include/raft/raft.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ src/raft_kv_app.c

build/raft_tcp_transport.o: build src/raft_tcp_transport.c include/raft/raft_tcp_transport.h include/raft/raft.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ src/raft_tcp_transport.c

build/raft_trace.o: build src/raft.c include/raft/raft.h src/raft_storage_internal.h
	$(CC) $(CPPFLAGS) $(TRACE_CFLAGS) -c -o $@ src/raft.c

build/raft_transport_trace.o: build src/raft_transport.c include/raft/raft.h
	$(CC) $(CPPFLAGS) $(TRACE_CFLAGS) -c -o $@ src/raft_transport.c

build/raft_storage_trace.o: build src/raft_storage.c include/raft/raft.h src/raft_storage_internal.h
	$(CC) $(CPPFLAGS) $(TRACE_CFLAGS) -c -o $@ src/raft_storage.c

build/raft_kv_app_trace.o: build src/raft_kv_app.c include/raft/raft_kv_app.h include/raft/raft.h
	$(CC) $(CPPFLAGS) $(TRACE_CFLAGS) -c -o $@ src/raft_kv_app.c

build/raft_tcp_transport_trace.o: build src/raft_tcp_transport.c include/raft/raft_tcp_transport.h include/raft/raft.h
	$(CC) $(CPPFLAGS) $(TRACE_CFLAGS) -c -o $@ src/raft_tcp_transport.c

build/libraft_rx.a: build/raft.o build/raft_transport.o build/raft_storage.o build/raft_kv_app.o build/raft_tcp_transport.o
	ar rcs $@ build/raft.o build/raft_transport.o build/raft_storage.o build/raft_kv_app.o build/raft_tcp_transport.o

build/libraft_rx_trace.a: build/raft_trace.o build/raft_transport_trace.o build/raft_storage_trace.o build/raft_kv_app_trace.o build/raft_tcp_transport_trace.o
	ar rcs $@ build/raft_trace.o build/raft_transport_trace.o build/raft_storage_trace.o build/raft_kv_app_trace.o build/raft_tcp_transport_trace.o

build/test_raft: build tests/test_raft.c build/libraft_rx.a $(RXNET_LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ tests/test_raft.c build/libraft_rx.a $(RXNET_LIB)

build/raft_kv_cluster: build examples/kv_cluster.c build/libraft_rx.a $(RXNET_LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ examples/kv_cluster.c build/libraft_rx.a $(RXNET_LIB)

build/raft_kv_cluster_trace: build examples/kv_cluster_trace.c build/libraft_rx_trace.a $(RXNET_TRACE_LIB)
	$(CC) $(CPPFLAGS) $(TRACE_CFLAGS) -o $@ examples/kv_cluster_trace.c build/libraft_rx_trace.a $(RXNET_TRACE_LIB)

test: build/test_raft build/raft_kv_cluster
	./build/test_raft
	./build/raft_kv_cluster
	./build/raft_kv_cluster

clean:
	rm -rf build

.PHONY: all test clean
