CC ?= cc
CFLAGS ?= -std=c99 -O2 -Wall -Wextra -Iinclude -I../../rxnet/c/include

RXNET_SRCS = \
	../../rxnet/c/src/runtime.c \
	../../rxnet/c/src/fsm.c \
	../../rxnet/c/src/cyclic.c \
	../../rxnet/c/src/coop.c \
	../../rxnet/c/src/thread.c \
	../../rxnet/c/src/trace.c

RAFT_SRCS = src/raft.c

all: build/libraft_rx.a build/test_raft build/raft_kv_cluster

build:
	mkdir -p build

build/raft.o: build src/raft.c include/raft/raft.h
	$(CC) $(CFLAGS) -c -o $@ src/raft.c

build/libraft_rx.a: build/raft.o
	ar rcs $@ build/raft.o

build/test_raft: build tests/test_raft.c $(RAFT_SRCS) $(RXNET_SRCS)
	$(CC) $(CFLAGS) -o $@ tests/test_raft.c $(RAFT_SRCS) $(RXNET_SRCS)

build/raft_kv_cluster: build examples/kv_cluster.c $(RAFT_SRCS) $(RXNET_SRCS)
	$(CC) $(CFLAGS) -o $@ examples/kv_cluster.c $(RAFT_SRCS) $(RXNET_SRCS)

test: build/test_raft
	./build/test_raft

clean:
	rm -rf build

.PHONY: all test clean
