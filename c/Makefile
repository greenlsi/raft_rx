CC ?= cc
CPPFLAGS ?= -D_POSIX_C_SOURCE=200809L -D_DARWIN_C_SOURCE
CFLAGS ?= -std=c99 -O2 -Wall -Wextra -Iinclude -I../../rxnet/c/include
TRACE_CFLAGS ?= $(CFLAGS) -DRX_TRACE_ENABLE

RXNET_LIB := ../../rxnet/c/build/librxnet.a
RXNET_TRACE_LIB := ../../rxnet/c/build/librxnet_trace.a

# ── coreMQTT ──────────────────────────────────────────────────────────────
COREMQTT_DIR  := third_party/coreMQTT/source
COREMQTT_INC  := -I$(COREMQTT_DIR)/include \
                 -I$(COREMQTT_DIR)/interface \
                 -I third_party
COREMQTT_SRCS := $(COREMQTT_DIR)/core_mqtt.c \
                 $(COREMQTT_DIR)/core_mqtt_serializer.c \
                 $(COREMQTT_DIR)/core_mqtt_state.c
COREMQTT_OBJS := build/core_mqtt.o \
                 build/core_mqtt_serializer.o \
                 build/core_mqtt_state.o
OPENSSL_FLAGS := $(shell pkg-config --cflags openssl 2>/dev/null || echo "")
OPENSSL_LIBS  := $(shell pkg-config --libs   openssl 2>/dev/null || echo "-lssl -lcrypto")

all: build/libraft_rx.a build/libraft_rx_trace.a \
     build/test_raft build/raft_kv_cluster build/raft_kv_cluster_trace \
     build/raft_mqtt_transport.o build/raft_mqtt_join.o \
     build/raft_mqtt_hal_posix.o $(COREMQTT_OBJS) \
     build/test_mqtt_transport

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

build/core_mqtt.o: build $(COREMQTT_DIR)/core_mqtt.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(COREMQTT_INC) -c -o $@ $(COREMQTT_DIR)/core_mqtt.c

build/core_mqtt_serializer.o: build $(COREMQTT_DIR)/core_mqtt_serializer.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(COREMQTT_INC) -c -o $@ $(COREMQTT_DIR)/core_mqtt_serializer.c

build/core_mqtt_state.o: build $(COREMQTT_DIR)/core_mqtt_state.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(COREMQTT_INC) -c -o $@ $(COREMQTT_DIR)/core_mqtt_state.c

build/raft_mqtt_transport.o: build src/raft_mqtt_transport.c \
    include/raft/raft_mqtt_transport.h include/raft/raft_mqtt_hal.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ src/raft_mqtt_transport.c

build/raft_mqtt_hal_posix.o: build src/raft_mqtt_hal_posix.c \
    include/raft/raft_mqtt_hal_posix.h $(COREMQTT_SRCS)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(COREMQTT_INC) $(OPENSSL_FLAGS) \
	    -c -o $@ src/raft_mqtt_hal_posix.c

build/raft_mqtt_join.o: build src/raft_mqtt_join.c \
    include/raft/raft_mqtt_transport.h
	$(CC) $(CPPFLAGS) $(CFLAGS) -c -o $@ src/raft_mqtt_join.c

build/test_mqtt_transport: build \
    tests/test_mqtt_transport.c \
    build/raft_mqtt_transport.o \
    build/raft_mqtt_join.o \
    $(RXNET_LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ \
	    tests/test_mqtt_transport.c \
	    build/raft_mqtt_transport.o \
	    build/raft_mqtt_join.o \
	    $(RXNET_LIB)

test: build/test_raft build/raft_kv_cluster build/test_mqtt_transport
	./build/test_raft
	./build/raft_kv_cluster
	./build/raft_kv_cluster
	./build/test_mqtt_transport

clean:
	rm -rf build

.PHONY: all test clean
