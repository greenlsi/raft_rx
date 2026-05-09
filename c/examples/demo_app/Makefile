CC ?= cc
CFLAGS ?= -std=c99 -O2 -Wall -Wextra

C_ROOT := ../..
RXNET_C_ROOT := ../../../../rxnet/c

INCLUDES := -I$(C_ROOT)/include -I$(RXNET_C_ROOT)/include
RAFT_LIB := $(C_ROOT)/build/libraft_rx.a
RXNET_LIB := $(RXNET_C_ROOT)/build/librxnet.a

TARGET := build/raft_node
SRC := src/main.c src/demo_args.c src/demo_kv_app.c src/cli_fsm.c \
       $(C_ROOT)/src/raft_kv_app.c $(C_ROOT)/src/raft_tcp_transport.c

all: $(TARGET)

build:
	mkdir -p build

$(RAFT_LIB):
	$(MAKE) -C $(C_ROOT) build/libraft_rx.a

$(RXNET_LIB):
	$(MAKE) -C $(RXNET_C_ROOT) build/librxnet.a

$(TARGET): build $(SRC) $(RAFT_LIB) $(RXNET_LIB)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $(SRC) $(RAFT_LIB) $(RXNET_LIB) -lpthread

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf build

.PHONY: all run clean
