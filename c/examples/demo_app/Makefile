CC ?= cc
CFLAGS ?= -std=c99 -O2 -Wall -Wextra

C_ROOT := ../..
RXNET_C_ROOT := ../../../../rxnet/c

INCLUDES := -I$(C_ROOT)/include -I$(RXNET_C_ROOT)/include
RAFT_LIB := $(C_ROOT)/build/libraft_rx.a
RXNET_LIB := $(RXNET_C_ROOT)/build/librxnet.a

TARGET := build/raft_kv_demo_app
SRC := src/main.c $(C_ROOT)/src/raft_kv_app.c

all: $(TARGET)

build:
	mkdir -p build

$(RAFT_LIB):
	$(MAKE) -C $(C_ROOT) build/libraft_rx.a

$(RXNET_LIB):
	$(MAKE) -C $(RXNET_C_ROOT) build/librxnet.a

$(TARGET): build $(SRC) $(RAFT_LIB) $(RXNET_LIB)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $(SRC) $(RAFT_LIB) $(RXNET_LIB)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf build

.PHONY: all run clean
