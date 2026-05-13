CC ?= cc
CPPFLAGS ?= -D_POSIX_C_SOURCE=200809L -D_DARWIN_C_SOURCE
CFLAGS ?= -std=c99 -O2 -Wall -Wextra

prefix ?= /opt/homebrew

INCLUDES := -I$(prefix)/include
RAFT_LIB := $(prefix)/lib/libraft_rx.a
RXNET_LIB := $(prefix)/lib/librxnet.a

TARGET := build/counter_node
SRC := src/main.c src/counter_cli.c

all: $(TARGET)

build:
	mkdir -p build

$(TARGET): build $(SRC) $(RAFT_LIB) $(RXNET_LIB)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(INCLUDES) -o $@ $(SRC) $(RAFT_LIB) $(RXNET_LIB) -lpthread

clean:
	rm -rf build

.PHONY: all run clean
