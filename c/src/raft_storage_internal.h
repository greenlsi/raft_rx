#pragma once

#include "raft/raft.h"

void raft_storage_node_save(raft_node_t *node);
void raft_storage_node_load(raft_node_t *node);
void raft_storage_save_snapshot(raft_node_t *node, int index, int term);
