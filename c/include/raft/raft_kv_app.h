// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include "raft/raft.h"

#ifndef RAFT_KV_MAX_PAIRS
#define RAFT_KV_MAX_PAIRS 128
#endif

typedef struct {
    char key[RAFT_MAX_KEY];
    char value[RAFT_MAX_VALUE];
    int used;
} raft_kv_pair_t;

typedef struct {
    raft_kv_pair_t pairs[RAFT_KV_MAX_PAIRS];
    size_t count;
} raft_kv_state_t;

void raft_kv_state_init(raft_kv_state_t *kv);
const char *raft_kv_get(const raft_kv_state_t *kv, const char *key);
raft_application_t raft_kv_make_application(raft_kv_state_t *kv);
