// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

#include "raft/raft.h"
#include "raft/raft_kv_app.h"

typedef void (*demo_peer_set_fn)(void *user,
                                 const char *node_id,
                                 const char *address);

typedef struct {
    raft_application_t kv_app;
    raft_kv_state_t *kv;
    demo_peer_set_fn peer_set;
    void *peer_set_user;
} demo_kv_app_t;

raft_application_t demo_kv_app_make(demo_kv_app_t *state,
                                    raft_kv_state_t *kv,
                                    demo_peer_set_fn peer_set,
                                    void *peer_set_user);
