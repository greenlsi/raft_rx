// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#include "demo_kv_app.h"

#include <string.h>

static void demo_apply(void *user, const raft_command_t *command) {
    demo_kv_app_t *state = (demo_kv_app_t *)user;
    if (strcmp(command->op, "cluster.peer_set") == 0) {
        if (state->peer_set)
            state->peer_set(state->peer_set_user, command->key, command->value);
        return;
    }
    if (state->kv_app.apply)
        state->kv_app.apply(state->kv_app.user, command);
}

static void demo_reload(void *user) {
    demo_kv_app_t *state = (demo_kv_app_t *)user;
    if (state->kv_app.reload)
        state->kv_app.reload(state->kv_app.user);
}

static size_t demo_snapshot(void *user, void *buf, size_t buf_size) {
    demo_kv_app_t *state = (demo_kv_app_t *)user;
    if (!state->kv_app.snapshot) return 0;
    return state->kv_app.snapshot(state->kv_app.user, buf, buf_size);
}

static void demo_restore_snapshot(void *user, const void *buf, size_t size) {
    demo_kv_app_t *state = (demo_kv_app_t *)user;
    if (state->kv_app.restore_snapshot)
        state->kv_app.restore_snapshot(state->kv_app.user, buf, size);
}

raft_application_t demo_kv_app_make(demo_kv_app_t *state,
                                    raft_kv_state_t *kv,
                                    demo_peer_set_fn peer_set,
                                    void *peer_set_user) {
    raft_application_t app;
    state->kv = kv;
    state->peer_set = peer_set;
    state->peer_set_user = peer_set_user;
    state->kv_app = raft_kv_make_application(kv);

    memset(&app, 0, sizeof(app));
    app.apply = demo_apply;
    app.reload = demo_reload;
    app.snapshot = demo_snapshot;
    app.restore_snapshot = demo_restore_snapshot;
    app.user = state;
    return app;
}
