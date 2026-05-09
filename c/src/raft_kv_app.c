// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-or-later

#include "raft/raft_kv_app.h"

#include <stdio.h>
#include <string.h>

void raft_kv_state_init(raft_kv_state_t *kv) {
    memset(kv, 0, sizeof(*kv));
}

const char *raft_kv_get(const raft_kv_state_t *kv, const char *key) {
    size_t i;
    for (i = 0; i < kv->count; ++i) {
        if (kv->pairs[i].used && strcmp(kv->pairs[i].key, key) == 0)
            return kv->pairs[i].value;
    }
    return "";
}

static void kv_apply(void *user, const raft_command_t *command) {
    raft_kv_state_t *kv = (raft_kv_state_t *)user;
    size_t i;

    if (strcmp(command->op, "set") == 0) {
        for (i = 0; i < kv->count; ++i) {
            if (kv->pairs[i].used && strcmp(kv->pairs[i].key, command->key) == 0) {
                strncpy(kv->pairs[i].value, command->value, RAFT_MAX_VALUE - 1);
                return;
            }
        }
        if (kv->count < RAFT_KV_MAX_PAIRS) {
            strncpy(kv->pairs[kv->count].key,   command->key,   RAFT_MAX_KEY   - 1);
            strncpy(kv->pairs[kv->count].value,  command->value, RAFT_MAX_VALUE - 1);
            kv->pairs[kv->count].used = 1;
            kv->count++;
        }
    } else if (strcmp(command->op, "del") == 0) {
        for (i = 0; i < kv->count; ++i) {
            if (kv->pairs[i].used && strcmp(kv->pairs[i].key, command->key) == 0) {
                kv->pairs[i].used = 0;
                return;
            }
        }
    }
}

static void kv_reload(void *user) {
    (void)user;
}

static size_t kv_snapshot(void *user, void *buf, size_t buf_size) {
    raft_kv_state_t *kv = (raft_kv_state_t *)user;
    char *out = (char *)buf;
    size_t written = 0;
    size_t i;

    for (i = 0; i < kv->count; ++i) {
        int n;
        if (!kv->pairs[i].used) continue;
        if (written >= buf_size) break;
        n = snprintf(out + written, buf_size - written, "%s=%s\n",
                     kv->pairs[i].key, kv->pairs[i].value);
        if (n <= 0 || (size_t)n >= buf_size - written) break;
        written += (size_t)n;
    }
    return written;
}

static void kv_restore_snapshot(void *user, const void *buf, size_t size) {
    raft_kv_state_t *kv = (raft_kv_state_t *)user;
    const char *p   = (const char *)buf;
    const char *end = p + size;

    memset(kv, 0, sizeof(*kv));
    while (p < end && kv->count < RAFT_KV_MAX_PAIRS) {
        const char *eq = (const char *)memchr(p,   '=', (size_t)(end - p));
        const char *nl = (const char *)memchr(p,   '\n', (size_t)(end - p));
        size_t klen, vlen;

        if (eq == NULL || nl == NULL || eq >= nl) break;
        klen = (size_t)(eq - p);
        vlen = (size_t)(nl - eq - 1);
        if (klen == 0 || klen >= RAFT_MAX_KEY || vlen >= RAFT_MAX_VALUE) {
            p = nl + 1;
            continue;
        }
        memcpy(kv->pairs[kv->count].key,   p,      klen);
        memcpy(kv->pairs[kv->count].value,  eq + 1, vlen);
        kv->pairs[kv->count].used = 1;
        kv->count++;
        p = nl + 1;
    }
}

raft_application_t raft_kv_make_application(raft_kv_state_t *kv) {
    raft_application_t app;
    app.apply            = kv_apply;
    app.reload           = kv_reload;
    app.snapshot         = kv_snapshot;
    app.restore_snapshot = kv_restore_snapshot;
    app.user             = kv;
    return app;
}
