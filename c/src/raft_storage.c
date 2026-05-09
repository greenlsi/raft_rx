// Copyright 2026 Jose M. Moya <jm.moya@upm.es>
// SPDX-License-Identifier: GPL-3.0-only

#include "raft_storage_internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static void mkdir_if_needed(const char *path) {
    char tmp[600];
    size_t i, len;
    memset(tmp, 0, sizeof(tmp));
    strncpy(tmp, path, sizeof(tmp) - 1);
    len = strlen(tmp);
    for (i = 1; i < len; ++i) {
        if (tmp[i] == '/') {
            tmp[i] = '\0';
            if (mkdir(tmp, 0777) != 0 && errno != EEXIST) {
                perror("mkdir");
                exit(1);
            }
            tmp[i] = '/';
        }
    }
    if (mkdir(tmp, 0777) != 0 && errno != EEXIST) {
        perror("mkdir");
        exit(1);
    }
}

void raft_file_storage_init(raft_file_storage_t *storage, const char *root_dir, const char *node_id) {
    memset(storage, 0, sizeof(*storage));
    strncpy(storage->root_dir, root_dir, sizeof(storage->root_dir) - 1);
    strncpy(storage->node_id, node_id, sizeof(storage->node_id) - 1);
}

static void storage_node_dir(const raft_file_storage_t *storage, char *out, size_t out_size) {
    snprintf(out, out_size, "%s/%s", storage->root_dir, storage->node_id);
}

static void storage_meta_path(const raft_file_storage_t *storage, char *out, size_t out_size) {
    char node_dir[512];
    storage_node_dir(storage, node_dir, sizeof(node_dir));
    snprintf(out, out_size, "%s/meta.txt", node_dir);
}

static void storage_log_path(const raft_file_storage_t *storage, char *out, size_t out_size) {
    char node_dir[512];
    storage_node_dir(storage, node_dir, sizeof(node_dir));
    snprintf(out, out_size, "%s/log.txt", node_dir);
}

static void storage_snapshot_path(const raft_file_storage_t *storage, char *out, size_t out_size) {
    char node_dir[512];
    storage_node_dir(storage, node_dir, sizeof(node_dir));
    snprintf(out, out_size, "%s/snapshot.bin", node_dir);
}

static void storage_ensure_dirs(const raft_file_storage_t *storage) {
    char node_dir[512];
    mkdir_if_needed(storage->root_dir);
    storage_node_dir(storage, node_dir, sizeof(node_dir));
    mkdir_if_needed(node_dir);
}

static void atomic_replace_path(const char *path, const char *mode,
                                void (*writer)(FILE *, void *), void *arg) {
    char tmp[600];
    FILE *fp;
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    fp = fopen(tmp, mode);
    if (fp == NULL) {
        perror("fopen");
        exit(1);
    }
    writer(fp, arg);
    fclose(fp);
    if (rename(tmp, path) != 0) {
        perror("rename");
        exit(1);
    }
}

typedef struct {
    int current_term;
    int commit_index;
    int snapshot_last_included_index;
    int snapshot_last_included_term;
    const char *voted_for;
    int compaction_threshold;
    const raft_cluster_configuration_t *config_state;
} meta_writer_arg_t;

static void write_meta(FILE *fp, void *arg) {
    size_t i;
    meta_writer_arg_t *meta = (meta_writer_arg_t *)arg;
    fprintf(fp, "current_term %d\n", meta->current_term);
    fprintf(fp, "voted_for %s\n", meta->voted_for[0] ? meta->voted_for : "-");
    fprintf(fp, "compaction_threshold %d\n", meta->compaction_threshold);
    fprintf(fp, "commit_index %d\n", meta->commit_index);
    fprintf(fp, "snapshot_last_included_index %d\n", meta->snapshot_last_included_index);
    fprintf(fp, "snapshot_last_included_term %d\n", meta->snapshot_last_included_term);
    fprintf(fp, "configuration_index %d\n", meta->config_state->index);
    fprintf(fp, "old_count %zu\n", meta->config_state->old_count);
    for (i = 0; i < meta->config_state->old_count; ++i) {
        fprintf(fp, "old_member %s\n", meta->config_state->old_members[i]);
    }
    fprintf(fp, "new_count %zu\n", meta->config_state->new_count);
    for (i = 0; i < meta->config_state->new_count; ++i) {
        fprintf(fp, "new_member %s\n", meta->config_state->new_members[i]);
    }
}

typedef struct {
    raft_node_t *node;
} log_writer_arg_t;

static void write_log(FILE *fp, void *arg) {
    size_t i;
    raft_node_t *node = ((log_writer_arg_t *)arg)->node;
    for (i = 0; i < node->log_count; ++i) {
        raft_log_entry_t *entry = &node->log[i];
        fprintf(fp, "%d|%d|%s|%s|%s|%s\n",
                entry->index, entry->term, entry->leader_id,
                entry->command.op, entry->command.key, entry->command.value);
    }
}

typedef struct {
    const uint8_t *data;
    size_t size;
    int32_t index;
    int32_t term;
} snapshot_writer_arg_t;

static void write_snapshot(FILE *fp, void *arg) {
    snapshot_writer_arg_t *s = (snapshot_writer_arg_t *)arg;
    int32_t size32 = (int32_t)s->size;
    fwrite(&s->index, sizeof(int32_t), 1, fp);
    fwrite(&s->term,  sizeof(int32_t), 1, fp);
    fwrite(&size32,   sizeof(int32_t), 1, fp);
    if (s->size > 0) {
        fwrite(s->data, 1, s->size, fp);
    }
}

void raft_storage_node_save(raft_node_t *node) {
    char meta_path[600];
    char log_path[600];
    meta_writer_arg_t meta;
    log_writer_arg_t log_arg;

    storage_ensure_dirs(&node->storage);
    storage_meta_path(&node->storage, meta_path, sizeof(meta_path));
    storage_log_path(&node->storage, log_path, sizeof(log_path));

    meta.current_term                 = node->current_term;
    meta.voted_for                    = node->voted_for;
    meta.compaction_threshold         = node->compaction_threshold;
    meta.commit_index                 = node->commit_index;
    meta.snapshot_last_included_index = node->snapshot_last_included_index;
    meta.snapshot_last_included_term  = node->snapshot_last_included_term;
    meta.config_state                 = &node->config_state;
    log_arg.node = node;

    atomic_replace_path(meta_path, "w", write_meta, &meta);
    atomic_replace_path(log_path,  "w", write_log,  &log_arg);
}

void raft_storage_node_load(raft_node_t *node) {
    char meta_path[600];
    char log_path[600];
    char snapshot_path[600];
    FILE *fp;
    char line[512];
    int snap_loaded = 0;

    storage_ensure_dirs(&node->storage);
    storage_meta_path(&node->storage, meta_path, sizeof(meta_path));
    storage_log_path(&node->storage, log_path, sizeof(log_path));
    storage_snapshot_path(&node->storage, snapshot_path, sizeof(snapshot_path));

    node->current_term = 0;
    node->voted_for[0] = '\0';
    memset(node->log, 0, sizeof(node->log));
    node->log_count = 0;
    node->snapshot_last_included_index = 0;
    node->snapshot_last_included_term = 0;
    node->commit_index = 0;
    node->last_applied = 0;
    node->storage.snapshot_buf_size = 0;
    memset(node->storage.snapshot_buf, 0, sizeof(node->storage.snapshot_buf));
    node->compaction_threshold = RAFT_MAX_LOG;
    memset(&node->config_state, 0, sizeof(node->config_state));

    fp = fopen(meta_path, "r");
    if (fp != NULL) {
        while (fgets(line, sizeof(line), fp) != NULL) {
            if (strncmp(line, "current_term ", 13) == 0) {
                node->current_term = atoi(line + 13);
            } else if (strncmp(line, "voted_for ", 10) == 0) {
                sscanf(line + 10, "%15s", node->voted_for);
                if (strcmp(node->voted_for, "-") == 0) node->voted_for[0] = '\0';
            } else if (strncmp(line, "compaction_threshold ", 21) == 0) {
                node->compaction_threshold = atoi(line + 21);
            } else if (strncmp(line, "commit_index ", 13) == 0) {
                node->commit_index = atoi(line + 13);
            } else if (strncmp(line, "snapshot_last_included_index ", 29) == 0) {
                node->snapshot_last_included_index = atoi(line + 29);
            } else if (strncmp(line, "snapshot_last_included_term ", 28) == 0) {
                node->snapshot_last_included_term = atoi(line + 28);
            } else if (strncmp(line, "configuration_index ", 20) == 0) {
                node->config_state.index = atoi(line + 20);
            } else if (strncmp(line, "old_count ", 10) == 0) {
                node->config_state.old_count = (size_t)atoi(line + 10);
                if (node->config_state.old_count > RAFT_MAX_NODES)
                    node->config_state.old_count = RAFT_MAX_NODES;
            } else if (strncmp(line, "old_member ", 11) == 0) {
                size_t idx = 0;
                while (idx < node->config_state.old_count &&
                       node->config_state.old_members[idx][0] != '\0') idx++;
                if (idx < RAFT_MAX_NODES)
                    sscanf(line + 11, "%15s", node->config_state.old_members[idx]);
            } else if (strncmp(line, "new_count ", 10) == 0) {
                node->config_state.new_count = (size_t)atoi(line + 10);
                if (node->config_state.new_count > RAFT_MAX_NODES)
                    node->config_state.new_count = RAFT_MAX_NODES;
            } else if (strncmp(line, "new_member ", 11) == 0) {
                size_t idx = 0;
                while (idx < node->config_state.new_count &&
                       node->config_state.new_members[idx][0] != '\0') idx++;
                if (idx < RAFT_MAX_NODES)
                    sscanf(line + 11, "%15s", node->config_state.new_members[idx]);
            }
        }
        fclose(fp);
    }

    /* Try to restore from snapshot */
    fp = fopen(snapshot_path, "rb");
    if (fp != NULL) {
        int32_t snap_index, snap_term, snap_size;
        if (fread(&snap_index, sizeof(int32_t), 1, fp) == 1 &&
            fread(&snap_term,  sizeof(int32_t), 1, fp) == 1 &&
            fread(&snap_size,  sizeof(int32_t), 1, fp) == 1 &&
            snap_index == (int32_t)node->snapshot_last_included_index &&
            snap_term  == (int32_t)node->snapshot_last_included_term) {
            if (snap_size > 0 && (size_t)snap_size <= RAFT_MAX_SNAPSHOT_SIZE) {
                node->storage.snapshot_buf_size =
                    fread(node->storage.snapshot_buf, 1, (size_t)snap_size, fp);
            } else {
                node->storage.snapshot_buf_size = 0;
            }
            snap_loaded = 1;
        }
        fclose(fp);
    }

    /* Load log entries that come after the snapshot */
    fp = fopen(log_path, "r");
    if (fp != NULL) {
        while (fgets(line, sizeof(line), fp) != NULL && node->log_count < RAFT_MAX_LOG) {
            raft_log_entry_t entry;
            memset(&entry, 0, sizeof(entry));
            /* New 6-field format: index|term|leader_id|op|key|value */
            if (sscanf(line, "%d|%d|%15[^|]|%31[^|]|%63[^|]|%127[^\n]",
                       &entry.index, &entry.term, entry.leader_id,
                       entry.command.op, entry.command.key, entry.command.value) != 6) {
                /* Backward-compat: old 5-field format without leader_id */
                if (sscanf(line, "%d|%d|%31[^|]|%63[^|]|%127[^\n]",
                           &entry.index, &entry.term, entry.command.op,
                           entry.command.key, entry.command.value) != 5) continue;
            }
            if (entry.index <= node->snapshot_last_included_index) continue;
            node->log[node->log_count++] = entry;
        }
        fclose(fp);
    }

    /* Clamp commit_index to what is actually present in the log + snapshot.
     * A mismatch means the log file was truncated or lost; accepting a higher
     * commit_index would cause the node to apply zeroed garbage entries and
     * corrupt vote-log comparisons (last_log_term / last_log_index). */
    {
        int actual_last = node->snapshot_last_included_index + (int)node->log_count;
        if (node->commit_index > actual_last)
            node->commit_index = actual_last;
    }

    /* Restore application state */
    if (snap_loaded) {
        if (node->application.restore_snapshot)
            node->application.restore_snapshot(node->application.user,
                                               node->storage.snapshot_buf,
                                               node->storage.snapshot_buf_size);
        node->last_applied = node->snapshot_last_included_index;
    } else {
        if (node->application.reload)
            node->application.reload(node->application.user);
        node->last_applied = 0;
    }
}

void raft_storage_save_snapshot(raft_node_t *node, int index, int term) {
    char snapshot_path[600];
    snapshot_writer_arg_t arg;

    storage_ensure_dirs(&node->storage);
    storage_snapshot_path(&node->storage, snapshot_path, sizeof(snapshot_path));

    arg.data  = node->storage.snapshot_buf;
    arg.size  = node->storage.snapshot_buf_size;
    arg.index = (int32_t)index;
    arg.term  = (int32_t)term;

    atomic_replace_path(snapshot_path, "wb", write_snapshot, &arg);
}
