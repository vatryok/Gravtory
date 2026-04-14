# SPDX-License-Identifier: AGPL-3.0-or-later OR Commercial
# Copyright (C) 2026 Gravtory Contributors
#
# This file is part of Gravtory, licensed under AGPL-3.0-or-later.
# See LICENSE file in the project root for full license information.

"""Database schema definitions shared by PostgreSQL and SQLite backends.

This module holds the logical schema as format-string templates.
Each backend fills in dialect-specific tokens (e.g. SERIAL vs INTEGER PRIMARY KEY AUTOINCREMENT).
"""

from __future__ import annotations

CURRENT_SCHEMA_VERSION = 2

# ── Dialect tokens ────────────────────────────────────────────────

POSTGRES_TOKENS: dict[str, str] = {
    "auto_id": "SERIAL PRIMARY KEY",
    "timestamp": "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    "timestamp_nullable": "TIMESTAMPTZ",
    "binary": "BYTEA",
    "boolean": "BOOLEAN",
    "now": "NOW()",
    "true": "TRUE",
    "false": "FALSE",
}

SQLITE_TOKENS: dict[str, str] = {
    "auto_id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "timestamp": "TEXT NOT NULL DEFAULT (datetime('now'))",
    "timestamp_nullable": "TEXT",
    "binary": "BLOB",
    "boolean": "INTEGER",
    "now": "datetime('now')",
    "true": "1",
    "false": "0",
}


def schema_sql(tokens: dict[str, str], prefix: str = "gravtory_") -> list[str]:
    """Return the list of CREATE TABLE / CREATE INDEX statements for the given dialect."""
    t = tokens
    p = prefix
    stmts: list[str] = []

    # ── 1. workflow_runs ──────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}workflow_runs (
    id                  TEXT PRIMARY KEY,
    workflow_name       TEXT NOT NULL,
    workflow_version    INTEGER NOT NULL DEFAULT 1,
    namespace           TEXT NOT NULL DEFAULT 'default',
    status              TEXT NOT NULL DEFAULT 'pending',
    current_step        INTEGER,
    input_data          {t["binary"]},
    output_data         {t["binary"]},
    error_message       TEXT,
    error_traceback     TEXT,
    parent_run_id       TEXT REFERENCES {p}workflow_runs(id),
    created_at          {t["timestamp"]},
    updated_at          {t["timestamp"]},
    completed_at        {t["timestamp_nullable"]},
    deadline_at         {t["timestamp_nullable"]}
)""")
    stmts.append(f"CREATE INDEX IF NOT EXISTS idx_{p}wr_status ON {p}workflow_runs(status)")
    stmts.append(f"CREATE INDEX IF NOT EXISTS idx_{p}wr_name ON {p}workflow_runs(workflow_name)")
    stmts.append(f"CREATE INDEX IF NOT EXISTS idx_{p}wr_namespace ON {p}workflow_runs(namespace)")
    stmts.append(f"CREATE INDEX IF NOT EXISTS idx_{p}wr_created ON {p}workflow_runs(created_at)")
    stmts.append(f"CREATE INDEX IF NOT EXISTS idx_{p}wr_parent ON {p}workflow_runs(parent_run_id)")

    # ── 2. step_outputs ───────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}step_outputs (
    id                  {t["auto_id"]},
    workflow_run_id     TEXT NOT NULL REFERENCES {p}workflow_runs(id),
    step_order          INTEGER NOT NULL,
    step_name           TEXT NOT NULL,
    output_data         {t["binary"]},
    output_type         TEXT,
    duration_ms         INTEGER,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'completed',
    error_message       TEXT,
    created_at          {t["timestamp"]},
    UNIQUE(workflow_run_id, step_order)
)""")
    stmts.append(f"CREATE INDEX IF NOT EXISTS idx_{p}so_run ON {p}step_outputs(workflow_run_id)")

    # ── 3. parallel_results ───────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}parallel_results (
    id                  {t["auto_id"]},
    workflow_run_id     TEXT NOT NULL REFERENCES {p}workflow_runs(id),
    step_order          INTEGER NOT NULL,
    item_index          INTEGER NOT NULL,
    output_data         {t["binary"]} NOT NULL,
    created_at          {t["timestamp"]},
    UNIQUE(workflow_run_id, step_order, item_index)
)""")
    stmts.append(
        f"CREATE INDEX IF NOT EXISTS idx_{p}pr_run_step "
        f"ON {p}parallel_results(workflow_run_id, step_order)"
    )

    # ── 4. pending_steps ──────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}pending_steps (
    id                  {t["auto_id"]},
    workflow_run_id     TEXT NOT NULL REFERENCES {p}workflow_runs(id),
    step_order          INTEGER NOT NULL,
    priority            INTEGER NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'pending',
    worker_id           TEXT,
    scheduled_at        {t["timestamp"]},
    started_at          {t["timestamp_nullable"]},
    completed_at        {t["timestamp_nullable"]},
    retry_count         INTEGER NOT NULL DEFAULT 0,
    max_retries         INTEGER NOT NULL DEFAULT 0,
    next_retry_at       {t["timestamp_nullable"]},
    created_at          {t["timestamp"]}
)""")
    stmts.append(
        f"CREATE INDEX IF NOT EXISTS idx_{p}ps_status_sched "
        f"ON {p}pending_steps(status, scheduled_at)"
    )
    stmts.append(
        f"CREATE INDEX IF NOT EXISTS idx_{p}ps_priority "
        f"ON {p}pending_steps(priority DESC, created_at ASC)"
    )
    stmts.append(f"CREATE INDEX IF NOT EXISTS idx_{p}ps_worker ON {p}pending_steps(worker_id)")

    # ── 5. signals ────────────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}signals (
    id                  {t["auto_id"]},
    workflow_run_id     TEXT NOT NULL,
    signal_name         TEXT NOT NULL,
    signal_data         {t["binary"]},
    consumed            {t["boolean"]} NOT NULL DEFAULT {t["false"]},
    created_at          {t["timestamp"]}
)""")
    stmts.append(
        f"CREATE INDEX IF NOT EXISTS idx_{p}sig_run_name "
        f"ON {p}signals(workflow_run_id, signal_name)"
    )

    # ── 6. signal_waits ───────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}signal_waits (
    id                  {t["auto_id"]},
    workflow_run_id     TEXT NOT NULL,
    signal_name         TEXT NOT NULL,
    timeout_at          {t["timestamp_nullable"]},
    created_at          {t["timestamp"]}
)""")
    stmts.append(
        f"CREATE INDEX IF NOT EXISTS idx_{p}sw_run ON {p}signal_waits(workflow_run_id, signal_name)"
    )

    # ── 7. compensations ──────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}compensations (
    id                  {t["auto_id"]},
    workflow_run_id     TEXT NOT NULL REFERENCES {p}workflow_runs(id),
    step_order          INTEGER NOT NULL,
    handler_name        TEXT NOT NULL,
    step_output         {t["binary"]},
    status              TEXT NOT NULL DEFAULT 'pending',
    error_message       TEXT,
    created_at          {t["timestamp"]}
)""")
    stmts.append(f"CREATE INDEX IF NOT EXISTS idx_{p}comp_run ON {p}compensations(workflow_run_id)")

    # ── 8. schedules ──────────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}schedules (
    id                  TEXT PRIMARY KEY,
    workflow_name       TEXT NOT NULL,
    schedule_type       TEXT NOT NULL,
    schedule_config     TEXT NOT NULL,
    namespace           TEXT NOT NULL DEFAULT 'default',
    enabled             {t["boolean"]} NOT NULL DEFAULT {t["true"]},
    last_run_at         {t["timestamp_nullable"]},
    next_run_at         {t["timestamp_nullable"]},
    created_at          {t["timestamp"]}
)""")
    stmts.append(
        f"CREATE INDEX IF NOT EXISTS idx_{p}sched_next ON {p}schedules(enabled, next_run_at)"
    )

    # ── 9. locks ──────────────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}locks (
    lock_name           TEXT PRIMARY KEY,
    holder_id           TEXT NOT NULL,
    acquired_at         {t["timestamp"]},
    expires_at          {t["timestamp"]}
)""")

    # ── Support: DLQ ──────────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}dlq (
    id                  {t["auto_id"]},
    workflow_run_id     TEXT NOT NULL,
    step_order          INTEGER NOT NULL DEFAULT 0,
    error_message       TEXT,
    error_traceback     TEXT,
    step_input          {t["binary"]},
    retry_count         INTEGER NOT NULL DEFAULT 0,
    created_at          {t["timestamp"]}
)""")

    # ── Support: workers ──────────────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}workers (
    worker_id           TEXT PRIMARY KEY,
    node_id             TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'active',
    last_heartbeat      {t["timestamp"]},
    current_task        TEXT,
    started_at          {t["timestamp"]}
)""")

    # ── Support: dynamic workflow definitions ───────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}workflow_definitions (
    name                TEXT NOT NULL,
    version             INTEGER NOT NULL,
    definition_json     TEXT NOT NULL,
    created_at          {t["timestamp"]},
    PRIMARY KEY (name, version)
)""")

    # ── Support: circuit breakers ────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}circuit_breakers (
    name                TEXT PRIMARY KEY,
    state_json          TEXT NOT NULL,
    updated_at          {t["timestamp"]}
)""")

    # ── Support: schema_version ───────────────────────────────────
    stmts.append(f"""
CREATE TABLE IF NOT EXISTS {p}schema_version (
    version             INTEGER NOT NULL,
    applied_at          {t["timestamp"]}
)""")

    return stmts
