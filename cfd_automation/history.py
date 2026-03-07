from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any

from .utils import ensure_dir


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _created_at_from_run_id(run_id: str) -> str:
    text = str(run_id).strip()
    if not text:
        return _utc_now_iso()
    try:
        parsed = datetime.strptime(text, "%Y%m%d_%H%M%S")
    except ValueError:
        return _utc_now_iso()
    return parsed.replace(tzinfo=timezone.utc).isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_json_dumps(value: Any, fallback: Any) -> str:
    payload = value if value is not None else fallback
    return json.dumps(payload)


def _safe_json_loads(text: str, fallback: Any) -> Any:
    if not text:
        return fallback
    try:
        return json.loads(text)
    except Exception:
        return fallback


class HistoryStore:
    """SQLite-backed persistent run/case history."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        ensure_dir(self.db_path.parent)
        self._lock = Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("PRAGMA journal_mode = WAL")
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS runs (
                        run_id TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        mode TEXT NOT NULL DEFAULT '',
                        run_dir TEXT NOT NULL DEFAULT '',
                        study_path TEXT NOT NULL DEFAULT '',
                        design_name TEXT NOT NULL DEFAULT '',
                        scenario_name TEXT NOT NULL DEFAULT '',
                        total_cases INTEGER NOT NULL DEFAULT 0,
                        selected_case_count INTEGER NOT NULL DEFAULT 0,
                        successful_cases INTEGER NOT NULL DEFAULT 0,
                        failed_cases INTEGER NOT NULL DEFAULT 0,
                        config_json TEXT NOT NULL DEFAULT '{}',
                        results_json TEXT NOT NULL DEFAULT '{}',
                        postprocess_json TEXT NOT NULL DEFAULT '{}',
                        summary_json TEXT NOT NULL DEFAULT '{}'
                    );

                    CREATE TABLE IF NOT EXISTS cases (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        run_id TEXT NOT NULL,
                        case_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        success INTEGER NOT NULL DEFAULT 0,
                        attempts INTEGER NOT NULL DEFAULT 1,
                        failure_type TEXT NOT NULL DEFAULT '',
                        failure_mode TEXT NOT NULL DEFAULT '',
                        failure_reason TEXT NOT NULL DEFAULT '',
                        metrics_json TEXT NOT NULL DEFAULT '{}',
                        params_json TEXT NOT NULL DEFAULT '{}',
                        physics_signature TEXT NOT NULL DEFAULT '',
                        screenshots_json TEXT NOT NULL DEFAULT '[]',
                        summary_csv TEXT NOT NULL DEFAULT '',
                        metrics_csv TEXT NOT NULL DEFAULT '',
                        UNIQUE(run_id, case_id),
                        FOREIGN KEY(run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                    );

                    CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_runs_study_path ON runs(study_path);
                    CREATE INDEX IF NOT EXISTS idx_cases_run_id ON cases(run_id);
                    CREATE INDEX IF NOT EXISTS idx_cases_case_id ON cases(case_id);
                    CREATE INDEX IF NOT EXISTS idx_cases_success ON cases(success);
                    """
                )

    def ingest_run(
        self,
        *,
        summary: dict[str, Any],
        config: dict[str, Any],
        cases: list[dict[str, Any]],
    ) -> bool:
        run_id = str(summary.get("run_id", "")).strip()
        if not run_id:
            return False

        study_cfg = config.get("study", {}) if isinstance(config.get("study", {}), dict) else {}
        created_at = str(summary.get("created_at", "")).strip() or _created_at_from_run_id(run_id)
        study_path = str(summary.get("study_path", "")).strip() or str(study_cfg.get("template_model", "")).strip()
        design_name = str(summary.get("design_name", "")).strip() or str(study_cfg.get("design_name", "")).strip()
        scenario_name = str(summary.get("scenario_name", "")).strip() or str(study_cfg.get("scenario_name", "")).strip()

        case_lookup: dict[str, dict[str, Any]] = {}
        for case in cases:
            if not isinstance(case, dict):
                continue
            case_id = str(case.get("case_id", "")).strip()
            if case_id:
                case_lookup[case_id] = case

        case_results_in = summary.get("case_results", []) if isinstance(summary.get("case_results", []), list) else []
        case_results: list[dict[str, Any]] = []
        for item in case_results_in:
            if not isinstance(item, dict):
                continue
            case_id = str(item.get("case_id", "")).strip()
            if not case_id:
                continue
            case_result = dict(item)
            if not isinstance(case_result.get("inputs"), dict):
                case_result["inputs"] = dict(case_lookup.get(case_id, {}))
            case_results.append(case_result)

        summary_to_store = dict(summary)
        summary_to_store["created_at"] = created_at
        summary_to_store["study_path"] = study_path
        summary_to_store["design_name"] = design_name
        summary_to_store["scenario_name"] = scenario_name
        summary_to_store["case_results"] = case_results

        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO runs (
                        run_id, created_at, mode, run_dir, study_path, design_name, scenario_name,
                        total_cases, selected_case_count, successful_cases, failed_cases,
                        config_json, results_json, postprocess_json, summary_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id) DO UPDATE SET
                        created_at=excluded.created_at,
                        mode=excluded.mode,
                        run_dir=excluded.run_dir,
                        study_path=excluded.study_path,
                        design_name=excluded.design_name,
                        scenario_name=excluded.scenario_name,
                        total_cases=excluded.total_cases,
                        selected_case_count=excluded.selected_case_count,
                        successful_cases=excluded.successful_cases,
                        failed_cases=excluded.failed_cases,
                        config_json=excluded.config_json,
                        results_json=excluded.results_json,
                        postprocess_json=excluded.postprocess_json,
                        summary_json=excluded.summary_json
                    """,
                    (
                        run_id,
                        created_at,
                        str(summary_to_store.get("mode", "")).strip(),
                        str(summary_to_store.get("run_dir", "")).strip(),
                        study_path,
                        design_name,
                        scenario_name,
                        _safe_int(summary_to_store.get("total_cases"), 0),
                        _safe_int(summary_to_store.get("selected_case_count"), 0),
                        _safe_int(summary_to_store.get("successful_cases"), 0),
                        _safe_int(summary_to_store.get("failed_cases"), 0),
                        _safe_json_dumps(config, {}),
                        _safe_json_dumps(summary_to_store.get("results", {}), {}),
                        _safe_json_dumps(summary_to_store.get("postprocess", {}), {}),
                        _safe_json_dumps(summary_to_store, {}),
                    ),
                )

                conn.execute("DELETE FROM cases WHERE run_id = ?", (run_id,))

                for case_result in case_results:
                    case_id = str(case_result.get("case_id", "")).strip()
                    if not case_id:
                        continue
                    failure_reason = str(
                        case_result.get("failure_reason", case_result.get("error", ""))
                    ).strip()
                    conn.execute(
                        """
                        INSERT INTO cases (
                            run_id, case_id, created_at, success, attempts, failure_type, failure_mode,
                            failure_reason, metrics_json, params_json, physics_signature, screenshots_json,
                            summary_csv, metrics_csv
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run_id,
                            case_id,
                            created_at,
                            1 if bool(case_result.get("success")) else 0,
                            _safe_int(case_result.get("attempts", case_result.get("attempt", 1)), 1),
                            str(case_result.get("failure_type", "")).strip(),
                            str(case_result.get("failure_mode", "")).strip(),
                            failure_reason,
                            _safe_json_dumps(case_result.get("metrics", {}), {}),
                            _safe_json_dumps(case_result.get("inputs", {}), {}),
                            str(case_result.get("physics_signature", "")).strip(),
                            _safe_json_dumps(case_result.get("screenshots", []), []),
                            str(case_result.get("summary_csv", "")).strip(),
                            str(case_result.get("metrics_csv", "")).strip(),
                        ),
                    )
        return True

    def list_runs(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        study_path: str = "",
        mode: str = "",
        case_id: str = "",
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 1000))
        offset = max(0, int(offset))

        where: list[str] = []
        args: list[Any] = []

        if study_path.strip():
            where.append("lower(r.study_path) LIKE ?")
            args.append(f"%{study_path.strip().lower()}%")
        if mode.strip():
            where.append("lower(r.mode) = ?")
            args.append(mode.strip().lower())
        if case_id.strip():
            where.append(
                "EXISTS (SELECT 1 FROM cases c WHERE c.run_id = r.run_id AND lower(c.case_id) LIKE ?)"
            )
            args.append(f"%{case_id.strip().lower()}%")

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        with self._lock:
            with self._connect() as conn:
                total_row = conn.execute(
                    f"SELECT COUNT(*) AS total FROM runs r {where_sql}",
                    tuple(args),
                ).fetchone()
                total = int(total_row["total"]) if total_row else 0

                rows = conn.execute(
                    f"""
                    SELECT
                        r.run_id,
                        r.created_at,
                        r.mode,
                        r.study_path,
                        r.design_name,
                        r.scenario_name,
                        r.total_cases,
                        r.selected_case_count,
                        r.successful_cases,
                        r.failed_cases
                    FROM runs r
                    {where_sql}
                    ORDER BY r.created_at DESC, r.run_id DESC
                    LIMIT ? OFFSET ?
                    """,
                    tuple(args + [limit, offset]),
                ).fetchall()

        runs = [dict(row) for row in rows]
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "runs": runs,
        }

    def get_run(self, run_id: str) -> dict[str, Any]:
        run_key = str(run_id).strip()
        if not run_key:
            return {}
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT summary_json FROM runs WHERE run_id = ?",
                    (run_key,),
                ).fetchone()
        if not row:
            return {}
        summary = _safe_json_loads(str(row["summary_json"]), {})
        if isinstance(summary, dict):
            return summary
        return {}

    def list_cases(
        self,
        *,
        limit: int = 200,
        offset: int = 0,
        study_path: str = "",
        case_id: str = "",
        success: bool | None = None,
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 2000))
        offset = max(0, int(offset))

        where: list[str] = []
        args: list[Any] = []
        if study_path.strip():
            where.append("lower(r.study_path) LIKE ?")
            args.append(f"%{study_path.strip().lower()}%")
        if case_id.strip():
            where.append("lower(c.case_id) LIKE ?")
            args.append(f"%{case_id.strip().lower()}%")
        if success is not None:
            where.append("c.success = ?")
            args.append(1 if success else 0)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        with self._lock:
            with self._connect() as conn:
                total_row = conn.execute(
                    f"""
                    SELECT COUNT(*) AS total
                    FROM cases c
                    JOIN runs r ON r.run_id = c.run_id
                    {where_sql}
                    """,
                    tuple(args),
                ).fetchone()
                total = int(total_row["total"]) if total_row else 0

                rows = conn.execute(
                    f"""
                    SELECT
                        c.run_id,
                        r.created_at,
                        r.study_path,
                        r.design_name,
                        r.scenario_name,
                        c.case_id,
                        c.success,
                        c.attempts,
                        c.failure_type,
                        c.failure_mode,
                        c.failure_reason,
                        c.metrics_json,
                        c.params_json,
                        c.physics_signature,
                        c.screenshots_json,
                        c.summary_csv,
                        c.metrics_csv
                    FROM cases c
                    JOIN runs r ON r.run_id = c.run_id
                    {where_sql}
                    ORDER BY r.created_at DESC, c.case_id ASC
                    LIMIT ? OFFSET ?
                    """,
                    tuple(args + [limit, offset]),
                ).fetchall()

        out_cases = []
        for row in rows:
            metrics = _safe_json_loads(str(row["metrics_json"]), {})
            params = _safe_json_loads(str(row["params_json"]), {})
            screenshots = _safe_json_loads(str(row["screenshots_json"]), [])
            out_cases.append(
                {
                    "run_id": row["run_id"],
                    "created_at": row["created_at"],
                    "study_path": row["study_path"],
                    "design_name": row["design_name"],
                    "scenario_name": row["scenario_name"],
                    "case_id": row["case_id"],
                    "success": bool(row["success"]),
                    "attempts": int(row["attempts"]),
                    "failure_type": row["failure_type"],
                    "failure_mode": row["failure_mode"],
                    "failure_reason": row["failure_reason"],
                    "metrics": metrics if isinstance(metrics, dict) else {},
                    "inputs": params if isinstance(params, dict) else {},
                    "physics_signature": row["physics_signature"],
                    "screenshots": screenshots if isinstance(screenshots, list) else [],
                    "summary_csv": row["summary_csv"],
                    "metrics_csv": row["metrics_csv"],
                }
            )
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "cases": out_cases,
        }
