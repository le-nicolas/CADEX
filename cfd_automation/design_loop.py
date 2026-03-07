from __future__ import annotations

from dataclasses import dataclass
import os
import random
from pathlib import Path
from typing import Any, Callable

from .config_io import cases_to_csv
from .llm_cases import LLMOptimizerNarrator
from .runner import AutomationRunner
from .utils import ensure_dir, now_utc_stamp, to_float, write_json


ProgressFn = Callable[[dict[str, Any]], None]
StopFn = Callable[[], bool]


@dataclass
class SpaceDimension:
    name: str
    kind: str
    low: float | None = None
    high: float | None = None
    choices: list[Any] | None = None


def _emit(callback: ProgressFn | None, **event: Any) -> None:
    if callback:
        callback(event)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _operator_holds(value: float | None, operator: str, threshold: float) -> bool:
    if value is None:
        return False
    if operator == "<":
        return value < threshold
    if operator == "<=":
        return value <= threshold
    if operator == ">":
        return value > threshold
    if operator == ">=":
        return value >= threshold
    if operator == "==":
        return value == threshold
    if operator == "!=":
        return value != threshold
    return False


def skopt_runtime_status() -> dict[str, Any]:
    try:
        import skopt  # type: ignore  # noqa: F401
    except Exception as ex:
        return {
            "available": False,
            "mode": "random_fallback",
            "warning": (
                f"scikit-optimize is not available ({ex.__class__.__name__}: {ex}). "
                "Design loop is using random sampling fallback."
            ),
        }
    return {"available": True, "mode": "bayesian_gp", "warning": ""}


class BayesianCaseOptimizer:
    def __init__(self, search_space: list[dict[str, Any]], seed: int = 42):
        if not search_space:
            raise ValueError("search_space is required.")
        self._random = random.Random(seed)
        self._dims = self._parse_search_space(search_space)
        self._names = [dim.name for dim in self._dims]
        self._skopt_optimizer, self._optimizer_warning = self._build_skopt_optimizer(self._dims, seed)

    def mode(self) -> str:
        return "bayesian_gp" if self._skopt_optimizer is not None else "random_fallback"

    def warning(self) -> str:
        return self._optimizer_warning

    @staticmethod
    def _parse_search_space(search_space: list[dict[str, Any]]) -> list[SpaceDimension]:
        dims: list[SpaceDimension] = []
        for raw in search_space:
            if not isinstance(raw, dict):
                raise ValueError("Each search_space item must be an object.")
            name = str(raw.get("name", "")).strip()
            kind = str(raw.get("type", "real")).strip().lower()
            if not name:
                raise ValueError("Each search_space item must include name.")
            if kind in {"real", "float"}:
                low = to_float(raw.get("min"))
                high = to_float(raw.get("max"))
                if low is None or high is None or high <= low:
                    raise ValueError(f"Invalid real range for {name}.")
                dims.append(SpaceDimension(name=name, kind="real", low=low, high=high))
                continue
            if kind in {"int", "integer"}:
                low = to_float(raw.get("min"))
                high = to_float(raw.get("max"))
                if low is None or high is None or high < low:
                    raise ValueError(f"Invalid integer range for {name}.")
                dims.append(
                    SpaceDimension(
                        name=name,
                        kind="int",
                        low=int(round(low)),
                        high=int(round(high)),
                    )
                )
                continue
            if kind in {"categorical", "category"}:
                choices = raw.get("choices", [])
                if not isinstance(choices, list) or not choices:
                    raise ValueError(f"Categorical dimension {name} needs non-empty choices.")
                dims.append(SpaceDimension(name=name, kind="categorical", choices=list(choices)))
                continue
            raise ValueError(f"Unsupported dimension type for {name}: {kind}")
        return dims

    @staticmethod
    def _build_skopt_optimizer(dims: list[SpaceDimension], seed: int):
        try:
            from skopt import Optimizer
            from skopt.space import Categorical, Integer, Real
        except Exception as ex:
            return None, (
                f"scikit-optimize is not available ({ex.__class__.__name__}: {ex}). "
                "Design loop is using random sampling fallback."
            )

        sk_dims = []
        for dim in dims:
            if dim.kind == "real":
                sk_dims.append(Real(float(dim.low), float(dim.high), name=dim.name))
            elif dim.kind == "int":
                sk_dims.append(Integer(int(dim.low), int(dim.high), name=dim.name))
            else:
                sk_dims.append(Categorical(dim.choices or [], name=dim.name))
        try:
            optimizer = Optimizer(
                dimensions=sk_dims,
                base_estimator="GP",
                acq_func="EI",
                random_state=seed,
            )
            return optimizer, ""
        except Exception as ex:
            return None, (
                f"scikit-optimize Optimizer initialization failed "
                f"({ex.__class__.__name__}: {ex}). Design loop is using random sampling fallback."
            )

    def _random_point(self) -> list[Any]:
        values = []
        for dim in self._dims:
            if dim.kind == "real":
                values.append(self._random.uniform(float(dim.low), float(dim.high)))
            elif dim.kind == "int":
                values.append(self._random.randint(int(dim.low), int(dim.high)))
            else:
                choices = dim.choices or []
                values.append(choices[self._random.randrange(len(choices))])
        return values

    @staticmethod
    def _normalize_scalar(value: Any) -> Any:
        if isinstance(value, float):
            return round(float(value), 6)
        return value

    def ask_rows(self, *, batch_index: int, batch_size: int, fixed_values: dict[str, Any]) -> list[dict[str, Any]]:
        n = max(1, int(batch_size))
        if self._skopt_optimizer is not None:
            points = self._skopt_optimizer.ask(n_points=n)
        else:
            points = [self._random_point() for _ in range(n)]

        rows: list[dict[str, Any]] = []
        for idx, point in enumerate(points, start=1):
            row: dict[str, Any] = {"case_id": f"LOOP_B{batch_index:02d}_C{idx:03d}"}
            for name, value in zip(self._names, point):
                row[name] = self._normalize_scalar(value)
            for key, value in fixed_values.items():
                row[str(key)] = value
            rows.append(row)
        return rows

    def tell(self, rows: list[dict[str, Any]], scores: list[float]) -> None:
        if self._skopt_optimizer is None:
            return
        if not rows or not scores:
            return
        if len(rows) != len(scores):
            return
        points = []
        for row in rows:
            point = [row.get(name) for name in self._names]
            points.append(point)
        self._skopt_optimizer.tell(points, scores)


class GenerativeDesignLoop:
    def __init__(self, runner: AutomationRunner):
        self.runner = runner
        self.project_root = runner.project_root

    @staticmethod
    def _default_objective(config: dict[str, Any]) -> tuple[str, str]:
        ranking = config.get("ranking", [])
        if isinstance(ranking, list) and ranking:
            first = ranking[0] if isinstance(ranking[0], dict) else {}
            alias = str(first.get("alias", "")).strip()
            goal = str(first.get("goal", "min")).strip().lower()
            if alias:
                return alias, ("max" if goal == "max" else "min")
        metrics = config.get("metrics", [])
        if isinstance(metrics, list) and metrics:
            first = metrics[0] if isinstance(metrics[0], dict) else {}
            alias = str(first.get("alias", "")).strip()
            if alias:
                return alias, "min"
        return "", "min"

    @staticmethod
    def _default_constraints(config: dict[str, Any]) -> list[dict[str, Any]]:
        raw = config.get("criteria", [])
        if not isinstance(raw, list):
            return []
        out: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            alias = str(item.get("alias", "")).strip()
            operator = str(item.get("operator", "<=")).strip()
            threshold = to_float(item.get("threshold"))
            if alias and threshold is not None:
                out.append({"alias": alias, "operator": operator, "threshold": threshold})
        return out

    @staticmethod
    def _merge_constraints(payload_constraints: Any, default_constraints: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if payload_constraints in (None, "", []):
            return default_constraints
        if not isinstance(payload_constraints, list):
            raise ValueError("constraints must be a list.")
        out: list[dict[str, Any]] = []
        for item in payload_constraints:
            if not isinstance(item, dict):
                raise ValueError("Each constraint must be an object.")
            alias = str(item.get("alias", "")).strip()
            operator = str(item.get("operator", "<=")).strip()
            threshold = to_float(item.get("threshold"))
            if not alias or threshold is None:
                raise ValueError("Constraint requires alias and threshold.")
            out.append({"alias": alias, "operator": operator, "threshold": threshold})
        return out

    @staticmethod
    def _fallback_batch_explanation(batch_records: list[dict[str, Any]], objective_alias: str) -> str:
        feasible = [item for item in batch_records if item.get("constraints_pass")]
        if not batch_records:
            return "No cases were evaluated in this batch."
        if feasible:
            best = min(feasible, key=lambda item: float(item.get("score", 1e18)))
            return (
                f"Feasible region found in this batch. Best feasible case is {best.get('case_id')} "
                f"with {objective_alias}={best.get('objective_value')} and score={best.get('score')}."
            )
        best = min(batch_records, key=lambda item: float(item.get("score", 1e18)))
        return (
            f"No feasible case in this batch. Best penalized case is {best.get('case_id')} "
            f"with score={best.get('score')}."
        )

    @staticmethod
    def _evaluate_case(
        *,
        case_result: dict[str, Any] | None,
        objective_alias: str,
        objective_goal: str,
        constraints: list[dict[str, Any]],
        penalty_missing_objective: float,
        penalty_constraint: float,
    ) -> dict[str, Any]:
        if not case_result or not case_result.get("success"):
            return {
                "objective_value": None,
                "constraints_pass": False,
                "constraint_violations": ["case_failed"],
                "score": float(penalty_missing_objective),
            }

        metrics = case_result.get("metrics", {}) if isinstance(case_result.get("metrics", {}), dict) else {}
        objective_value = to_float(metrics.get(objective_alias))
        if objective_value is None:
            return {
                "objective_value": None,
                "constraints_pass": False,
                "constraint_violations": ["objective_missing"],
                "score": float(penalty_missing_objective),
            }

        base_score = float(objective_value if objective_goal == "min" else -objective_value)
        violations: list[str] = []
        penalty = 0.0
        for constraint in constraints:
            alias = str(constraint.get("alias", "")).strip()
            operator = str(constraint.get("operator", "<=")).strip()
            threshold = float(constraint.get("threshold"))
            value = to_float(metrics.get(alias))
            if _operator_holds(value, operator, threshold):
                continue
            violations.append(f"{alias} {operator} {threshold} violated (value={value})")
            penalty += penalty_constraint

        return {
            "objective_value": objective_value,
            "constraints_pass": len(violations) == 0,
            "constraint_violations": violations,
            "score": base_score + penalty,
        }

    def run(
        self,
        *,
        payload: dict[str, Any],
        progress: ProgressFn | None = None,
        should_stop: StopFn | None = None,
    ) -> dict[str, Any]:
        should_stop = should_stop or (lambda: False)
        config = self.runner.get_config()
        design_cfg = config.get("design_loop", {}) if isinstance(config.get("design_loop", {}), dict) else {}
        objective_alias_default, objective_goal_default = self._default_objective(config)

        objective_alias = str(payload.get("objective_alias", objective_alias_default)).strip()
        objective_goal = str(payload.get("objective_goal", objective_goal_default)).strip().lower() or "min"
        if objective_goal not in {"min", "max"}:
            objective_goal = "min"
        if not objective_alias:
            raise ValueError("objective_alias is required.")

        search_space = payload.get("search_space")
        if not isinstance(search_space, list) or not search_space:
            raise ValueError("search_space is required and must be a non-empty list.")

        constraints = self._merge_constraints(payload.get("constraints"), self._default_constraints(config))
        batch_size = max(1, _safe_int(payload.get("batch_size"), _safe_int(design_cfg.get("batch_size_default"), 8)))
        max_batches = max(1, _safe_int(payload.get("max_batches"), _safe_int(design_cfg.get("max_batches_default"), 4)))
        seed = _safe_int(payload.get("random_seed"), _safe_int(design_cfg.get("random_seed"), 42))
        penalty_missing = _safe_float(payload.get("penalty_missing_objective"), _safe_float(design_cfg.get("penalty_missing_objective"), 1e9))
        penalty_constraint = _safe_float(payload.get("penalty_constraint"), _safe_float(design_cfg.get("penalty_constraint"), 1e6))
        fixed_values = payload.get("fixed_values", {})
        if not isinstance(fixed_values, dict):
            raise ValueError("fixed_values must be an object.")

        restore_cases = bool(payload.get("restore_cases_csv", design_cfg.get("restore_cases_csv_after_run", True)))
        llm_explain = bool(payload.get("use_llm_explanations", design_cfg.get("use_llm_explanations", True)))
        preflight_enabled = bool(
            payload.get("metric_contract_preflight", design_cfg.get("metric_contract_preflight", True))
        )
        dry_run = os.environ.get("CFD_AUTOMATION_DRY_RUN", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

        preflight_result: dict[str, Any] = {}
        if preflight_enabled and not dry_run:
            preflight_result = self.runner.validate_metric_contract(
                study_override=str(payload.get("study_path", "")).strip() or None
            )
            if not preflight_result.get("ok", False):
                missing = preflight_result.get("missing_metrics", [])
                if isinstance(missing, list) and missing:
                    sample = []
                    for item in missing[:5]:
                        if not isinstance(item, dict):
                            continue
                        sample.append(
                            f"{item.get('alias', '<unknown>')} "
                            f"[{item.get('section', '')} :: {item.get('quantity', '')}]"
                        )
                    sample_text = "; ".join(sample) if sample else "unknown metric mismatch"
                    raise ValueError(
                        "Metric contract preflight failed. Configured metrics missing from Autodesk CFD summary catalog: "
                        + sample_text
                    )
                raise ValueError("Metric contract preflight failed with unknown metric mismatch.")
            _emit(
                progress,
                type="loop_preflight_ok",
                checked_metrics=preflight_result.get("checked_metrics", 0),
                available_metric_pairs=preflight_result.get("available_metric_pairs", 0),
            )
        elif preflight_enabled and dry_run:
            preflight_result = {
                "ok": True,
                "skipped": True,
                "reason": "dry_run",
            }
            _emit(
                progress,
                type="loop_preflight_skipped",
                reason="dry_run",
            )
        else:
            preflight_result = {
                "ok": True,
                "skipped": True,
                "reason": "disabled",
            }
            _emit(
                progress,
                type="loop_preflight_skipped",
                reason="disabled",
            )

        loop_id = now_utc_stamp()
        loop_dir = ensure_dir(self.runner.runtime_dir / "design_loops" / loop_id)
        original_cases_csv = self.runner.get_cases_csv()
        optimizer = BayesianCaseOptimizer(search_space, seed=seed)
        optimizer_mode = optimizer.mode()
        optimizer_warning = optimizer.warning()

        narrator = None
        if llm_explain:
            try:
                narrator = LLMOptimizerNarrator(config.get("llm", {}))
            except Exception:
                narrator = None

        history: list[dict[str, Any]] = []
        best_record: dict[str, Any] | None = None
        stopped = False

        _emit(
            progress,
            type="loop_started",
            loop_id=loop_id,
            objective_alias=objective_alias,
            objective_goal=objective_goal,
            batch_size=batch_size,
            max_batches=max_batches,
            constraints=constraints,
            optimizer_mode=optimizer_mode,
            optimizer_warning=optimizer_warning,
        )

        try:
            for batch_index in range(1, max_batches + 1):
                if should_stop():
                    stopped = True
                    _emit(progress, type="loop_stopped", loop_id=loop_id, batch_index=batch_index)
                    break

                rows = optimizer.ask_rows(
                    batch_index=batch_index,
                    batch_size=batch_size,
                    fixed_values=fixed_values,
                )
                csv_text = cases_to_csv(rows)

                batch_dir = ensure_dir(loop_dir / f"batch_{batch_index:02d}")
                (batch_dir / "cases.csv").write_text(csv_text, encoding="utf-8")
                self.runner.save_cases_csv(csv_text)

                _emit(
                    progress,
                    type="loop_batch_started",
                    loop_id=loop_id,
                    batch_index=batch_index,
                    batch_size=len(rows),
                )

                def on_run_progress(event: dict[str, Any]) -> None:
                    _emit(progress, type="loop_run_event", loop_id=loop_id, batch_index=batch_index, event=event)

                run_summary = self.runner.run(mode="all", progress=on_run_progress)
                run_results = {
                    str(item.get("case_id", "")): item
                    for item in run_summary.get("case_results", [])
                    if isinstance(item, dict)
                }

                optimizer_rows: list[dict[str, Any]] = []
                optimizer_scores: list[float] = []
                batch_records: list[dict[str, Any]] = []
                for row in rows:
                    case_id = str(row.get("case_id", ""))
                    case_result = run_results.get(case_id)
                    evaluated = self._evaluate_case(
                        case_result=case_result,
                        objective_alias=objective_alias,
                        objective_goal=objective_goal,
                        constraints=constraints,
                        penalty_missing_objective=penalty_missing,
                        penalty_constraint=penalty_constraint,
                    )
                    success = bool(case_result.get("success")) if case_result else False
                    failure_type = case_result.get("failure_type", "") if case_result else ""
                    failure_reason = (
                        case_result.get("failure_reason", case_result.get("error", ""))
                        if case_result
                        else "missing_result"
                    )
                    if success and evaluated.get("objective_value") is None:
                        success = False
                        failure_type = "null_metric"
                        failure_reason = (
                            f"Objective alias '{objective_alias}' returned null/NaN for this case."
                        )

                    record = {
                        "case_id": case_id,
                        "batch_index": batch_index,
                        "params": {k: v for k, v in row.items() if k != "case_id"},
                        "success": success,
                        "failure_type": failure_type,
                        "failure_reason": failure_reason,
                        "metrics": case_result.get("metrics", {}) if case_result else {},
                        **evaluated,
                    }
                    if evaluated.get("objective_value") is None:
                        violations = (
                            list(record.get("constraint_violations", []))
                            if isinstance(record.get("constraint_violations", []), list)
                            else []
                        )
                        if "objective_missing" not in violations:
                            violations.append("objective_missing")
                        record["constraint_violations"] = violations
                        record["constraints_pass"] = False

                    batch_records.append(record)
                    if record.get("objective_value") is not None:
                        optimizer_rows.append(row)
                        optimizer_scores.append(float(record["score"]))

                    if best_record is None or float(record["score"]) < float(best_record["score"]):
                        best_record = dict(record)

                optimizer.tell(optimizer_rows, optimizer_scores)
                if not optimizer_rows:
                    _emit(
                        progress,
                        type="loop_batch_warning",
                        loop_id=loop_id,
                        batch_index=batch_index,
                        message=(
                            "No valid objective values in this batch. "
                            "Cases were marked as failure_type=null_metric."
                        ),
                    )

                explanation = self._fallback_batch_explanation(batch_records, objective_alias)
                narration = {"provider": "", "model": "", "text": explanation}
                if narrator is not None:
                    try:
                        narration = narrator.narrate_batch(
                            objective_alias=objective_alias,
                            objective_goal=objective_goal,
                            constraints=constraints,
                            batch_records=batch_records,
                            prior_best=best_record,
                        )
                    except Exception:
                        narration = {"provider": "", "model": "", "text": explanation}

                batch_summary = {
                    "batch_index": batch_index,
                    "cases": batch_records,
                    "run_id": run_summary.get("run_id", ""),
                    "narration": narration,
                    "best_case_in_batch": min(batch_records, key=lambda item: float(item["score"])) if batch_records else None,
                }
                write_json(batch_dir / "batch_summary.json", batch_summary)
                history.append(batch_summary)

                _emit(
                    progress,
                    type="loop_batch_finished",
                    loop_id=loop_id,
                    batch_index=batch_index,
                    run_id=run_summary.get("run_id", ""),
                    best_case=batch_summary.get("best_case_in_batch"),
                    narration=narration,
                )

            final_summary = {
                "loop_id": loop_id,
                "status": "stopped" if stopped else "finished",
                "objective_alias": objective_alias,
                "objective_goal": objective_goal,
                "constraints": constraints,
                "batch_size": batch_size,
                "max_batches": max_batches,
                "completed_batches": len(history),
                "optimizer_mode": optimizer_mode,
                "optimizer_warning": optimizer_warning,
                "metric_contract_preflight": preflight_result,
                "best_case": best_record or {},
                "history": history,
                "loop_dir": str(loop_dir),
            }
            write_json(loop_dir / "loop_summary.json", final_summary)
            _emit(progress, type="loop_finished", loop_id=loop_id, summary=final_summary)
            return final_summary
        finally:
            if restore_cases:
                self.runner.save_cases_csv(original_cases_csv)
