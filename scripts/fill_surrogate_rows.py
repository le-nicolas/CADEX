from __future__ import annotations

import argparse
import copy
import json
import random
from pathlib import Path
from typing import Any

from cfd_automation import AutomationRunner, SurrogateEngine
from cfd_automation.config_io import cases_to_csv


def lhs_values(rng: random.Random, *, low: float, high: float, n: int) -> list[float]:
    values = [low + ((i + rng.random()) / n) * (high - low) for i in range(n)]
    rng.shuffle(values)
    return values


def existing_param_keys(engine: SurrogateEngine) -> set[str]:
    harvest = engine.harvest_training_rows(include_design_loops=True, objective_alias="temp_max_c")
    keys: set[str] = set()
    for row in harvest.rows:
        params = row.get("params", {}) if isinstance(row.get("params", {}), dict) else {}
        keys.add(json.dumps(params, sort_keys=True, separators=(",", ":")))
    return keys


def make_fill_rows(*, count: int, seed: int, existing_keys: set[str]) -> list[dict[str, Any]]:
    rng = random.Random(seed)

    v_vals = lhs_values(rng, low=1.0, high=5.0, n=count * 2)
    t_vals = lhs_values(rng, low=25.0, high=40.0, n=count * 2)
    q_vals = lhs_values(rng, low=70.0, high=130.0, n=count * 2)

    rows: list[dict[str, Any]] = []
    used = set(existing_keys)
    for idx in range(min(len(v_vals), len(t_vals), len(q_vals))):
        row = {
            "case_id": f"SURR_FILL_{len(rows) + 1:03d}",
            "inlet_velocity_ms": round(float(v_vals[idx]), 6),
            "ambient_temp_c": round(float(t_vals[idx]), 6),
            "total_heat_w": round(float(q_vals[idx]), 6),
            "force_solve": True,
        }
        params = {k: v for k, v in row.items() if k != "case_id"}
        key = json.dumps(params, sort_keys=True, separators=(",", ":"))
        if key in used:
            continue
        rows.append(row)
        used.add(key)
        if len(rows) >= count:
            break

    if len(rows) < count:
        raise RuntimeError(
            f"Could not generate enough unique fill rows. Requested={count}, built={len(rows)}."
        )
    return rows


def run_fill(
    *,
    project_root: Path,
    count: int,
    seed: int,
    temp_iterations: int | None,
    temp_convergence_threshold: float | None,
) -> None:
    runner = AutomationRunner(project_root)
    surrogate = SurrogateEngine(project_root, runner)
    original_config = runner.get_config()
    original_cases = runner.get_cases_csv()

    existing = existing_param_keys(surrogate)
    print(f"Existing surrogate rows (deduplicated): {len(existing)}")

    rows = make_fill_rows(count=count, seed=seed, existing_keys=existing)
    print(f"Generated {len(rows)} spread-out fill rows.")

    try:
        if temp_iterations is not None or temp_convergence_threshold is not None:
            temp_cfg = copy.deepcopy(original_config)
            solve_cfg = temp_cfg.setdefault("solve", {})
            overrides = solve_cfg.setdefault("scenario_overrides", {})
            if temp_iterations is not None:
                overrides["iterations"] = int(temp_iterations)
            if temp_convergence_threshold is not None:
                overrides["convergenceThreshold"] = float(temp_convergence_threshold)
            runner.save_config(temp_cfg)
            print(
                "Temporary solver overrides:",
                f"iterations={overrides.get('iterations')}",
                f"convergenceThreshold={overrides.get('convergenceThreshold')}",
            )

        runner.save_cases_csv(cases_to_csv(rows))
        print("Running Mode A (all) with real CFD...")
        summary = runner.run(mode="all")
    finally:
        runner.save_config(original_config)
        runner.save_cases_csv(original_cases)

    print(
        "Run finished:",
        f"run_id={summary.get('run_id')}",
        f"success={summary.get('successful_cases')}",
        f"failed={summary.get('failed_cases')}",
        sep=" ",
    )

    status_before = surrogate.status()
    print(
        "Surrogate status before retrain:",
        f"rows={status_before.get('row_count', 0)} ready={status_before.get('ready', False)}",
    )

    try:
        trained = surrogate.train(min_rows=50, include_design_loops=True)
        print(
            "Surrogate retrained:",
            f"rows={trained.get('row_count', 0)}",
            f"r2={trained.get('best_r2', 0.0):.4f}",
            f"ready={trained.get('ready', False)}",
        )
    except Exception as ex:
        print(f"Surrogate retrain skipped/failed: {ex}")
        status_after = surrogate.status()
        print(
            "Surrogate status after run:",
            f"rows={status_after.get('row_count', 0)} ready={status_after.get('ready', False)}",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate spread-out Mode A cases and fill surrogate row count with real CFD runs."
    )
    parser.add_argument("--count", type=int, default=29, help="Number of new real cases to run.")
    parser.add_argument("--seed", type=int, default=20260306, help="Random seed for spread sampling.")
    parser.add_argument(
        "--temp-iterations",
        type=int,
        default=None,
        help="Temporary solve.scenario_overrides.iterations for this fill run.",
    )
    parser.add_argument(
        "--temp-convergence-threshold",
        type=float,
        default=None,
        help="Temporary solve.scenario_overrides.convergenceThreshold for this fill run.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Project root path.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_fill(
        project_root=args.project_root.resolve(),
        count=max(1, int(args.count)),
        seed=int(args.seed),
        temp_iterations=args.temp_iterations,
        temp_convergence_threshold=args.temp_convergence_threshold,
    )
