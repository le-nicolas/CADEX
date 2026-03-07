from __future__ import annotations

import shutil
from pathlib import Path

import yaml

from cfd_automation.runner import AutomationRunner


def _make_project(tmp_path: Path, *, cases_csv: str, study_path: str = "C:/data/demo_a.cfdst") -> Path:
    project = tmp_path / "project"
    (project / "config").mkdir(parents=True, exist_ok=True)
    (project / "scripts").mkdir(parents=True, exist_ok=True)

    repo_root = Path(__file__).resolve().parents[1]
    shutil.copy2(repo_root / "scripts" / "cfd_case_runner.py", project / "scripts" / "cfd_case_runner.py")
    shutil.copy2(repo_root / "scripts" / "cfd_introspect.py", project / "scripts" / "cfd_introspect.py")

    config = {
        "study": {
            "template_model": study_path,
            "design_name": "Design 1",
            "scenario_name": "Scenario 1",
        },
        "automation": {
            "cfd_executable": "CFD.exe",
            "timeout_minutes": 2,
            "max_retries": 1,
        },
        "solve": {
            "enabled": False,
            "skip_if_results_exist": True,
        },
        "outputs": {
            "save_all_summary": True,
            "screenshots": {"enabled": False, "views": ["default"]},
            "cutplanes": [],
            "report": {"enabled": True},
        },
        "metrics": [],
        "criteria": [],
        "ranking": [],
        "parameter_mappings": [],
    }
    (project / "config" / "study_config.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False),
        encoding="utf-8",
    )
    (project / "config" / "cases.csv").write_text(cases_csv, encoding="utf-8")
    return project


def test_history_store_ingests_run_and_case_inputs(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(
        tmp_path,
        cases_csv=(
            "case_id,inlet_velocity_ms,total_heat_w\n"
            "CASE_A,1.5,100\n"
            "CASE_B,2.0,120\n"
        ),
    )
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")
    runner = AutomationRunner(project)
    summary = runner.run(mode="all")

    db_path = project / "runtime" / "history.db"
    assert db_path.exists()

    runs_payload = runner.history_store.list_runs(limit=10)
    assert runs_payload["total"] == 1
    assert runs_payload["runs"][0]["run_id"] == summary["run_id"]

    detail = runner.history_store.get_run(summary["run_id"])
    assert detail["run_id"] == summary["run_id"]
    assert detail["study_path"] == "C:/data/demo_a.cfdst"
    assert len(detail["case_results"]) == 2
    first_inputs = detail["case_results"][0].get("inputs", {})
    assert isinstance(first_inputs, dict)
    assert "case_id" in first_inputs

    cases_payload = runner.history_store.list_cases(limit=10)
    assert cases_payload["total"] == 2
    assert all("inputs" in row for row in cases_payload["cases"])


def test_history_store_filters_runs_and_cases(tmp_path: Path, monkeypatch) -> None:
    project = _make_project(
        tmp_path,
        cases_csv=(
            "case_id,inlet_velocity_ms\n"
            "ALPHA_1,1.0\n"
            "BETA_2,2.0\n"
        ),
        study_path="C:/data/kani_yawa.cfdst",
    )
    monkeypatch.setenv("CFD_AUTOMATION_DRY_RUN", "1")
    runner = AutomationRunner(project)
    runner.run(mode="all")

    filtered_runs = runner.history_store.list_runs(limit=10, study_path="kani_yawa", case_id="alpha")
    assert filtered_runs["total"] == 1
    assert filtered_runs["runs"][0]["study_path"] == "C:/data/kani_yawa.cfdst"

    filtered_cases = runner.history_store.list_cases(limit=10, case_id="beta")
    assert filtered_cases["total"] == 1
    assert filtered_cases["cases"][0]["case_id"] == "BETA_2"
