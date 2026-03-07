from __future__ import annotations

from typing import Any


def test_history_runs_endpoint(monkeypatch) -> None:
    import app as web_app

    captured: dict[str, Any] = {}

    class FakeHistory:
        def list_runs(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {
                "total": 1,
                "limit": kwargs.get("limit", 40),
                "offset": kwargs.get("offset", 0),
                "runs": [
                    {
                        "run_id": "20260307_101010",
                        "created_at": "2026-03-07T10:10:10+00:00",
                        "mode": "all",
                        "study_path": "C:/Users/User/Downloads/Kani yawa/Kani yawa.cfdst",
                        "design_name": "Design 1",
                        "scenario_name": "Scenario 1",
                        "total_cases": 6,
                        "selected_case_count": 6,
                        "successful_cases": 5,
                        "failed_cases": 1,
                    }
                ],
            }

    monkeypatch.setattr(web_app.runner, "history_store", FakeHistory())
    client = web_app.app.test_client()
    response = client.get("/api/history/runs?limit=5&offset=2&study_path=kani&case_id=CASE_A")
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["ok"] is True
    assert payload["total"] == 1
    assert captured["limit"] == 5
    assert captured["offset"] == 2
    assert captured["study_path"] == "kani"
    assert captured["case_id"] == "CASE_A"


def test_history_run_detail_endpoint_enriches_assets(monkeypatch) -> None:
    import app as web_app

    screenshot = web_app.runner.runtime_dir / "runs" / "RUN1" / "cases" / "CASE_1" / "screenshots" / "v1.png"
    summary_csv = web_app.runner.runtime_dir / "runs" / "RUN1" / "cases" / "CASE_1" / "summary_all.csv"
    metrics_csv = web_app.runner.runtime_dir / "runs" / "RUN1" / "cases" / "CASE_1" / "metrics.csv"

    class FakeHistory:
        def get_run(self, run_id: str) -> dict[str, Any]:
            if run_id != "RUN1":
                return {}
            return {
                "run_id": "RUN1",
                "results": {},
                "case_results": [
                    {
                        "case_id": "CASE_1",
                        "summary_csv": str(summary_csv),
                        "metrics_csv": str(metrics_csv),
                        "screenshots": [str(screenshot)],
                        "success": True,
                    }
                ],
            }

    monkeypatch.setattr(web_app.runner, "history_store", FakeHistory())
    client = web_app.app.test_client()
    response = client.get("/api/history/runs/RUN1")
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["ok"] is True
    case = payload["summary"]["case_results"][0]
    assert case["summary_csv_url"].startswith("/runtime/")
    assert case["metrics_csv_url"].startswith("/runtime/")
    assert case["screenshot_urls"][0].startswith("/runtime/")


def test_history_cases_endpoint_supports_success_filter(monkeypatch) -> None:
    import app as web_app

    captured: dict[str, Any] = {}

    class FakeHistory:
        def list_cases(self, **kwargs: Any) -> dict[str, Any]:
            captured.update(kwargs)
            return {
                "total": 1,
                "limit": kwargs.get("limit", 120),
                "offset": kwargs.get("offset", 0),
                "cases": [
                    {
                        "run_id": "RUNX",
                        "case_id": "CASE_OK",
                        "success": True,
                        "metrics": {"temp_max_c": 42.1},
                        "inputs": {"inlet_velocity_ms": 2.0},
                        "screenshots": [],
                        "summary_csv": "",
                        "metrics_csv": "",
                    }
                ],
            }

    monkeypatch.setattr(web_app.runner, "history_store", FakeHistory())
    client = web_app.app.test_client()
    response = client.get("/api/history/cases?success=true&study_path=kani")
    payload = response.get_json()
    assert response.status_code == 200
    assert payload["ok"] is True
    assert captured["success"] is True
    assert captured["study_path"] == "kani"
    assert payload["cases"][0]["case_id"] == "CASE_OK"
