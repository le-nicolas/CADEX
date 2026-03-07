from __future__ import annotations

from cfd_automation.cfd_driver import _detect_phase_marker


def test_detect_phase_marker_mesh() -> None:
    assert _detect_phase_marker("Generating volume mesh ...") == "mesh"


def test_detect_phase_marker_solve() -> None:
    assert _detect_phase_marker("Iteration 45 residual = 1.2e-3") == "solve"


def test_detect_phase_marker_results() -> None:
    assert _detect_phase_marker("Summary CSV and metrics CSV exported.") == "results"


def test_detect_phase_marker_none() -> None:
    assert _detect_phase_marker("Case configuration updated.") is None
