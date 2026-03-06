from __future__ import annotations

from cfd_automation.config_io import case_fingerprint


def test_case_fingerprint_changes_when_study_fluid_preset_changes() -> None:
    case = {"case_id": "CASE_001", "inlet_velocity_ms": "3.0"}
    base = {
        "parameter_mappings": [],
        "solve": {},
        "mesh": {},
        "metrics": [],
        "study": {"fluid_preset": "air"},
        "fluid_presets": {},
    }
    changed = {
        **base,
        "study": {"fluid_preset": "water"},
    }

    assert case_fingerprint(case, base) != case_fingerprint(case, changed)


def test_case_fingerprint_changes_when_fluid_preset_definition_changes() -> None:
    case = {"case_id": "CASE_001", "inlet_velocity_ms": "3.0"}
    base = {
        "parameter_mappings": [],
        "solve": {},
        "mesh": {},
        "metrics": [],
        "study": {"fluid_preset": "oil"},
        "fluid_presets": {
            "oil": {
                "match": {"type": "fluid"},
                "properties": {"dynamic_viscosity": {"value": 0.065, "units": "Pa.s"}},
            }
        },
    }
    changed = {
        **base,
        "fluid_presets": {
            "oil": {
                "match": {"type": "fluid"},
                "properties": {"dynamic_viscosity": {"value": 0.075, "units": "Pa.s"}},
            }
        },
    }

    assert case_fingerprint(case, base) != case_fingerprint(case, changed)

