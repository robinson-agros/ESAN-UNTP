from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dpp_logic import run_scenario


def load_expected(name: str) -> dict:
    return json.loads((ROOT / name).read_text(encoding="utf-8"))


def test_valid_3_stage_generates_dpp() -> None:
    result = run_scenario(sorted((ROOT / "dataset_valid_3_stage").glob("*.xlsx")))
    expected = load_expected("expected_dpp_valid_3_stage.json")

    dpp = result["dpp"]
    assert dpp["validation"]["status"] == "ready"
    assert dpp["dpp_id"] == expected["dpp_id"]
    assert dpp["product_id"] == expected["product_id"]
    assert dpp["final_lot_id"] == expected["final_lot_id"]
    assert dpp["batch_number"] == expected["batch_number"]
    assert dpp["timeline"]["stage_count"] == expected["stage_count"]
    assert dpp["traceability"]["event_ids"] == expected["traceability"]["events"]
    assert dpp["traceability"]["lot_ids"] == expected["traceability"]["lots"]


def test_valid_5_stage_generates_dpp() -> None:
    result = run_scenario(sorted((ROOT / "dataset_valid_5_stage").glob("*.xlsx")))
    expected = load_expected("expected_dpp_valid_5_stage.json")

    dpp = result["dpp"]
    assert dpp["validation"]["status"] == "ready"
    assert dpp["dpp_id"] == expected["dpp_id"]
    assert dpp["product_id"] == expected["product_id"]
    assert dpp["final_lot_id"] == expected["final_lot_id"]
    assert dpp["batch_number"] == expected["batch_number"]
    assert dpp["timeline"]["stage_count"] == expected["stage_count"]
    assert dpp["traceability"]["event_ids"] == expected["traceability"]["events"]
    assert dpp["traceability"]["lot_ids"] == expected["traceability"]["lots"]


def test_invalid_dataset_blocks_dpp() -> None:
    result = run_scenario(sorted((ROOT / "dataset_invalid_4_stage_missing_link").glob("*.xlsx")))

    assert result["report"]["errors"]
    assert result["report"]["metrics"]["broken_reference_count"] > 0
    assert result["dpp"]["validation"]["status"] == "invalid"
