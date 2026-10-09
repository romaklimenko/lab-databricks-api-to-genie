import copy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from agent.agent import design
from weather_lab.artifacts import render
from weather_lab.config import ROOT, Settings
from weather_lab.contracts import (
    Intent,
    Review,
    Route,
    reference_contract,
    reference_spec,
    validate,
)
from weather_lab.evaluation import compare
from weather_lab.local import replay
from weather_lab.service import LabService
from weather_lab.source import read_json, rows
from weather_lab.state import RunStore


@pytest.fixture
def snapshot():
    return read_json(ROOT / "fixtures/weather.json")


class ScriptedBrain:
    """Test double only. Cloud and UI runs always use ModelBrain."""

    def __init__(self, reject=False):
        self.calls = []
        self.reject = reject

    async def structured(self, role, instructions, context, schema):
        self.calls.append(role)
        if role == "supervisor":
            available = context["available"]
            selected = "steward" if "steward" in available else available[0]
            return Route(specialist=selected, reason="Scripted test decision")
        if role == "ingestion specialist":
            return reference_spec()
        if role == "data steward":
            return reference_contract()
        if role == "request router":
            return Intent(action="onboard", reason="Scripted test request")
        return Review(
            passed=not self.reject, findings=["Rejected by test reviewer"] if self.reject else []
        )


async def ignore(event):
    pass


def test_replay_and_exact_source_values(snapshot, tmp_path):
    first = replay(snapshot, tmp_path / "weather.sqlite")
    second = replay(snapshot, tmp_path / "weather.sqlite")
    assert first == second
    assert first["row_count"] == 21
    assert first["reference_results"]["missing_period"] == []
    assert len({(row["city_id"], row["weather_date"]) for row in rows(snapshot)}) == 21


@pytest.mark.parametrize("fault", ["unit", "missing", "duplicate", "null", "date"])
def test_invalid_source_rejected_before_render(snapshot, fault):
    snapshot = copy.deepcopy(snapshot)
    response = snapshot["locations"][0]["response"]
    if fault == "unit":
        response["daily_units"]["wind_speed_10m_max"] = "km/h"
    elif fault == "missing":
        del response["daily"]["precipitation_sum"]
    elif fault == "duplicate":
        snapshot["locations"][1] = snapshot["locations"][0]
    elif fault == "null":
        response["daily"]["precipitation_sum"][0] = None
    else:
        response["daily"]["time"][0] = "2025-01-01"
    with pytest.raises(ValueError, match="Artifact validation failed"):
        render(snapshot, reference_spec(), reference_contract(), Settings())


async def test_deterministic_fault_overrides_model_approval(snapshot):
    events = []

    async def emit(event):
        events.append(event)

    result = await design(ScriptedBrain(), snapshot, "Load the fixed week", emit, inject_fault=True)
    failed = [event for event in events if event["phase"] == "repair needed"]
    assert len(failed) == 1
    assert failed[0]["model_passed"] is True
    assert "max_wind_ms.unit" in failed[0]["deterministic_failures"][0]
    assert result["reviews"] == 2
    assert not validate(snapshot, reference_spec(), reference_contract())


async def test_repeated_work_reuses_phases(tmp_path):
    brain = ScriptedBrain()
    service = LabService(Settings(runs_dir=tmp_path), RunStore(tmp_path), brain=brain)
    first = await service.onboard("Load fixed week", ignore)
    calls = len(brain.calls)
    second = await service.onboard("Please load same fixed week", ignore)
    assert first["run_id"] == second["run_id"]
    assert first["loaded"] == second["loaded"]
    assert len(brain.calls) == calls


async def test_review_failure_has_no_external_writes(tmp_path):
    class Publisher:
        def commit(self, *args):
            pytest.fail("Rejected design reached publisher")

    service = LabService(
        Settings(runs_dir=tmp_path),
        RunStore(tmp_path),
        brain=ScriptedBrain(reject=True),
        publisher=Publisher(),
    )
    with pytest.raises(ValueError, match="Review did not pass"):
        await service.onboard("Load fixed week", ignore)
    assert not (tmp_path / "weather.sqlite").exists()


async def test_approval_with_findings_cannot_publish(snapshot):
    class InconsistentReviewer(ScriptedBrain):
        async def structured(self, role, instructions, context, schema):
            if role == "independent reviewer":
                return Review(passed=True, findings=["A defect still needs repair"])
            return await super().structured(role, instructions, context, schema)

    with pytest.raises(ValueError, match="Review did not pass"):
        await design(InconsistentReviewer(), snapshot, "Load fixed week", ignore)


def test_public_artifacts_and_genie_examples(snapshot):
    files = render(
        snapshot, reference_spec(), reference_contract(), Settings(catalog="YOUR_CATALOG")
    )
    config = json.loads(files["genie.json"])
    assert config["data_sources"]["tables"][0]["identifier"].startswith("YOUR_CATALOG.")
    assert "weekly" not in json.dumps(config["instructions"]["example_question_sqls"])
    assert all(len(item["id"]) == 32 for item in config["instructions"]["text_instructions"])


async def test_stop_prevents_new_work(tmp_path):
    store = RunStore(tmp_path)
    store.put("resources/control.json", {"stopped": True})
    brain = ScriptedBrain()
    service = LabService(Settings(runs_dir=tmp_path), store, brain=brain)
    with pytest.raises(RuntimeError, match="Lab is stopped"):
        await service.onboard("Load fixed week", ignore)
    assert not brain.calls


def test_query_evaluation_rejects_wrong_values():
    reference = {"status": {"state": "SUCCEEDED"}, "result": {"data_array": [["Odense", "10"]]}}
    answer = {"queries": ["SELECT city, total_mm"], "data": [{"statement_response": reference}]}
    assert compare("precipitation", reference, answer)["passed"]
    wrong = copy.deepcopy(answer)
    wrong["data"][0]["statement_response"]["result"]["data_array"][0][1] = "20"
    assert not compare("precipitation", reference, wrong)["passed"]


def test_invocation_protocol_and_event_replay(monkeypatch):
    monkeypatch.setenv("DATABRICKS_AGENT_RUNTIME_STORE", "memory")
    from runtime.main import create_app

    async def onboard(request, emit, **kwargs):
        await emit({"role": "reviewer", "phase": "passed", "detail": "Fixture accepted"})
        return {"status": "completed", "text": "Test result"}

    lab = SimpleNamespace(brain=ScriptedBrain(), onboard=onboard)
    with TestClient(create_app(lambda: lab)) as client:
        invocation_id = str(uuid4())
        response = client.post(
            "/api/invocations", json={"id": invocation_id, "input": {"prompt": "Load fixed week"}}
        )
        assert response.status_code == 200, response.text
        state = client.get(f"/api/invocations/{invocation_id}").json()
        assert state["status"] == "completed"
        assert state["output"]["text"] == "Test result"
        events = client.get(f"/api/invocations/{invocation_id}/events").text
        assert '"phase": "passed"' in events
        assert client.get("/").status_code == 200
