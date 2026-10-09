"""Coordinate reviewed artifacts and idempotent external phases."""

import asyncio

from weather_lab.artifacts import render
from weather_lab.config import ROOT, Settings
from weather_lab.contracts import DatasetContract, IngestionSpec, validate
from weather_lab.github import GitHubPublisher
from weather_lab.local import replay
from weather_lab.source import digest, encoded, read_json
from weather_lab.state import Receipt

DEFAULT_REQUEST = "Load weather for Copenhagen, Aarhus, and Odense for September 21-27, 2026, configure Genie, and save the implementation."


class LabService:
    def __init__(self, settings, store, cloud=None, brain=None, publisher=None):
        self.settings, self.store, self.cloud, self.brain, self.publisher = (
            settings,
            store,
            cloud,
            brain,
            publisher,
        )
        self.lock = asyncio.Lock()

    async def onboard(self, request: str, emit, *, inject_fault=False) -> dict:
        async with self.lock:
            return await self._onboard(request, emit, inject_fault=inject_fault)

    async def _onboard(self, request, emit, *, inject_fault):
        from agent.agent import design

        snapshot = read_json(ROOT / "fixtures/weather.json")
        implementation = {
            str(path.relative_to(ROOT)): digest(path.read_text(encoding="utf-8"))
            for folder in ("agent", "weather_lab")
            for path in sorted((ROOT / folder).glob("*.py"))
            if path.name not in {"cli.py", "service.py", "__init__.py"}
        }
        fingerprint = digest(
            {
                "snapshot": digest(snapshot),
                "table": self.settings.table,
                "model": self.settings.model,
                "fault": inject_fault,
                "target": self.settings.target,
                "repository": self.settings.github_repo,
                "implementation": implementation,
                "version": 1,
            }
        )
        run_id = fingerprint[:24]
        receipt = Receipt(self.store, run_id, fingerprint)

        async def progress(event):
            receipt.data["events"].append(event)
            await asyncio.to_thread(receipt.save)
            await emit(event)

        async def phase(name, function):
            if (previous := receipt.completed(name)) is not None:
                await emit({"role": "tool", "phase": name, "detail": "Reusing completed phase."})
                return previous
            await asyncio.to_thread(self.store.require_running)
            await progress({"role": "tool", "phase": name, "detail": "Running."})
            result = await asyncio.to_thread(function)
            await asyncio.to_thread(receipt.finish, name, result)
            return result

        await asyncio.to_thread(self.store.require_running)
        if (result := receipt.completed("design")) is None:
            result = await design(
                self.brain, snapshot, request, progress, inject_fault=inject_fault
            )
            receipt.finish("design", result)
        spec = IngestionSpec.model_validate(result["spec"])
        contract = DatasetContract.model_validate(result["contract"])
        if errors := validate(snapshot, spec, contract):
            raise ValueError("Saved design failed validation: " + "; ".join(errors))
        files = render(snapshot, spec, contract, self.settings)
        for name, content in files.items():
            await asyncio.to_thread(self.store.put_bytes, f"runs/{run_id}/{name}", content)
        # Account-specific table names and resource IDs never enter public Git artifacts.
        public_files = render(snapshot, spec, contract, Settings(catalog="YOUR_CATALOG"))
        publisher = self.publisher
        owned_publisher = False
        if publisher is None and self.settings.github_repo:
            publisher = GitHubPublisher(self.settings.github_repo)
            owned_publisher = True
        try:
            if self.settings.target == "cloud" and publisher is None:
                raise ValueError(
                    "Cloud onboarding requires WEATHER_GITHUB_REPO for reproducibility"
                )
            source_commit = (
                await phase(
                    "source_commit", lambda: publisher.commit(run_id, public_files, "source")
                )
                if publisher
                else {"mode": "local artifacts; no remote commit"}
            )
            if self.cloud:
                loaded = await phase("load", lambda: self.cloud.load(run_id, files, receipt))
                genie = await phase("genie", lambda: self.cloud.configure_genie(contract))
                question = "Which city had the most total precipitation during September 21-27, 2026? Give the total in millimeters."
                answer = await phase("answer", lambda: self.cloud.ask(question))
            else:
                loaded = await phase(
                    "load", lambda: replay(snapshot, self.settings.runs_dir / "weather.sqlite")
                )
                genie, answer = (
                    None,
                    {"text": ["Offline dataset loaded. Genie requires cloud mode."]},
                )
            public_result = {
                "snapshot_sha256": digest(snapshot),
                "row_count": loaded["row_count"],
                "validated": loaded["validated"],
                "source_commit": source_commit.get("sha"),
                "genie_configured": bool(genie),
                "live_query_returned": bool(answer.get("queries")),
            }
            evidence_commit = (
                await phase(
                    "evidence_commit",
                    lambda: publisher.commit(
                        run_id, {"result.json": encoded(public_result)}, "evidence"
                    ),
                )
                if publisher
                else None
            )
            return {
                "run_id": run_id,
                "target": self.settings.target,
                "source_commit": source_commit,
                "evidence_commit": evidence_commit,
                "loaded": loaded,
                "genie": genie,
                "answer": answer,
                "events": receipt.data["events"],
                "status": "completed",
            }
        finally:
            if owned_publisher:
                publisher.close()
