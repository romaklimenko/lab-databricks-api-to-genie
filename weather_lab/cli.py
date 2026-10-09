"""Setup, run, inspect, and stop the weather demonstration."""

import argparse
import asyncio
import json
import os
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from dotenv import load_dotenv, set_key

from weather_lab.artifacts import render
from weather_lab.config import ROOT, Settings
from weather_lab.contracts import reference_contract, reference_spec
from weather_lab.local import QUESTIONS, replay
from weather_lab.service import DEFAULT_REQUEST, LabService
from weather_lab.source import encoded, fetch_snapshot, read_json
from weather_lab.state import RunStore


def output(value):
    print(json.dumps(value, ensure_ascii=True, indent=2, default=str), flush=True)


def make_service(settings: Settings, *, with_brain=True):
    from databricks.sdk import WorkspaceClient

    from agent.agent import ModelBrain
    from weather_lab.cloud import CloudTarget

    client = WorkspaceClient()
    if (
        os.getenv("DATABRICKS_APP_NAME")
        and settings.target == "cloud"
        and not settings.state_volume
    ):
        raise ValueError("Hosted cloud runs require WEATHER_STATE_VOLUME for persistent receipts")
    if settings.state_volume and settings.state_volume != settings.volume:
        raise ValueError("State volume must be the configured lab artifacts volume")
    store = RunStore(settings.runs_dir, client, settings.state_volume)
    cloud = CloudTarget(settings, client, store) if settings.target == "cloud" else None
    brain = ModelBrain(settings.model, client) if with_brain else None
    return LabService(settings, store, cloud, brain), client


def setup(settings, create_warehouse, tracing=False):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import sql

    from weather_lab.cloud import CloudTarget

    client = WorkspaceClient()
    store = RunStore(settings.runs_dir, client, settings.state_volume)
    target = CloudTarget(settings, client, store)
    resources = target.setup()
    if create_warehouse:
        name = f"weather-lab-{settings.schema}"
        existing = [item for item in client.warehouses.list() if item.name == name]
        receipt = store.get("resources/warehouse.json")
        if existing and (not receipt or existing[0].id != receipt["id"]):
            raise ValueError("A warehouse with this name exists without this lab's receipt")
        if existing:
            warehouse_id = existing[0].id
        else:
            pending = client.warehouses.create(
                name=name,
                cluster_size="2X-Small",
                enable_serverless_compute=True,
                warehouse_type=sql.CreateWarehouseRequestWarehouseType.PRO,
                min_num_clusters=1,
                max_num_clusters=1,
                auto_stop_mins=5,
            )
            warehouse_id = pending.id
            store.put("resources/warehouse.json", {"id": warehouse_id, "owned": True, "name": name})
        set_key(ROOT / ".env", "WEATHER_WAREHOUSE_ID", warehouse_id)
        resources["warehouse_id"] = warehouse_id
    elif not settings.warehouse_id:
        raise ValueError("Set WEATHER_WAREHOUSE_ID or pass --create-warehouse")
    if tracing:
        import mlflow
        from mlflow.entities.trace_location import UnityCatalog

        mlflow.set_tracking_uri("databricks")
        trace_warehouse = resources.get("warehouse_id", settings.warehouse_id)
        os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = trace_warehouse
        experiment = mlflow.set_experiment(
            f"/Shared/weather-api-to-genie-{settings.catalog}-{settings.schema}-uc",
            trace_location=UnityCatalog(catalog_name=settings.catalog, schema_name=settings.schema),
        )
        store.put("resources/experiment.json", {"experiment_id": experiment.experiment_id})
        set_key(ROOT / ".env", "MLFLOW_TRACKING_URI", "databricks")
        set_key(ROOT / ".env", "MLFLOW_EXPERIMENT_ID", experiment.experiment_id)
        set_key(ROOT / ".env", "MLFLOW_TRACING_SQL_WAREHOUSE_ID", trace_warehouse)
        resources["experiment_id"] = experiment.experiment_id
    return resources


def status_or_stop(settings, stop=False):
    service, client = make_service(settings, with_brain=False)
    result = {"mode": settings.target, "resources": {}}
    if stop:
        service.store.put("resources/control.json", {"stopped": True})
    app_name = os.getenv("WEATHER_APP_NAME")
    if app_name:
        if not app_name.startswith("agent-bricks-weather"):
            raise ValueError("WEATHER_APP_NAME must identify this weather agent deployment")
        if stop:
            client.apps.stop(app_name).result(timeout=timedelta(minutes=5))
        app = client.apps.get(app_name)
        result["resources"]["app"] = {"name": app.name, "state": app.compute_status.state.value}
    job = service.store.get("resources/job.json")
    if job:
        active = list(client.jobs.list_runs(job_id=job["job_id"], active_only=True))
        if stop:
            for run in active:
                client.jobs.cancel_run(run.run_id).result(timeout=timedelta(minutes=3))
            active = list(client.jobs.list_runs(job_id=job["job_id"], active_only=True))
        result["resources"]["job"] = {
            "id": job["job_id"],
            "active_runs": [run.run_id for run in active],
            "scheduled": False,
        }
    warehouse = service.store.get("resources/warehouse.json")
    if warehouse:
        if stop and warehouse.get("owned"):
            current = client.warehouses.get(warehouse["id"])
            if current.name != warehouse["name"]:
                raise ValueError("Warehouse ownership receipt does not match its current name")
            client.warehouses.stop(warehouse["id"]).result(timeout=timedelta(minutes=5))
        current = client.warehouses.get(warehouse["id"])
        result["resources"]["warehouse"] = {
            "id": current.id,
            "state": current.state.value,
            "auto_stop_mins": current.auto_stop_mins,
        }
    elif settings.warehouse_id:
        current = client.warehouses.get(settings.warehouse_id)
        result["resources"]["warehouse"] = {
            "id": current.id,
            "state": current.state.value,
            "note": "User-supplied warehouse; stop manually if not shared.",
        }
    result["new_work_stopped"] = (service.store.get("resources/control.json") or {}).get(
        "stopped", False
    )
    result["remaining_costs"] = (
        "Stored Delta data, raw fixtures, traces, and any deployed Runtime Store can remain after compute stops. This command deletes no data."
    )
    return result


def configure_traces():
    from databricks_agentkit.langgraph import configure_tracing

    configure_tracing()


async def onboard(settings, request, fault):
    import mlflow
    from databricks_agentkit.langgraph import start_trace

    service, _ = make_service(settings)
    configure_traces()

    async def emit(event):
        output(event)

    try:
        with start_trace("weather_onboarding", inputs={"request": request, "fault": fault}) as span:
            result = await service.onboard(request, emit, inject_fault=fault)
            if span:
                span.set_outputs(result)
    finally:
        trace_id = mlflow.get_last_active_trace_id()
        if trace_id:
            service.store.put("last-trace.json", {"trace_id": trace_id})
            output({"trace_id": trace_id})
    return result


def main():
    load_dotenv(ROOT / ".env", override=False)
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("fetch", help="Refresh the public fixture intentionally")
    replay_parser = sub.add_parser("replay", help="Replay fixture locally without agents or cloud")
    replay_parser.add_argument("--artifacts", type=Path)
    sub.add_parser("preflight", help="Read-only access and resource checks")
    setup_parser = sub.add_parser("setup", help="Create the dedicated lab schema and volume")
    setup_parser.add_argument(
        "--create-warehouse",
        action="store_true",
        help="Create a dedicated 2X-Small serverless warehouse; starts paid compute",
    )
    setup_parser.add_argument(
        "--tracing", action="store_true", help="Create the lab MLflow experiment"
    )
    run_parser = sub.add_parser("onboard", help="Run the supervisor and specialists")
    run_parser.add_argument("--request", default=DEFAULT_REQUEST)
    run_parser.add_argument(
        "--fault", action="store_true", help="Inject a labeled unit error for review"
    )
    reproduce_parser = sub.add_parser(
        "reproduce",
        help="Load committed artifacts into the configured cloud schema without authoring agents",
    )
    reproduce_parser.add_argument("--artifacts", type=Path, required=True)
    ask_parser = sub.add_parser("ask", help="Ask the configured Genie Agent")
    ask_parser.add_argument("question")
    sub.add_parser("evaluate", help="Run five held-out Genie questions and save raw evidence")
    sub.add_parser("status", help="Show lab compute state")
    sub.add_parser("start", help="Allow new work after stop; compute starts on demand")
    sub.add_parser("stop", help="Stop owned warehouse, active lab jobs, and configured app")
    args = parser.parse_args()
    settings = Settings.from_env()
    if args.command == "fetch":
        snapshot = fetch_snapshot()
        (ROOT / "fixtures/weather.json").write_bytes(encoded(snapshot))
        output({"saved": "fixtures/weather.json", "rows": 21})
    elif args.command == "replay":
        directory = args.artifacts or ROOT / "fixtures"
        filename = "snapshot.json" if args.artifacts else "weather.json"
        snapshot = read_json(directory / filename)
        if args.artifacts:
            from weather_lab.contracts import DatasetContract, IngestionSpec

            spec = IngestionSpec.model_validate(read_json(directory / "ingestion.json"))
            contract = DatasetContract.model_validate(read_json(directory / "contract.json"))
        else:
            spec, contract = reference_spec(), reference_contract()
        files = render(snapshot, spec, contract, settings)
        store = RunStore(settings.runs_dir)
        for name, value in files.items():
            store.put_bytes("replay/" + name, value)
        output(replay(snapshot, settings.runs_dir / "weather.sqlite"))
    elif args.command == "preflight":
        import importlib.metadata

        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
        output(
            {
                "authenticated": bool(client.current_user.me().active),
                "catalog": client.catalogs.get(settings.catalog).name,
                "warehouses": [
                    {
                        "id": item.id,
                        "name": item.name,
                        "state": item.state.value,
                        "size": item.cluster_size,
                    }
                    for item in client.warehouses.list()
                ],
                "agentbricks_version": importlib.metadata.version("databricks-agentbricks"),
            }
        )
    elif args.command == "setup":
        output(setup(settings, args.create_warehouse, args.tracing))
    elif args.command == "onboard":
        output(asyncio.run(onboard(settings, args.request, args.fault)))
    elif args.command == "reproduce":
        from weather_lab.contracts import DatasetContract, IngestionSpec
        from weather_lab.source import digest
        from weather_lab.state import Receipt

        snapshot = read_json(args.artifacts / "snapshot.json")
        spec = IngestionSpec.model_validate(read_json(args.artifacts / "ingestion.json"))
        contract = DatasetContract.model_validate(read_json(args.artifacts / "contract.json"))
        files = render(snapshot, spec, contract, settings)
        fingerprint = digest({name: value.hex() for name, value in files.items()})
        run_id = "replay-" + fingerprint[:20]
        service, _ = make_service(replace(settings, target="cloud"), with_brain=False)
        receipt = Receipt(service.store, run_id, fingerprint)
        loaded = receipt.completed("load") or receipt.finish(
            "load", service.cloud.load(run_id, files, receipt)
        )
        genie = receipt.completed("genie") or receipt.finish(
            "genie", service.cloud.configure_genie(contract)
        )
        output(
            {
                "loaded": loaded,
                "genie": genie,
                "mode": "cloud reproduction; no authoring model calls",
            }
        )
    elif args.command in {"status", "stop"}:
        output(status_or_stop(settings, stop=args.command == "stop"))
    elif args.command == "start":
        service, _ = make_service(settings, with_brain=False)
        service.store.put("resources/control.json", {"stopped": False})
        output(
            {
                "new_work_stopped": False,
                "note": "Warehouse starts on the next query. Start any hosted app separately.",
            }
        )
    else:
        service, _ = make_service(replace(settings, target="cloud"), with_brain=False)
        if args.command == "ask":
            output(service.cloud.ask(args.question))
        else:
            results = []
            for name, question, sql in QUESTIONS:
                reference = service.cloud.sql(sql.format(table=settings.table))
                answer = service.cloud.ask(question)
                from weather_lab.evaluation import compare

                result = {
                    "name": name,
                    "reference": reference,
                    "genie": answer,
                    **compare(name, reference, answer),
                }
                results.append(result)
                output({"captured": name, "checks": result["checks"]})
            service.store.put("evaluation.json", results)
            passed = sum(item["passed"] for item in results)
            output(
                {
                    "saved": "evaluation.json",
                    "questions": len(results),
                    "passed": passed,
                    "note": "Deterministic result checks. Inspect saved SQL and prose for semantic review.",
                }
            )
            if passed != len(results):
                raise SystemExit(1)


if __name__ == "__main__":
    main()
