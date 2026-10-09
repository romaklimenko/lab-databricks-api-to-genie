"""Narrow Databricks tools for the configured lab resources."""

import io
import json
import time
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.service import catalog, compute, jobs, workspace

from weather_lab.artifacts import canonical_space, genie_config
from weather_lab.config import Settings

OWNER = "weather-api-to-genie lab"


class CloudTarget:
    def __init__(self, settings: Settings, client, store):
        self.settings, self.client, self.store = settings, client, store

    def setup(self):
        s, w = self.settings, self.client
        try:
            schema = w.schemas.get(f"{s.catalog}.{s.schema}")
            if schema.comment != OWNER:
                raise ValueError("Existing schema is not owned by this lab")
        except NotFound:
            w.schemas.create(s.schema, s.catalog, comment=OWNER)
        try:
            volume = w.volumes.read(f"{s.catalog}.{s.schema}.artifacts")
            if volume.comment != OWNER:
                raise ValueError("Existing volume is not owned by this lab")
        except NotFound:
            w.volumes.create(
                s.catalog, s.schema, "artifacts", catalog.VolumeType.MANAGED, comment=OWNER
            )
        return {"volume": s.volume, "table": s.table}

    def load(self, run_id: str, files: dict[str, bytes], receipt) -> dict:
        self.store.require_running()
        s, w = self.settings, self.client
        root = f"{s.volume}/runs/{run_id}"
        w.files.create_directory(root)
        for name, content in files.items():
            w.files.upload(f"{root}/{name}", io.BytesIO(content), overwrite=True)
        script_path = f"/Workspace/Shared/weather-api-to-genie/{s.schema}/{run_id}/ingest.py"
        w.workspace.mkdirs(script_path.rsplit("/", 1)[0])
        w.workspace.upload(
            script_path,
            io.BytesIO(files["ingest.py"]),
            format=workspace.ImportFormat.AUTO,
            overwrite=True,
        )
        job_key = "resources/job.json"
        resource = self.store.get(job_key)
        if resource is None:
            matches = list(
                w.jobs.list(name=f"weather-api-to-genie-{s.catalog}-{s.schema}", expand_tasks=True)
            )
            if matches:
                if (
                    len(matches) != 1
                    or (matches[0].settings.tags or {}).get("lab") != "weather-api-to-genie"
                ):
                    raise ValueError("Existing job is not uniquely owned by this lab")
                job_id = matches[0].job_id
            else:
                created = w.jobs.create(
                    name=f"weather-api-to-genie-{s.catalog}-{s.schema}",
                    description=OWNER,
                    max_concurrent_runs=1,
                    timeout_seconds=900,
                    performance_target=jobs.PerformanceTarget.STANDARD,
                    tags={"lab": "weather-api-to-genie", "aidevkit_project": "ai-dev-kit"},
                    tasks=[
                        jobs.Task(
                            task_key="ingest",
                            environment_key="default",
                            spark_python_task=jobs.SparkPythonTask(
                                python_file=script_path,
                                parameters=[
                                    "--artifact-root",
                                    "{{job.parameters.artifact_root}}",
                                    "--catalog",
                                    s.catalog,
                                    "--schema",
                                    s.schema,
                                ],
                            ),
                        )
                    ],
                    parameters=[jobs.JobParameterDefinition(name="artifact_root", default=root)],
                    environments=[
                        jobs.JobEnvironment(
                            environment_key="default",
                            spec=compute.Environment(environment_version=s.job_environment),
                        )
                    ],
                )
                job_id = created.job_id
            resource = {"job_id": job_id, "script_path": script_path}
            self.store.put(job_key, resource)
        # Update only this lab job's task to the reviewed version. Preserve the resource itself.
        if resource["script_path"] != script_path:
            current = w.jobs.get(resource["job_id"])
            if (current.settings.tags or {}).get("lab") != "weather-api-to-genie":
                raise ValueError("Job ownership changed")
            task = current.settings.tasks[0]
            task.spark_python_task.python_file = script_path
            w.jobs.update(resource["job_id"], new_settings=jobs.JobSettings(tasks=[task]))
            resource["script_path"] = script_path
            self.store.put(job_key, resource)
        pending = receipt.data.get("pending_job")
        if pending is None:
            pending = w.jobs.run_now(
                resource["job_id"],
                idempotency_token=f"weather-{run_id}",
                job_parameters={"artifact_root": root},
            ).run_id
            receipt.data["pending_job"] = pending
            receipt.save()
        run = w.jobs.wait_get_run_job_terminated_or_skipped(pending, timeout=timedelta(minutes=18))
        if run.state.result_state != jobs.RunResultState.SUCCESS:
            raise RuntimeError(f"Ingestion job did not succeed: {run.state.state_message}")
        response = w.files.download(f"{root}/load-result.json")
        with response.contents as stream:
            report = json.loads(stream.read())
        if not report.get("validated") or report.get("row_count") != 21:
            raise ValueError("Job returned an invalid load report")
        return {**report, "job_id": resource["job_id"], "run_id": pending}

    def sql(self, statement: str) -> dict:
        self.store.require_running()
        if not self.settings.warehouse_id:
            raise ValueError("Configure WEATHER_WAREHOUSE_ID before executing SQL")
        response = self.client.statement_execution.execute_statement(
            statement=statement, warehouse_id=self.settings.warehouse_id, wait_timeout="50s"
        )
        deadline = time.monotonic() + 120
        while response.status.state.value in {"PENDING", "RUNNING"}:
            if time.monotonic() > deadline:
                self.client.statement_execution.cancel_execution(response.statement_id)
                raise TimeoutError("SQL exceeded 120 seconds")
            time.sleep(1)
            response = self.client.statement_execution.get_statement(response.statement_id)
        if response.status.state.value != "SUCCEEDED":
            raise RuntimeError(f"SQL failed: {response.status.error}")
        return response.as_dict()

    def configure_genie(self, contract) -> dict:
        self.store.require_running()
        s, w = self.settings, self.client
        for name, column in contract.columns.items():
            text = column.description.replace("'", "''")
            self.sql(f"COMMENT ON COLUMN {s.table}.{name} IS '{text}'")
        self.sql(
            f"COMMENT ON TABLE {s.table} IS 'Daily ERA5 reanalysis. One row per city and local date in Europe/Copenhagen. September 21-27, 2026.'"
        )
        config = genie_config(s, contract)
        for example in config["instructions"]["example_question_sqls"]:
            self.sql("EXPLAIN " + "\n".join(example["sql"]))
        resource = self.store.get("resources/genie.json")
        title = f"Weather lab - {s.catalog}.{s.schema}"
        if resource is None:
            matches, token = [], None
            while True:
                page = w.genie.list_spaces(page_token=token)
                matches.extend(space for space in page.spaces or [] if space.title == title)
                token = page.next_page_token
                if not token:
                    break
            if matches:
                if len(matches) != 1 or matches[0].description != OWNER:
                    raise ValueError("Existing Genie Agent is not uniquely owned by this lab")
                resource = {"space_id": matches[0].space_id}
            else:
                space = w.genie.create_space(
                    title=title,
                    description=OWNER,
                    warehouse_id=s.warehouse_id,
                    serialized_space=json.dumps(config),
                )
                resource = {"space_id": space.space_id}
            self.store.put("resources/genie.json", resource)
        path = f"/api/2.0/genie/spaces/{resource['space_id']}"
        current = w.api_client.do("GET", path, query={"include_serialized_space": True})
        if current.get("description") != OWNER:
            raise ValueError("Genie Agent ownership marker changed")
        if canonical_space(json.loads(current["serialized_space"])) != canonical_space(config):
            body = {"serialized_space": json.dumps(config)}
            if current.get("etag"):
                body["etag"] = current["etag"]
            w.api_client.do("PATCH", path, body=body)
        applied = w.api_client.do("GET", path, query={"include_serialized_space": True})
        actual = canonical_space(json.loads(applied["serialized_space"]))
        attached = {item["identifier"] for item in actual["data_sources"]["tables"]}
        if (
            attached != {s.table}
            or actual.get("instructions") != canonical_space(config)["instructions"]
        ):
            raise ValueError("Genie readback differs from accepted configuration")
        return resource

    def ask(self, question: str) -> dict:
        self.store.require_running()
        resource = self.store.get("resources/genie.json")
        if resource is None:
            raise ValueError("Onboard the dataset before asking Genie")
        space_id = resource["space_id"]
        message = self.client.genie.start_conversation_and_wait(
            space_id, question, timeout=timedelta(minutes=3)
        )
        answer = {"question": question, "text": [], "queries": [], "data": []}
        for attachment in message.attachments or []:
            if attachment.text:
                answer["text"].append(attachment.text.content)
            if attachment.query:
                answer["queries"].append(attachment.query.query)
                result = self.client.genie.get_message_attachment_query_result(
                    space_id, message.conversation_id, message.id, attachment.attachment_id
                )
                answer["data"].append(result.as_dict())
        return answer
