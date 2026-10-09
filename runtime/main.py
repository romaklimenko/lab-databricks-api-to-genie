"""Chat and invocation API for the weather supervisor."""

import asyncio
import os
from functools import lru_cache

import uvicorn
from databricks_agentkit import DurableAgentServer, InvocationContext
from databricks_agentkit.langgraph import start_trace
from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

from weather_lab.cli import configure_traces, make_service
from weather_lab.config import ROOT, Settings
from weather_lab.contracts import Intent


class ChatInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    prompt: str = Field(min_length=1, max_length=5000)
    inject_fault: bool = False


@lru_cache(maxsize=1)
def service():
    return make_service(Settings.from_env())[0]


def create_app(service_factory=service):
    app = DurableAgentServer()

    async def invoke(value, context: InvocationContext):
        payload = ChatInput.model_validate(value)
        lab = service_factory()
        with start_trace(
            "weather_chat", inputs=payload.model_dump(), session_id=context.session_id
        ) as span:
            intent = await lab.brain.structured(
                "request router",
                "Use onboard only for a request to load/configure the supported fixed dataset: Copenhagen, Aarhus, Odense, September 21-27, 2026. Use ask for data questions. Clarify requests for other cities, periods, APIs, or undefined preferences. All operations stay inside the configured lab.",
                {"prompt": payload.prompt},
                Intent,
            )
            if intent.action == "clarify":
                result = {"text": intent.reason, "status": "needs clarification"}
            elif intent.action == "ask":
                if lab.cloud is None:
                    result = {
                        "text": "Genie requires cloud mode. Use weather-lab replay for offline reference results.",
                        "status": "not configured",
                    }
                else:
                    await context.emit(
                        {
                            "role": "Genie",
                            "phase": "query",
                            "detail": "Querying the configured weather dataset.",
                        }
                    )
                    result = await asyncio.to_thread(
                        lab.cloud.ask, intent.question or payload.prompt
                    )
            else:
                result = await lab.onboard(
                    payload.prompt, context.emit, inject_fault=payload.inject_fault
                )
            if span:
                span.set_outputs(result)
            return result

    app.invoke(invoke)
    # No automatic worker recovery in v1. Persisted receipts allow a deliberate retry
    # without repeating completed external phases. Runtime transport still supports reconnect.

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(ROOT / "ui/index.html")

    @app.get("/api/whoami")
    async def whoami(request: Request):
        return {
            "viewer": request.headers.get("x-forwarded-email", "Local developer"),
            "execution_identity": "App service principal"
            if os.getenv("DATABRICKS_APP_NAME")
            else "Configured local Databricks profile",
            "target": Settings.from_env().target,
        }

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


def main():
    load_dotenv(ROOT / ".env", override=False)
    configure_traces()
    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", "8000")))
    host = "0.0.0.0" if os.getenv("DATABRICKS_APP_NAME") else "127.0.0.1"
    uvicorn.run(create_app(), host=host, port=port)
