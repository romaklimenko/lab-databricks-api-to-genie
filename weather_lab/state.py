"""Phase receipts on local disk or a Unity Catalog volume."""

import io
import json
import re
from pathlib import Path

from weather_lab.source import encoded


class RunStore:
    def __init__(self, root: Path, workspace=None, volume: str = ""):
        self.root, self.workspace, self.volume = root, workspace, volume.rstrip("/")

    def _path(self, key: str) -> str:
        if (
            not re.fullmatch(r"[a-zA-Z0-9_./-]+", key)
            or ".." in key.split("/")
            or key.startswith("/")
        ):
            raise ValueError("Invalid state key")
        return key

    def get(self, key: str):
        value = self.get_bytes(key)
        return json.loads(value) if value is not None else None

    def get_bytes(self, key: str):
        key = self._path(key)
        if self.volume:
            from databricks.sdk.errors import NotFound

            try:
                response = self.workspace.files.download(f"{self.volume}/{key}")
                with response.contents as stream:
                    return stream.read()
            except NotFound:
                return None
        path = self.root / key
        return path.read_bytes() if path.exists() else None

    def put(self, key: str, value):
        self.put_bytes(key, encoded(value))

    def require_running(self):
        if (self.get("resources/control.json") or {}).get("stopped"):
            raise RuntimeError("Lab is stopped. Run weather-lab start before new cloud work.")

    def put_bytes(self, key: str, value: bytes):
        key = self._path(key)
        if self.volume:
            remote = f"{self.volume}/{key}"
            self.workspace.files.create_directory(remote.rsplit("/", 1)[0])
            self.workspace.files.upload(remote, io.BytesIO(value), overwrite=True)
        else:
            path = self.root / key
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_bytes(value)
            temporary.replace(path)


class Receipt:
    def __init__(self, store: RunStore, run_id: str, fingerprint: str):
        self.store, self.key = store, f"runs/{run_id}/receipt.json"
        self.data = store.get(self.key) or {"fingerprint": fingerprint, "phases": {}, "events": []}
        if self.data["fingerprint"] != fingerprint:
            raise ValueError("Run ID already belongs to different inputs")

    def save(self):
        self.store.put(self.key, self.data)

    def completed(self, phase: str):
        return self.data["phases"].get(phase)

    def finish(self, phase: str, result):
        self.data["phases"][phase] = result
        self.save()
        return result
