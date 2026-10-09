"""Commit an explicit public artifact set on a dedicated GitHub branch."""

import base64
import os
import subprocess

import httpx

from weather_lab.source import digest


class GitHubPublisher:
    def __init__(self, repository: str):
        self.repository = repository
        token = os.getenv("GITHUB_TOKEN")
        if not token:
            result = subprocess.run(
                ["gh", "auth", "token"], check=True, capture_output=True, text=True
            )
            token = result.stdout.strip()
        self.client = httpx.Client(
            base_url=f"https://api.github.com/repos/{repository}",
            timeout=30,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        )

    def _get(self, path):
        response = self.client.get(path)
        response.raise_for_status()
        return response.json()

    def _post(self, path, value):
        response = self.client.post(path, json=value)
        response.raise_for_status()
        return response.json()

    def commit(self, run_id: str, files: dict[str, bytes], phase: str) -> dict:
        allowed = {
            "snapshot.json",
            "ingestion.json",
            "contract.json",
            "genie.json",
            "ingest.py",
            "manifest.json",
            "result.json",
        }
        if not set(files).issubset(allowed) or not files:
            raise ValueError("Publisher accepts only known artifact filenames")
        branch = f"demo/{run_id}"
        fingerprint = digest({name: value.hex() for name, value in files.items()})
        message = f"Save weather {phase}\n\nArtifact content: {fingerprint}"
        response = self.client.get(f"/git/ref/heads/{branch}")
        if response.status_code == 404:
            default = self._get(f"https://api.github.com/repos/{self.repository}")["default_branch"]
            base = self._get(f"/git/ref/heads/{default}")["object"]["sha"]
            created = self.client.post(
                "/git/refs", json={"ref": f"refs/heads/{branch}", "sha": base}
            )
            if created.status_code != 422:
                created.raise_for_status()
            head = self._get(f"/git/ref/heads/{branch}")["object"]["sha"]
        else:
            response.raise_for_status()
            head = response.json()["object"]["sha"]
        # Search a bounded task-branch history after a lost response, including an earlier source commit.
        for commit in self._get(f"/commits?sha={branch}&per_page=20"):
            if commit["commit"]["message"] == message:
                return {"sha": commit["sha"], "branch": branch, "reused": True}
        tree = self._get(f"/git/commits/{head}")["tree"]["sha"]
        entries = []
        for name, value in files.items():
            blob = self._post(
                "/git/blobs", {"content": base64.b64encode(value).decode(), "encoding": "base64"}
            )
            entries.append(
                {
                    "path": f"artifacts/{run_id}/{name}",
                    "mode": "100644",
                    "type": "blob",
                    "sha": blob["sha"],
                }
            )
        new_tree = self._post("/git/trees", {"base_tree": tree, "tree": entries})["sha"]
        commit = self._post(
            "/git/commits", {"message": message, "tree": new_tree, "parents": [head]}
        )
        if self._get(f"/git/ref/heads/{branch}")["object"]["sha"] != head:
            raise RuntimeError("Task branch changed concurrently; refusing to overwrite it")
        response = self.client.patch(
            f"/git/refs/heads/{branch}", json={"sha": commit["sha"], "force": False}
        )
        response.raise_for_status()
        return {"sha": commit["sha"], "branch": branch, "reused": False}

    def close(self):
        self.client.close()
