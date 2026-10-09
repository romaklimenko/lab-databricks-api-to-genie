import httpx

from weather_lab.github import GitHubPublisher


def test_publish_uses_exact_repository_url_and_safe_parent(monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    publisher = GitHubPublisher("example/weather")
    publisher.client.close()
    head_reads = 0
    paths = []

    def handler(request):
        nonlocal head_reads
        path = request.url.path
        paths.append(path)
        prefix = "/repos/example/weather"
        if path == prefix and request.method == "GET":
            return httpx.Response(200, json={"default_branch": "main"})
        if path == prefix + "/git/ref/heads/demo/test":
            head_reads += 1
            return httpx.Response(404 if head_reads == 1 else 200, json={"object": {"sha": "base"}})
        if path == prefix + "/git/ref/heads/main":
            return httpx.Response(200, json={"object": {"sha": "base"}})
        if path == prefix + "/commits":
            return httpx.Response(200, json=[])
        if path == prefix + "/git/commits/base":
            return httpx.Response(200, json={"tree": {"sha": "tree"}})
        if path == prefix + "/git/refs/heads/demo/test":
            assert b'"force":false' in request.content
        return httpx.Response(201, json={"sha": "new"})

    publisher.client = httpx.Client(
        base_url="https://api.github.com/repos/example/weather",
        transport=httpx.MockTransport(handler),
    )
    try:
        result = publisher.commit("test", {"manifest.json": b"{}"}, "source")
        assert result["sha"] == "new"
        assert "/repos/example/weather/" not in paths
    finally:
        publisher.close()
