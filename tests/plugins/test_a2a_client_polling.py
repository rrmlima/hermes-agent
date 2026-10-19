"""Client-side tests for immediate return and polling via a2a_call."""

from __future__ import annotations

import pytest

from plugins.platforms.a2a import tools


class TestA2aClientPolling:
    def test_a2a_call_rejects_both_message_and_task_id(self):
        res = tools.a2a_call({"agent": "peer", "message": "msg", "task_id": "t-1"})
        assert "Error: cannot provide both 'message' and 'task_id'" in res

    def test_a2a_call_polls_to_completion(self, monkeypatch):
        calls = []

        def fake_fetch_card(base_url, headers, timeout):
            return {"url": base_url, "supportedInterfaces": [{"protocolBinding": "JSONRPC", "url": base_url + "/rpc"}]}

        def fake_http_post_json(url, body, headers, timeout):
            calls.append(body)
            method = body["method"]
            if method == "SendMessage":
                return {
                    "result": {
                        "task": {
                            "id": "task-xyz",
                            "contextId": "ctx-1",
                            "status": {"state": "TASK_STATE_WORKING"},
                        }
                    }
                }
            if method == "GetTask":
                return {
                    "result": {
                        "task": {
                            "id": "task-xyz",
                            "contextId": "ctx-1",
                            "status": {"state": "TASK_STATE_COMPLETED"},
                            "artifacts": [{"artifactId": "a1", "parts": [{"text": "polled result"}]}],
                        }
                    }
                }
            raise AssertionError(f"unexpected method {method}")

        monkeypatch.setattr(tools, "_fetch_card", fake_fetch_card)
        monkeypatch.setattr(tools, "_http_post_json", fake_http_post_json)
        monkeypatch.setattr(tools, "_configured_peers", lambda: {"peer-vps": {"url": "http://peer.internal"}})

        # Fast forward sleep
        monkeypatch.setattr(tools, "_sleep", lambda s: None)

        out = tools.a2a_call({"agent": "peer-vps", "message": "compute something"})
        assert "[peer-vps · context ctx-1 · task task-xyz · completed]" in out
        assert "polled result" in out
        assert len(calls) == 2

    def test_a2a_orchestrate_with_return_immediately_peer(self, monkeypatch):
        monkeypatch.setattr(tools, "_configured_peers", lambda: {
            "p1": {"url": "http://p1.internal", "capabilities": ["test"]},
        })
        # Simulate a2a_call answering working first then completed on poll
        calls = 0

        def fake_call(args):
            nonlocal calls
            calls += 1
            if calls == 1:
                return "[p1 · context ctx-1 · task t-1 · working]\nThe task is currently in progress"
            return "[p1 · context ctx-1 · task t-1 · completed]\nfinal result"

        monkeypatch.setattr(tools, "a2a_call", fake_call)
        res = tools.a2a_orchestrate({"capability": "test", "message": "ping", "mode": "all"})
        assert "final result" not in res  # first call was working, so not treated as success
