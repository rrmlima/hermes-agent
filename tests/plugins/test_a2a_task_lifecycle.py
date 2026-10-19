"""A2A task lifecycle regression tests for long-running inbound turns."""

from __future__ import annotations

import asyncio
import threading

import pytest

from plugins.platforms.a2a import adapter as adapter_module
from plugins.platforms.a2a import protocol
from tests.plugins.test_a2a_plugin import _make_live_adapter, _post_json
from tests.waiting import async_wait_until


class TestImmediateSendLifecycle:
    def test_cancel_does_not_cross_talk_future(self, monkeypatch):
        adapter, base = _make_live_adapter(monkeypatch)

        async def run():
            assert await adapter.connect() is True
            # Create task
            adapter.tasks.create("t-cancel", "ctx-c", "peer")
            fut = adapter._add_pending("t-cancel", "ctx-c")
            cancel_resp = await asyncio.to_thread(_post_json, base + "/", {
                "jsonrpc": "2.0", "id": "can-1", "method": "CancelTask", "params": {"id": "t-cancel"}
            })
            assert cancel_resp["result"]["status"]["state"] == protocol.STATE_CANCELED
            assert not fut.done()  # future not contaminated
            await adapter.disconnect()

        asyncio.run(run())

    @pytest.mark.parametrize("return_immediately", [True, None])
    def test_long_turn_stays_working_then_persists_late_reply(self, monkeypatch, return_immediately):
        """Both an explicit immediate send and a blocking wait expiry keep a live turn queryable."""
        monkeypatch.delenv("A2A_BEARER_TOKEN", raising=False)
        monkeypatch.delenv("A2A_PEER_TOKENS", raising=False)
        monkeypatch.setattr(adapter_module, "_reply_timeout", lambda: 0.0)
        adapter, base = _make_live_adapter(monkeypatch)
        started = threading.Event()

        async def run():
            release = asyncio.Event()

            async def held_reply(event):
                started.set()
                await release.wait()
                await adapter.send(event.source.chat_id, "late but valid reply", metadata={"notify": True})

            adapter.handle_message = held_reply
            try:
                assert await adapter.connect() is True
                params = {"message": protocol.text_message(protocol.ROLE_USER, "long task")}
                if return_immediately is True:
                    params["configuration"] = {"returnImmediately": True}
                body = {"jsonrpc": "2.0", "id": "req-1", "method": "SendMessage", "params": params}
                response = await asyncio.to_thread(_post_json, base + "/", body)
                task = response["result"]["task"]
                assert task["status"]["state"] == protocol.STATE_WORKING
                assert started.wait(timeout=2)

                get_body = {"jsonrpc": "2.0", "id": "get-1", "method": "GetTask",
                            "params": {"id": task["id"]}}
                current = await asyncio.to_thread(_post_json, base + "/", get_body)
                assert current["result"]["status"]["state"] == protocol.STATE_WORKING

                release.set()

                def completed_task():
                    result = _post_json(base + "/", get_body)["result"]
                    return result if result["status"]["state"] == protocol.STATE_COMPLETED else None

                completed = await async_wait_until(completed_task, timeout=5, what="late task completion")
                assert protocol.extract_text(completed["artifacts"][0]) == "late but valid reply"
                assert adapter.tasks.get(task["id"])["reply"] == "late but valid reply"
            finally:
                release.set()
                await adapter.disconnect()

        asyncio.run(run())
