"""Unit tests for DetectorTCPClient reconnect and timeout behavior."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from a1soft.ioc import DetectorTCPClient


@pytest.fixture
def tcp_client():
    """Create a DetectorTCPClient without connecting to anything."""
    return DetectorTCPClient(auto_reconnect=False)


@pytest.fixture
def mock_json_writer():
    """Create a mock StreamWriter that accepts writes without error."""
    writer = MagicMock()
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    writer.close = MagicMock()
    writer.wait_closed = AsyncMock()
    return writer


def _prepare_client_for_send(tcp_client, mock_json_writer):
    """Put the tcp_client into a state where send_command can write."""
    tcp_client.connected = True
    tcp_client.json_writer = mock_json_writer


@pytest.mark.asyncio
async def test_cleanup_connections_cancels_pending_responses(tcp_client):
    """_cleanup_connections must cancel all pending futures and clear the dict.

    This is the core fix: during reconnection, orphaned futures would previously
    cause send_command callers (like the sync scan) to hang forever.
    """
    future_a = asyncio.Future()
    future_b = asyncio.Future()
    tcp_client._pending_responses[1] = future_a
    tcp_client._pending_responses[2] = future_b

    await tcp_client._cleanup_connections()

    assert future_a.cancelled()
    assert future_b.cancelled()
    assert tcp_client._pending_responses == {}


@pytest.mark.asyncio
async def test_send_command_returns_none_on_timeout(tcp_client, mock_json_writer):
    """send_command must return None after RESPONSE_TIMEOUT, not hang forever.

    Uses a patched short timeout to keep the test fast.
    """
    _prepare_client_for_send(tcp_client, mock_json_writer)
    tcp_client.RESPONSE_TIMEOUT = 0.1  # Override for test speed

    result = await tcp_client.send_command("GET", parameter="*")

    assert result is None
    assert tcp_client._pending_responses == {}


@pytest.mark.asyncio
async def test_send_command_returns_none_on_cancelled_future(
    tcp_client, mock_json_writer
):
    """send_command must catch CancelledError from an externally cancelled future.

    This happens when _cleanup_connections cancels pending futures during reconnect.
    The CancelledError must NOT propagate up to the caller (e.g. the sync scan).
    """
    _prepare_client_for_send(tcp_client, mock_json_writer)

    async def cancel_pending_after_delay():
        await asyncio.sleep(0.05)
        for future in tcp_client._pending_responses.values():
            if not future.done():
                future.cancel()

    asyncio.create_task(cancel_pending_after_delay())
    result = await tcp_client.send_command("GET", parameter="*")

    assert result is None


@pytest.mark.asyncio
async def test_send_command_returns_response_on_success(tcp_client, mock_json_writer):
    """Happy path: send_command returns the response dict when delivered normally."""
    _prepare_client_for_send(tcp_client, mock_json_writer)
    expected_response = {"id": 0, "cmd": "GET", "values": [{"name": "CenterKE"}]}

    async def deliver_response():
        await asyncio.sleep(0.05)
        # The command ID starts at 0, so the pending future is keyed on 0
        future = tcp_client._pending_responses.get(0)
        if future and not future.done():
            future.set_result(expected_response)

    asyncio.create_task(deliver_response())
    result = await tcp_client.send_command("GET", parameter="CenterKE")

    assert result == expected_response


@pytest.mark.asyncio
async def test_reconnect_unblocks_pending_send_command(tcp_client, mock_json_writer):
    """End-to-end scenario: _cleanup_connections during an in-flight send_command.

    This reproduces the actual bug: the sync scan calls send_command, the TCP
    connection drops mid-flight, _reconnect_loop calls _cleanup_connections,
    and send_command must return None instead of hanging forever.
    """
    _prepare_client_for_send(tcp_client, mock_json_writer)

    async def simulate_reconnect():
        # Wait for send_command to register its pending future
        await asyncio.sleep(0.05)
        assert len(tcp_client._pending_responses) == 1
        # Simulate what _reconnect_loop does
        await tcp_client._cleanup_connections()

    asyncio.create_task(simulate_reconnect())
    result = await tcp_client.send_command("GET", parameter="*")

    assert result is None
    assert tcp_client._pending_responses == {}
