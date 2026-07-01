"""Tests for SignalR WebSocket client message handling."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest

from custom_components.zaptec.zaptec.const import SIGNALR_RECORD_SEPARATOR
from custom_components.zaptec.zaptec.signalr import SignalRClient

RS = SIGNALR_RECORD_SEPARATOR


def make_client(**kwargs) -> SignalRClient:
    """Create a SignalRClient for testing message processing methods."""
    defaults = dict(
        session=None,
        device_id="ZAP000001",
        charger_id="test-uuid",
        access_token_factory=lambda: "test-token",
        request_func=None,
    )
    defaults.update(kwargs)
    return SignalRClient(**defaults)


class TestMessageFraming:
    """Test SignalR message framing and parsing."""

    def test_single_message_frame(self):
        """Test parsing a single-message frame with RS terminator."""
        client = make_client()
        msg = json.dumps({"type": 6}) + RS
        client._process_frame(msg)
        # Ping message (type 6) is silently ignored, no state change
        assert len(client.state) == 0

    def test_multi_message_frame(self):
        """Test parsing multiple messages in one frame separated by RS."""
        client = make_client()
        obs1 = {
            "type": 1,
            "target": "notifyObservation",
            "arguments": [
                "ZAP000001",
                {
                    "ObservationId": 513,
                    "ObservedAt": "2026-04-06T10:00:00Z",
                    "Value": "100.0",
                    "OldValue": "50.0",
                },
            ],
        }
        obs2 = {
            "type": 1,
            "target": "notifyObservation",
            "arguments": [
                "ZAP000001",
                {
                    "ObservationId": 509,
                    "ObservedAt": "2026-04-06T10:00:01Z",
                    "Value": "5.5",
                    "OldValue": "3.2",
                },
            ],
        }
        frame = json.dumps(obs1) + RS + json.dumps(obs2) + RS
        client._process_frame(frame)

        assert 513 in client.state
        assert 509 in client.state
        assert client.state[513][1] == "100.0"
        assert client.state[509][1] == "5.5"

    def test_frame_with_three_messages(self):
        """Test three messages in one frame."""
        client = make_client()
        msgs = []
        for obs_id, val in [(513, "10"), (509, "20"), (507, "30")]:
            msgs.append(
                json.dumps(
                    {
                        "type": 1,
                        "target": "notifyObservation",
                        "arguments": [
                            "ZAP000001",
                            {
                                "ObservationId": obs_id,
                                "ObservedAt": "2026-04-06T10:00:00Z",
                                "Value": val,
                                "OldValue": None,
                            },
                        ],
                    }
                )
            )
        frame = RS.join(msgs) + RS
        client._process_frame(frame)
        assert len(client.state) == 3

    def test_empty_parts_ignored(self):
        """Test that empty strings between RS separators are ignored."""
        client = make_client()
        frame = RS + RS + json.dumps({"type": 6}) + RS + RS
        client._process_frame(frame)
        assert len(client.state) == 0

    def test_invalid_json_skipped(self):
        """Test that invalid JSON is skipped without crashing."""
        client = make_client()
        frame = "not-json" + RS + json.dumps({"type": 6}) + RS
        client._process_frame(frame)
        assert len(client.state) == 0


class TestPingHandling:
    """Test ping (type 6) message handling."""

    def test_ping_is_ignored(self):
        """Test that ping messages don't affect state."""
        client = make_client()
        frame = json.dumps({"type": 6}) + RS
        client._process_frame(frame)
        assert len(client.state) == 0


class TestObservationParsing:
    """Test observation message (type 1) parsing."""

    def test_valid_observation(self):
        """Test parsing a valid notifyObservation message."""
        client = make_client()
        msg = {
            "type": 1,
            "target": "notifyObservation",
            "arguments": [
                "ZAP000001",
                {
                    "ObservationId": 513,
                    "ObservedAt": "2026-04-06T11:39:53.592913Z",
                    "Value": "5752.139",
                    "OldValue": "1333.673",
                },
            ],
        }
        frame = json.dumps(msg) + RS
        client._process_frame(frame)

        assert 513 in client.state
        ts, val = client.state[513]
        assert val == "5752.139"
        assert ts.year == 2026

    def test_observation_with_all_fields(self):
        """Test that all observation fields are handled correctly."""
        received = []

        def on_obs(device_id, obs_id, value, observed_at):
            received.append((device_id, obs_id, value, observed_at))

        client = make_client(on_observation=on_obs)
        msg = {
            "type": 1,
            "target": "notifyObservation",
            "arguments": [
                "ZAP000001",
                {
                    "ObservationId": 509,
                    "ObservedAt": "2026-04-06T10:00:00.123456Z",
                    "Value": "11.732",
                    "OldValue": "0.473",
                },
            ],
        }
        frame = json.dumps(msg) + RS
        client._process_frame(frame)

        assert len(received) == 1
        device_id, obs_id, value, observed_at = received[0]
        assert device_id == "ZAP000001"
        assert obs_id == 509
        assert value == "11.732"
        assert observed_at.microsecond == 123456

    def test_missing_observation_fields_skipped(self):
        """Test that observations with missing required fields are skipped."""
        client = make_client()
        msg = {
            "type": 1,
            "target": "notifyObservation",
            "arguments": [
                "ZAP000001",
                {
                    "ObservationId": 513,
                    # Missing ObservedAt and Value
                },
            ],
        }
        frame = json.dumps(msg) + RS
        client._process_frame(frame)
        assert len(client.state) == 0

    def test_unknown_invocation_target_ignored(self):
        """Test that unknown invocation targets are ignored."""
        client = make_client()
        msg = {
            "type": 1,
            "target": "unknownMethod",
            "arguments": ["some", "data"],
        }
        frame = json.dumps(msg) + RS
        client._process_frame(frame)
        assert len(client.state) == 0


class TestCloseMessage:
    """Test close (type 7) message handling."""

    def test_close_message_handled(self):
        """Test that close messages are processed without error."""
        client = make_client()
        msg = {"type": 7, "error": "Server shutting down"}
        frame = json.dumps(msg) + RS
        # Should not raise
        client._process_frame(frame)
        assert len(client.state) == 0

    def test_close_message_without_error(self):
        """Test close message with no error field."""
        client = make_client()
        msg = {"type": 7}
        frame = json.dumps(msg) + RS
        client._process_frame(frame)
        assert len(client.state) == 0


class TestEndOfFrameCallback:
    """Test the on_end_of_frame callback invoked at the end of each frame."""

    def test_callback_invoked_once_per_frame(self):
        """on_end_of_frame should be called once after processing a frame."""
        calls = []
        client = make_client(on_end_of_frame=lambda: calls.append(1))
        msg = {
            "type": 1,
            "target": "notifyObservation",
            "arguments": [
                "ZAP000001",
                {
                    "ObservationId": 513,
                    "ObservedAt": "2026-04-06T10:00:00Z",
                    "Value": "100.0",
                },
            ],
        }
        frame = json.dumps(msg) + RS
        client._process_frame(frame)
        assert len(calls) == 1

    def test_callback_invoked_once_for_multi_message_frame(self):
        """on_end_of_frame should fire once even with multiple messages."""
        calls = []
        client = make_client(on_end_of_frame=lambda: calls.append(1))
        msgs = []
        for obs_id, val in [(513, "10"), (509, "20")]:
            msgs.append(
                json.dumps(
                    {
                        "type": 1,
                        "target": "notifyObservation",
                        "arguments": [
                            "ZAP000001",
                            {
                                "ObservationId": obs_id,
                                "ObservedAt": "2026-04-06T10:00:00Z",
                                "Value": val,
                            },
                        ],
                    }
                )
            )
        frame = RS.join(msgs) + RS
        client._process_frame(frame)
        assert len(calls) == 1
        assert len(client.state) == 2

    def test_callback_not_invoked_when_none(self):
        """on_end_of_frame should not be called when not provided."""
        client = make_client()  # no on_end_of_frame
        msg = {
            "type": 1,
            "target": "notifyObservation",
            "arguments": [
                "ZAP000001",
                {
                    "ObservationId": 513,
                    "ObservedAt": "2026-04-06T10:00:00Z",
                    "Value": "100.0",
                },
            ],
        }
        frame = json.dumps(msg) + RS
        # Should not raise
        client._process_frame(frame)
        assert len(client.state) == 1

    def test_callback_exception_does_not_propagate(self):
        """An exception in on_end_of_frame should not propagate."""
        def boom():
            raise RuntimeError("boom")

        client = make_client(on_end_of_frame=boom)
        msg = {
            "type": 1,
            "target": "notifyObservation",
            "arguments": [
                "ZAP000001",
                {
                    "ObservationId": 513,
                    "ObservedAt": "2026-04-06T10:00:00Z",
                    "Value": "100.0",
                },
            ],
        }
        frame = json.dumps(msg) + RS
        # Should not raise
        client._process_frame(frame)
        assert len(client.state) == 1

    def test_callback_not_invoked_for_ping_only_frame(self):
        """on_end_of_frame should not fire for a ping-only frame."""
        calls = []
        client = make_client(on_end_of_frame=lambda: calls.append(1))
        frame = json.dumps({"type": 6}) + RS
        client._process_frame(frame)
        assert len(calls) == 0
        assert len(client.state) == 0

    def test_callback_invoked_after_observations(self):
        """on_end_of_frame should fire after observations are stored."""
        order = []

        def on_obs(*args):
            order.append("obs")

        def on_eof():
            order.append("eof")

        client = make_client(on_observation=on_obs, on_end_of_frame=on_eof)
        msg = {
            "type": 1,
            "target": "notifyObservation",
            "arguments": [
                "ZAP000001",
                {
                    "ObservationId": 513,
                    "ObservedAt": "2026-04-06T10:00:00Z",
                    "Value": "100.0",
                },
            ],
        }
        frame = json.dumps(msg) + RS
        client._process_frame(frame)
        assert order == ["obs", "eof"]


class TestFetchInitialStateEndOfFrame:
    """Test on_end_of_frame is called at the end of _fetch_initial_state."""

    @pytest.mark.asyncio
    async def test_end_of_frame_called_after_initial_state(self):
        """on_end_of_frame should fire after _fetch_initial_state completes."""
        calls = []
        state_list = [
            {
                "StateId": 513,
                "Value": "100.0",
                "Timestamp": "2026-04-06T10:00:00Z",
            }
        ]

        async def request_func(path):
            return state_list

        client = make_client(
            request_func=request_func,
            on_end_of_frame=lambda: calls.append(1),
        )
        await client._fetch_initial_state()
        assert len(calls) == 1
        assert 513 in client.state

    @pytest.mark.asyncio
    async def test_end_of_frame_exception_does_not_propagate(self):
        """An exception in on_end_of_frame should not propagate."""
        def boom():
            raise RuntimeError("boom")

        state_list = [
            {
                "StateId": 513,
                "Value": "100.0",
                "Timestamp": "2026-04-06T10:00:00Z",
            }
        ]

        async def request_func(path):
            return state_list

        client = make_client(
            request_func=request_func,
            on_end_of_frame=boom,
        )
        # Should not raise
        await client._fetch_initial_state()
        assert 513 in client.state

    @pytest.mark.asyncio
    async def test_end_of_frame_not_called_on_request_failure(self):
        """on_end_of_frame should not fire when the REST request fails."""
        calls = []

        async def request_func(path):
            raise RuntimeError("request failed")

        client = make_client(
            request_func=request_func,
            on_end_of_frame=lambda: calls.append(1),
        )
        # Should not raise; failure is logged internally
        await client._fetch_initial_state()
        assert len(calls) == 0
        assert len(client.state) == 0
