"""SignalR WebSocket client for Zaptec charger observations."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
import json
import logging
from typing import Any

import aiohttp

from .const import (
    CHARGER_EXCLUDES,
    SIGNALR_PING_INTERVAL,
    SIGNALR_RECONNECT_FACTOR,
    SIGNALR_RECONNECT_INIT_DELAY,
    SIGNALR_RECONNECT_MAX_DELAY,
    SIGNALR_RECORD_SEPARATOR,
    SIGNALR_WS_URL,
)

_LOGGER = logging.getLogger(__name__)

# Callback type: receives (device_id, observation_id, value, observed_at)
ObservationCallback = Callable[[str, int, str, datetime], Any]
EndOfFrameCallback = Callable[[], Any]


class SignalRClient:
    """SignalR WebSocket client for streaming charger observations.

    Connects to the Zaptec SignalR endpoint for a single charger device,
    handles the handshake, keepalive pings, and observation message parsing
    with timestamp-based deduplication.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        device_id: str,
        charger_id: str,
        access_token_factory: Callable[[], str | None],
        request_func: Callable,
        *,
        on_observation: ObservationCallback | None = None,
        on_end_of_frame: EndOfFrameCallback | None = None,
    ) -> None:
        """Initialize the SignalR client.

        Args:
            session: aiohttp client session for HTTP and WS connections.
            device_id: The charger device ID (e.g. "ZAP123456").
            charger_id: The charger UUID for REST API calls.
            access_token_factory: Callable returning the current access token.
            request_func: The Zaptec.request() method for fetching charger state.
            on_observation: Optional callback for each new observation.
            on_end_of_frame: Optional callback invoked at the end of each frame.
        """
        self._session = session
        self._device_id = device_id
        self._charger_id = charger_id
        self._access_token_factory = access_token_factory
        self._request_func = request_func
        self._on_observation = on_observation
        self._on_end_of_frame = on_end_of_frame

        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ping_task: asyncio.Task | None = None
        self._receive_task: asyncio.Task | None = None
        self._running = False
        self._cancel_event = asyncio.Event()

        # Dedup state: ObservationId -> (ObservedAt, Value)
        self._state: dict[int, tuple[datetime, str]] = {}

    @property
    def state(self) -> dict[int, tuple[datetime, str]]:
        """Return the current observation state."""
        return self._state

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self) -> asyncio.Task:
        """Start the SignalR connection in the background.

        Returns the receive task.
        """
        if self._running:
            raise RuntimeError("SignalR client is already running")
        self._cancel_event.clear()
        self._receive_task = asyncio.create_task(self._run())
        return self._receive_task

    async def stop(self) -> None:
        """Gracefully stop the SignalR connection."""
        self._cancel_event.set()
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        self._running = False

    async def cancel(self) -> None:
        """Cancel and cleanup. Alias for stop()."""
        await self.stop()

    async def _run(self) -> None:
        """Main connection loop with automatic reconnect on failure."""
        self._running = True
        delay = SIGNALR_RECONNECT_INIT_DELAY

        try:
            while not self._cancel_event.is_set():
                try:
                    await self._connect_and_receive()
                    # Clean exit (e.g. server sent close) — still reconnect
                    if self._cancel_event.is_set():
                        break
                    _LOGGER.info(
                        "SignalR connection closed for %s, reconnecting in %ds",
                        self._device_id,
                        delay,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    _LOGGER.exception(
                        "SignalR connection failed for %s, reconnecting in %ds",
                        self._device_id,
                        delay,
                    )

                # Wait before reconnecting (interruptible by cancel)
                try:
                    await asyncio.wait_for(
                        self._cancel_event.wait(), timeout=delay
                    )
                    break  # cancel_event was set
                except asyncio.TimeoutError:
                    pass  # Timeout expired, proceed with reconnect

                delay = min(delay * SIGNALR_RECONNECT_FACTOR, SIGNALR_RECONNECT_MAX_DELAY)

        except asyncio.CancelledError:
            _LOGGER.debug("SignalR connection cancelled for %s", self._device_id)
        finally:
            self._running = False
            _LOGGER.debug("SignalR connection stopped for %s", self._device_id)

    async def _connect_and_receive(self) -> None:
        """Single connection attempt: connect, handshake, receive."""
        token = self._access_token_factory()
        url = f"{SIGNALR_WS_URL}?deviceId={self._device_id}&access_token={token}"

        _LOGGER.debug("Connecting to SignalR for device %s", self._device_id)

        async with self._session.ws_connect(url) as ws:
            self._ws = ws

            try:
                # Handshake
                await self._handshake(ws)

                # Start keepalive pings
                self._ping_task = asyncio.create_task(self._ping_loop(ws))

                # Start receiving messages before fetching initial state
                # so no observations are lost during the REST call
                receive_task = asyncio.create_task(self._receive_loop(ws))

                # Fetch full state via REST (checks timestamps against
                # observations that may have already arrived via stream)
                await self._fetch_initial_state()

                # Await the receive loop
                await receive_task

            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except asyncio.CancelledError:
                        pass
                self._ws = None

    async def _handshake(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Perform the SignalR handshake."""
        handshake_msg = json.dumps({"protocol": "json", "version": 1}) + SIGNALR_RECORD_SEPARATOR
        await ws.send_str(handshake_msg)

        # Read handshake response
        msg = await ws.receive()
        if msg.type == aiohttp.WSMsgType.TEXT:
            # Expect {}<RS>
            parts = msg.data.split(SIGNALR_RECORD_SEPARATOR)
            for part in parts:
                if not part:
                    continue
                resp = json.loads(part)
                if "error" in resp:
                    raise ConnectionError(f"SignalR handshake failed: {resp['error']}")
            _LOGGER.debug("SignalR handshake complete for %s", self._device_id)
        else:
            raise ConnectionError(f"Unexpected handshake response type: {msg.type}")

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Send periodic ping messages."""
        ping_msg = json.dumps({"type": 6}) + SIGNALR_RECORD_SEPARATOR
        try:
            while not ws.closed and not self._cancel_event.is_set():
                await asyncio.sleep(SIGNALR_PING_INTERVAL)
                if not ws.closed:
                    await ws.send_str(ping_msg)
                    _LOGGER.debug("Sent ping for %s", self._device_id)
        except asyncio.CancelledError:
            pass
        except Exception:
            _LOGGER.exception("Ping loop error for %s", self._device_id)

    async def _fetch_initial_state(self) -> None:
        """Fetch full charger state via REST and populate the dedup map."""
        _LOGGER.debug("Fetching initial state for charger %s", self._charger_id)
        try:
            state_list = await self._request_func(
                f"chargers/{self._charger_id}/state"
            )
            if not state_list:
                return

            for item in state_list:
                state_id = item.get("StateId")
                if state_id is None:
                    continue
                if str(state_id) in CHARGER_EXCLUDES:
                    continue
                value = item.get("Value", item.get("ValueAsString", ""))
                timestamp_str = item.get("Timestamp")
                if timestamp_str:
                    try:
                        observed_at = datetime.fromisoformat(timestamp_str)
                        if observed_at.tzinfo is None:
                            observed_at = observed_at.replace(tzinfo=timezone.utc)
                    except ValueError:
                        observed_at = datetime.now(timezone.utc)
                else:
                    observed_at = datetime.now(timezone.utc)

                obs_id = int(state_id)
                # Check if stream already delivered a fresher value
                existing = self._state.get(obs_id)
                if existing is not None and existing[0] >= observed_at:
                    continue
                self._state[obs_id] = (observed_at, str(value))

                if self._on_observation:
                    try:
                        self._on_observation(
                            self._device_id, obs_id, str(value), observed_at
                        )
                    except Exception:
                        _LOGGER.exception("Observation callback error")

            _LOGGER.debug(
                "Loaded %d initial observations for %s",
                len(self._state),
                self._device_id,
            )
            if self._on_end_of_frame:
                try:
                    self._on_end_of_frame()
                except Exception:
                    _LOGGER.exception("End of frame callback error")
        except Exception:
            _LOGGER.exception(
                "Failed to fetch initial state for %s", self._charger_id
            )

    async def _receive_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Process incoming WebSocket messages.

        The server replies to every ping with a ping, so if no message
        arrives within ~2x the ping interval the connection is dead and
        should be closed to trigger a reconnect.
        """
        timeout = SIGNALR_PING_INTERVAL * 2
        while not self._cancel_event.is_set():
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=timeout)
            except asyncio.TimeoutError:
                _LOGGER.warning(
                    "No SignalR message from %s within %ds, closing connection",
                    self._device_id,
                    timeout,
                )
                break

            if self._cancel_event.is_set():
                break

            if msg.type == aiohttp.WSMsgType.TEXT:
                self._process_frame(msg.data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                _LOGGER.error(
                    "WebSocket error for %s: %s",
                    self._device_id,
                    ws.exception(),
                )
                break
            elif msg.type in (
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
            ):
                _LOGGER.debug("WebSocket closing for %s", self._device_id)
                break

    def _process_frame(self, data: str) -> None:
        """Process a WebSocket text frame which may contain multiple SignalR messages."""
        parts = data.split(SIGNALR_RECORD_SEPARATOR)
        observations = 0
        for part in parts:
            if not part:
                continue
            try:
                message = json.loads(part)
            except json.JSONDecodeError:
                _LOGGER.warning("Failed to parse SignalR message: %r", part)
                continue

            msg_type = message.get("type")
            if msg_type == 1:
                observations += 1
                self._handle_invocation(message)
            elif msg_type == 6:
                pass  # Ping response, nothing to do
            elif msg_type == 7:
                error = message.get("error", "")
                _LOGGER.warning(
                    "SignalR close message for %s: %s", self._device_id, error
                )
            else:
                _LOGGER.debug(
                    "Unknown SignalR message type %s for %s",
                    msg_type,
                    self._device_id,
                )

        if self._on_end_of_frame and observations > 0:
            try:
                self._on_end_of_frame()
            except Exception:
                _LOGGER.exception("End of frame callback error")

    def _handle_invocation(self, message: dict[str, Any]) -> None:
        """Handle a type 1 (Invocation) message."""
        target = message.get("target")
        if target != "notifyObservation":
            _LOGGER.debug("Unknown invocation target: %s", target)
            return

        arguments = message.get("arguments", [])
        if len(arguments) < 2:
            _LOGGER.warning("Invalid notifyObservation arguments: %s", arguments)
            return

        device_id = arguments[0]
        obs_data = arguments[1]

        obs_id = obs_data.get("ObservationId")
        observed_at_str = obs_data.get("ObservedAt")
        value = obs_data.get("Value")

        if obs_id is None or observed_at_str is None or value is None:
            _LOGGER.warning("Missing fields in observation: %s", obs_data)
            return

        obs_id = int(obs_id)

        try:
            observed_at = datetime.fromisoformat(observed_at_str)
            if observed_at.tzinfo is None:
                observed_at = observed_at.replace(tzinfo=timezone.utc)
        except ValueError:
            _LOGGER.warning("Invalid ObservedAt timestamp: %s", observed_at_str)
            return

        # Dedup: reject older timestamps
        existing = self._state.get(obs_id)
        if existing is not None:
            existing_ts, _ = existing
            if observed_at <= existing_ts:
                _LOGGER.debug(
                    "Ignoring stale observation %d: %s <= %s",
                    obs_id,
                    observed_at,
                    existing_ts,
                )
                return

        self._state[obs_id] = (observed_at, str(value))

        _LOGGER.debug(
            "Observation (%d) = %s for %s at %s",
            obs_id,
            value,
            device_id,
            observed_at,
        )

        if self._on_observation:
            try:
                self._on_observation(device_id, obs_id, str(value), observed_at)
            except Exception:
                _LOGGER.exception("Observation callback error")
