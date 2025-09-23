#!/usr/bin/env python3
"""
Caproto server implementing TCP interface to LabView detector system.
Exposes EPICS PVs that control acquisition, parameters, and monitoring.
"""

import asyncio
import json
import logging
import time
from pathlib import Path
from textwrap import dedent
from typing import Any, Literal

from nexusformat.nexus import (
    NXdata,
    NXfield,
    NXroot,
    NXentry,
    nxopen,
    NXdetector,
    NXinstrument,
    NXlink,
)
from caproto.server import PVGroup, ioc_arg_parser, pvproperty, run, PvpropertyData
from caproto import ChannelType
import numpy as np

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DetectorTCPClient:
    """TCP client to communicate with LabView detector system using asyncio streams."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        json_port: int = 1234,
        data_port: int = 1235,
        live_port: int = 1236,
        auto_reconnect: bool = True,
    ) -> None:
        self.host: str = host
        self.json_port: int = json_port
        self.data_port: int = data_port
        self.live_port: int = live_port
        self.auto_reconnect: bool = auto_reconnect

        # StreamReader/StreamWriter pairs for each connection
        self.json_reader: asyncio.StreamReader | None = None
        self.json_writer: asyncio.StreamWriter | None = None
        self.data_reader: asyncio.StreamReader | None = None
        self.data_writer: asyncio.StreamWriter | None = None
        self.live_reader: asyncio.StreamReader | None = None
        self.live_writer: asyncio.StreamWriter | None = None

        self.connected: bool = False
        self._json_lock: asyncio.Lock = asyncio.Lock()
        self._data_lock: asyncio.Lock = asyncio.Lock()

        # Response handling infrastructure
        self._response_reader_task: asyncio.Task | None = None
        self._pending_responses: dict[int, asyncio.Future] = {}
        self._response_reader_running: bool = False
        self._data_reader_task: asyncio.Task | None = None
        self._data_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._data_reader_running: bool = False
        self._current_cmd_id = 0

        # Live data monitoring infrastructure
        self._live_reader_task: asyncio.Task | None = None
        self._live_queue: asyncio.Queue = asyncio.Queue(
            maxsize=1
        )  # Only store the latest frame
        self._live_reader_running: bool = False

        # Auto-reconnect infrastructure
        self._reconnect_task: asyncio.Task | None = None
        self._reconnect_event: asyncio.Event = asyncio.Event()
        self._shutdown_event: asyncio.Event = asyncio.Event()

        # Statistics infrastructure
        self._stats_task: asyncio.Task | None = None
        self._json_bytes_received: int = 0
        self._data_bytes_received: int = 0
        self._live_bytes_received: int = 0
        self._json_messages_processed: int = 0
        self._data_frames_processed: int = 0
        self._live_frames_processed: int = 0

    @property
    def _cmd_id(self) -> int:
        return self._current_cmd_id

    @_cmd_id.setter
    def _cmd_id(self, value: int) -> None:
        if value > 100000:
            self._current_cmd_id = 0
        else:
            self._current_cmd_id = value

    async def get_data(self) -> dict[str, Any] | None:
        """Get data from the data queue."""
        return await self._data_queue.get()

    async def get_live_data(self) -> dict[str, Any] | None:
        """Get live data from the live queue (non-blocking)."""
        try:
            return self._live_queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def connect(self) -> None:
        """Connect to all three TCP ports using asyncio streams."""
        try:
            # Connect to JSON port
            self.json_reader, self.json_writer = await asyncio.open_connection(
                self.host, self.json_port
            )

            # Connect to data port
            self.data_reader, self.data_writer = await asyncio.open_connection(
                self.host, self.data_port
            )

            # Connect to live port
            self.live_reader, self.live_writer = await asyncio.open_connection(
                self.host, self.live_port
            )

            self.connected = True

            # Start background response reader
            self._response_reader_running = True
            self._response_reader_task = asyncio.create_task(
                self._response_reader_loop()
            )

            # Start background data reader
            self._data_reader_running = True
            self._data_reader_task = asyncio.create_task(self._data_reader_loop())

            # Start background live data reader
            self._live_reader_running = True
            self._live_reader_task = asyncio.create_task(self._live_reader_loop())

            # Start auto-reconnect task if enabled
            if self.auto_reconnect and not self._reconnect_task:
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())

            # Start statistics monitoring task
            self._stats_task = asyncio.create_task(self._stats_monitor_loop())

            logger.info("Connected to detector TCP interface using asyncio streams")
        except Exception as e:
            logger.error(f"Failed to connect to detector: {e}")
            await self._cleanup_connections()
            self.connected = False

    async def _reconnect_loop(self) -> None:
        """Background task that handles automatic reconnection."""
        logger.info("Auto-reconnect loop started")

        while not self._shutdown_event.is_set():
            try:
                # Wait for reconnection to be requested
                await self._reconnect_event.wait()

                if self._shutdown_event.is_set():
                    break

                logger.info("Connection lost, attempting to reconnect...")

                # Keep trying to reconnect indefinitely
                while not self._shutdown_event.is_set():
                    try:
                        # Clean up existing connections
                        await self._cleanup_connections()

                        # Clear the reconnect event before attempting connection
                        self._reconnect_event.clear()

                        # Attempt to connect
                        await self._attempt_connection()

                        if self.connected:
                            logger.info("Successfully reconnected to detector")
                            break
                        else:
                            logger.info(
                                "Reconnection attempt failed, retrying in 2 seconds..."
                            )
                            await asyncio.sleep(2.0)

                    except Exception as e:
                        logger.debug(f"Reconnection attempt failed: {e}")
                        await asyncio.sleep(2.0)

            except asyncio.CancelledError:
                logger.info("Auto-reconnect loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in reconnect loop: {e}")
                await asyncio.sleep(2.0)

        logger.info("Auto-reconnect loop stopped")

    async def _attempt_connection(self) -> None:
        """Attempt to establish connection without starting background tasks."""
        try:
            # Connect to JSON port
            self.json_reader, self.json_writer = await asyncio.open_connection(
                self.host, self.json_port
            )

            # Connect to data port
            self.data_reader, self.data_writer = await asyncio.open_connection(
                self.host, self.data_port
            )

            # Connect to live port
            self.live_reader, self.live_writer = await asyncio.open_connection(
                self.host, self.live_port
            )

            self.connected = True

            # Start background response reader
            self._response_reader_running = True
            self._response_reader_task = asyncio.create_task(
                self._response_reader_loop()
            )

            # Start background data reader
            self._data_reader_running = True
            self._data_reader_task = asyncio.create_task(self._data_reader_loop())

            # Start background live data reader
            self._live_reader_running = True
            self._live_reader_task = asyncio.create_task(self._live_reader_loop())

            # Start statistics monitoring task
            self._stats_task = asyncio.create_task(self._stats_monitor_loop())

        except Exception as e:
            await self._cleanup_connections()
            self.connected = False
            raise e

    def _trigger_reconnect(self) -> None:
        """Trigger reconnection attempt."""
        if self.auto_reconnect and not self._shutdown_event.is_set():
            self.connected = False
            self._reconnect_event.set()

    async def _cleanup_connections(self) -> None:
        """Helper to close all stream connections."""
        for writer in [self.json_writer, self.data_writer, self.live_writer]:
            if writer:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception as e:
                    logger.debug(f"Error closing writer: {e}")

        self.json_reader = self.json_writer = None
        self.data_reader = self.data_writer = None
        self.live_reader = self.live_writer = None

    async def disconnect(self) -> None:
        """Disconnect from all streams."""
        # Signal shutdown to prevent reconnection
        self._shutdown_event.set()

        # Stop reconnect task
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        # Stop response reader
        self._response_reader_running = False
        if self._response_reader_task:
            self._response_reader_task.cancel()
            try:
                await self._response_reader_task
            except asyncio.CancelledError:
                pass
            self._response_reader_task = None

        # Stop data reader
        self._data_reader_running = False
        if self._data_reader_task:
            self._data_reader_task.cancel()
            try:
                await self._data_reader_task
            except asyncio.CancelledError:
                pass
            self._data_reader_task = None

        # Stop live data reader
        self._live_reader_running = False
        if self._live_reader_task:
            self._live_reader_task.cancel()
            try:
                await self._live_reader_task
            except asyncio.CancelledError:
                pass
            self._live_reader_task = None

        # Stop statistics monitor
        if self._stats_task:
            self._stats_task.cancel()
            try:
                await self._stats_task
            except asyncio.CancelledError:
                pass
            self._stats_task = None

        # Clean up pending responses
        for future in self._pending_responses.values():
            if not future.done():
                future.cancel()
        self._pending_responses.clear()

        # Close all connections
        await self._cleanup_connections()
        self.connected = False

    async def _response_reader_loop(self) -> None:
        """Background task that waits for signals and then reads responses."""
        logger.info("Starting event-driven response reader")

        while (
            self._response_reader_running
            and self.connected
            and not self._shutdown_event.is_set()
        ):
            try:
                if not self.json_reader:
                    break

                response = await self._read_response()
                if response is None:
                    continue

                # Extract command ID from response
                cmd_id = response.get("id")
                if cmd_id is not None:
                    # Route response to waiting task
                    future = self._pending_responses.pop(cmd_id, None)
                    if future and not future.done():
                        future.set_result(response)
                    else:
                        logger.debug(
                            f"Received response for unknown/expired command ID {cmd_id}"
                        )
                else:
                    logger.warning(f"Received response without ID: {response}")

            except asyncio.CancelledError:
                logger.info("Response reader cancelled")
                break
            except (
                ConnectionResetError,
                ConnectionAbortedError,
                BrokenPipeError,
                OSError,
            ) as e:
                logger.warning(f"Connection error in response reader: {e}")
                self._trigger_reconnect()
                break
            except Exception as e:
                logger.error(f"Error in response reader: {e}")
                await asyncio.sleep(0.1)  # Brief delay before retrying

        logger.info("Event-driven response reader stopped")

    async def _data_reader_loop(self) -> None:
        logger.info("Starting data reader loop")
        while (
            self._data_reader_running
            and self.connected
            and not self._shutdown_event.is_set()
        ):
            try:
                if not self.data_reader:
                    break

                response = await self._read_data()
                if response is None:
                    continue

                self._data_queue.put_nowait(response)
            except asyncio.CancelledError:
                logger.info("Data reader loop cancelled")
                break
            except (
                ConnectionResetError,
                ConnectionAbortedError,
                BrokenPipeError,
                OSError,
            ) as e:
                logger.warning(f"Connection error in data reader: {e}")
                self._trigger_reconnect()
                break
            except Exception as e:
                logger.error(f"Error in data reader loop: {e}")
                await asyncio.sleep(0.1)

        logger.info("Data reader loop stopped")

    async def _live_reader_loop(self) -> None:
        """Background task that continuously reads live data from live_port."""
        logger.info("Starting live data reader loop")
        while (
            self._live_reader_running
            and self.connected
            and not self._shutdown_event.is_set()
        ):
            try:
                if not self.live_reader:
                    break

                live_data = await self._read_live_data()
                if live_data is None:
                    continue

                # Put live data in queue, drop oldest if full (non-blocking)
                try:
                    self._live_queue.put_nowait(live_data)
                except asyncio.QueueFull:
                    # Drop oldest data to make room for newest
                    try:
                        self._live_queue.get_nowait()
                        self._live_queue.put_nowait(live_data)
                    except asyncio.QueueEmpty:
                        pass  # Queue was emptied by another task

            except asyncio.CancelledError:
                logger.info("Live data reader cancelled")
                break
            except (
                ConnectionResetError,
                ConnectionAbortedError,
                BrokenPipeError,
                OSError,
            ) as e:
                logger.warning(f"Connection error in live data reader: {e}")
                self._trigger_reconnect()
                break
            except Exception as e:
                logger.error(f"Error in live data reader loop: {e}")
                await asyncio.sleep(0.1)  # Brief delay before retrying

        logger.info("Live data reader loop stopped")

    async def _stats_monitor_loop(self) -> None:
        """Background task that prints statistics every 30 seconds."""
        logger.info("Starting statistics monitor loop")
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(30.0)

                # Calculate totals
                total_bytes = (
                    self._json_bytes_received
                    + self._data_bytes_received
                    + self._live_bytes_received
                )
                total_messages = (
                    self._json_messages_processed
                    + self._data_frames_processed
                    + self._live_frames_processed
                )

                # Print statistics
                print("=== TCP Server Statistics (last 30s) ===")
                print(f"Total bytes received: {total_bytes:,} bytes")
                print(f"  JSON port: {self._json_bytes_received:,} bytes")
                print(f"  Data port: {self._data_bytes_received:,} bytes")
                print(f"  Live port: {self._live_bytes_received:,} bytes")
                print(f"Total messages/frames: {total_messages}")
                print(f"  JSON messages: {self._json_messages_processed}")
                print(f"  Data frames: {self._data_frames_processed}")
                print(f"  Live frames: {self._live_frames_processed}")
                print(
                    f"Queue sizes - Data: {self._data_queue.qsize()}, Live: {self._live_queue.qsize()}"
                )
                print(f"Avg throughput: {total_bytes / 30.0:.1f} bytes/sec")
                print()

                # Reset counters for next interval
                self._json_bytes_received = 0
                self._data_bytes_received = 0
                self._live_bytes_received = 0
                self._json_messages_processed = 0
                self._data_frames_processed = 0
                self._live_frames_processed = 0

            except asyncio.CancelledError:
                logger.info("Statistics monitor cancelled")
                break
            except Exception as e:
                logger.error(f"Error in statistics monitor: {e}")
                await asyncio.sleep(1.0)

        logger.info("Statistics monitor loop stopped")

    async def _read_response(self) -> dict[str, Any] | None:
        """Read response from JSON stream using asyncio streams."""
        if not self.json_reader:
            return None

        try:
            # Blocking read until \r\n terminator
            response_data = await self.json_reader.readuntil(b"\r\n")

            # Update statistics
            self._json_bytes_received += len(response_data)
            self._json_messages_processed += 1

            # Remove the terminator and decode
            response_bytes = response_data.rstrip(b"\r\n")

            try:
                response = response_bytes.decode("utf-8")
                parsed_response = json.loads(response.replace("\\", "/"))
                return parsed_response
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.error(
                    f"Failed to parse response: {e}, response: {response_bytes}"
                )
                return None

        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for response data")
            return None
        except asyncio.CancelledError:
            logger.info("Response reading cancelled")
            return None
        except asyncio.IncompleteReadError:
            logger.error("Connection closed while reading response")
            self._trigger_reconnect()
            return None
        except (
            ConnectionResetError,
            ConnectionAbortedError,
            BrokenPipeError,
            OSError,
        ) as e:
            logger.warning(f"Connection error reading response: {e}")
            self._trigger_reconnect()
            return None
        except Exception as e:
            logger.error(f"Error reading response: {e}")
            return None

    async def send_command(
        self,
        cmd_type: str,
        *,
        action: str | None = None,
        parameter: str | None = None,
        value: str | None = None,
        get_response: bool = True,
    ) -> dict[str, Any] | None:
        """Send JSON command to detector and wait for response."""
        if not self.connected or not self.json_writer:
            return None

        cmd_id = self._cmd_id
        self._cmd_id += 1
        if cmd_type == "ACTION":
            cmd = f'{{"cmd":"ACTION","id":{cmd_id},"values":"{action}"}}'
        elif cmd_type == "GET":
            if parameter == "*":
                cmd = f'{{"cmd":"GET","id":{cmd_id},"values":"*"}}'
            else:
                cmd = f'{{"cmd":"GET","id":{cmd_id},"values":"{parameter}"}}'
        elif cmd_type == "SET":
            cmd = f'{{"cmd":"SET","id":{cmd_id},"values":{{"{parameter}":"{value}"}}}}'
        else:
            return None

        if get_response:
            # Create future for response
            response_future = asyncio.Future()
            self._pending_responses[cmd_id] = response_future

        # Send command while holding lock, then signal response reader
        async with self._json_lock:
            try:
                msg: bytes = (cmd + "\r\n").encode()
                self.json_writer.write(msg)
                await self.json_writer.drain()

            except (
                ConnectionResetError,
                ConnectionAbortedError,
                BrokenPipeError,
                OSError,
            ) as e:
                logger.warning(f"Connection error sending command: {e}")
                # Clean up pending response
                self._pending_responses.pop(cmd_id, None)
                response_future.cancel()
                self._trigger_reconnect()
                return None
            except Exception as e:
                logger.error(f"Command failed: {e}")
                # Clean up pending response
                self._pending_responses.pop(cmd_id, None)
                response_future.cancel()
                return None

        # Return immediately if no response needed
        if not get_response:
            return None

        # Wait for response from background reader
        try:
            response = await response_future
            return response
        except asyncio.TimeoutError:
            logger.error(f"Timeout waiting for response to {cmd_type} command")
            # Clean up pending response
            self._pending_responses.pop(cmd_id, None)
            response_future.cancel()
            return None

    async def _read_data(self) -> dict[str, Any] | None:
        """Read image data from detector data stream.

        Returns dict with parsed header info and pixel data, or None on error/timeout.

        The first data channel appears to be the current image, the second data channel is the sum of the images so far.

        The timing on the first data channel needs to be extremely precise, because the start of the next scan will
        0 out all of the data.
        """
        if not self.connected or not self.data_reader:
            return None

        try:
            # Read 40-byte header
            header_data = await self.data_reader.readexactly(40)

            # Update statistics for header
            self._data_bytes_received += len(header_data)

            # Parse header
            marker = int.from_bytes(header_data[0:4], byteorder="big", signed=False)

            if marker != 0xF0F0:
                logger.error(f"Invalid marker={marker:#x}, expected 0xf0f0")
                return None

            index = int.from_bytes(header_data[4:8], byteorder="big", signed=True)
            state = int.from_bytes(header_data[8:12], byteorder="big", signed=False)
            reserved = int.from_bytes(header_data[12:16], byteorder="big", signed=False)
            width = int.from_bytes(header_data[16:20], byteorder="big", signed=False)
            height = int.from_bytes(header_data[20:24], byteorder="big", signed=False)
            length = int.from_bytes(header_data[24:28], byteorder="big", signed=False)
            cur_width = int.from_bytes(
                header_data[28:32], byteorder="big", signed=False
            )
            cur_height = int.from_bytes(
                header_data[32:36], byteorder="big", signed=False
            )
            cur_length = int.from_bytes(
                header_data[36:40], byteorder="big", signed=False
            )

            result = {
                "index": index,
                "state": state,
                "reserved": reserved,
                "width": width,
                "height": height,
                "length": length,
                "cur_width": cur_width,
                "cur_height": cur_height,
                "cur_length": cur_length,
                "channel_1_data": None,
                "channel_2_data": None,
                "channel_1_sum": 0,
                "channel_2_sum": 0,
            }

            logger.info(
                f"Data header - index: {index}, state: {state}, "
                f"dimensions: {height}x{width}, length: {length}"
            )

            # Read first data channel if length > 0
            if length > 0:
                channel_1_data = await self._read_channel_data(length, timeout=10.0)

                if channel_1_data is None:
                    return None

                # Update statistics for channel 1 data
                self._data_bytes_received += len(channel_1_data)

                # Use numpy to directly interpret the binary data (much faster than Python loops)
                pixel_data_1 = np.frombuffer(
                    channel_1_data, dtype="<u4"
                )  # little-endian uint32
                result["channel_1_data"] = pixel_data_1.reshape((cur_height, cur_width))
                result["channel_1_sum"] = int(pixel_data_1.sum())

                logger.info(f"Channel 1 sum: {result['channel_1_sum']}")

            # Read second data channel if cur_length > 0
            if cur_length > 0:
                channel_2_data = await self._read_channel_data(cur_length, timeout=10.0)

                if channel_2_data is None:
                    return None

                # Update statistics for channel 2 data
                self._data_bytes_received += len(channel_2_data)

                # Use numpy to directly interpret the binary data (much faster than Python loops)
                pixel_data_2 = np.frombuffer(
                    channel_2_data, dtype="<u4"
                )  # little-endian uint32
                result["channel_2_data"] = pixel_data_2.reshape((cur_height, cur_width))
                result["channel_2_sum"] = int(pixel_data_2.sum())

                logger.info(f"Channel 2 sum: {result['channel_2_sum']}")

            # Update frame count statistics
            self._data_frames_processed += 1

            return result

        except asyncio.TimeoutError:
            logger.error("Timeout reading data header")
            return None
        except asyncio.IncompleteReadError:
            logger.error("Connection closed while reading data")
            self._trigger_reconnect()
            return None
        except (
            ConnectionResetError,
            ConnectionAbortedError,
            BrokenPipeError,
            OSError,
        ) as e:
            logger.warning(f"Connection error reading data: {e}")
            self._trigger_reconnect()
            return None
        except Exception as e:
            logger.error(f"Error reading data: {e}")
            return None

    async def _read_channel_data(
        self, expected_length: int, timeout: float = 10.0
    ) -> bytes | None:
        """Helper method to read a complete data channel with timeout."""
        if not self.data_reader:
            return None

        try:
            # Use readexactly to read exactly the expected number of bytes
            data = await asyncio.wait_for(
                self.data_reader.readexactly(expected_length), timeout=timeout
            )
            return data

        except asyncio.TimeoutError:
            logger.error(
                f"Timeout reading channel data, expected {expected_length} bytes"
            )
            return None
        except asyncio.IncompleteReadError as e:
            logger.error(
                f"Connection closed while reading channel data, got {len(e.partial)}/{expected_length} bytes"
            )
            self._trigger_reconnect()
            return None
        except (
            ConnectionResetError,
            ConnectionAbortedError,
            BrokenPipeError,
            OSError,
        ) as e:
            logger.warning(f"Connection error reading channel data: {e}")
            self._trigger_reconnect()
            return None
        except Exception as e:
            logger.error(f"Error reading channel data: {e}")
            return None

    async def _read_live_data(self) -> dict[str, Any] | None:
        """Read live data from detector live stream.

        Returns dict with parsed header info and pixel data summary, or None on error/timeout.
        Uses the 16-byte header format as documented in README.md Live Socket schema.
        """
        if not self.connected or not self.live_reader:
            return None

        try:
            # Read 16-byte header according to Live Socket schema
            header_data = await self.live_reader.readexactly(20)

            # Update statistics for header
            self._live_bytes_received += len(header_data)

            # Parse header according to documented schema:
            # Marker (0-4): 4 bytes - Message start marker
            # Index (4-8): 4 bytes - Current frame number
            # Width (8-12): 4 bytes - Width of image
            # Height (12-16): 4 bytes - Height of image
            # Length (16-20): 4 bytes - Length of image in bytes
            marker = int.from_bytes(header_data[0:4], byteorder="big", signed=False)
            if marker != 0xF0F0:
                logger.error(f"Invalid marker={marker:#x}, expected 0xf0f0")
                return None
            index = int.from_bytes(header_data[4:8], byteorder="big", signed=True)
            width = int.from_bytes(header_data[8:12], byteorder="big", signed=False)
            height = int.from_bytes(header_data[12:16], byteorder="big", signed=False)
            length = int.from_bytes(header_data[16:20], byteorder="big", signed=False)

            result = {
                "marker": marker,
                "index": index,
                "width": width,
                "height": height,
                "length": length,
                "max_count": 0,
                "timestamp": time.time(),
            }

            # Read pixel data if length > 0
            if length > 0:
                pixel_data_bytes = await self.live_reader.readexactly(length)

                # Update statistics for pixel data
                self._live_bytes_received += len(pixel_data_bytes)

                # Convert to uint32 numpy array (same format as data port)
                pixel_data = np.frombuffer(
                    pixel_data_bytes, dtype=">u4"
                )  # big-endian uint32

                # Calculate statistics for monitoring purposes
                result["max_count"] = int(np.max(pixel_data))

                # Don't store the full pixel array to save memory - just the statistics
                # Full arrays are available via the data port when needed

            # Update frame count statistics
            self._live_frames_processed += 1

            return result

        except asyncio.TimeoutError:
            logger.error("Timeout reading live data header")
            return None
        except asyncio.IncompleteReadError:
            logger.error("Connection closed while reading live data")
            self._trigger_reconnect()
            return None
        except (
            ConnectionResetError,
            ConnectionAbortedError,
            BrokenPipeError,
            OSError,
        ) as e:
            logger.warning(f"Connection error reading live data: {e}")
            self._trigger_reconnect()
            return None
        except Exception as e:
            logger.error(f"Error reading live data: {e}")
            return None

    async def get_all_parameters(self) -> dict[str, Any] | None:
        """Get all parameters from detector."""
        return await self.send_command("GET", parameter="*")

    async def get_parameter(self, param_name: str) -> dict[str, Any] | None:
        """Get specific parameter from detector."""
        return await self.send_command("GET", parameter=param_name)

    async def set_parameter(self, param_name: str, value: Any) -> dict[str, Any] | None:
        """Set a parameter on the detector."""
        return await self.send_command("SET", parameter=param_name, value=value)


class DetectorWriter:
    """Writer for detector data."""

    def __init__(self) -> None:
        self._image_queue: asyncio.Queue | None = None
        self._full_file_path: Path | None = None
        self._image_writer_task: asyncio.Task | None = None

    async def write_image(self, index: int, data: dict[str, Any]) -> None:
        """Queue an image for writing."""
        if self._image_queue is None:
            raise RuntimeError("Image queue not set")
        if data:
            try:
                # Put data in queue without blocking (will raise QueueFull if full)
                self._image_queue.put_nowait((index, data))
            except asyncio.QueueFull:
                logger.warning(
                    f"Image queue is full ({self._image_queue.qsize()} items), dropping frame"
                )
        else:
            logger.error("Failed to get current frame for file writing")

    def write_field(self, path: str, array: np.ndarray, name: str, units: str, **kwargs: dict[str, Any]) -> None:
        """Write an array to the file."""
        if self._full_file_path is None:
            raise RuntimeError("File path not set")
        with nxopen(self._full_file_path, "a", libver="latest") as file_handle:
            group = file_handle[path]
            group[name] = NXfield(
                array,
                name=name,
                units=units,
                **kwargs,
            )

    def _create_structure(self, root: NXroot) -> None:
        if "entry" not in root:
            root.entry = NXentry(name="entry")
        if "instrument" not in root.entry:
            root.entry.instrument = NXinstrument(name="instrument")
        if "analyzer" not in root.entry.instrument:
            root.entry.instrument.analyzer = NXdetector(name="analyzer")

    async def _image_writer(self) -> None:
        """Background task that continuously writes image data from queue to file."""
        if self._full_file_path is None:
            raise RuntimeError("File path not set")

        with nxopen(self._full_file_path, "w", libver="latest") as file_handle:
            self._create_structure(file_handle)

        first_pass = True
        while True:
            try:
                # Wait for data from the queue
                item = await self._image_queue.get()

                # Check for shutdown signal (None is used as sentinel)
                if item is None:
                    break

                with nxopen(self._full_file_path, "a", libver="latest") as file_handle:
                    entry = file_handle.entry
                    detector = entry.instrument.analyzer
                    index, data = item
                    if first_pass:
                        data_field = NXfield(
                            name="data",
                            shape=(0, data["cur_height"], data["cur_width"]),
                            dtype=np.uint32,
                            maxshape=(None, data["cur_height"], data["cur_width"]),
                            # Chunk by full images for optimal read performance
                            chunks=(1, data["cur_height"], data["cur_width"]),
                        )
                        detector["data"] = data_field
                        deflx_field = NXfield(
                            name="deflector_x",
                            shape=(0,),
                            dtype=np.float64,
                            maxshape=(None,),
                            # Reasonable chunk size for 1D array
                            chunks=(1024,),
                        )
                        detector["deflector_x"] = deflx_field
                        first_pass = False

                    # We continually overwrite the last frame in the data field in-case of
                    # an error during acquisition.
                    # The index value is incremented when acquisition state transitions to STANDBY
                    # This way, the last frame of each acquisition is saved (which is the cumulative sum
                    # of all the "act_scans" in the acquisition).
                    size = index + 1
                    if data_field.shape[0] < size:
                        data_field.resize(size, axis=0)
                    if deflx_field.shape[0] < size:
                        deflx_field.resize(size, axis=0)

                    data_field[index, :, :] = data["channel_2_data"]
                    deflx_field[index] = data["deflX"]
                    logger.info(f"Writing frame {size} to file")
                    # Update the file immediately to avoid losing data
                    file_handle.nxfile.file.flush()

                # Mark the task as done
                self._image_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in background file writer: {e}")
                # Mark the task as done even on error
                self._image_queue.task_done()

    async def open(self, path: str, name: str) -> None:
        """Initialize file for writing."""
        if not path:
            msg = f"File path not set, got: {path}"
            logger.error(msg)
            raise RuntimeError(msg)
        if not name or not name.endswith(".nxs"):
            msg = f"File name must be set and end with .nxs, got: {name}"
            logger.error(msg)
            raise RuntimeError(msg)
        path = Path(path)
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

        self._full_file_path = path / name
        if self._full_file_path.exists():
            logger.warning(f"File already exists, overwriting: {self._full_file_path}")
        self._image_queue = asyncio.Queue(maxsize=100)

        # Start background file writer task
        self._image_writer_task = asyncio.create_task(self._image_writer())

    def _link_results(self) -> None:
        if self._full_file_path is None:
            raise RuntimeError("File path not set")

        with nxopen(self._full_file_path, "a", libver="latest") as file_handle:
            # Add the final data field to the file
            if (
                "entry" not in file_handle
                or "instrument" not in file_handle.entry
                or "analyzer" not in file_handle.entry.instrument
            ):
                logger.warning("File was never initialized, skipping linking")
                return
            deflector_x_exists = "deflector_x" in file_handle.entry.instrument.analyzer
            angles_exists = "angles" in file_handle.entry.instrument.analyzer
            energies_exists = "energies" in file_handle.entry.instrument.analyzer
            data_exists = "data" in file_handle.entry.instrument.analyzer
            if deflector_x_exists:
                dfl = NXlink(file_handle.entry.instrument.analyzer.deflector_x)
            if angles_exists:
                an = NXlink(file_handle.entry.instrument.analyzer.angles)
            if energies_exists:
                en = NXlink(file_handle.entry.instrument.analyzer.energies)
            if data_exists:
                counts = NXlink(file_handle.entry.instrument.analyzer.data)
            if all((deflector_x_exists, angles_exists, energies_exists, data_exists)):
                file_handle.entry.data = NXdata(
                    counts,
                    [dfl, an, en],
                )

    async def close(self) -> None:
        # Stop background writer task
        if self._image_writer_task:
            # Send shutdown signal to background writer
            await self._image_queue.put(None)
            # Wait for the writer task to finish
            try:
                await asyncio.wait_for(self._image_writer_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("File writer task did not shutdown cleanly")
                self._image_writer_task.cancel()
            self._image_writer_task = None
        # Finalize the file
        self._link_results()
        # Clear any remaining items in queue
        while not self._image_queue.empty():
            try:
                self._image_queue.get_nowait()
                self._image_queue.task_done()
            except asyncio.QueueEmpty:
                break

        self._full_file_path = None
        self._image_queue = None
        self._image_writer_task = None


class DetectorIOC(PVGroup):
    """EPICS IOC for detector control via TCP interface."""

    async def _param_write(self, instance: PvpropertyData, value: Any) -> Any:
        """Set a detector parameter and return the value that was actually set."""
        if (isinstance(value, float) and np.isclose(instance.value, value)) or (
            not isinstance(value, float) and instance.value == value
        ):
            return instance.value
        param_name = self._pvs_to_param_names[instance]
        response = await self.tcp_client.set_parameter(param_name, value)
        if not response:
            logger.error(f"Failed to set {param_name} to {value}")
            return None
        response = await self.tcp_client.get_parameter(param_name)
        if not response:
            logger.error(f"Failed to get new value of {param_name}")
            return None
        result = next(
            (item for item in response["values"] if item["name"] == param_name), None
        )
        if not result:
            logger.error(f"Failed to get new value of {param_name}")
            return None
        actual_value = result["value"]
        if (
            isinstance(actual_value, float) and not np.isclose(actual_value, value)
        ) or (not isinstance(actual_value, float) and actual_value != value):
            logger.warning(
                f"Failed to set {param_name} to {value}, was set to {actual_value} instead."
            )
        return actual_value

    # Acquisition control
    acquire = pvproperty(value=0, name="ACQUIRE")
    acquisition_status = pvproperty(value=0, name="ACQ:STATUS", read_only=True)

    # Detector control
    det_off = pvproperty(
        value="No", name="DET:OFF", enum_strings=("No", "Yes"), dtype=bool
    )
    """Turn off the detector when this is set to 'On'"""

    # Live data monitoring
    live_monitoring = pvproperty(
        value="Off", name="LIVE:MONITORING", enum_strings=("Off", "On"), dtype=bool
    )
    """Enable/disable live data monitoring from live_port"""
    live_max_count = pvproperty(
        value=0, name="LIVE:MAX_COUNT", read_only=True, dtype=int
    )
    """Current maximum pixel count from live data stream"""
    live_update_rate = pvproperty(
        value=10.0, name="LIVE:UPDATE_RATE", dtype=float, precision=1
    )
    """Live data PV update rate in Hz (0.1 to 50.0)"""
    live_last_update = pvproperty(
        value="", name="LIVE:LAST_UPDATE", read_only=True, dtype=ChannelType.STRING
    )
    """Timestamp of last live data update"""
    max_count_threshold = pvproperty(value=150, name="LIVE:MAX_COUNT_THRESH", dtype=int)
    """Threshold for the maximum value of a single pixel of the detector"""
    max_count_exceeded = pvproperty(
        value="No",
        name="LIVE:MAX_COUNT_EXCEEDED",
        enum_strings=("No", "Yes"),
        dtype=bool,
    )
    """Indicates if the maximum count has exceeded the threshold"""

    # File writing
    file_capture = pvproperty(value=False, name="FILE:CAPTURE", dtype=bool)
    file_name = pvproperty(
        value="",
        name="FILE:NAME",
        dtype=str,
        max_length=1024,
    )
    file_path = pvproperty(
        value="",
        name="FILE:PATH",
        dtype=str,
        max_length=1024,
    )
    num_captured = pvproperty(
        value=0, name="FILE:NUM_CAPTURED", read_only=True, dtype=int
    )
    """Number of images captured while file capture is on"""
    num_processed = pvproperty(
        value=0, name="FILE:NUM_PROCESSED", read_only=True, dtype=int
    )
    """To track the number of scans processed during a single acquisition"""

    # Status and info
    connection_status = pvproperty(value=0, name="SYS:CONNECTED", read_only=True)
    last_sync = pvproperty(
        value="", name="SYS:LAST_SYNC", read_only=True, dtype=ChannelType.STRING
    )
    sync = pvproperty(
        value="ON",
        name="SYS:SYNC",
        dtype=ChannelType.ENUM,
        enum_strings=("OFF", "ON"),
        record="bi",
    )
    """Enable/disable automatic parameter synchronization, set interval via SYS:SYNC.SCAN"""

    # Detector parameters
    state = pvproperty(name="STATE", dtype=ChannelType.STRING, read_only=True)
    endX = pvproperty(name="ENDX", dtype=int, read_only=True)
    startY = pvproperty(name="STARTY", dtype=int, read_only=True)
    num_slice = pvproperty(name="NUM_SLICE", dtype=int, read_only=True)
    endY = pvproperty(name="ENDY", dtype=int, read_only=True)
    startX = pvproperty(name="STARTX", dtype=int, read_only=True)
    frames = pvproperty(put=_param_write, name="FRAMES", dtype=int)
    num_steps = pvproperty(put=_param_write, name="NUM_STEPS", dtype=int)
    pass_energy = pvproperty(
        put=_param_write,
        name="PASS_ENERGY",
        dtype=ChannelType.ENUM,
        enum_strings=(
            "PE001",
            "PE002",
            "PE005",
            "PE010",
            "PE020",
            "PE050",
            "PE100",
            "PE200",
        ),
    )
    lens_mode = pvproperty(
        put=_param_write,
        name="LENS_MODE",
        dtype=ChannelType.ENUM,
        enum_strings=(
            "L4Ang0d6",
            "L4Ang0d8",
            "L4Ang1d6",
            "L4Ang3d9",
            "L4MAng0d7",
            "L4MSpat5",
            "L4Spat5",
        ),
    )
    num_scans = pvproperty(put=_param_write, name="NUM_SCANS", dtype=int)
    reg_num = pvproperty(name="REG_NUM", dtype=int, read_only=True)
    tot_steps = pvproperty(name="TOT_STEPS", dtype=int, read_only=True)
    add_fms = pvproperty(name="ADD_FMS", dtype=int, read_only=True)
    act_scans = pvproperty(name="ACT_SCANS", dtype=int, read_only=True)
    dith_steps = pvproperty(put=_param_write, name="DITH_STEPS", dtype=int)
    start_ke = pvproperty(put=_param_write, name="START_KE", dtype=float, precision=6)
    step_size = pvproperty(put=_param_write, name="STEP_SIZE", dtype=float, precision=6)
    end_ke = pvproperty(put=_param_write, name="END_KE", dtype=float, precision=6)
    spin_offs = pvproperty(put=_param_write, name="SPIN_OFFS", dtype=float, precision=6)
    width = pvproperty(put=_param_write, name="WIDTH", dtype=float, precision=6)
    center_ke = pvproperty(put=_param_write, name="CENTER_KE", dtype=float, precision=6)
    first_energy = pvproperty(name="FIRST_ENERGY", dtype=float, read_only=True)
    deflX = pvproperty(put=_param_write, name="DEFLX", dtype=float, precision=6)
    deflY = pvproperty(put=_param_write, name="DEFLY", dtype=float, precision=6)
    dbl10 = pvproperty(name="DBL10", dtype=float, read_only=True)
    acq_mode = pvproperty(
        put=_param_write,
        name="ACQ_MODE",
        dtype=ChannelType.ENUM,
        enum_strings=("Fixed", "FixedTrigd", "Swept", "Dither"),
    )
    date_number = pvproperty(
        name="DATE_NUMBER", enum_strings=("FALSE", "TRUE"), dtype=bool, read_only=True
    )
    loc_det = pvproperty(
        name="LOC_DET", enum_strings=("FALSE", "TRUE"), dtype=bool, read_only=True
    )
    xtab = pvproperty(
        put=_param_write, name="XTAB", enum_strings=("FALSE", "TRUE"), dtype=bool
    )
    spin = pvproperty(
        put=_param_write, name="SPIN", enum_strings=("FALSE", "TRUE"), dtype=bool
    )
    reg_name = pvproperty(name="REG_NAME", dtype=ChannelType.STRING, read_only=True)
    name_string = pvproperty(
        name="NAME_STRING", dtype=ChannelType.STRING, read_only=True
    )
    generated_name = pvproperty(
        name="GENERATED_NAME", dtype=ChannelType.STRING, read_only=True
    )
    comment1 = pvproperty(put=_param_write, name="COMMENT1", dtype=ChannelType.STRING)
    start_time = pvproperty(
        put=_param_write, name="START_TIME", dtype=ChannelType.STRING
    )
    discr = pvproperty(name="DISCR", dtype=int, read_only=True)
    adc_mask = pvproperty(name="ADC_MASK", dtype=int, read_only=True)
    adc_offset = pvproperty(name="ADC_OFFSET", dtype=int, read_only=True)
    p_cnt_type = pvproperty(name="P_CNT_TYPE", dtype=int, read_only=True)
    pc_mask = pvproperty(name="PC_MASK", dtype=int, read_only=True)
    soft_bin_x = pvproperty(put=_param_write, name="SOFT_BIN_X", dtype=int)
    soft_bin_y = pvproperty(put=_param_write, name="SOFT_BIN_Y", dtype=int)
    escale_mult = pvproperty(
        put=_param_write, name="ESCALE_MULT", dtype=float, precision=6
    )
    escale_max = pvproperty(name="ESCALE_MAX", dtype=float, read_only=True)
    escale_min = pvproperty(name="ESCALE_MIN", dtype=float, read_only=True)
    yscale_mult = pvproperty(name="YSCALE_MULT", dtype=float, read_only=True)
    yscale_max = pvproperty(name="YSCALE_MAX", dtype=float, read_only=True)
    yscale_min = pvproperty(name="YSCALE_MIN", dtype=float, read_only=True)
    yscale_name = pvproperty(
        name="YSCALE_NAME", dtype=ChannelType.STRING, read_only=True
    )
    xscale_mult = pvproperty(name="XSCALE_MULT", dtype=float, read_only=True)
    xscale_max = pvproperty(name="XSCALE_MAX", dtype=float, read_only=True)
    xscale_min = pvproperty(name="XSCALE_MIN", dtype=float, read_only=True)
    xscale_name = pvproperty(
        name="XSCALE_NAME", dtype=ChannelType.STRING, read_only=True
    )
    psu_mode = pvproperty(name="PSU_MODE", dtype=ChannelType.STRING, read_only=True)
    over_r_arr = pvproperty(name="OVER_R_ARR", dtype=ChannelType.STRING, read_only=True)
    over_range = pvproperty(name="OVER_RANGE", dtype=int, read_only=True)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.tcp_client: DetectorTCPClient = DetectorTCPClient()
        self.writer: DetectorWriter = DetectorWriter()
        self._state_lock = asyncio.Lock()
        self._acquire_lock = asyncio.Lock()
        self._file_capture_lock = asyncio.Lock()
        self._live_monitor_lock = asyncio.Lock()

        # Convenience mappings for parameter names to PVs and vice versa
        self._pvs_to_param_names: dict[PvpropertyData, str] = {
            self.state: "state",
            self.endX: "endX",
            self.startY: "startY",
            self.num_slice: "numSlice",
            self.endY: "endY",
            self.startX: "startX",
            self.frames: "frames",
            self.num_steps: "numSteps",
            self.pass_energy: "passEnergy",
            self.lens_mode: "lensMode",
            self.num_scans: "numScans",
            self.reg_num: "regNum",
            self.tot_steps: "totSteps",
            self.add_fms: "addFms",
            self.act_scans: "actScans",
            self.dith_steps: "dithSteps",
            self.start_ke: "startKe",
            self.step_size: "stepSize",
            self.end_ke: "endKe",
            self.spin_offs: "spinOffs",
            self.width: "width",
            self.center_ke: "centreKe",
            self.first_energy: "firstEnergy",
            self.deflX: "deflX",
            self.deflY: "deflY",
            self.dbl10: "dbl10",
            self.acq_mode: "acqMode",
            self.date_number: "dateNumber",
            self.loc_det: "locDet",
            self.xtab: "xtab",
            self.spin: "spin",
            self.reg_name: "regName",
            self.name_string: "nameString",
            self.generated_name: "generatedName",
            self.comment1: "comment1",
            self.start_time: "startTime",
            self.discr: "discr",
            self.adc_mask: "adcMask",
            self.adc_offset: "adcOffset",
            self.p_cnt_type: "pCntType",
            self.pc_mask: "pcMask",
            self.soft_bin_x: "softBinX",
            self.soft_bin_y: "softBinY",
            self.escale_mult: "EScaleMult",
            self.escale_max: "EScaleMax",
            self.escale_min: "EScaleMin",
            self.yscale_mult: "YScaleMult",
            self.yscale_max: "YScaleMax",
            self.yscale_min: "YScaleMin",
            self.yscale_name: "YScaleName",
            self.xscale_mult: "XScaleMult",
            self.xscale_max: "XScaleMax",
            self.xscale_min: "XScaleMin",
            self.xscale_name: "XScaleName",
            self.psu_mode: "PsuMode",
            self.over_r_arr: "OverRArr",
            self.over_range: "OverRange",
        }
        self._param_names_to_pvs = {v: k for k, v in self._pvs_to_param_names.items()}

    async def _get_current_frame(self) -> dict[str, Any] | None:
        _, response = await asyncio.gather(
            self.tcp_client.send_command(
                "ACTION", action="GET_IMAGE", get_response=False
            ),
            self.tcp_client.get_parameter(self._pvs_to_param_names[self.deflX]),
        )

        data = await self.tcp_client.get_data()
        if data:
            # Note: max_count is now monitored via live data port when live monitoring is enabled
            # This provides much more frequent monitoring for emergency detection
            data["deflX"] = response["values"][0]["value"]
            return data
        else:
            logger.warning("Failed to read image data")
        return None

    @file_capture.putter
    async def file_capture(
        self, instance: PvpropertyData, value: Literal["On", "Off"]
    ) -> bool:
        """Start or stop file capture."""
        if instance.value == value:
            msg = f"File capture is already '{value}'"
            logger.error(msg)
            raise RuntimeError(msg)

        async with self._file_capture_lock:
            if value == "On":
                file_path = self.file_path.value
                filename = self.file_name.value
                await self.writer.open(file_path, filename)
                await self.num_captured.write(0)
            else:
                await self.writer.close()
        return value

    def _write_metadata(self) -> None:
        self.writer.write_field(
            "entry/instrument/analyzer",
            np.linspace(
                self.xscale_min.value,
                self.xscale_max.value,
                self.num_slice.value,
                endpoint=True,
            ),
            name="angles",
            units="deg",
            chunks=(2048,),
        )
        self.writer.write_field(
            "entry/instrument/analyzer",
            np.linspace(
                self.escale_min.value,
                self.escale_max.value,
                self.num_steps.value,
                endpoint=True,
            ),
            name="energies",
            units="eV",
            chunks=(2048,),
        )

    @act_scans.scan(period=0.05)
    async def act_scans(self, instance: PvpropertyData, async_lib: Any) -> Any:
        """Scan for acutal number of scans completed."""
        if self.acquisition_status.value == 1 and self.tcp_client.connected:
            num_processed = self.num_processed.value

            # Get actScans parameter
            response = await self.tcp_client.get_parameter(
                self._pvs_to_param_names[self.act_scans]
            )

            if response and "values" in response:
                act_scans_value = response["values"][0]["value"]
                if act_scans_value != self.act_scans.value:
                    await self.act_scans.write(act_scans_value)

                if act_scans_value > num_processed:
                    if act_scans_value > num_processed + 1:
                        logger.critical(
                            f"FRAME SKIP: seen only {num_processed} so far but expected {act_scans_value}"
                        )

                    if self.file_capture.value == "On":
                        # We only care about committing the final frame of each acquisition to the file since
                        # it contains the cumulative sum of the frames in one acquisition.
                        # But intermediate frames are still useful in-case of an error during acquisition.
                        # Therefore, we try to get the intermediate frame with a timeout.
                        index = self.num_captured.value
                        if act_scans_value < self.num_scans.value:
                            try:
                                data = await asyncio.wait_for(
                                    self._get_current_frame(), timeout=0.1
                                )
                                await self.writer.write_image(index, data)
                            except asyncio.TimeoutError:
                                logger.warning(
                                    "Failed to get current frame in 100ms, skipping current frame"
                                )
                        else:
                            data = await self._get_current_frame()
                            await self.writer.write_image(index, data)
                            # Capture metadata for the first frame
                            if index == 0:
                                self._write_metadata()
                            await self.num_captured.write(index + 1)
                            logger.info(
                                f"Committing frame {self.num_captured.value} to file"
                            )
                    await self.num_processed.write(self.num_processed.value + 1)
            else:
                logger.error(f"Failed to get actual number of scans, got: {response}")

    @state.scan(period=0.05)  # 50ms scan - state changes can be frequent in fixed mode
    async def state(self, instance: PvpropertyData, async_lib: Any) -> Any:
        """Scan for state changes."""
        if self.acquisition_status.value == 1 and self.tcp_client.connected:
            response = await self.tcp_client.get_parameter(
                self._pvs_to_param_names[self.state]
            )
            if response and "values" in response:
                value = response["values"][0]["value"]
                await self.state.write(value)
            else:
                logger.error(f"Failed to get state update, got: {response}")

    @sync.scan(period=1.0, use_scan_field=True)  # 1s - parameters change infrequently
    async def sync(self, instance: PvpropertyData, async_lib: Any) -> Any:
        """Synchronize parameters with detector."""
        if instance.value == "ON" and self.tcp_client.connected:
            try:
                response = await self.tcp_client.get_all_parameters()
                if response and "values" in response:
                    await self._update_pvs_from_response(response, async_lib)
                    current_time: str = time.strftime("%Y-%m-%d %H:%M:%S")
                    await self.last_sync.write(current_time)
                else:
                    logger.error(
                        "Failed to sync parameters - no response or invalid response"
                    )
            except asyncio.CancelledError:
                # Handle graceful shutdown
                raise
            except Exception as e:
                logger.error(f"Sync error: {e}")

    @state.putter
    async def state(
        self, instance: Any, value: Literal["STANDBY", "RUNNING", "MOVING"]
    ) -> Literal["STANDBY", "RUNNING", "MOVING"]:
        """Set the state of the detector."""
        async with self._state_lock:
            if value == "STANDBY" and self.acquire.value == 1:
                await self.acquire.write(0)
            return value

    @connection_status.startup
    async def connection_status(self, instance: Any, async_lib: Any) -> None:
        """Initialize TCP connection and background tasks on IOC startup."""
        logger.info("Initializing detector connection...")
        await self.tcp_client.connect()
        await self.connection_status.write(1 if self.tcp_client.connected else 0)

        if not self.tcp_client.connected:
            if self.tcp_client.auto_reconnect:
                logger.warning(
                    "Initial connection failed, auto-reconnect will attempt to connect"
                )
                # Start the reconnect task even if initial connection fails
                if not self.tcp_client._reconnect_task:
                    self.tcp_client._reconnect_task = asyncio.create_task(
                        self.tcp_client._reconnect_loop()
                    )
                    self.tcp_client._trigger_reconnect()
            else:
                raise RuntimeError("Failed to connect to detector")

        logger.info("Detector connection established")

    @connection_status.scan(period=2.0)
    async def connection_status(self, instance: Any, async_lib: Any) -> None:
        """Monitor connection status and update PV."""
        current_status = 1 if self.tcp_client.connected else 0
        if instance.value != current_status:
            await self.connection_status.write(current_status)
            if current_status:
                logger.info("Connection restored")
            else:
                logger.warning("Connection lost")

    async def _update_pvs_from_response(
        self, response: dict[str, Any], async_lib: Any
    ) -> None:
        """Update PVs based on detector response."""
        try:
            values: dict[str, Any] = response.get("values", [])
            write_tasks = []
            for item in values:
                if item["name"] in self._param_names_to_pvs:
                    pv = self._param_names_to_pvs[item["name"]]
                    new_value = item["value"]
                    # Only update if the value is different
                    if pv.value != new_value:
                        write_tasks.append(pv.write(new_value))
            await async_lib.library.gather(*write_tasks)
        except Exception as e:
            logger.error(f"Error updating PVs: {e}")

    @acquire.putter
    async def acquire(self, instance: Any, value: int) -> int:
        """Start acquisition when PV is written to."""
        async with self._acquire_lock:
            if instance.value == value:
                msg = f"Acquire is already '{value}'"
                logger.error(msg)
                return value

            if value > 0:
                if self.max_count_exceeded.value == "Yes":
                    raise RuntimeError(
                        (
                            "Acquisition cannot be started due to max count threshold exceeded. "
                            "If it is safe to do so, reset the max count exceeded flag."
                        )
                    )
                response: dict[str, Any] | None = await self.tcp_client.send_command(
                    "ACTION", action="START"
                )
                if response:
                    logger.info("Acquisition started")
                    await self.acquisition_status.write(1)
                    await self.num_processed.write(0)
                else:
                    logger.error("Failed to start acquisition")
                    return 0
            else:
                response: dict[str, Any] | None = await self.tcp_client.send_command(
                    "ACTION", action="STOP"
                )
                if response:
                    await self.acquisition_status.write(0)
                    logger.info("Acquisition stopped")
                else:
                    logger.error("Failed to stop acquisition")
                    return instance.value

            return value

    @det_off.putter
    async def det_off(self, instance: Any, value: bool) -> bool:
        """Turn off the detector."""
        if value:
            response = await self.tcp_client.send_command("ACTION", action="DET_OFF")
            if not response:
                logger.error("Failed to turn off the detector")
                return False
        return value

    @max_count_exceeded.putter
    async def max_count_exceeded(self, instance: Any, value: bool) -> bool:
        """Reset/acknowledge the max_count_exceeded flag."""
        if value == "No" and instance.value == "Yes":
            # Allow reset from Yes to No (acknowledge condition)
            logger.info("Max count exceeded flag acknowledged and reset by operator")
            return "No"
        return value

    @live_monitoring.putter
    async def live_monitoring(self, instance: Any, value: bool) -> bool:
        """Enable or disable live data monitoring."""
        async with self._live_monitor_lock:
            if instance.value == value:
                return value

            if value == "On":
                # Start live monitoring
                if not self.tcp_client.connected:
                    return "Off"

                self._live_monitor_task = asyncio.create_task(self._live_monitor_loop())
                logger.info("Live data monitoring started")
            else:
                # Stop live monitoring
                if self._live_monitor_task:
                    self._live_monitor_task.cancel()
                    try:
                        await self._live_monitor_task
                    except asyncio.CancelledError:
                        pass
                    self._live_monitor_task = None

                logger.info("Live data monitoring stopped")

            return value

    async def _live_monitor_loop(self) -> None:
        """Background task for live data monitoring and emergency detection."""
        logger.info("Starting live data monitoring loop")

        # Validate update rate
        update_rate = self.live_update_rate.value
        update_interval = 1.0 / update_rate

        try:
            while self.live_monitoring.value == "On":
                try:
                    # Get live data from TCP client
                    live_data = await self.tcp_client.get_live_data()

                    if live_data:
                        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
                        max_count = live_data["max_count"]

                        # Update PVs with live data
                        await asyncio.gather(
                            self.live_max_count.write(max_count),
                            self.live_last_update.write(current_time),
                        )

                        # Emergency detection: check if max_count exceeds threshold
                        if max_count > self.max_count_threshold.value:
                            logger.critical(
                                f"EMERGENCY: Live max count {max_count} exceeds threshold {self.max_count_threshold.value}!"
                            )
                            await self._emergency_shutdown(max_count)
                            break

                    # Sleep for the configured update interval
                    await asyncio.sleep(update_interval)

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in live monitor loop: {e}")
                    await self.live_status.write(f"Error: {str(e)}")
                    await asyncio.sleep(1.0)  # Wait before retrying

        except asyncio.CancelledError:
            pass

        logger.info("Live data monitoring loop stopped")

    async def _emergency_shutdown(self, max_count: int) -> None:
        """Emergency shutdown procedure when max_count threshold is exceeded."""
        logger.critical(f"Executing emergency shutdown: max_count={max_count}")

        try:
            # Set emergency status
            await asyncio.gather(
                self.max_count_exceeded.write("Yes"),
            )

            # Stop acquisition if running
            if self.acquire.value == 1:
                logger.critical("Emergency: Stopping acquisition")
                await self.acquire.write(0)

            # Turn off detector
            if self.det_off.value == "No":
                logger.critical("Emergency: Turning off detector")
                await self.det_off.write("Yes")

            # Stop live monitoring
            await self.live_monitoring.write("Off")

        except Exception as e:
            logger.error(f"Error during emergency shutdown: {e}")

    @live_update_rate.putter
    async def live_update_rate(self, instance: Any, value: float) -> float:
        """Set the live data update rate, with bounds checking."""
        # Clamp to reasonable range
        clamped_value = max(0.1, min(50.0, value))
        if clamped_value != value:
            logger.warning(
                f"Live update rate clamped from {value} to {clamped_value} Hz"
            )
        return clamped_value

    async def cleanup(self) -> None:
        """Clean up background tasks and connections."""
        logger.info("Starting IOC cleanup...")
        # Close the file writer if it is open
        async with self._file_capture_lock:
            if self.file_capture.value == "On":
                await self.writer.close()
        # Disconnect TCP client (this will handle auto-reconnect cleanup)
        await self.tcp_client.disconnect()
        logger.info("Cleanup completed")


if __name__ == "__main__":
    ioc_options, run_options = ioc_arg_parser(
        default_prefix="A1Soft:", desc=dedent(DetectorIOC.__doc__)
    )

    ioc = DetectorIOC(**ioc_options)
    run(ioc.pvdb, **run_options)
