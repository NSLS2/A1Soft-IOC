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

from nexusformat.nexus import NXdata, NXfield, NXroot, NXentry, nxopen
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
    ) -> None:
        self.host: str = host
        self.json_port: int = json_port
        self.data_port: int = data_port
        self.live_port: int = live_port

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

            logger.info("Connected to detector TCP interface using asyncio streams")
        except Exception as e:
            logger.error(f"Failed to connect to detector: {e}")
            await self._cleanup_connections()
            self.connected = False

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
        # Stop response reader
        self._response_reader_running = False
        if self._response_reader_task:
            self._response_reader_task.cancel()
            try:
                await self._response_reader_task
            except asyncio.CancelledError:
                pass
            self._response_reader_task = None

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

        while self._response_reader_running and self.connected:
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
            except Exception as e:
                logger.error(f"Error in response reader: {e}")
                await asyncio.sleep(0.1)  # Brief delay before retrying

        logger.info("Event-driven response reader stopped")

    async def _data_reader_loop(self) -> None:
        logger.info("Starting data reader loop")
        while self._data_reader_running and self.connected:
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
            except Exception as e:
                logger.error(f"Error in data reader loop: {e}")
                await asyncio.sleep(0.1)

        logger.info("Data reader loop stopped")

    async def _read_response(self) -> dict[str, Any] | None:
        """Read response from JSON stream using asyncio streams."""
        if not self.json_reader:
            return None

        try:
            # Blocking read until \r\n terminator
            response_data = await self.json_reader.readuntil(b"\r\n")

            # Remove the terminator and decode
            response_bytes = response_data.rstrip(b"\r\n")

            try:
                response = response_bytes.decode("utf-8")
                parsed_response = json.loads(response.replace("\\", "/"))
                return parsed_response
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.error(
                    f"Failed to parse response: {e}, response: {response_bytes[:100]}..."
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

                # Use numpy to directly interpret the binary data (much faster than Python loops)
                pixel_data_2 = np.frombuffer(
                    channel_2_data, dtype="<u4"
                )  # little-endian uint32
                result["channel_2_data"] = pixel_data_2.reshape((cur_height, cur_width))
                result["channel_2_sum"] = int(pixel_data_2.sum())

                logger.info(f"Channel 2 sum: {result['channel_2_sum']}")

            return result

        except asyncio.TimeoutError:
            logger.error("Timeout reading data header")
            return None
        except asyncio.IncompleteReadError:
            logger.error("Connection closed while reading data")
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
            return None
        except Exception as e:
            logger.error(f"Error reading channel data: {e}")
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


class DetectorIOC(PVGroup):
    """EPICS IOC for detector control via TCP interface."""

    async def _param_write(self, instance: PvpropertyData, value: Any) -> Any:
        """Set a detector parameter and return the value that was actually set."""
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
            logger.error(
                f"Failed to set {param_name} to {value}, was set to {actual_value} instead."
            )
            raise ValueError(
                f"Failed to set {param_name} to {value}, was set to {actual_value} instead."
            )
        return actual_value

    # Acquisition control
    acquire = pvproperty(value=0, name="ACQUIRE")
    acquisition_status = pvproperty(value=0, name="ACQ:STATUS", read_only=True)

    # File writing
    file_capture = pvproperty(value=False, name="FILE:CAPTURE", dtype=bool)
    file_name = pvproperty(value="", name="FILE:NAME", dtype=ChannelType.STRING)
    file_path = pvproperty(value="", name="FILE:PATH", dtype=ChannelType.STRING)
    file_status = pvproperty(
        value="", name="FILE:STATUS", read_only=True, dtype=ChannelType.STRING
    )
    num_captured = pvproperty(
        value=0, name="FILE:NUM_CAPTURED", read_only=True, dtype=int
    )
    """Total number of images captured during capture session"""
    num_processed = pvproperty(
        value=0, name="FILE:NUM_PROCESSED", read_only=True, dtype=int
    )
    """Number of images processed during a single acquisition"""

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
        enum_strings=("PE001", "PE002", "PE005", "PE010", "PE020", "PE050"),
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
        enum_strings=("Fixed", "Swept", "Dither"),
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
        self._sync_task: asyncio.Task | None = None
        self._update_listener_task: asyncio.Task | None = None
        self._file_writer_task: asyncio.Task | None = None
        self._image_queue: asyncio.Queue = asyncio.Queue(
            maxsize=100
        )  # Buffer up to 100 images
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
        self._full_file_path: Path | None = None
        self._file_handle: NXroot | None = None

    async def _get_current_frame(self) -> dict[str, Any] | None:
        await self.tcp_client.send_command(
            "ACTION", action="GET_IMAGE", get_response=False
        )
        data = await self.tcp_client.get_data()
        if data:
            return data
        else:
            logger.warning("Failed to read image data")
        return None

    async def _background_file_writer(self) -> None:
        """Background task that continuously writes image data from queue to file."""
        if self._file_handle is None:
            raise RuntimeError("File handle not initialized")

        entry = self._file_handle["entry1"]
        detector = entry["analyzer"]

        if "data" in detector:
            data_field = detector["data"]
        else:
            data_field = None

        while True:
            try:
                # Wait for data from the queue
                item = await self._image_queue.get()

                # Check for shutdown signal (None is used as sentinel)
                if item is None:
                    break

                index, data = item
                if data_field is None:
                    data_field = NXfield(
                        name="data",
                        shape=(0, data["cur_height"], data["cur_width"]),
                        dtype=np.uint32,
                        maxshape=(None, data["cur_height"], data["cur_width"]),
                    )
                    detector["data"] = data_field

                # We continually overwrite the last frame in the data field in-case of
                # an error during acquisition.
                # The index value is incremented when acquisition state transitions to STANDBY
                # This way, the last frame of each acquisition is saved (which is the cumulative sum
                # of all the "act_scans" in the acquisition).
                size = index + 1
                if data_field.shape[0] < size:
                    data_field.resize(size, axis=0)
                data_field[index, :, :] = data["channel_2_data"]
                logger.info(f"Writing frame {size} to file")
                # Update the file immediately to avoid losing data
                self._file_handle.nxfile.file.flush()

                # Mark the task as done
                self._image_queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in background file writer: {e}")
                # Mark the task as done even on error
                self._image_queue.task_done()

    async def _write_image_to_file(self) -> None:
        """Request image and queue it for writing."""
        index = self.num_captured.value
        data = await self._get_current_frame()

        if data:
            try:
                # Put data in queue without blocking (will raise QueueFull if full)
                # The num_captured value is the index of the frame to be written to file
                self._image_queue.put_nowait((index, data))

            except asyncio.QueueFull:
                logger.warning(
                    f"Image queue is full ({self._image_queue.qsize()} items), dropping frame"
                )
        else:
            logger.error("Failed to get current frame for file writing")

    def _create_file_structure(self, file_handle: NXroot) -> None:
        if "entry1" not in file_handle:
            file_handle["entry1"] = NXentry(name="entry1")
        if "analyzer" not in file_handle["entry1"]:
            file_handle["entry1"]["analyzer"] = NXdata(name="analyzer")

    @file_capture.putter
    async def file_capture(
        self, instance: PvpropertyData, value: Literal["On", "Off"]
    ) -> bool:
        """Start or stop file capture."""
        if self.file_capture.value == value:
            msg = f"File capture is already '{value}'"
            logger.error(msg)
            raise RuntimeError(msg)

        if value == "On":
            file_path = self.file_path.value
            if not file_path:
                msg = f"File path not set, got: {file_path}"
                logger.error(msg)
                raise RuntimeError(msg)
            filename = self.file_name.value
            if not filename or not filename.endswith(".nxs"):
                msg = f"File name must be set and end with .nxs, got: {filename}"
                logger.error(msg)
                raise RuntimeError(msg)

            self._full_file_path = Path(file_path) / filename

            # Create fresh queue for this capture session
            self._image_queue = asyncio.Queue(maxsize=100)
            self._file_handle = nxopen(self._full_file_path, "a")
            self._create_file_structure(self._file_handle)
            if "data" in self._file_handle["entry1"]["analyzer"]:
                size = self._file_handle["entry1"]["analyzer"]["data"].shape[0]
                logger.warning(f"Appending to existing file with {size} frames")
                await self.num_captured.write(size)
            else:
                await self.num_captured.write(0)

            # Start background file writer task
            self._file_writer_task = asyncio.create_task(self._background_file_writer())

            await self.file_status.write(
                f"File capture started, writing to {self._full_file_path}"
            )
        else:
            # Stop background writer task
            if self._file_writer_task:
                # Send shutdown signal to background writer
                await self._image_queue.put(None)
                # Wait for the writer task to finish
                try:
                    await asyncio.wait_for(self._file_writer_task, timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("File writer task did not shutdown cleanly")
                    self._file_writer_task.cancel()
                self._file_writer_task = None

            self._full_file_path = None
            self._file_handle.close()
            self._file_handle = None

            # Clear any remaining items in queue
            while not self._image_queue.empty():
                try:
                    self._image_queue.get_nowait()
                    self._image_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            await self.file_status.write(
                f"File capture stopped, wrote to {self._full_file_path}"
            )
        return value

    @act_scans.scan(period=0.05)
    async def act_scans(self, instance: PvpropertyData, async_lib: Any) -> Any:
        """Scan for acutal number of scans completed."""
        if self.acquisition_status.value == 1 and self.file_capture.value == "On":
            num_processed = self.num_processed.value

            # Get actScans parameter
            response = await self.tcp_client.get_parameter(
                self._pvs_to_param_names[self.act_scans]
            )

            if response and "values" in response:
                act_scans_value = response["values"][0]["value"]

                if act_scans_value > num_processed:
                    logger.info(
                        f"New scan detected: {act_scans_value} (was {num_processed})"
                    )
                    if act_scans_value > num_processed + 1:
                        logger.warning(
                            f"FRAME SKIPPED: {act_scans_value} (was {num_processed})"
                        )

                    await self._write_image_to_file()

                    await async_lib.library.gather(
                        self.num_processed.write(act_scans_value),
                        self.act_scans.write(act_scans_value),
                    )

                    # Increment num_captured when the last scan is completed
                    if act_scans_value == self.num_scans.value:
                        await self.num_captured.write(self.num_captured.value + 1)
                        logger.info(
                            f"Committing frame {self.num_captured.value} to file"
                        )
            else:
                logger.error(f"Failed to get actual number of scans, got: {response}")

    @state.scan(period=0.1)  # 100ms scan - state changes are infrequent
    async def state(self, instance: PvpropertyData, async_lib: Any) -> Any:
        """Scan for state changes."""
        response = await self.tcp_client.get_parameter(
            self._pvs_to_param_names[self.state]
        )
        if response and "values" in response:
            value = response["values"][0]["value"]
            if value == "STANDBY":
                await async_lib.library.gather(
                    self.acquire.write(0),
                    self.state.write(value),
                )
        else:
            logger.error(f"Failed to get state update, got: {response}")

    @sync.scan(period=1.0, use_scan_field=True)  # 1s - parameters change infrequently
    async def sync(self, instance: PvpropertyData, async_lib: Any) -> Any:
        """Synchronize parameters with detector."""
        if instance.value == "ON":
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

    @connection_status.startup
    async def connection_status(self, instance: Any, async_lib: Any) -> None:
        """Initialize TCP connection and background tasks on IOC startup."""
        logger.info("Initializing detector connection...")
        await self.tcp_client.connect()
        await self.connection_status.write(1 if self.tcp_client.connected else 0)

        if not self.tcp_client.connected:
            raise RuntimeError("Failed to connect to detector")

        logger.info("Detector connection established")

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
        if self.acquire.value == value:
            msg = f"Acquire is already '{value}'"
            logger.error(msg)
            raise RuntimeError(msg)

        if value > 0:
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="START"
            )
            if response:
                await self.acquisition_status.write(1)
                if self.file_capture.value == "On":
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
                return self.acquire.value

        return value

    async def cleanup(self) -> None:
        """Clean up background tasks and connections."""
        if self._sync_task:
            self._sync_task.cancel()
        if self._update_listener_task:
            self._update_listener_task.cancel()
        if self._file_writer_task:
            # Send shutdown signal and wait for graceful shutdown
            try:
                await self._image_queue.put(None)
                await asyncio.wait_for(self._file_writer_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "File writer task did not shutdown cleanly during cleanup"
                )
                self._file_writer_task.cancel()
            except Exception as e:
                logger.error(f"Error during file writer cleanup: {e}")
                self._file_writer_task.cancel()
        await self.tcp_client.disconnect()
        logger.info("Cleanup completed")


if __name__ == "__main__":
    ioc_options, run_options = ioc_arg_parser(
        default_prefix="A1Soft:", desc=dedent(DetectorIOC.__doc__)
    )

    ioc = DetectorIOC(**ioc_options)
    run(ioc.pvdb, **run_options)
