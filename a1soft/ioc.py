#!/usr/bin/env python3
"""
Caproto server implementing TCP interface to LabView detector system.
Exposes EPICS PVs that control acquisition, parameters, and monitoring.
"""

import asyncio
import json
import logging
import random
import socket
import time
from pathlib import Path
from textwrap import dedent
from typing import Any, cast, Literal

from caproto.server import PVGroup, ioc_arg_parser, pvproperty, run, PvpropertyData
from caproto import ChannelType
import numpy as np

# Configure logging for performance monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DetectorTCPClient:
    """TCP client to communicate with LabView detector system."""

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
        self.json_socket: socket.socket | None = None
        self.data_socket: socket.socket | None = None
        self.live_socket: socket.socket | None = None
        self.connected: bool = False
        self._json_lock: asyncio.Lock = asyncio.Lock()
        self._data_lock: asyncio.Lock = asyncio.Lock()

    async def connect(self) -> None:
        """Connect to all three TCP ports."""
        try:
            self.json_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.json_socket.setblocking(False)  # Make socket non-blocking
            await asyncio.get_event_loop().sock_connect(
                self.json_socket, (self.host, self.json_port)
            )

            self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.data_socket.setblocking(False)  # Make socket non-blocking
            await asyncio.get_event_loop().sock_connect(
                self.data_socket, (self.host, self.data_port)
            )

            self.live_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.live_socket.setblocking(False)  # Make socket non-blocking
            await asyncio.get_event_loop().sock_connect(
                self.live_socket, (self.host, self.live_port)
            )

            self.connected = True
            print("Connected to detector TCP interface")
        except Exception as e:
            print(f"Failed to connect to detector: {e}")
            self.connected = False

    async def disconnect(self) -> None:
        """Disconnect from all sockets."""
        for sock in [self.json_socket, self.data_socket, self.live_socket]:
            if sock:
                sock.close()
        self.connected = False

    async def send_command(
        self,
        cmd_type: str,
        *,
        action: str | None = None,
        parameter: str | None = None,
        value: str | None = None,
    ) -> dict[str, Any] | None:
        """Send JSON command to detector."""
        if not self.connected:
            return None

        json_socket = cast(socket.socket, self.json_socket)

        cmd_id: int = random.randrange(100)

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

        async with self._json_lock:
            try:
                command_start = time.perf_counter()
                loop = asyncio.get_event_loop()
                msg: bytes = (cmd + "\r\n").encode()
                
                send_start = time.perf_counter()
                await loop.sock_sendall(json_socket, msg)
                send_time = time.perf_counter() - send_start

                # Read response using asyncio-compatible operations
                response: str = ""
                char_ret: bool = False

                read_start = time.perf_counter()
                while True:
                    try:
                        # Use asyncio.wait_for to add timeout capability
                        data: bytes = await asyncio.wait_for(
                            loop.sock_recv(json_socket, 1), timeout=5.0
                        )

                        if data == b"\r":
                            char_ret = True
                        elif data == b"\n" and char_ret:
                            break
                        else:
                            char_ret = False
                            response += data.decode("utf-8")

                    except asyncio.TimeoutError:
                        logger.error("Command timeout after 5 seconds")
                        print("Command timeout after 5 seconds")
                        return None

                read_time = time.perf_counter() - read_start
                total_time = time.perf_counter() - command_start
                
                logger.debug(f"TCP command '{cmd_type}': send {send_time*1000:.2f}ms, "
                           f"read {read_time*1000:.2f}ms, total {total_time*1000:.2f}ms")

                return json.loads(response.replace("\\", "/"))
            except Exception as e:
                logger.error(f"Command failed: {e}")
                print(f"Command failed: {e}")
                return None

    async def read_data(self) -> dict[str, Any] | None:
        """Read image data from detector data socket.
        
        Returns dict with parsed header info and pixel data, or None on error/timeout.

        The first data channel appears to be the current image, the second data channel is the sum of the images so far.

        The timing on the first data channel needs to be extremely precise, because the start of the next scan will
        0 out all of the data.
        """
        if not self.connected or not self.data_socket:
            return None

        async with self._data_lock:
            try:
                read_start = time.perf_counter()
                loop = asyncio.get_event_loop()
                
                # Read 40-byte header
                header_start = time.perf_counter()
                header_data = b""
                while len(header_data) < 40:
                    try:
                        chunk = await asyncio.wait_for(
                            loop.sock_recv(self.data_socket, 40 - len(header_data)), 
                            timeout=5.0
                        )
                        if not chunk:
                            raise ConnectionError("Socket closed while reading header")
                        header_data += chunk
                    except asyncio.TimeoutError:
                        logger.error("Timeout reading data header")
                        print("Timeout reading data header")
                        return None
                
                header_time = time.perf_counter() - header_start
                
                # Parse header
                marker = int.from_bytes(header_data[0:4], byteorder="big", signed=False)
                
                if marker != 0xf0f0:
                    logger.error(f"Invalid marker={marker:#x}, expected 0xf0f0")
                    print(f"ERROR: Invalid marker={marker:#x}, expected 0xf0f0")
                    return None
                    
                index = int.from_bytes(header_data[4:8], byteorder="big", signed=True)
                state = int.from_bytes(header_data[8:12], byteorder="big", signed=False)
                reserved = int.from_bytes(header_data[12:16], byteorder="big", signed=False)
                width = int.from_bytes(header_data[16:20], byteorder="big", signed=False)
                height = int.from_bytes(header_data[20:24], byteorder="big", signed=False)
                length = int.from_bytes(header_data[24:28], byteorder="big", signed=False)
                cur_width = int.from_bytes(header_data[28:32], byteorder="big", signed=False)
                cur_height = int.from_bytes(header_data[32:36], byteorder="big", signed=False)
                cur_length = int.from_bytes(header_data[36:40], byteorder="big", signed=False)
                
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
                    "channel_2_sum": 0
                }
                
                print(f"Data header - index: {index}, state: {state}, "
                    f"dimensions: {height}x{width}, length: {length}")
                
                # Read first data channel if length > 0
                if length > 0:
                    ch1_start = time.perf_counter()
                    channel_1_data = await self._read_channel_data(length, timeout=10.0)
                    ch1_read_time = time.perf_counter() - ch1_start
                    
                    if channel_1_data is None:
                        return None
                        
                    # Parse pixel data from bytes to integers
                    parse_start = time.perf_counter()
                    pixel_data_1 = []
                    sum_1 = 0
                    for i in range(length // 4):
                        pixel = int.from_bytes(
                            channel_1_data[i * 4:(i * 4) + 4],
                            byteorder="little",
                            signed=False
                        )
                        pixel_data_1.append(pixel)
                        sum_1 += pixel
                        
                    result["channel_1_data"] = np.reshape(pixel_data_1, (cur_height, cur_width))
                    result["channel_1_sum"] = sum_1
                    ch1_parse_time = time.perf_counter() - parse_start
                    logger.info(f"Channel 1: read {ch1_read_time*1000:.2f}ms, parse {ch1_parse_time*1000:.2f}ms, sum: {sum_1}")
                    print(f"Channel 1 sum: {sum_1}")
                
                # Read second data channel if cur_length > 0  
                if cur_length > 0:
                    ch2_start = time.perf_counter()
                    channel_2_data = await self._read_channel_data(cur_length, timeout=10.0)
                    ch2_read_time = time.perf_counter() - ch2_start
                    
                    if channel_2_data is None:
                        return None
                        
                    # Parse pixel data from bytes to integers
                    parse_start = time.perf_counter()
                    pixel_data_2 = []
                    sum_2 = 0
                    for i in range(cur_length // 4):
                        pixel = int.from_bytes(
                            channel_2_data[i * 4:(i * 4) + 4],
                            byteorder="little", 
                            signed=False
                        )
                        pixel_data_2.append(pixel)
                        sum_2 += pixel
                        
                    result["channel_2_data"] = np.reshape(pixel_data_2, (cur_height, cur_width))
                    result["channel_2_sum"] = sum_2
                    ch2_parse_time = time.perf_counter() - parse_start
                    logger.info(f"Channel 2: read {ch2_read_time*1000:.2f}ms, parse {ch2_parse_time*1000:.2f}ms, sum: {sum_2}")
                    print(f"Channel 2 sum: {sum_2}")
                    
                total_read_time = time.perf_counter() - read_start
                logger.info(f"Total data read: header {header_time*1000:.2f}ms, "
                          f"total {total_read_time*1000:.2f}ms, "
                          f"data size: ch1={length}bytes, ch2={cur_length}bytes")
                    
                return result
                
            except Exception as e:
                logger.error(f"Error reading data: {e}")
                print(f"Error reading data: {e}")
                return None
    
    async def _read_channel_data(self, expected_length: int, timeout: float = 10.0) -> bytes | None:
        """Helper method to read a complete data channel with timeout."""
        if not self.data_socket:
            return None
            
        try:
            loop = asyncio.get_event_loop()
            data = b""
            
            while len(data) < expected_length:
                remaining = expected_length - len(data)
                try:
                    chunk = await asyncio.wait_for(
                        loop.sock_recv(self.data_socket, remaining),
                        timeout=timeout
                    )
                    if not chunk:
                        raise ConnectionError("Socket closed while reading channel data")
                    data += chunk
                except asyncio.TimeoutError:
                    print(f"Timeout reading channel data, got {len(data)}/{expected_length} bytes")
                    return None
                    
            return data
            
        except Exception as e:
            print(f"Error reading channel data: {e}")
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
            print(f"Failed to set {param_name} to {value}")
            return None
        response = await self.tcp_client.get_parameter(param_name)
        if not response:
            print(f"Failed to get new value of {param_name}")
            return None
        result = next(
            (item for item in response["values"] if item["name"] == param_name), None
        )
        if not result:
            print(f"Failed to get new value of {param_name}")
            return None
        actual_value = result["value"]
        if actual_value != value:
            print(
                f"Failed to set {param_name} to {value}, was set to {actual_value} instead."
            )
        return actual_value

    # Acquisition control
    acquire = pvproperty(value=0, name="ACQUIRE")
    acquisition_status = pvproperty(value=0, name="ACQ:STATUS", read_only=True)

    # Monitor control
    monitor_on = pvproperty(value=0, name="MON:ON")
    monitor_off = pvproperty(value=0, name="MON:OFF")
    monitor_status = pvproperty(value=0, name="MON:STATUS", read_only=True)

    # Detector control
    detector_off = pvproperty(value=0, name="DET:OFF")
    detector_status = pvproperty(value=1, name="DET:STATUS", read_only=True)

    # Image acquisition
    get_image = pvproperty(value=0, name="IMG:GET")
    get_stats = pvproperty(value=0, name="ACQ:STATS")

    # File writing
    file_capture = pvproperty(value=False, name="FILE:CAPTURE", dtype=bool)
    file_name = pvproperty(value="", name="FILE:NAME", dtype=ChannelType.STRING)
    file_path = pvproperty(value="", name="FILE:PATH", dtype=ChannelType.STRING)
    file_status = pvproperty(value="", name="FILE:STATUS", read_only=True, dtype=ChannelType.STRING)
    num_captured = pvproperty(value=0, name="FILE:NUM_CAPTURED", read_only=True, dtype=int)

    # Status and info
    connection_status = pvproperty(value=0, name="SYS:CONNECTED", read_only=True)
    last_sync = pvproperty(value="", name="SYS:LAST_SYNC", read_only=True, dtype=ChannelType.STRING)
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
    endX = pvproperty(put=_param_write, name="ENDX", dtype=int)
    startY = pvproperty(put=_param_write, name="STARTY", dtype=int)
    num_slice = pvproperty(put=_param_write, name="NUM_SLICE", dtype=int)
    endY = pvproperty(put=_param_write, name="ENDY", dtype=int)
    startX = pvproperty(put=_param_write, name="STARTX", dtype=int)
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
    reg_num = pvproperty(put=_param_write, name="REG_NUM", dtype=int)
    tot_steps = pvproperty(put=_param_write, name="TOT_STEPS", dtype=int)
    add_fms = pvproperty(put=_param_write, name="ADD_FMS", dtype=int)
    act_scans = pvproperty(name="ACT_SCANS", dtype=int, read_only=True)
    dith_steps = pvproperty(put=_param_write, name="DITH_STEPS", dtype=int)
    start_ke = pvproperty(put=_param_write, name="START_KE", dtype=float)
    step_size = pvproperty(put=_param_write, name="STEP_SIZE", dtype=float)
    end_ke = pvproperty(put=_param_write, name="END_KE", dtype=float)
    spin_offs = pvproperty(put=_param_write, name="SPIN_OFFS", dtype=float)
    width = pvproperty(put=_param_write, name="WIDTH", dtype=float)
    center_ke = pvproperty(put=_param_write, name="CENTER_KE", dtype=float)
    first_energy = pvproperty(put=_param_write, name="FIRST_ENERGY", dtype=float)
    deflX = pvproperty(put=_param_write, name="DEFLX", dtype=float)
    deflY = pvproperty(put=_param_write, name="DEFLY", dtype=float)
    dbl10 = pvproperty(put=_param_write, name="DBL10", dtype=float)
    acq_mode = pvproperty(
        put=_param_write,
        name="ACQ_MODE",
        dtype=ChannelType.ENUM,
        enum_strings=("Fixed", "Swept", "Dither"),
    )
    date_number = pvproperty(
        put=_param_write, name="DATE_NUMBER", enum_strings=("FALSE", "TRUE"), dtype=bool
    )
    loc_det = pvproperty(
        put=_param_write, name="LOC_DET", enum_strings=("FALSE", "TRUE"), dtype=bool
    )
    xtab = pvproperty(
        put=_param_write, name="XTAB", enum_strings=("FALSE", "TRUE"), dtype=bool
    )
    spin = pvproperty(
        put=_param_write, name="SPIN", enum_strings=("FALSE", "TRUE"), dtype=bool
    )
    reg_name = pvproperty(put=_param_write, name="REG_NAME", dtype=ChannelType.STRING)
    name_string = pvproperty(put=_param_write, name="NAME_STRING", dtype=ChannelType.STRING)
    generated_name = pvproperty(put=_param_write, name="GENERATED_NAME", dtype=ChannelType.STRING)
    comment1 = pvproperty(put=_param_write, name="COMMENT1", dtype=ChannelType.STRING)
    start_time = pvproperty(put=_param_write, name="START_TIME", dtype=ChannelType.STRING)
    discr = pvproperty(put=_param_write, name="DISCR", dtype=int)
    adc_mask = pvproperty(put=_param_write, name="ADC_MASK", dtype=int)
    adc_offset = pvproperty(put=_param_write, name="ADC_OFFSET", dtype=int)
    p_cnt_type = pvproperty(put=_param_write, name="P_CNT_TYPE", dtype=int)
    pc_mask = pvproperty(put=_param_write, name="PC_MASK", dtype=int)
    soft_bin_x = pvproperty(put=_param_write, name="SOFT_BIN_X", dtype=int)
    soft_bin_y = pvproperty(put=_param_write, name="SOFT_BIN_Y", dtype=int)
    escale_mult = pvproperty(put=_param_write, name="ESCALE_MULT", dtype=float)
    escale_max = pvproperty(put=_param_write, name="ESCALE_MAX", dtype=float)
    escale_min = pvproperty(put=_param_write, name="ESCALE_MIN", dtype=float)
    yscale_mult = pvproperty(put=_param_write, name="YSCALE_MULT", dtype=float)
    yscale_max = pvproperty(put=_param_write, name="YSCALE_MAX", dtype=float)
    yscale_min = pvproperty(put=_param_write, name="YSCALE_MIN", dtype=float)
    yscale_name = pvproperty(put=_param_write, name="YSCALE_NAME", dtype=ChannelType.STRING)
    xscale_mult = pvproperty(put=_param_write, name="XSCALE_MULT", dtype=float)
    xscale_max = pvproperty(put=_param_write, name="XSCALE_MAX", dtype=float)
    xscale_min = pvproperty(put=_param_write, name="XSCALE_MIN", dtype=float)
    xscale_name = pvproperty(put=_param_write, name="XSCALE_NAME", dtype=ChannelType.STRING)
    psu_mode = pvproperty(put=_param_write, name="PSU_MODE", dtype=ChannelType.STRING)
    over_r_arr = pvproperty(put=_param_write, name="OVER_R_ARR", dtype=ChannelType.STRING)
    over_range = pvproperty(put=_param_write, name="OVER_RANGE", dtype=int)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.tcp_client: DetectorTCPClient = DetectorTCPClient()
        self._sync_task: asyncio.Task | None = None
        self._update_listener_task: asyncio.Task | None = None
        self._file_writer_task: asyncio.Task | None = None
        self._image_queue: asyncio.Queue = asyncio.Queue(maxsize=100)  # Buffer up to 100 images
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
        self._file_handle: Any | None = None
        self._full_file_path: Path | None = None
        self._last_array: np.ndarray | None = None
        
        # Performance tracking
        self._perf_stats = {
            "total_cycles": 0,
            "total_cycle_time": 0.0,
            "total_frame_time": 0.0,
            "total_write_time": 0.0,
            "queue_full_drops": 0,
            "failed_frames": 0,
            "last_reset": time.perf_counter()
        }

    async def _get_current_frame(self) -> dict[str, Any]| None:
        start_time = time.perf_counter()
        response: dict[str, Any] | None = await self.tcp_client.send_command(
            "ACTION", action="GET_IMAGE"
        )
        command_time = time.perf_counter()
        logger.info(f"GET_IMAGE command took {(command_time - start_time)*1000:.2f}ms")
        
        if response:
            print("Image requested")
            data = await self.tcp_client.read_data()
            read_time = time.perf_counter()
            logger.info(f"read_data took {(read_time - command_time)*1000:.2f}ms")
            
            if data:
                total_time = read_time - start_time
                logger.info(f"Total frame acquisition took {total_time*1000:.2f}ms")
                print(f"Image received: {data}")
                return data
            else:
                logger.warning("Failed to read image data")
                print("Failed to read image")
        else:
            logger.warning("Failed to request image")
            print("Failed to request image")

        return None

    async def _background_file_writer(self) -> None:
        """Background task that continuously writes image data from queue to file."""
        while True:
            try:
                # Wait for data from the queue
                queue_start = time.perf_counter()
                data = await self._image_queue.get()
                queue_wait_time = time.perf_counter() - queue_start
                logger.info(f"Queue wait took {queue_wait_time*1000:.2f}ms, queue size: {self._image_queue.qsize()}")
                
                # Check for shutdown signal (None is used as sentinel)
                if data is None:
                    break
                    
                if self._file_handle:
                    process_start = time.perf_counter()
                    
                    # Hack to recover the current frame from the sum of the scans so far
                    # This is needed because the current frame is already reset to all zeros
                    # when the act_scans parameter is incremented...
                    current_frame = data["channel_2_data"] - self._last_array if self._last_array is not None else data["channel_2_data"]
                    data["channel_1_data"] = current_frame
                    self._last_array = data["channel_2_data"]
                    
                    data_processing_time = time.perf_counter() - process_start
                    
                    # Write to file (this is the potentially blocking operation)
                    write_start = time.perf_counter()
                    self._file_handle.write(str(data))
                    self._file_handle.flush()
                    write_time = time.perf_counter() - write_start
                    
                    total_processing_time = time.perf_counter() - process_start
                    logger.info(f"File write: data processing {data_processing_time*1000:.2f}ms, "
                              f"write+flush {write_time*1000:.2f}ms, total {total_processing_time*1000:.2f}ms")
                    
                # Mark the task as done
                self._image_queue.task_done()
                
            except asyncio.CancelledError:
                # Handle graceful shutdown
                break
            except Exception as e:
                logger.error(f"Error in background file writer: {e}")
                print(f"Error in background file writer: {e}")
                # Mark the task as done even on error
                self._image_queue.task_done()

    async def _write_image_to_file(self) -> None:
        """Request image and queue it for writing."""
        if not self._file_handle:
            return
            
        start_time = time.perf_counter()
        data = await self._get_current_frame()
        frame_time = time.perf_counter() - start_time
        
        if data:
            try:
                queue_start = time.perf_counter()
                # Put data in queue without blocking (will raise QueueFull if full)
                self._image_queue.put_nowait(data)
                queue_time = time.perf_counter() - queue_start
                total_time = time.perf_counter() - start_time
                
                logger.info(f"Image queued: frame acquisition {frame_time*1000:.2f}ms, "
                          f"queue insertion {queue_time*1000:.2f}ms, "
                          f"total {total_time*1000:.2f}ms, queue size: {self._image_queue.qsize()}")
                
                # Update performance stats
                self._perf_stats["total_frame_time"] += frame_time
            except asyncio.QueueFull:
                logger.warning(f"Image queue is full ({self._image_queue.qsize()} items), dropping frame")
                print("Image queue is full, dropping frame")
                self._perf_stats["queue_full_drops"] += 1
        else:
            logger.error(f"Failed to get current frame for file writing (took {frame_time*1000:.2f}ms)")
            print("Failed to get current frame for file writing")
            self._perf_stats["failed_frames"] += 1

    def _log_performance_summary(self) -> None:
        """Log performance statistics summary."""
        stats = self._perf_stats
        cycles = stats["total_cycles"]
        
        if cycles > 0:
            avg_cycle_time = (stats["total_cycle_time"] / cycles) * 1000
            avg_frame_time = (stats["total_frame_time"] / cycles) * 1000  
            avg_write_time = (stats["total_write_time"] / cycles) * 1000
            elapsed = time.perf_counter() - stats["last_reset"]
            cycle_rate = cycles / elapsed
            
            logger.info(f"=== PERFORMANCE SUMMARY ({cycles} cycles, {elapsed:.1f}s) ===")
            logger.info(f"Cycle rate: {cycle_rate:.2f} Hz")
            logger.info(f"Average times: cycle {avg_cycle_time:.1f}ms, frame {avg_frame_time:.1f}ms, write {avg_write_time:.1f}ms")
            logger.info(f"Queue drops: {stats['queue_full_drops']}, Failed frames: {stats['failed_frames']}")
            logger.info(f"Queue size: {self._image_queue.qsize()}")
            logger.info("=" * 50)

    @file_capture.putter
    async def file_capture(self, instance: PvpropertyData, value: Literal["On", "Off"]) -> bool:
        """Start or stop file capture."""
        if value == "On":
            # TODO: Construct nexus file format here?
            if self._file_handle:
                print("File capture already in progress")
                return False
            file_path = self.file_path.value
            if not file_path:
                print(f"File path not set, got: {file_path}")
                return False
            filename = self.file_name.value
            if not filename or not filename.endswith(".nxs"):
                print(f"File name must be set and end with .nxs, got: {filename}")
                return False
            self._full_file_path = Path(file_path) / filename
            self._file_handle = open(self._full_file_path, "a")
            
            # Create fresh queue for this capture session
            self._image_queue = asyncio.Queue(maxsize=100)
            
            # Start background file writer task
            self._file_writer_task = asyncio.create_task(self._background_file_writer())
            
            await self.file_status.write(f"File capture started, writing to {self._full_file_path}")
        else:
            # Stop background writer task
            if self._file_writer_task:
                # Send shutdown signal to background writer
                await self._image_queue.put(None)
                # Wait for the writer task to finish
                try:
                    await asyncio.wait_for(self._file_writer_task, timeout=5.0)
                except asyncio.TimeoutError:
                    print("Warning: File writer task did not shutdown cleanly")
                    self._file_writer_task.cancel()
                self._file_writer_task = None
                
            if self._file_handle:
                self._file_handle.flush()
                self._file_handle.close()
                self._file_handle = None
                self._full_file_path = None
                self._last_array = None
                
                # Clear any remaining items in queue
                while not self._image_queue.empty():
                    try:
                        self._image_queue.get_nowait()
                        self._image_queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                        
                await self.file_status.write(f"File capture stopped, wrote to {self._full_file_path}")
            else:
                print("No file capture in progress")
        return value

    @act_scans.scan(period=0.001)
    async def act_scans(self, instance: PvpropertyData, async_lib: Any) -> Any:
        """Scan for acutal number of scans completed."""
        if self.acquisition_status.value == 1 and self.file_capture.value == "On":
            cycle_start = time.perf_counter()
            num_captured = self.num_captured.value
            
            param_start = time.perf_counter()
            response = await self.tcp_client.get_parameter(self._pvs_to_param_names[self.act_scans])
            param_time = time.perf_counter() - param_start
            
            if response and "values" in response:
                value = response["values"][0]["value"]
                if value > num_captured:
                    logger.info(f"New scan detected: {value} (was {num_captured}), "
                              f"parameter check took {param_time*1000:.2f}ms")
                    
                    write_start = time.perf_counter()
                    await self._write_image_to_file()
                    write_time = time.perf_counter() - write_start
                    
                    update_start = time.perf_counter()
                    await async_lib.library.gather(
                        self.num_captured.write(value),
                        self.act_scans.write(value),
                    )
                    update_time = time.perf_counter() - update_start
                    
                    total_cycle_time = time.perf_counter() - cycle_start
                    logger.info(f"Scan cycle complete: parameter {param_time*1000:.2f}ms, "
                              f"write {write_time*1000:.2f}ms, "
                              f"update {update_time*1000:.2f}ms, "
                              f"total {total_cycle_time*1000:.2f}ms")
                    
                    # Update performance statistics
                    self._perf_stats["total_cycles"] += 1
                    self._perf_stats["total_cycle_time"] += total_cycle_time
                    self._perf_stats["total_write_time"] += write_time
                    
                    # Log performance summary every 10 cycles
                    if self._perf_stats["total_cycles"] % 10 == 0:
                        self._log_performance_summary()
            else:
                logger.error(f"Failed to get actual number of scans, got: {response}")
                print(f"Failed to get actual number of scans, got: {response}")

    @state.scan(period=0.001)
    async def state(self, instance: PvpropertyData, async_lib: Any) -> Any:
        """Scan for state changes."""
        if self.acquisition_status.value == 1:
            response = await self.tcp_client.get_parameter(self._pvs_to_param_names[self.state])
            if response and "values" in response:
                value = response["values"][0]["value"]
                if value == "STANDBY" and value != instance.value:
                    await async_lib.library.gather(self.acquire.write(0), self.state.write(value))
            else:
                print(f"Failed to get state update, got: {response}")

    @sync.scan(period=0.1, use_scan_field=True)
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
                    print(
                        "Failed to sync parameters - no response or invalid response"
                    )
            except asyncio.CancelledError:
                # Handle graceful shutdown
                raise
            except Exception as e:
                print(f"Sync error: {e}")
                print(f"Sync error: {e}")  # Also log to console

    @connection_status.startup
    async def connection_status(self, instance: Any, async_lib: Any) -> None:
        """Initialize TCP connection and background tasks on IOC startup."""
        print("Initializing detector connection...")
        await self.tcp_client.connect()
        await self.connection_status.write(1 if self.tcp_client.connected else 0)

        if not self.tcp_client.connected:
            raise RuntimeError("Failed to connect to detector")

        print("Detector connection established")

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
            print(f"Error updating PVs: {e}")

    @acquire.putter
    async def acquire(self, instance: Any, value: int) -> int:
        """Start acquisition when PV is written to."""
        if value > 0:
            if instance.value > 0:
                print("Acquisition already in progress")
                return instance.value
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="START"
            )
            if response:
                await self.acquisition_status.write(1)
            else:
                print("Failed to start acquisition")
                return 0
        else:
            if instance.value == 0:
                print("Acquisition not in progress")
                return 0
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="STOP"
            )
            if response:
                await self.acquisition_status.write(0)
                print("Acquisition stopped")
            else:
                print("Failed to stop acquisition")
                return self.acquire.value

        return value

    @monitor_on.putter
    async def monitor_on(self, instance: Any, value: int) -> int:
        """Turn monitoring on."""
        if value == 1:
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="MONITOR_ON"
            )
            if response:
                await self.monitor_status.write(1)
                print("Monitor enabled")
            else:
                print("Failed to enable monitor")
        return value

    @monitor_off.putter
    async def monitor_off(self, instance: Any, value: int) -> int:
        """Turn monitoring off."""
        if value == 1:
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="MONITOR_OFF"
            )
            if response:
                await self.monitor_status.write(0)
                print("Monitor disabled")
            else:
                print("Failed to disable monitor")
        return value

    @detector_off.putter
    async def detector_off(self, instance: Any, value: int) -> int:
        """Turn detector off."""
        if value == 1:
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="DET_OFF"
            )
            if response:
                await self.detector_status.write(0)
                print("Detector disabled")
            else:
                print("Failed to disable detector")
        return value

    @get_image.putter
    async def get_image(self, instance: Any, value: int) -> int:
        """Request an image from detector."""
        if value == 1:
            data = await self._get_current_frame()
            if data:
                print(f"Image received: {data}")
            else:
                print("Failed to get image")
        return value

    @get_stats.putter
    async def get_stats(self, instance: Any, value: int) -> int:
        """Get acquisition statistics."""
        if value == 1:
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="GET_ACQ_STATS"
            )
            if response:
                print("Stats requested")
            else:
                print("Failed to get stats")
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
                print("Warning: File writer task did not shutdown cleanly during cleanup")
                self._file_writer_task.cancel()
            except Exception as e:
                print(f"Error during file writer cleanup: {e}")
                self._file_writer_task.cancel()
        await self.tcp_client.disconnect()
        print("Cleanup completed")


if __name__ == "__main__":
    ioc_options, run_options = ioc_arg_parser(
        default_prefix="A1Soft:", desc=dedent(DetectorIOC.__doc__)
    )

    ioc = DetectorIOC(**ioc_options)
    run(ioc.pvdb, **run_options)
