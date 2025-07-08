#!/usr/bin/env python3
"""
Caproto server implementing TCP interface to LabView detector system.
Exposes EPICS PVs that control acquisition, parameters, and monitoring.
"""

import asyncio
import json
import random
import socket
import time
from textwrap import dedent
from typing import Any, cast

from caproto.server import PVGroup, ioc_arg_parser, pvproperty, run, PvpropertyData
from caproto import ChannelType


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
                loop = asyncio.get_event_loop()
                msg: bytes = (cmd + "\r\n").encode()
                await loop.sock_sendall(json_socket, msg)

                # Read response using asyncio-compatible operations
                response: str = ""
                char_ret: bool = False

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
                        print("Command timeout after 5 seconds")
                        return None

                return json.loads(response.replace("\\", "/"))
            except Exception as e:
                print(f"Command failed: {e}")
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
            await self.last_error.write(f"Failed to set {param_name} to {value}")
            return None
        response = await self.tcp_client.get_parameter(param_name)
        if not response:
            await self.last_error.write(f"Failed to get new value of {param_name}")
            return None
        result = next(
            (item for item in response["values"] if item["name"] == param_name), None
        )
        if not result:
            await self.last_error.write(f"Failed to get new value of {param_name}")
            return None
        actual_value = result["value"]
        if actual_value != value:
            await self.last_error.write(
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

    # Status and info
    connection_status = pvproperty(value=0, name="SYS:CONNECTED", read_only=True)
    last_error = pvproperty(value="", name="SYS:ERROR", read_only=True, dtype=str)
    last_sync = pvproperty(value="", name="SYS:LAST_SYNC", read_only=True, dtype=str)
    sync = pvproperty(
        value="ON",
        name="SYS:SYNC",
        dtype=ChannelType.ENUM,
        enum_strings=("OFF", "ON"),
        record="bi",
    )
    """Enable/disable automatic parameter synchronization, set interval via SYS:SYNC.SCAN"""

    # Detector parameters
    state = pvproperty(name="STATE", dtype=str, read_only=True)
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
    date_number = pvproperty(put=_param_write, name="DATE_NUMBER", enum_strings=("FALSE", "TRUE"), dtype=bool)
    loc_det = pvproperty(put=_param_write, name="LOC_DET", enum_strings=("FALSE", "TRUE"), dtype=bool)
    xtab = pvproperty(put=_param_write, name="XTAB", enum_strings=("FALSE", "TRUE"), dtype=bool)
    spin = pvproperty(put=_param_write, name="SPIN", enum_strings=("FALSE", "TRUE"), dtype=bool)
    reg_name = pvproperty(put=_param_write, name="REG_NAME", dtype=str)
    name_string = pvproperty(put=_param_write, name="NAME_STRING", dtype=str)
    generated_name = pvproperty(put=_param_write, name="GENERATED_NAME", dtype=str)
    comment1 = pvproperty(put=_param_write, name="COMMENT1", dtype=str)
    start_time = pvproperty(put=_param_write, name="START_TIME", dtype=str)
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
    yscale_name = pvproperty(put=_param_write, name="YSCALE_NAME", dtype=str)
    xscale_mult = pvproperty(put=_param_write, name="XSCALE_MULT", dtype=float)
    xscale_max = pvproperty(put=_param_write, name="XSCALE_MAX", dtype=float)
    xscale_min = pvproperty(put=_param_write, name="XSCALE_MIN", dtype=float)
    xscale_name = pvproperty(put=_param_write, name="XSCALE_NAME", dtype=str)
    psu_mode = pvproperty(put=_param_write, name="PSU_MODE", dtype=str)
    over_r_arr = pvproperty(put=_param_write, name="OVER_R_ARR", dtype=str)
    over_range = pvproperty(put=_param_write, name="OVER_RANGE", dtype=int)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.tcp_client: DetectorTCPClient = DetectorTCPClient()
        self._sync_task: asyncio.Task | None = None
        self._update_listener_task: asyncio.Task | None = None
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
                    await self.last_error.write(
                        "Failed to sync parameters - no response or invalid response"
                    )
            except asyncio.CancelledError:
                # Handle graceful shutdown
                raise
            except Exception as e:
                await self.last_error.write(f"Sync error: {e}")
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
        # TODO: Handle if state is not idle
        if value > 0:
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="START"
            )
            if response:
                await self.acquisition_status.write(1)
                await self.last_error.write("Acquisition started")
            else:
                await self.last_error.write("Failed to start acquisition")
        else:
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="STOP"
            )
            if response:
                await self.acquisition_status.write(0)
                await self.last_error.write("Acquisition stopped")
            else:
                await self.last_error.write("Failed to stop acquisition")

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
                await self.last_error.write("Monitor enabled")
            else:
                await self.last_error.write("Failed to enable monitor")
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
                await self.last_error.write("Monitor disabled")
            else:
                await self.last_error.write("Failed to disable monitor")
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
                await self.last_error.write("Detector disabled")
            else:
                await self.last_error.write("Failed to disable detector")
        return value

    @get_image.putter
    async def get_image(self, instance: Any, value: int) -> int:
        """Request an image from detector."""
        if value == 1:
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="GET_IMAGE"
            )
            if response:
                await self.last_error.write("Image requested")
            else:
                await self.last_error.write("Failed to request image")
        return value

    @get_stats.putter
    async def get_stats(self, instance: Any, value: int) -> int:
        """Get acquisition statistics."""
        if value == 1:
            response: dict[str, Any] | None = await self.tcp_client.send_command(
                "ACTION", action="GET_ACQ_STATS"
            )
            if response:
                await self.last_error.write("Stats requested")
            else:
                await self.last_error.write("Failed to get stats")
        return value

    async def cleanup(self) -> None:
        """Clean up background tasks and connections."""
        if self._sync_task:
            self._sync_task.cancel()
        if self._update_listener_task:
            self._update_listener_task.cancel()
        await self.tcp_client.disconnect()
        print("Cleanup completed")


if __name__ == "__main__":
    ioc_options, run_options = ioc_arg_parser(
        default_prefix="A1Soft:", desc=dedent(DetectorIOC.__doc__)
    )

    ioc = DetectorIOC(**ioc_options)
    run(ioc.pvdb, **run_options)
