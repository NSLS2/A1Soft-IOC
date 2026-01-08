import time
from pathlib import Path
from typing import Optional
from itertools import count

import numpy as np
from bluesky.protocols import WritesStreamAssets, WritesExternalAssets, Readable
from bluesky.utils import SyncOrAsyncIterator, StreamAsset, Asset
from event_model import compose_stream_resource, compose_resource, DataKey
from area_detector_handlers.handlers import HDF5DatasetSliceHandler
from ophyd import Device, Component as Cpt, EpicsSignal, EpicsSignalRO, Staged
from ophyd.status import Status


class A1SoftFileHandler(HDF5DatasetSliceHandler):
    specs = {"A1_HDF5"} | HDF5DatasetSliceHandler.specs

    def __init__(self, filename, frame_per_point=1):
        hardcoded_key = "entry/instrument/analyzer/data"
        super().__init__(filename, key=hardcoded_key, frame_per_point=frame_per_point)


class SpectrumAnalyzer(Device, Readable):
    # Acquisition control
    acquire = Cpt(EpicsSignal, "ACQUIRE")
    acquisition_status = Cpt(EpicsSignalRO, "ACQ:STATUS")

    # Detector control
    det_off = Cpt(EpicsSignal, "DET:OFF")

    # Live data monitoring
    live_monitoring = Cpt(EpicsSignal, "LIVE:MONITORING")
    live_max_count = Cpt(EpicsSignalRO, "LIVE:MAX_COUNT")
    live_last_update = Cpt(EpicsSignalRO, "LIVE:LAST_UPDATE")
    live_max_count_threshold = Cpt(EpicsSignal, "LIVE:MAX_COUNT_THRESH")
    live_max_count_exceeded = Cpt(EpicsSignal, "LIVE:MAX_COUNT_EXCEEDED")
    live_max_count_avg_n = Cpt(EpicsSignal, "LIVE:MAX_COUNT_AVG_N")

    # Status and info
    connection_status = Cpt(EpicsSignalRO, "SYS:CONNECTED")
    last_sync = Cpt(EpicsSignalRO, "SYS:LAST_SYNC")
    sync = Cpt(EpicsSignal, "SYS:SYNC")

    # File writing
    file_capture = Cpt(EpicsSignal, "FILE:CAPTURE")
    file_prefix = Cpt(EpicsSignal, "FILE:PREFIX", string=True)
    file_name = Cpt(EpicsSignalRO, "FILE:NAME", string=True, kind="config")
    file_path = Cpt(EpicsSignal, "FILE:PATH", string=True, kind="config")
    num_captured = Cpt(EpicsSignalRO, "FILE:NUM_CAPTURED")
    num_processed = Cpt(EpicsSignalRO, "FILE:NUM_PROCESSED")
    total_intensity = Cpt(EpicsSignalRO, "TOTAL_INTENSITY")

    # Detector parameters
    state = Cpt(EpicsSignalRO, "STATE", string=True)
    endX = Cpt(EpicsSignal, "ENDX")
    startY = Cpt(EpicsSignal, "STARTY")
    num_slice = Cpt(EpicsSignal, "NUM_SLICE", kind="config")
    endY = Cpt(EpicsSignal, "ENDY")
    startX = Cpt(EpicsSignal, "STARTX")
    frames = Cpt(EpicsSignal, "FRAMES", kind="config")
    num_steps = Cpt(EpicsSignal, "NUM_STEPS", kind="config")
    pass_energy = Cpt(EpicsSignal, "PASS_ENERGY", string=True, kind="config")
    lens_mode = Cpt(EpicsSignal, "LENS_MODE", string=True, kind="config")
    num_scans = Cpt(EpicsSignal, "NUM_SCANS", kind="config")
    reg_num = Cpt(EpicsSignal, "REG_NUM")
    tot_steps = Cpt(EpicsSignal, "TOT_STEPS", kind="config")
    add_fms = Cpt(EpicsSignal, "ADD_FMS")
    act_scans = Cpt(EpicsSignalRO, "ACT_SCANS")
    dith_steps = Cpt(EpicsSignal, "DITH_STEPS")
    start_ke = Cpt(EpicsSignal, "START_KE", kind="config")
    step_size = Cpt(EpicsSignal, "STEP_SIZE", kind="config")
    end_ke = Cpt(EpicsSignal, "END_KE", kind="config")
    spin_offs = Cpt(EpicsSignal, "SPIN_OFFS")
    width = Cpt(EpicsSignal, "WIDTH", kind="config")
    center_ke = Cpt(EpicsSignal, "CENTER_KE", kind="config")
    first_energy = Cpt(EpicsSignal, "FIRST_ENERGY")
    deflX = Cpt(EpicsSignal, "DEFLX")
    deflY = Cpt(EpicsSignal, "DEFLY")
    dbl10 = Cpt(EpicsSignal, "DBL10")
    acq_mode = Cpt(EpicsSignal, "ACQ_MODE", kind="config")
    date_number = Cpt(EpicsSignal, "DATE_NUMBER")
    loc_det = Cpt(EpicsSignal, "LOC_DET")
    xtab = Cpt(EpicsSignal, "XTAB")
    spin = Cpt(EpicsSignal, "SPIN")
    reg_name = Cpt(EpicsSignal, "REG_NAME")
    name_string = Cpt(EpicsSignal, "NAME_STRING")
    generated_name = Cpt(EpicsSignal, "GENERATED_NAME")
    comment1 = Cpt(EpicsSignal, "COMMENT1")
    start_time = Cpt(EpicsSignal, "START_TIME")
    discr = Cpt(EpicsSignal, "DISCR")
    adc_mask = Cpt(EpicsSignal, "ADC_MASK")
    adc_offset = Cpt(EpicsSignal, "ADC_OFFSET")
    p_cnt_type = Cpt(EpicsSignal, "P_CNT_TYPE")
    pc_mask = Cpt(EpicsSignal, "PC_MASK")
    soft_bin_x = Cpt(EpicsSignal, "SOFT_BIN_X")
    soft_bin_y = Cpt(EpicsSignal, "SOFT_BIN_Y")
    escale_mult = Cpt(EpicsSignal, "ESCALE_MULT", kind="config")
    escale_max = Cpt(EpicsSignal, "ESCALE_MAX", kind="config")
    escale_min = Cpt(EpicsSignal, "ESCALE_MIN", kind="config")
    yscale_mult = Cpt(EpicsSignal, "YSCALE_MULT", kind="config")
    yscale_max = Cpt(EpicsSignal, "YSCALE_MAX", kind="config")
    yscale_min = Cpt(EpicsSignal, "YSCALE_MIN", kind="config")
    yscale_name = Cpt(EpicsSignal, "YSCALE_NAME")
    xscale_mult = Cpt(EpicsSignal, "XSCALE_MULT", kind="config")
    xscale_max = Cpt(EpicsSignal, "XSCALE_MAX", kind="config")
    xscale_min = Cpt(EpicsSignal, "XSCALE_MIN", kind="config")
    xscale_name = Cpt(EpicsSignal, "XSCALE_NAME")
    psu_mode = Cpt(EpicsSignal, "PSU_MODE", kind="config")
    over_r_arr = Cpt(EpicsSignal, "OVER_R_ARR")
    over_range = Cpt(EpicsSignal, "OVER_RANGE")

    _min_frames = 100
    """TCP server can't keep up with frame rate faster than this value in non-swept mode"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._status = None
        self._index = 0
        self._last_emitted_index = 0
        self._composer = None
        self._full_path = None

    def stage(self):
        if self.file_capture.get(as_string=True) == "On":
            raise RuntimeError(
                "File capture must be off to stage the detector, otherwise the file will be corrupted"
            )

        # Must be in standby to start
        if self.state.get(as_string=True) == "RUNNING":
            self.acquire.set(0).wait(3.0)

        # Must be live monitoring to start
        if self.live_monitoring.get(as_string=True) == "Off":
            self.live_monitoring.set("On").wait(3.0)

        # File capture must be on and then turned off at unstage
        self.stage_sigs.update(
            [
                (self.file_capture, 1),
            ]
        )

        # Frame rate can't be faster than 200ms in any mode except swept
        if (
            self.frames.get() < self._min_frames
            and self.acq_mode.get(as_string=True) != "Swept"
        ):
            self.stage_sigs.update(
                [(self.frames, self._min_frames)],
            )

        path = Path(self.file_path.get())
        # Subscribe to state and live max count exceeded to
        # handle the acquisition status
        self.state.subscribe(self._state_changed, run=False)
        self.live_max_count_exceeded.subscribe(
            self._live_max_count_exceeded_monitor, run=False
        )

        ret = super().stage()

        file_name = Path(self.file_name.get())
        self._full_path = str(path / file_name)
        self._index = 0
        self._last_emitted_index = 0

        return ret

    def _state_changed(self, value=None, old_value=None, **kwargs):
        if (
            self._status is not None
            and value == "STANDBY"
            and (old_value == "RUNNING" or old_value == "MOVING")
        ):
            self._status.set_finished()
            self._index += 1
            self._status = None

    def _live_max_count_exceeded_monitor(self, value=None, **kwargs):
        if self._status is not None and value:
            self._status.set_exception(
                RuntimeError(
                    f"Max count safety limit exceeded: {self.live_max_count.get()} > {self.live_max_count_threshold.get()}"
                )
            )
            self._status = None

    def trigger(self):
        if self._staged != Staged.yes:
            raise RuntimeError(
                "This detector is not ready to trigger."
                "Call the stage() method before triggering."
            )

        self._status = Status()
        self.acquire.put(1)
        return self._status

    def unstage(self):
        if self.state.get(as_string=True) == "RUNNING":
            self.acquire.set(0).wait(3.0)
        self.det_off.set(1).wait(3.0)
        super().unstage()
        self.state.unsubscribe(self._state_changed)
        self.live_max_count_exceeded.unsubscribe(self._live_max_count_exceeded_monitor)
        self._composer = None

    @property
    def index(self) -> int:
        return self._index


class SpectrumAnalyzerFileStore(SpectrumAnalyzer, WritesExternalAssets):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._datum_uids = []
        self._asset_docs_cache = []
        self._point_counter = None

    def _generate_resource(self):
        self._composer = compose_resource(
            spec="A1_HDF5",
            root=str(Path(self._full_path).parent),
            resource_path=self._full_path,
            resource_kwargs={"frame_per_point": 1},
            path_semantics="posix",
        )
        self._asset_docs_cache.append(("resource", self._composer.resource_doc))

    def generate_datum(self):
        timestamp = time.time()
        i = next(self._point_counter)
        datum = self._composer.compose_datum({"point_number": i})
        self._datum_uids.append({"value": datum["datum_id"], "timestamp": timestamp})
        self._asset_docs_cache.append(("datum", datum))

    def stage(self):
        self._datum_uids = []
        ret = super().stage()
        self._generate_resource()
        self._point_counter = count()
        return ret

    def trigger(self):
        s = super().trigger()
        self.generate_datum()
        return s

    def unstage(self):
        self._point_counter = None
        return super().unstage()

    def describe(self) -> dict[str, DataKey]:
        describe = super().describe()
        describe.update(
            {
                f"{self.name}_image": DataKey(
                    source=f"{self._full_path}",
                    shape=(1, self.num_slice.get(), self.num_steps.get()),
                    dtype="array",
                    dtype_numpy=np.dtype(np.uint32).str,
                    external="FILESTORE:",
                ),
            }
        )
        return describe

    def read(self):
        res = super().read()
        res[f"{self.name}_image"] = self._datum_uids[-1]
        return res

    def collect_asset_docs(self) -> SyncOrAsyncIterator[Asset]:
        items = list(self._asset_docs_cache)
        self._asset_docs_cache = []
        for item in items:
            yield item


class SpectrumAnalyzerStream(SpectrumAnalyzer, WritesStreamAssets):
    def describe(self) -> dict[str, DataKey]:
        describe = super().describe()
        describe.update(
            {
                f"{self.name}_image": DataKey(
                    source=f"{self._full_path}",
                    shape=(1, self.num_slice.get(), self.num_steps.get()),
                    dtype="array",
                    dtype_numpy=np.dtype(np.uint32).str,
                    external="STREAM:",
                ),
            }
        )
        return describe

    def collect_asset_docs(
        self, index: Optional[int] = None
    ) -> SyncOrAsyncIterator[StreamAsset]:
        if index is not None:
            msg = f"Indexing is not supported for this detector, got: {index}, current index: {self.index}"
            raise NotImplementedError(msg)

        index = self.index
        if index:
            if not self._composer:
                self._composer = compose_stream_resource(
                    data_key=f"{self.name}_image",
                    mimetype="application/x-hdf5",
                    uri=f"file://{self._full_path}",
                    parameters={"dataset": "entry1/analyzer/data"},
                )
                yield "stream_resource", self._composer.stream_resource_doc

            if index >= self._last_emitted_index:
                indices = {
                    "start": self._last_emitted_index,
                    "stop": index,
                }
                self._last_emitted_index = index
                yield "stream_datum", self._composer.compose_stream_datum(indices)
