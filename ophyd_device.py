from ophyd import Device, Component as Cpt, EpicsSignal, EpicsSignalRO, Staged
from ophyd.status import Status


class SpectrumAnalyzer(Device):
    # Acquisition control
    acquire = Cpt(EpicsSignal, "ACQUIRE")
    acquisition_status = Cpt(EpicsSignalRO, "ACQ:STATUS")

    # Monitor control
    monitor_on = Cpt(EpicsSignal, "MON:ON")
    monitor_off = Cpt(EpicsSignal, "MON:OFF")
    monitor_status = Cpt(EpicsSignalRO, "MON:STATUS")

    # Detector control
    detector_off = Cpt(EpicsSignal, "DET:OFF")
    detector_status = Cpt(EpicsSignalRO, "DET:STATUS")

    # Image acquisition
    get_image = Cpt(EpicsSignal, "IMG:GET")
    get_stats = Cpt(EpicsSignal, "ACQ:STATS")

    # Status and info
    connection_status = Cpt(EpicsSignalRO, "SYS:CONNECTED")
    last_error = Cpt(EpicsSignalRO, "SYS:ERROR")
    last_sync = Cpt(EpicsSignalRO, "SYS:LAST_SYNC")
    sync = Cpt(EpicsSignal, "SYS:SYNC")

    # Detector parameters
    state = Cpt(EpicsSignalRO, "STATE", string=True)
    endX = Cpt(EpicsSignal, "ENDX")
    startY = Cpt(EpicsSignal, "STARTY")
    num_slice = Cpt(EpicsSignal, "NUM_SLICE")
    endY = Cpt(EpicsSignal, "ENDY")
    startX = Cpt(EpicsSignal, "STARTX")
    frames = Cpt(EpicsSignal, "FRAMES")
    num_steps = Cpt(EpicsSignal, "NUM_STEPS")
    pass_energy = Cpt(EpicsSignal, "PASS_ENERGY")
    lens_mode = Cpt(EpicsSignal, "LENS_MODE")
    num_scans = Cpt(EpicsSignal, "NUM_SCANS")
    reg_num = Cpt(EpicsSignal, "REG_NUM")
    tot_steps = Cpt(EpicsSignal, "TOT_STEPS")
    add_fms = Cpt(EpicsSignal, "ADD_FMS")
    act_scans = Cpt(EpicsSignalRO, "ACT_SCANS")
    dith_steps = Cpt(EpicsSignal, "DITH_STEPS")
    start_ke = Cpt(EpicsSignal, "START_KE")
    step_size = Cpt(EpicsSignal, "STEP_SIZE")
    end_ke = Cpt(EpicsSignal, "END_KE")
    spin_offs = Cpt(EpicsSignal, "SPIN_OFFS")
    width = Cpt(EpicsSignal, "WIDTH")
    center_ke = Cpt(EpicsSignal, "CENTER_KE")
    first_energy = Cpt(EpicsSignal, "FIRST_ENERGY")
    deflX = Cpt(EpicsSignal, "DEFLX")
    deflY = Cpt(EpicsSignal, "DEFLY")
    dbl10 = Cpt(EpicsSignal, "DBL10")
    acq_mode = Cpt(EpicsSignal, "ACQ_MODE")
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
    escale_mult = Cpt(EpicsSignal, "ESCALE_MULT")
    escale_max = Cpt(EpicsSignal, "ESCALE_MAX")
    escale_min = Cpt(EpicsSignal, "ESCALE_MIN")
    yscale_mult = Cpt(EpicsSignal, "YSCALE_MULT")
    yscale_max = Cpt(EpicsSignal, "YSCALE_MAX")
    yscale_min = Cpt(EpicsSignal, "YSCALE_MIN")
    yscale_name = Cpt(EpicsSignal, "YSCALE_NAME")
    xscale_mult = Cpt(EpicsSignal, "XSCALE_MULT")
    xscale_max = Cpt(EpicsSignal, "XSCALE_MAX")
    xscale_min = Cpt(EpicsSignal, "XSCALE_MIN")
    xscale_name = Cpt(EpicsSignal, "XSCALE_NAME")
    psu_mode = Cpt(EpicsSignal, "PSU_MODE")
    over_r_arr = Cpt(EpicsSignal, "OVER_R_ARR")
    over_range = Cpt(EpicsSignal, "OVER_RANGE")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stage_sigs.update([
            (self.acquire, 0),
        ])
        self._status = None

    def stage(self):
        self.state.subscribe(self._stage_changed, run=False)
        return super().stage()

    def _stage_changed(self, value=None, old_value=None, **kwargs): 
        if self._status is None:
            return
        print(f"Stage changed: {value} -> {old_value}")
        if value == "STANDBY" and old_value == "RUNNING":
            self._status.set_finished()
            self._status = None

    def trigger(self):
        if self._staged != Staged.yes:
            raise RuntimeError(
                "This detector is not ready to trigger."
                "Call the stage() method before triggering."
            )
        self.acquire.set(1)
        self._status = Status()
        return self._status

    def unstage(self):
        super().unstage()
        self.state.unsubscribe(self._stage_changed)
