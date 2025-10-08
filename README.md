# A1Soft-IOC
Caproto IOC to control A1Soft spectrum analyzer from MB Scientific AB https://www.mbscientific.se/ through the provided TCP Server.

> [!NOTE]
> Much of the behavior described below was deduced through trial & error. There are a significant amount of unknowns that we still need to discover through this process.

# GenAcqTE TCP Server

The "General Acquisition Tango-EPICS" VI when specified to use the "EPICS" Instance Name, runs a TCP server which exports some data and controls over socket connections.

## TCP Sockets

There are three TCP socket connections.

| Usage | Description |
|-------|-------------|
| Data | For images accessed via the `GET_IMAGE` [Action](#Actions). |
| Commands | For commands sent from a client and responses to that client (in JSON format) |
| Live plots | For live data monitoring |

## Commands Types

Here are a list of the types of commands able to be processed by the TCP server. It is currently unknown what the "DATA" command type does.

| Type | Description |
|------|-------------|
| GET  | Get the current value of a [Parameter](#Parameters) |
| SET  | Set the value of a non-read-only [Parameter](#Parameters) |
| ACTION | Perform one of the [Actions](#Actions) |

## Parameters

> [!NOTE]
> For the `acqMode` parameter, we manually changed the LabView TypeDef to include FixedTrigd, which was not included by default.

| Name | Type | Description | Read-Only? | 
|------|------|-------------|------------|
| state | enum | The state of the acquisition. Options are: RUNNING, MOVING, STANDBY | Y |
| endX | int | <unknown> | Y |
| startY | int | <unknown> | Y |
| numSlice | int | <unknown> | Y |
| endY | int | <unknown> | Y |
| startX | int | <unknown> | Y |
| frames | int | The number of frames to capture | N |
| numSteps | int | <unknown> | N |
| passEnergy | enum | Options are PE001, PE002, PE005, PE010, PE020, PE050 | N |
| lensMode | enum | Options are L4Ang0d6, L4Ang0d8, L4Ang1d6, L4Ang3d9, L4MSpat5, L4Spat5 | N |
| numScans | int | The number of scans to take | N |
| regNum | int | <unknown> | Y |
| totSteps | int | <unknown> | Y |
| addFms | int | <unknown> | Y |
| actScans | int | The number of actual scans taken | Y |
| dithSteps | int | <unknown> | N |
| startKe | float | The starting kinetic energy of the scan | N |
| stepSize | float | The energy step size of the scan | N |
| endKe | float | The ending kinetic energy of the scan | N |
| spinOffs | float | <unknown> | N |
| width | float | <unknown> | N |
| centreKe | float | The center kinetic energy | N |
| firstEnergy | float | <unknown> | N |
| deflX | float | <unknown> | N |
| deflY | float | <unknown> | N |
| dbl10 | float | <unknown> | Y |
| acqMode | enum | Options are Fixed, FixedTrigd, Swept, Dither | N |
| dateNumber | bool | <unknown> | Y |
| locDet | bool | <unknown> | Y |
| xtab | bool | <unknown> | N |
| spin | bool | <unknown> | ? |
| regName | str | <unknown> | Y |
| nameString | str | <unknown> | Y |
| generatedName | str | <unknown> | Y |
| comment1 | str | <unknown> | N |
| startTime | str | <unknown> | N |
| discr | int | <unknown> | Y |
| adcMask | int | <unknown> | Y |
| adcOffset | int | <unknown> | Y |
| pCntType | int | <unknown> | Y |
| pcMask | int | <unknown> | Y |
| softBinX | int | <unknown> | N |
| softBinY | int | <unknown> | N |
| EScaleMult | float | <unknown> | N |
| EScaleMax | float | <unknown> | Y |
| EScaleMin | float | <unknown> | Y |
| YScaleMult | float | <unknown> | Y |
| YScaleMax | float | <unknown> | Y |
| YScaleMin | float | <unknown> | Y |
| YScaleName | str | <unknown> | Y |
| XScaleMult | float | <unknown> | Y |
| XScaleMax | float | <unknown> | Y |
| XScaleMin | float | <unknown> | Y |
| XScaleName | str | <unknown> | Y |
| PsuMode | str | <unknown> | Y |
| OverRArr | str | <unknown> | Y |
| OverRange | int | <unknown> | Y |

## Actions

| Name | Description |
|------|-------------|
| START | Starts the acquisition |
| STOP | Stops the acquisition |
| DET_OFF | Turns the detector off |
| GET_IMAGE | Requests an image on the data socket |
| GET_ACQ_STATS | Gets the acquisition state and index |


## Data Socket

You will only receive data on this socket after sending a `GET_IMAGE` [Action](#Actions) over the JSON socket. The schema is

| Name | Byte offset | Description |
|------|-------------|-------------|
| Header | 0 - 40 | Contains the header data |
| Marker | 0 - 4 | <unknown> |
| Index | 4 - 8 | The current frame number requested since acquisition start |
| State | 8 - 12 | <unknown> |
| Reserved | 12 - 16 | <unknown> |
| Width | 16 - 20 | The width of the first channel image |
| Height | 20 - 24 | The height of the first channel image |
| Length | 24 - 28 | The length of the first channel image in bytes |
| Current width | 28 - 32 | The width of the second channel image |
| Current height | 32 - 36 | The height of the second channel image |
| Current length | 36 - 40 | The length of the second channel image in bytes |
| First channel image | 40 - (40 + Length) | The uint32 byte array for the first channel image, representing the current image being displayed |
| Second channel image | (40 + length) - (40 + length + current length) | The uint32 byte array for the second channel image, representing the sum of the first channel images in the acquisition so far |

In Fixed acquisition mode, you can only capture data at a maximum of ~12.5 frames per second. The A1Soft latency between receiving a GET_IMAGE action and putting all of this data into the data socket is greater than the rate at which images are produced. This means that the IOC will never be able to keep up and will drop frames!

## Live Socket

There is also a live data socket for streaming data from the detector. This is only available while acquiring.

The "Live Data" must be configured to be on and the monitor must be enabled. There is no way to control this from
the TCP server, so you must manually configure this in LabView. You can control the rate at which the TCP server adds
live data by configuring it in the `parameters.ini` file that should have come with the server.

> [!NOTE]
> There were some bugs in the `dataTransferLoop.vi` that was causing data to be added to the live socket every 5ms! This overloaded the socket and caused
> many issues. You have to fix this by either increasing the 5ms to 1000ms or moving the error wire after the socket connection heartbeat. We did both to make it work.

The schema is

| Name | Byte offset | Description |
|------|-------------|-------------|
| Marker | 0 - 4 | Message start marker (delimits frames) |
| Index | 4 - 8 | The current frame number requested since acquisition start |
| Width | 8 - 12 | The width of the image |
| Height | 12 - 16 | The height of the image |
| Length | 16 - 20 | The length of the image in bytes |
| Image | 20 - (20 + Length) | The uint32 byte array for the first channel image, representing the current image being displayed |

# Process Variables (PVs)

Here are the tables of EPICS PVs that are hosted by the Caproto server. They are roughly grouped into categories.

`$(P)` corresponds to the specified prefix set via the command-line argument.

## Acquisition Control

| PV Name | Description |
|---------|-------------|
| $(P)ACQUIRE | Starts acquiring when set to 1. Stops when set to 0. Does not do anything if already running. |
| $(P)ACQ:STATUS | Status of the acquisition started via EPICS |
| $(P)LIVE:MONITORING | Enable/disable live data monitoring from live_port |
| $(P)LIVE:MAX_COUNT | Current maximum pixel count from live data stream |
| $(P)LIVE:LAST_UPDATE | Timestamp of last live data update |
| $(P)LIVE:MAX_COUNT_THRESH | Threshold for the maximum value of a single pixel of the detector |
| $(P)LIVE:MAX_COUNT_EXCEEDED | Indicates if the maximum count has exceeded the threshold |
| $(P)LIVE:MAX_COUNT_AVG_N | Window size for averaging the max count (i.e. average over the last n max counts) |

## Detector Control

| PV Name | Description |
|---------|-------------|
| $(P)DET:OFF | Perform [Action](#Actions) DET_OFF |

## System Control

| PV Name | Description |
|---------|-------------|
| $(P)SYS:CONNECTED | Connection status of the client to the TCP Server, true if all ports are connected. |
| $(P)SYS:LAST_SYNC | Timestamp of the last time the parameters from A1Soft were synced with the [PVs](#process-variables-pvs) |
| $(P)SYS:SYNC | Enable/Disable syncing [Detector Parameters](#detector-parameters). Set polling frequency via `.SCAN` record |

## File Writing

The IOC will open a [NeXus](https://www.nexusformat.org/) file for writing when you turn on `$(P)FILE:CAPTURE`. The file only remains open when there is data available to write.
Within a single acquisition, it will continually *update* the frame every time `actScans` increases by 1. Once `actScans` is equal to `numScans`, it will commit this frame to the file. Subsequent acquisitions will append a new frame to the same dataset.
This way if you stop a single acquisition in the middle, there will be partial frame data available.


The number of full acquisitions that complete will be equal to the number of frames committed to the file. So if I call `caput $(P)ACQUIRE 1` 5 separate times (waiting for each acquisition to complete in between calls), there will be 5 frames in the file.

| PV Name | Description |
|---------|-------------|
| $(P)FILE:CAPTURE | Enable/disable file capture |
| $(P)FILE:NAME | File name for captured data |
| $(P)FILE:PATH | File path for captured data |
| $(P)FILE:NUM_CAPTURED | Number of images captured while file capture is on |
| $(P)FILE:NUM_PROCESSED | Number of scans processed during a single acquisition |

## Detector Parameters

When synchronization is active, these parameters will be updated, by default every second, to match what the parameter values are in LabView. This way, when someone changes one of these parameters in LabView, it gets reflected in the PV values.

| PV Name | Description |
|---------|-------------|
| $(P)STATE | Sets and gets `state` |
| $(P)ENDX | Sets and gets `endX` |
| $(P)STARTY | Sets and gets `startY` |
| $(P)NUM_SLICE | Sets and gets `numSlice` |
| $(P)ENDY | Sets and gets `endY` |
| $(P)STARTX | Sets and gets `startX` |
| $(P)FRAMES | Sets and gets `frames` |
| $(P)NUM_STEPS | Sets and gets `numSteps` |
| $(P)PASS_ENERGY | Sets and gets `passEnergy` |
| $(P)LENS_MODE | Sets and gets `lensMode` |
| $(P)NUM_SCANS | Sets and gets `numScans` |
| $(P)REG_NUM | Sets and gets `regNum` |
| $(P)TOT_STEPS | Sets and gets `totSteps` |
| $(P)ADD_FMS | Sets and gets `addFms` |
| $(P)ACT_SCANS | Sets and gets `actScans` |
| $(P)DITH_STEPS | Sets and gets `dithSteps` |
| $(P)START_KE | Sets and gets `startKe` |
| $(P)STEP_SIZE | Sets and gets `stepSize` |
| $(P)END_KE | Sets and gets `endKe` |
| $(P)SPIN_OFFS | Sets and gets `spinOffs` |
| $(P)WIDTH | Sets and gets `width` |
| $(P)CENTER_KE | Sets and gets `centreKe` |
| $(P)FIRST_ENERGY | Sets and gets `firstEnergy` |
| $(P)DEFLX | Sets and gets `deflX` |
| $(P)DEFLY | Sets and gets `deflY` |
| $(P)DBL10 | Sets and gets `dbl10` |
| $(P)ACQ_MODE | Sets and gets `acqMode` |
| $(P)DATE_NUMBER | Sets and gets `dateNumber` |
| $(P)LOC_DET | Sets and gets `locDet` |
| $(P)XTAB | Sets and gets `xtab` |
| $(P)SPIN | Sets and gets `spin` |
| $(P)REG_NAME | Sets and gets `regName` |
| $(P)NAME_STRING | Sets and gets `nameString` |
| $(P)GENERATED_NAME | Sets and gets `generatedName` |
| $(P)COMMENT1 | Sets and gets `comment1` |
| $(P)START_TIME | Sets and gets `startTime` |
| $(P)DISCR | Sets and gets `discr` |
| $(P)ADC_MASK | Sets and gets `adcMask` |
| $(P)ADC_OFFSET | Sets and gets `adcOffset` |
| $(P)P_CNT_TYPE | Sets and gets `pCntType` |
| $(P)PC_MASK | Sets and gets `pcMask` |
| $(P)SOFT_BIN_X | Sets and gets `softBinX` |
| $(P)SOFT_BIN_Y | Sets and gets `softBinY` |
| $(P)ESCALE_MULT | Sets and gets `EScaleMult` |
| $(P)ESCALE_MAX | Sets and gets `EScaleMax` |
| $(P)ESCALE_MIN | Sets and gets `EScaleMin` |
| $(P)YSCALE_MULT | Sets and gets `YScaleMult` |
| $(P)YSCALE_MAX | Sets and gets `YScaleMax` |
| $(P)YSCALE_MIN | Sets and gets `YScaleMin` |
| $(P)YSCALE_NAME | Sets and gets `YScaleName` |
| $(P)XSCALE_MULT | Sets and gets `XScaleMult` |
| $(P)XSCALE_MAX | Sets and gets `XScaleMax` |
| $(P)XSCALE_MIN | Sets and gets `XScaleMin` |
| $(P)XSCALE_NAME | Sets and gets `XScaleName` |
| $(P)PSU_MODE | Sets and gets `PsuMode` |
| $(P)OVER_R_ARR | Sets and gets `OverRArr` |
| $(P)OVER_RANGE | Sets and gets `OverRange` |

## Installation

To install the package:

```bash
pip install .
```

Or for editable install:

```bash
pip install -e .
```

## Usage

To launch the Caproto server:

```bash
python -m a1soft.ioc --list-pvs --interfaces=127.0.0.1
```

To use the Ophyd device in scripts:

```python
from a1soft.ophyd import SpectrumAnalyzer

# Example usage
device = SpectrumAnalyzer('PREFIX:', name='sa')
```
