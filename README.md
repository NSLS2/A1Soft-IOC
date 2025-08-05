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
| Live plots | For live data monitoring (DOES NOT WORK) |

## Commands Types

Here are a list of the types of commands able to be processed by the TCP server. It is currently unknown what the "DATA" command type does.

| Type | Description |
|------|-------------|
| GET  | Get the current value of a [Parameter](#Parameters) |
| SET  | Set the value of a non-read-only [Parameter](#Parameters) |
| DATA | <unknown> |
| ACTION | Perform one of the [Actions](#Actions) |

## Parameters


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
| acqMode | enum | Options are Fixed, Swept, Dither | N |
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
| DET_OFF | <unknown> |
| MONITOR_ON | <unknown> |
| MONITOR_OFF | <unknown> |
| GET_IMAGE | Requests an image on the data socket |
| GET_ACQ_STATS | <unknown> |


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


This is not a problem in Swept mode since the acquisition is much slower. Although, in this mode, the first channel data will fill-in from the leftmost column to the rightmost column and timing the command for the full image is impossible (to my knowledge). Therefore, we compute the differences in the second channel images to recover the first channel image, in practice.

# Process Variables (PVs)

Here are the tables of EPICS PVs that are hosted by the Caproto server. They are roughly grouped into categories.

`$(P)` corresponds to the specified prefix set via the command-line argument.

## Acquisition Control

| PV Name | Description |
|---------|-------------|
| $(P)ACQUIRE | Starts acquiring when set to 1. Stops when set to 0. Does not do anything if already running. |
| $(P)ACQ:STATUS | Status of the acquisition started via EPICS |

## Monitor Control

| PV Name | Description |
|---------|-------------|
| $(P)MON:ON | Perform [Action](#Actions) MONITOR_ON |
| $(P)MON:OFF | Perform [Action](#Actions) MONITOR_OFF |
| $(P)MON:STATUS | Status of the monitor started via EPICS |

## Detector Control

| PV Name | Description |
|---------|-------------|
| $(P)DET:OFF | Perform [Action](#Actions) DET_OFF |
| $(P)MON:STATUS | Status of the detector set via EPICS |

## Image Acquisition

| PV Name | Description |
|---------|-------------|
| $(P)IMG:GET | Perform [Action](#Actions) GET_IMAGE |
| $(P)ACQ:STATS | Perform [Action](#Actions) GET_ACQ_STATS |

## System Control

| PV Name | Description |
|---------|-------------|
| $(P)SYS:CONNECTED | Connection status of the client to the TCP Server, true if all ports are connected. |
| $(P)SYS:ERROR | Displays the last error that occurred in the IOC |
| $(P)SYS:LAST_SYNC | Timestamp of the last time the parameters from A1Soft were synced with the [PVs](#process-variables-pvs) |
| $(P)SYS:SYNC | Enable/Disable syncing [Detector Parameters](#detector-parameters). Set polling frequency via `.SCAN` record |

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
