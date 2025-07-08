# A1Soft-IOC
Caproto IOC to control A1Soft spectrum analyzer from MB Scientific AB https://www.mbscientific.se/ through the provided TCP Server.

> [!NOTE]
> Much of the behavior described below was deduced through trial & error.

# GenAcqTE TCP Server

The "General Acquisition Tango-EPICS" VI when specified to use the "EPICS" Instance Name, runs a TCP server which exports some data and controls over socket connections.

## TCP Sockets

There are three TCP socket connections.

| Usage | Description |
|-------|-------------|
| Data | For finsihed images accessed via the `GET_IMAGE` Action. |
| Commands | For commands sent from a client and responses to that client (in JSON format) |
| Live plots | For live data monitoring |

## Commands Types

Here are a list of the types of commands able to be processed by the TCP server. It is currently unknown what the "DATA" command type does.

| Type | Description |
|------|-------------|
| GET  | Get the current value of a [Parameter](#Parameters) |
| SET  | Set the value of a non-read-only [Parameter](#Parameters) |
| DATA | ? |
| ACTION | Perform one of the [Actions](#Actions) |

## Parameters




## Actions



