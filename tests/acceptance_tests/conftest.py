import pytest
import time
import asyncio
import os
from pathlib import Path

from a1soft.device import SpectrumAnalyzer


def pytest_addoption(parser):
    """Add command line options for pytest."""
    parser.addoption(
        "--prefix",
        default="A1Soft:",
        help="EPICS PV prefix for the IOC (default: A1Soft:)",
    )


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="function")
def analyzer_device(request):
    """Fixture providing a SpectrumAnalyzer device instance.

    This assumes the IOC is running on the local machine.
    Use --prefix command line option to specify a different EPICS PV prefix.
    """
    prefix = request.config.getoption("--prefix")
    device = SpectrumAnalyzer(prefix=prefix, name="analyzer")

    # Wait for initial connection
    device.wait_for_connection(timeout=10.0)

    # Verify we're actually connected to the IOC
    if not device.connection_status.get():
        pytest.skip("IOC is not connected to detector hardware")

    yield device

    # Cleanup - ensure acquisition is stopped and file capture is off
    try:
        if device.acquire.get():
            device.acquire.set(0).wait(5.0)
        if device.file_capture.get(as_string=True) == "On":
            device.file_capture.set("Off").wait(5.0)
    except Exception:
        pass  # Best effort cleanup


@pytest.fixture
def test_output_dir():
    """Fixture providing an OS-specific temporary directory for test outputs."""
    # Hardcoded Windows temporary path
    # Currently the IOC always runs on Windows
    temp_base = Path(os.environ.get("TEMP", r"C:\Windows\Temp"))
    test_dir = temp_base / "A1Soft_IOC_Tests"
    test_dir.mkdir(exist_ok=True, parents=True)
    return test_dir


@pytest.fixture
def detector_in_standby(analyzer_device):
    """Fixture ensuring the detector starts in STANDBY state."""
    device = analyzer_device

    # Stop any running acquisition
    if device.acquire.get():
        device.acquire.set(0).wait(5.0)
        # Wait for state to transition to STANDBY
        start_time = time.time()
        while device.state.get() != "STANDBY" and (time.time() - start_time) < 10:
            time.sleep(0.1)

    # Ensure file capture is off
    if device.file_capture.get(as_string=True) == "On":
        device.file_capture.set("Off").wait(5.0)

    # Ensure live monitoring is off
    if device.live_monitoring.get(as_string=True) == "On":
        device.live_monitoring.set("Off").wait(5.0)

    # Ensure safety limit is reset
    if device.live_max_count_exceeded.get():
        device.live_max_count_exceeded.set(False).wait(5.0)

    # Default mode is swept
    device.acq_mode.set("Swept").wait(5.0)
    device.num_steps.set(10).wait(5.0)

    return device


def wait_for_state(device, expected_state, timeout=10.0):
    """Utility function to wait for device state change."""
    start_time = time.time()
    while device.state.get() != expected_state:
        if time.time() - start_time > timeout:
            raise TimeoutError(
                f"Timeout waiting for state {expected_state}, current: {device.state.get()}"
            )
        time.sleep(1.0)


def wait_for_condition(condition_func, timeout=10.0, poll_interval=0.1):
    """Utility function to wait for a condition to be true."""
    start_time = time.time()
    while not condition_func():
        if time.time() - start_time > timeout:
            raise TimeoutError("Timeout waiting for condition")
        time.sleep(poll_interval)
