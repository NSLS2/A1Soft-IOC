import pytest
import time
import asyncio

from a1soft.device import SpectrumAnalyzer


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def analyzer_device():
    """Fixture providing a SpectrumAnalyzer device instance.

    This assumes the IOC is running with prefix 'A1Soft:' on the local machine.
    Modify the prefix if your IOC uses a different prefix.
    """
    device = SpectrumAnalyzer(prefix="A1Soft:", name="analyzer")

    # Wait for initial connection
    device.wait_for_connection(timeout=10.0)

    # Verify we're actually connected to the IOC
    if not device.connection_status.get():
        pytest.skip("IOC is not connected to detector hardware")

    yield device

    # Cleanup - ensure acquisition is stopped and file capture is off
    try:
        if device.acquire.get():
            device.acquire.set(0).wait(1.0)
        if device.file_capture.get(as_string=True) == "On":
            device.file_capture.set("Off").wait(1.0)
    except Exception:
        pass  # Best effort cleanup


@pytest.fixture
def test_output_dir(tmp_path):
    """Fixture providing a temporary directory for test outputs."""
    test_dir = tmp_path / "test_data"
    test_dir.mkdir(exist_ok=True)
    return test_dir


@pytest.fixture
def detector_in_standby(analyzer_device):
    """Fixture ensuring the detector starts in STANDBY state."""
    device = analyzer_device

    # Stop any running acquisition
    if device.acquire.get():
        device.acquire.set(0).wait(1.0)
        # Wait for state to transition to STANDBY
        start_time = time.time()
        while device.state.get() != "STANDBY" and (time.time() - start_time) < 10:
            time.sleep(0.1)

    # Ensure file capture is off
    if device.file_capture.get(as_string=True) == "On":
        device.file_capture.set("Off").wait(1.0)

    return device


def wait_for_state(device, expected_state, timeout=10.0):
    """Utility function to wait for device state change."""
    start_time = time.time()
    while device.state.get() != expected_state:
        if time.time() - start_time > timeout:
            raise TimeoutError(
                f"Timeout waiting for state {expected_state}, current: {device.state.get()}"
            )
        time.sleep(0.1)


def wait_for_condition(condition_func, timeout=10.0, poll_interval=0.1):
    """Utility function to wait for a condition to be true."""
    start_time = time.time()
    while not condition_func():
        if time.time() - start_time > timeout:
            raise TimeoutError("Timeout waiting for condition")
        time.sleep(poll_interval)
