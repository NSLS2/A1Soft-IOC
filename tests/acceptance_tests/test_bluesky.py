"""
Acceptance tests for device interface functionality.
Tests the SpectrumAnalyzer device both with bluesky integration.
"""

import pytest

from bluesky import RunEngine
from bluesky.plans import count, scan
from bluesky.utils import FailedStatus
from ophyd.status import StatusBase, WaitTimeoutError
from ophyd import Staged


class TestDeviceWithBluesky:
    """Test device interface using bluesky RunEngine and plans."""

    @pytest.fixture
    def run_engine(self):
        """Fixture providing a bluesky RunEngine."""
        RE = RunEngine({})
        yield RE

        # Cleanup
        try:
            RE.halt()
        except Exception:
            pass

    def test_device_in_simple_count_plan(
        self, detector_in_standby, run_engine, test_output_dir
    ):
        """Test device works with bluesky count plan."""
        device = detector_in_standby
        RE = run_engine

        # Configure device for quick acquisition
        device.file_path.set(str(test_output_dir)).wait(5.0)
        device.file_name.set("test_count.nxs").wait(5.0)
        device.num_scans.set(1).wait(5.0)

        # Execute count plan
        documents = []

        def collect_docs(name, doc):
            documents.append((name, doc))

        RE.subscribe(collect_docs)

        # Run the plan
        RE(count([device], num=1))

        # Verify documents were generated
        doc_types = [name for name, _ in documents]
        assert "start" in doc_types, "Should generate start document"
        assert "descriptor" in doc_types, "Should generate descriptor document"
        assert "event" in doc_types, "Should generate event document"
        assert "stop" in doc_types, "Should generate stop document"

        # Verify device returned to unstaged state
        assert device._staged == Staged.no, "Device should be unstaged after plan"

    def test_device_staging_with_bluesky(self, detector_in_standby, test_output_dir):
        """Test device staging behavior within bluesky context."""
        device = detector_in_standby

        # Configure device
        device.file_path.set(str(test_output_dir)).wait(5.0)
        device.file_name.set("test_staging.nxs").wait(5.0)

        # Initially unstaged
        assert device._staged == Staged.no, "Should start unstaged"

        # Stage device
        staged_signals = device.stage()
        assert device._staged == Staged.yes, "Should be staged"
        assert device.file_capture.get(as_string=True) == "On", (
            "File capture should be on when staged"
        )

        # Verify staging signals are set correctly
        assert len(staged_signals) > 0, "Should return list of staged signals"

        # Test triggering when staged
        status = device.trigger()
        assert isinstance(status, StatusBase), "Should return Status when staged"

        # Unstage
        device.unstage()
        assert device._staged == Staged.no, "Should be unstaged"
        assert device.file_capture.get(as_string=True) == "Off", (
            "File capture should be off when unstaged"
        )

    def test_device_with_multiple_triggers(
        self, detector_in_standby, run_engine, test_output_dir
    ):
        """Test device can handle multiple triggers in sequence."""
        device = detector_in_standby
        RE = run_engine

        # Configure device
        device.file_path.set(str(test_output_dir)).wait(5.0)
        device.file_name.set("test_multi_trigger.nxs").wait(5.0)
        device.num_scans.set(1).wait(5.0)

        documents = []
        RE.subscribe(lambda name, doc: documents.append((name, doc)))

        RE(count([device], num=3))

        # Should have multiple events
        events = [doc for name, doc in documents if name == "event"]
        assert len(events) >= 3, "Should have at least 3 event documents"

    def test_device_with_safety_limits(
        self, detector_in_standby, run_engine, test_output_dir
    ):
        """Test device stops the scan if the safety limits are exceeded."""
        device = detector_in_standby
        RE = run_engine

        # Configure device
        device.file_path.set(str(test_output_dir)).wait(5.0)
        device.file_name.set("test_safety_limits.nxs").wait(5.0)
        device.num_scans.set(1).wait(5.0)
        device.live_monitoring.set("On").wait(5.0)
        # Set threshold to -1 to ensure that the safety limits are exceeded
        device.live_max_count_threshold.set(-1).wait(5.0)

        # Should not run the second iteration
        with pytest.raises((FailedStatus, WaitTimeoutError)):
            RE(count([device], num=2))

    def test_device_with_scan(self, detector_in_standby, run_engine, test_output_dir):
        """Test device can handle multiple triggers in sequence."""
        device = detector_in_standby
        RE = run_engine

        # Configure device
        device.file_path.set(str(test_output_dir)).wait(5.0)
        device.file_name.set("test_multi_trigger.nxs").wait(5.0)
        device.num_scans.set(1).wait(5.0)

        documents = []
        RE.subscribe(lambda name, doc: documents.append((name, doc)))

        RE(scan([device], device.deflX, 0.1, 8.7, 5))

        # Should have multiple events
        events = [doc for name, doc in documents if name == "event"]
        assert len(events) >= 5, "Should have at least 5 event documents"

    def test_device_with_fixed_scan(
        self, detector_in_standby, run_engine, test_output_dir
    ):
        """Test device can handle multiple triggers in sequence."""
        device = detector_in_standby
        RE = run_engine

        # Configure device
        device.acq_mode.set("Fixed").wait(5.0)
        device.file_path.set(str(test_output_dir)).wait(5.0)
        device.file_name.set("test_multi_trigger.nxs").wait(5.0)
        device.num_scans.set(1).wait(5.0)

        documents = []
        RE.subscribe(lambda name, doc: documents.append((name, doc)))

        RE(scan([device], device.deflX, 0.1, 8.7, 5))

        # Should have multiple events
        events = [doc for name, doc in documents if name == "event"]
        assert len(events) >= 5, "Should have at least 5 event documents"

    def test_device_with_dither_scan(
        self, detector_in_standby, run_engine, test_output_dir
    ):
        """Test device can handle dither scan."""
        device = detector_in_standby
        RE = run_engine

        # Configure device
        device.acq_mode.set("Dither").wait(5.0)
        device.file_path.set(str(test_output_dir)).wait(5.0)
        device.file_name.set("test_dither_scan.nxs").wait(5.0)
        device.num_scans.set(1).wait(5.0)
        device.dith_steps.set(10).wait(5.0)

        documents = []
        RE.subscribe(lambda name, doc: documents.append((name, doc)))

        RE(scan([device], device.deflX, 0.1, 8.7, 5))

        # Should have multiple events
        events = [doc for name, doc in documents if name == "event"]
        assert len(events) >= 5, "Should have at least 5 event documents"

    def test_device_with_fixed_trigd_scan(
        self, detector_in_standby, run_engine, test_output_dir
    ):
        """Test device can handle FixedTrigd scan."""
        device = detector_in_standby
        RE = run_engine

        # Configure device
        device.acq_mode.set("FixedTrigd").wait(5.0)
        device.file_path.set(str(test_output_dir)).wait(5.0)
        device.file_name.set("test_fixed_trigd_scan.nxs").wait(5.0)
        device.num_scans.set(1).wait(5.0)

        documents = []
        RE.subscribe(lambda name, doc: documents.append((name, doc)))

        RE(scan([device], device.deflX, 0.1, 8.7, 5))

        # Should have multiple events
        events = [doc for name, doc in documents if name == "event"]
        assert len(events) >= 5, "Should have at least 5 event documents"
