"""
Acceptance tests for device interface functionality.
Tests the SpectrumAnalyzer device both with bluesky integration.
"""

import pytest

from bluesky import RunEngine
from bluesky.plans import count
import bluesky.plan_stubs as bps
from ophyd.status import Status
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
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("test_count.nxs").wait(1.0)
        device.num_scans.set(1).wait(1.0)

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

    def test_device_readable_protocol(self, detector_in_standby):
        """Test that device properly implements Readable protocol."""
        device = detector_in_standby

        # Test describe method
        description = device.describe()
        assert isinstance(description, dict), "describe() should return dict"

        # Check that all signals are described
        expected_signals = [
            "num_scans",
            "num_steps",
            "start_ke",
            "end_ke",
            "pass_energy",
            "lens_mode",
            "acq_mode",
        ]

        for signal_name in expected_signals:
            assert signal_name in description, (
                f"Missing signal {signal_name} in description"
            )
            assert "source" in description[signal_name], (
                f"Missing source for {signal_name}"
            )
            assert "dtype" in description[signal_name], (
                f"Missing dtype for {signal_name}"
            )

        # Test read method
        reading = device.read()
        assert isinstance(reading, dict), "read() should return dict"

        for signal_name in expected_signals:
            if signal_name in reading:  # Some signals may not be in reading
                assert "value" in reading[signal_name], (
                    f"Missing value for {signal_name}"
                )
                assert "timestamp" in reading[signal_name], (
                    f"Missing timestamp for {signal_name}"
                )

    def test_device_stream_assets_protocol(self, detector_in_standby, test_output_dir):
        """Test WritesStreamAssets protocol implementation."""
        device = detector_in_standby

        # Configure device for file writing
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("test_stream.nxs").wait(1.0)
        device.num_scans.set(1).wait(1.0)

        # Stage device (required for stream assets)
        device.stage()

        try:
            # Check describe includes stream asset
            description = device.describe()
            stream_key = f"{device.name}_image"
            assert stream_key in description, f"Missing stream asset key {stream_key}"

            stream_desc = description[stream_key]
            assert stream_desc.get("external") == "STREAM:", (
                "Should be marked as external stream"
            )
            assert "shape" in stream_desc, "Stream asset should have shape"
            assert "dtype" in stream_desc, "Stream asset should have dtype"

            # Trigger acquisition to generate assets
            status = device.trigger()
            assert isinstance(status, Status), "trigger() should return Status"

            # Wait for completion
            status.wait(timeout=30.0)

            # Test collect_asset_docs
            asset_docs = list(device.collect_asset_docs())

            # Should have stream resource and datum documents
            if asset_docs:  # Only if acquisition completed
                doc_names = [doc[0] for doc in asset_docs]
                assert "stream_resource" in doc_names, (
                    "Should generate stream_resource doc"
                )
                if len(asset_docs) > 1:
                    assert "stream_datum" in doc_names, (
                        "Should generate stream_datum doc"
                    )

        finally:
            device.unstage()

    def test_device_staging_with_bluesky(self, detector_in_standby, test_output_dir):
        """Test device staging behavior within bluesky context."""
        device = detector_in_standby

        # Configure device
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("test_staging.nxs").wait(1.0)

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
        assert isinstance(status, Status), "Should return Status when staged"

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
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("test_multi_trigger.nxs").wait(1.0)
        device.num_scans.set(1).wait(1.0)

        def multi_trigger_plan():
            yield from bps.stage(device)
            try:
                # Trigger multiple times
                for i in range(3):
                    yield from bps.trigger(device, wait=True)
                    yield from bps.read(device)
            finally:
                yield from bps.unstage(device)

        documents = []
        RE.subscribe(lambda name, doc: documents.append((name, doc)))

        RE(multi_trigger_plan())

        # Should have multiple events
        events = [doc for name, doc in documents if name == "event"]
        assert len(events) >= 3, "Should have at least 3 event documents"
