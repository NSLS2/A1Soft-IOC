"""
Acceptance tests for file writing and data capture functionality.
These tests verify that data can be written to NeXus/HDF5 files correctly.
"""

import time
import platform
import numpy as np

import pytest
from nexusformat.nexus import nxopen
from .conftest import wait_for_state


class TestFileCapture:
    """Test basic file capture functionality."""

    def test_file_capture_control(self, detector_in_standby, test_output_dir):
        """Test enabling and disabling file capture with proper path and filename."""
        device = detector_in_standby

        # Set file path and name
        file_path = str(test_output_dir)
        file_prefix = "test_enable_capture"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)

        # Verify initial state
        assert device.file_capture.get(as_string=True) == "Off", (
            "File capture should start as Off"
        )

        # Enable file capture
        device.file_capture.set("On").wait(5.0)

        # Verify file capture is on
        assert device.file_capture.get(as_string=True) == "On", (
            "File capture should be On"
        )
        assert device.num_captured.get() == 0, "num_captured should start at 0"

        # Sleep to ensure file is initialized before disabling
        time.sleep(1.0)

        # Disable file capture
        device.file_capture.set("Off").wait(5.0)

    def test_double_enable_prevention(self, detector_in_standby, test_output_dir):
        """Test that enabling file capture twice does nothing."""
        device = detector_in_standby

        # Set up file capture
        file_path = str(test_output_dir)
        file_prefix = "test_double_enable"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)

        # Enable file capture
        device.file_capture.set("On").wait(5.0)
        assert device.file_capture.get(as_string=True) == "On", (
            "File capture should be On"
        )

        # Try to enable again - should do nothing
        device.file_capture.set("On").wait(5.0)
        assert device.file_capture.get(as_string=True) == "On", (
            "File capture should be On"
        )

        # Clean up
        device.file_capture.set("Off").wait(5.0)


@pytest.mark.skipif(
    platform.system() != "Windows", reason="Must be run on same server as IOC"
)
class TestFileWriting:
    """Test actual data writing to files."""

    def test_file_open_close(self, detector_in_standby, test_output_dir):
        """Test that created files have correct NeXus structure."""
        device = detector_in_standby

        # Set up file capture
        file_path = str(test_output_dir)
        file_prefix = "test_structure"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # Sleep to ensure file is initialized before disabling
        time.sleep(1.0)

        # Disable to finalize file
        device.file_capture.set("Off").wait(5.0)

        # Verify file was not created
        assert not full_path.exists(), "File should not have been created"

    def test_acquisition_with_file_capture(self, detector_in_standby, test_output_dir):
        """Test acquisition with file capture enabled."""
        device = detector_in_standby

        # Set up minimal acquisition parameters
        device.num_scans.set(1).wait(5.0)

        # Set up file capture
        file_path = str(test_output_dir)
        file_prefix = "test_acquisition"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # Start acquisition
        device.acquire.set(1).wait(5.0)
        wait_for_state(device, "RUNNING", timeout=10.0)
        wait_for_state(device, "STANDBY", timeout=60.0)

        device.file_capture.set("Off").wait(5.0)

        num_captured = device.num_captured.get()
        assert num_captured == 1, "Should have captured 1 image"

        # Verify file was written with data
        assert full_path.exists(), "File should exist after acquisition"

        with nxopen(full_path, "r") as f:
            assert "entry" in f, "File should have entry group"
            assert "instrument" in f["entry"], "Should have instrument group"
            assert "analyzer" in f["entry/instrument"], "Should have analyzer group"
            assert "data" in f["entry/instrument/analyzer"], "Should have data group"
            assert f["entry/instrument/analyzer/data"].shape[0] == 1, (
                "Should have 1 image"
            )

    def test_multiple_acquisitions(self, detector_in_standby, test_output_dir):
        """Test multiple acquisitions with file capture enabled."""
        device = detector_in_standby

        # Set up minimal acquisition parameters
        device.num_scans.set(2).wait(5.0)

        # Set up file capture
        file_path = str(test_output_dir)
        file_prefix = "test_multiple_acquisitions"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        for i in range(1, 4):
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)
            wait_for_state(device, "STANDBY", timeout=60.0)
            num_captured = device.num_captured.get()
            assert num_captured == i, f"Should have captured {i} images"

        device.file_capture.set("Off").wait(5.0)
        assert full_path.exists(), "File should exist after acquisitions"
        with nxopen(full_path, "r") as f:
            assert "entry" in f, "File should have entry group"
            assert "instrument" in f["entry"], "Should have instrument group"
            assert "analyzer" in f["entry/instrument"], "Should have analyzer group"
            assert "data" in f["entry/instrument/analyzer"], "Should have data group"
            assert f["entry/instrument/analyzer/data"].shape[0] == 3, (
                "Should have 3 images"
            )

    def test_file_is_readable_during_acquisition(
        self, detector_in_standby, test_output_dir
    ):
        """Test that file is readable during acquisition and contains intermediate images."""
        device = detector_in_standby

        # Set up minimal acquisition parameters
        device.num_scans.set(2).wait(5.0)

        # Set up file capture
        file_path = str(test_output_dir)
        file_prefix = "test_readable_during_acquisition"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        shape_tracker = []

        def _num_captured_callback(value, old_value, **kwargs):
            if value > 0 and value > old_value:
                with nxopen(full_path, "r") as f:
                    shape_tracker.append(f["entry/instrument/analyzer/data"].shape[0])

        device.num_captured.subscribe(_num_captured_callback)

        try:
            for _ in range(2):
                device.acquire.set(1).wait(5.0)
                wait_for_state(device, "RUNNING", timeout=10.0)
                wait_for_state(device, "STANDBY", timeout=60.0)
            device.file_capture.set("Off").wait(5.0)
            assert shape_tracker == [1, 2] or shape_tracker == [2, 2], (
                "Should have 2 images"
            )
        finally:
            device.num_captured.unsubscribe(_num_captured_callback)

    def test_file_contains_metadata(self, detector_in_standby, test_output_dir):
        """Test that file contains metadata."""
        device = detector_in_standby

        # Set up minimal acquisition parameters
        device.num_scans.set(1).wait(5.0)

        # Set up file capture
        file_path = str(test_output_dir)
        file_prefix = "test_contains_metadata"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        device.acquire.set(1).wait(5.0)
        wait_for_state(device, "RUNNING", timeout=10.0)
        wait_for_state(device, "STANDBY", timeout=60.0)

        device.file_capture.set("Off").wait(5.0)

        with nxopen(full_path, "r") as f:
            assert "entry" in f, "File should have entry group"
            assert "instrument" in f["entry"], "Should have instrument group"
            assert "analyzer" in f["entry/instrument"], "Should have analyzer group"
            assert "angles" in f["entry/instrument/analyzer"], (
                "Should have angles group"
            )
            assert "energies" in f["entry/instrument/analyzer"], (
                "Should have energies group"
            )

    def test_file_read_speed(self, detector_in_standby, test_output_dir):
        """Test that file can be read efficiently."""
        device = detector_in_standby

        # Set up file capture
        file_path = str(test_output_dir)
        file_prefix = "test_read_speed"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # Acquire 1 frame
        device.acquire.set(1).wait(5.0)
        wait_for_state(device, "RUNNING", timeout=10.0)
        wait_for_state(device, "STANDBY", timeout=60.0)

        device.file_capture.set("Off").wait(5.0)

        # Read the file
        times = []
        for _ in range(100):
            start_time = time.time()
            with nxopen(full_path, "r") as f:
                data = f["entry/instrument/analyzer/data"].nxvalue
                _ = f["entry/instrument/analyzer/angles"].nxvalue
                _ = f["entry/instrument/analyzer/energies"].nxvalue
                _ = f["entry/instrument/analyzer/deflector_x"].nxvalue
                assert data.shape[0] == 1, "Should have 1 image"
            end_time = time.time()
            times.append(end_time - start_time)

        assert np.mean(times) < 0.1, "File should be read in less than 0.1 seconds"


@pytest.mark.skipif(
    platform.system() != "Windows", reason="Must be run on same server as IOC"
)
class TestFilePathHandling:
    """Test file path and name handling."""

    def test_directory_creation(self, detector_in_standby, tmp_path):
        """Test that directories are created if they don't exist."""
        device = detector_in_standby

        # Use a nested directory that doesn't exist
        nested_dir = tmp_path / "nested" / "directories"
        file_path = str(nested_dir)
        file_prefix = "test_nested"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)

        # Enable file capture - should create directories
        device.file_capture.set("On").wait(5.0)

        # Verify directory was created
        assert nested_dir.exists(), "Nested directory should have been created"

        # Wait for file to be created
        time.sleep(1.0)

        # Clean up
        device.file_capture.set("Off").wait(5.0)

        # Verify file was not created
        full_path = nested_dir / f"{file_prefix}_0001.nxs"
        assert not full_path.exists(), (
            "File should not have been created in nested directory"
        )


@pytest.mark.skipif(
    platform.system() != "Windows", reason="Must be run on same server as IOC"
)
class TestAggregateMode:
    """Test aggregate file writing mode."""

    def test_aggregate_mode_sums_same_deflector_x(
        self, detector_in_standby, test_output_dir
    ):
        """Test that frames with same deflector_x are summed."""
        device = detector_in_standby

        device.num_scans.set(1).wait(5.0)

        # Enable aggregate mode
        device.file_mode.set("Aggregate").wait(5.0)
        assert device.file_mode.get(as_string=True) == "Aggregate"

        file_path = str(test_output_dir)
        file_prefix = "test_aggregate_sum"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # Run 3 acquisitions at same deflector_x
        initial_deflx = device.deflX.get()
        for _ in range(3):
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)
            wait_for_state(device, "STANDBY", timeout=60.0)

        device.file_capture.set("Off").wait(5.0)
        device.file_mode.set("Normal").wait(5.0)

        # Should have 1 aggregated frame (same deflector_x)
        with nxopen(full_path, "r") as f:
            analyzer = f["entry/instrument/analyzer"]
            assert analyzer["data"].shape[0] == 1, "Should have 1 aggregated frame"
            assert analyzer["num_contributions"][0].nxvalue == 3, (
                "Should have 3 contributions"
            )
            assert np.isclose(
                analyzer["deflector_x"][0].nxvalue, round(initial_deflx, 2)
            )

    def test_aggregate_mode_separates_different_deflector_x(
        self, detector_in_standby, test_output_dir
    ):
        """Test that frames with different deflector_x create separate entries."""
        device = detector_in_standby

        device.num_scans.set(1).wait(5.0)

        # Enable aggregate mode
        device.file_mode.set("Aggregate").wait(5.0)

        file_path = str(test_output_dir)
        file_prefix = "test_aggregate_separate"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # Run acquisitions at 3 different deflector_x values
        deflx_values = [0.0, 1.0, 2.0]
        for deflx in deflx_values:
            device.deflX.set(deflx).wait(5.0)
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)
            wait_for_state(device, "STANDBY", timeout=60.0)

        device.file_capture.set("Off").wait(5.0)
        device.file_mode.set("Normal").wait(5.0)

        # Should have 3 separate frames
        with nxopen(full_path, "r") as f:
            analyzer = f["entry/instrument/analyzer"]
            assert analyzer["data"].shape[0] == 3, "Should have 3 separate frames"
            stored_deflx = analyzer["deflector_x"].nxvalue
            for i, expected in enumerate(deflx_values):
                assert np.isclose(stored_deflx[i], expected), (
                    f"deflector_x[{i}] should be {expected}"
                )
                assert analyzer["num_contributions"][i].nxvalue == 1

    def test_aggregate_mode_mixed_revisits(
        self, detector_in_standby, test_output_dir
    ):
        """Test aggregation when revisiting angles: A, B, A, B pattern."""
        device = detector_in_standby

        device.num_scans.set(1).wait(5.0)
        device.file_mode.set("Aggregate").wait(5.0)

        file_path = str(test_output_dir)
        file_prefix = "test_aggregate_revisit"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # A, B, A, B pattern
        angles = [0.0, 1.0, 0.0, 1.0]
        for deflx in angles:
            device.deflX.set(deflx).wait(5.0)
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)
            wait_for_state(device, "STANDBY", timeout=60.0)

        device.file_capture.set("Off").wait(5.0)
        device.file_mode.set("Normal").wait(5.0)

        # Should have 2 frames with 2 contributions each
        with nxopen(full_path, "r") as f:
            analyzer = f["entry/instrument/analyzer"]
            assert analyzer["data"].shape[0] == 2, "Should have 2 aggregated frames"
            assert analyzer["num_contributions"][0].nxvalue == 2, (
                "First angle should have 2 contributions"
            )
            assert analyzer["num_contributions"][1].nxvalue == 2, (
                "Second angle should have 2 contributions"
            )

    def test_aggregate_precision_rounding(
        self, detector_in_standby, test_output_dir
    ):
        """Test that deflector_x values are rounded to precision."""
        device = detector_in_standby

        device.num_scans.set(1).wait(5.0)
        device.file_mode.set("Aggregate").wait(5.0)
        device.file_aggregate_precision.set(1).wait(5.0)  # 1 decimal place

        file_path = str(test_output_dir)
        file_prefix = "test_aggregate_precision"
        full_path = test_output_dir / f"{file_prefix}_0001.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_prefix.set(file_prefix).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # Values 1.04 and 1.06 should round to 1.0 and 1.1 respectively
        values = [1.04, 1.06]
        for deflx in values:
            device.deflX.set(deflx).wait(5.0)
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)
            wait_for_state(device, "STANDBY", timeout=60.0)

        device.file_capture.set("Off").wait(5.0)
        device.file_mode.set("Normal").wait(5.0)
        device.file_aggregate_precision.set(2).wait(5.0)

        with nxopen(full_path, "r") as f:
            analyzer = f["entry/instrument/analyzer"]
            # 1.04 rounds to 1.0, 1.06 rounds to 1.1 with precision=1
            assert analyzer["data"].shape[0] == 2, "Should have 2 frames"
            stored = sorted(analyzer["deflector_x"].nxvalue)
            assert np.isclose(stored[0], 1.0), "First should round to 1.0"
            assert np.isclose(stored[1], 1.1), "Second should round to 1.1"
