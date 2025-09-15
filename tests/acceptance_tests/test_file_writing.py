"""
Acceptance tests for file writing and data capture functionality.
These tests verify that data can be written to NeXus/HDF5 files correctly.
"""

import time
from nexusformat.nexus import nxopen
from .conftest import wait_for_state


class TestFileCapture:
    """Test basic file capture functionality."""

    def test_file_capture_control(self, detector_in_standby, test_output_dir):
        """Test enabling and disabling file capture with proper path and filename."""
        device = detector_in_standby

        # Set file path and name
        file_path = str(test_output_dir)
        file_name = "test_enable_capture.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)

        # Verify initial state
        assert device.file_capture.get(as_string=True) == "Off", (
            "File capture should start as Off"
        )
        assert device.num_captured.get() == 0, "num_captured should start at 0"

        # Enable file capture
        device.file_capture.set("On").wait(5.0)

        # Verify file capture is on
        assert device.file_capture.get(as_string=True) == "On", (
            "File capture should be On"
        )

        # Sleep to ensure file is initialized before disabling
        time.sleep(1.0)

        # Disable file capture
        device.file_capture.set("Off").wait(5.0)

        # Verify file was created
        full_path = test_output_dir / file_name
        assert full_path.exists(), f"File should have been created at {full_path}"

    def test_double_enable_prevention(self, detector_in_standby, test_output_dir):
        """Test that enabling file capture twice does nothing."""
        device = detector_in_standby

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_double_enable.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)

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


class TestFileWriting:
    """Test actual data writing to files."""

    def test_file_creation_structure(self, detector_in_standby, test_output_dir):
        """Test that created files have correct NeXus structure."""
        device = detector_in_standby

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_structure.nxs"
        full_path = test_output_dir / file_name

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # Sleep to ensure file is initialized before disabling
        time.sleep(1.0)

        # Disable to finalize file
        device.file_capture.set("Off").wait(5.0)

        # Verify file structure
        assert full_path.exists(), "File should have been created"

        with nxopen(full_path, "r") as f:
            # Check basic NeXus structure
            assert "entry" in f, "Should have entry group"
            assert "instrument" in f["entry"], "Should have instrument group"
            assert "analyzer" in f["entry/instrument"], "Should have analyzer group"

    def test_acquisition_with_file_capture(self, detector_in_standby, test_output_dir):
        """Test acquisition with file capture enabled."""
        device = detector_in_standby

        # Set up minimal acquisition parameters
        device.num_scans.set(1).wait(5.0)

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_acquisition.nxs"
        full_path = test_output_dir / file_name

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
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
            assert f["entry/instrument/analyzer/data"].shape[0] == 1, "Should have 1 image"

    def test_multiple_acquisitions(self, detector_in_standby, test_output_dir):
        """Test multiple acquisitions with file capture enabled."""
        device = detector_in_standby

        # Set up minimal acquisition parameters
        device.num_scans.set(2).wait(5.0)
        
        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_multiple_acquisitions.nxs"
        full_path = test_output_dir / file_name

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
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
            assert f["entry/instrument/analyzer/data"].shape[0] == 3, "Should have 3 images"

    def test_file_is_readable_during_acquisition(self, detector_in_standby, test_output_dir):
        """Test that file is readable during acquisition and contains intermediate images."""
        device = detector_in_standby

        # Set up minimal acquisition parameters
        device.num_scans.set(5).wait(5.0)
        
        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_readable_during_acquisition.nxs"
        full_path = test_output_dir / file_name

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        def _num_captured_callback(value, old_value, **kwargs):
            if value > 0 and value > old_value:
                with nxopen(full_path, "r") as f:
                    assert f["entry/instrument/analyzer/data"].shape[0] == value, f"Should have {value} images"

        device.num_captured.subscribe(_num_captured_callback)

        try:
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)
            wait_for_state(device, "STANDBY", timeout=60.0)
        finally:
            device.num_captured.unsubscribe(_num_captured_callback)

    def test_file_contains_metadata(self, detector_in_standby, test_output_dir):
        """Test that file contains metadata."""
        device = detector_in_standby

        # Set up minimal acquisition parameters
        device.num_scans.set(1).wait(5.0)

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_contains_metadata.nxs"
        full_path = test_output_dir / file_name

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        device.acquire.set(1).wait(5.0)
        wait_for_state(device, "RUNNING", timeout=10.0)
        wait_for_state(device, "STANDBY", timeout=60.0)

        device.file_capture.set("Off").wait(5.0)

        with nxopen(full_path, "r") as f:
            assert "entry" in f, "File should have entry group"
            assert "instrument" in f["entry"], "Should have instrument group"
            assert "analyzer" in f["entry/instrument"], "Should have analyzer group"
            assert "angles" in f["entry/instrument/analyzer"], "Should have angles group"
            assert "energies" in f["entry/instrument/analyzer"], "Should have energies group"


class TestFilePathHandling:
    """Test file path and name handling."""

    def test_directory_creation(self, detector_in_standby, tmp_path):
        """Test that directories are created if they don't exist."""
        device = detector_in_standby

        # Use a nested directory that doesn't exist
        nested_dir = tmp_path / "nested" / "directories"
        file_path = str(nested_dir)
        file_name = "test_nested.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)

        # Enable file capture - should create directories
        device.file_capture.set("On").wait(5.0)

        # Verify directory was created
        assert nested_dir.exists(), "Nested directory should have been created"

        # Wait for file to be created
        time.sleep(1.0)

        # Clean up
        device.file_capture.set("Off").wait(5.0)

        # Verify file was created
        full_path = nested_dir / file_name
        assert full_path.exists(), "File should have been created in nested directory"
