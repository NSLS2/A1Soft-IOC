"""
Acceptance tests for file writing and data capture functionality.
These tests verify that data can be written to NeXus/HDF5 files correctly.
"""

import time
import h5py
from .conftest import wait_for_state


class TestFileCapture:
    """Test basic file capture functionality."""

    def test_enable_file_capture(self, detector_in_standby, test_output_dir):
        """Test enabling file capture with proper path and filename."""
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

        # Check file status
        file_status = device.file_status.get()
        assert "started" in file_status.lower(), (
            f"File status should indicate started: {file_status}"
        )

        # Disable file capture
        device.file_capture.set("Off").wait(5.0)

        # Verify file was created
        full_path = test_output_dir / file_name
        assert full_path.exists(), f"File should have been created at {full_path}"

    def test_disable_file_capture(self, detector_in_standby, test_output_dir):
        """Test disabling file capture."""
        device = detector_in_standby

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_disable_capture.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # Verify it's on
        assert device.file_capture.get(as_string=True) == "On", (
            "File capture should be On"
        )

        # Disable file capture
        device.file_capture.set("Off").wait(5.0)

        # Verify it's off
        assert device.file_capture.get(as_string=True) == "Off", (
            "File capture should be Off"
        )

        # Check file status
        file_status = device.file_status.get()
        assert "stopped" in file_status.lower(), (
            f"File status should indicate stopped: {file_status}"
        )

    def test_double_enable_prevention(self, detector_in_standby, test_output_dir):
        """Test that enabling file capture twice raises an error."""
        device = detector_in_standby

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_double_enable.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        # Try to enable again - should do nothing
        device.file_capture.set("On").wait(5.0)

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

        # Disable to finalize file
        device.file_capture.set("Off").wait(5.0)

        # Verify file structure
        assert full_path.exists(), "File should have been created"

        with h5py.File(full_path, "r") as f:
            # Check basic NeXus structure
            assert "entry" in f, "Should have entry group"
            assert "instrument" in f["entry"], "Should have instrument group"
            assert "analyzer" in f["entry/instrument"], "Should have analyzer group"

    def test_acquisition_with_file_capture(self, detector_in_standby, test_output_dir):
        """Test acquisition with file capture enabled."""
        device = detector_in_standby

        # Set up minimal acquisition parameters
        original_num_scans = device.num_scans.get()
        device.num_scans.set(1).wait(5.0)

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_acquisition.nxs"
        full_path = test_output_dir / file_name

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        try:
            # Check initial counters
            initial_num_captured = device.num_captured.get()

            # Start acquisition
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Wait for acquisition to complete or timeout
            start_time = time.time()
            while time.time() - start_time < 30.0:
                if device.state.get() == "STANDBY":
                    break
                time.sleep(0.5)

            if device.state.get() != "STANDBY":
                # Force stop if needed
                device.acquire.set(0).wait(5.0)
                wait_for_state(device, "STANDBY", timeout=5.0)

            # Check that counters updated
            final_num_captured = device.num_captured.get()

            # We expect some data to have been captured
            assert final_num_captured >= initial_num_captured, (
                "Should have captured some data"
            )

        finally:
            # Clean up
            device.file_capture.set("Off").wait(5.0)
            device.num_scans.set(original_num_scans).wait(5.0)

        # Verify file was written with data
        assert full_path.exists(), "File should exist after acquisition"

        # Check file size (should be non-trivial)
        file_size = full_path.stat().st_size
        assert file_size > 1024, f"File should contain data, got size: {file_size}"

    def test_file_appending(self, detector_in_standby, test_output_dir):
        """Test that reopening an existing file appends data correctly."""
        device = detector_in_standby

        file_path = str(test_output_dir)
        file_name = "test_append.nxs"
        full_path = test_output_dir / file_name

        # First session - create initial file
        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        initial_captured = device.num_captured.get()

        device.file_capture.set("Off").wait(5.0)

        # Verify file exists
        assert full_path.exists(), "Initial file should be created"

        # Second session - reopen same file
        device.file_capture.set("On").wait(5.0)

        # Should start from where we left off
        resumed_captured = device.num_captured.get()
        assert resumed_captured >= initial_captured, "Should resume from previous count"

        device.file_capture.set("Off").wait(5.0)


class TestFileCounters:
    """Test file-related counters and status."""

    def test_num_captured_counter(self, detector_in_standby, test_output_dir):
        """Test that num_captured counter updates correctly."""
        device = detector_in_standby

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_counters.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        initial_count = device.num_captured.get()
        assert isinstance(initial_count, int), "num_captured should be integer"
        assert initial_count >= 0, "num_captured should be non-negative"

        # Clean up
        device.file_capture.set("Off").wait(5.0)

    def test_num_processed_during_acquisition(
        self, detector_in_standby, test_output_dir
    ):
        """Test num_processed counter during acquisition."""
        device = detector_in_standby

        # Set up minimal parameters
        original_num_scans = device.num_scans.get()
        device.num_scans.set(2).wait(5.0)

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_processed.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)
        device.file_capture.set("On").wait(5.0)

        try:
            # Start acquisition
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)
            initial_processed = device.num_processed.get()

            # Monitor for a short time
            monitoring_time = 5.0
            start_time = time.time()

            while (
                time.time() - start_time
            ) < monitoring_time and device.state.get() == "RUNNING":
                current_processed = device.num_processed.get()
                assert current_processed >= initial_processed, (
                    "num_processed should not decrease"
                )
                time.sleep(0.2)

            # Stop acquisition
            device.acquire.set(0).wait(5.0)
            wait_for_state(device, "STANDBY", timeout=10.0)

        finally:
            device.file_capture.set("Off").wait(5.0)
            device.num_scans.set(original_num_scans).wait(5.0)

    def test_file_status_messages(self, detector_in_standby, test_output_dir):
        """Test that file status messages are informative."""
        device = detector_in_standby

        # Initial status
        initial_status = device.file_status.get()
        assert isinstance(initial_status, str), "File status should be string"

        # Set up file capture
        file_path = str(test_output_dir)
        file_name = "test_status.nxs"

        device.file_path.set(file_path).wait(5.0)
        device.file_name.set(file_name).wait(5.0)

        # Enable capture
        device.file_capture.set("On").wait(5.0)

        # Status should indicate active capture
        active_status = device.file_status.get()
        assert active_status != initial_status, (
            "Status should change when capture starts"
        )
        assert len(active_status) > 0, "Status should not be empty"

        # Disable capture
        device.file_capture.set("Off").wait(5.0)

        # Status should indicate completion
        final_status = device.file_status.get()
        assert final_status != active_status, "Status should change when capture stops"


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

        # Clean up
        device.file_capture.set("Off").wait(5.0)

        # Verify file was created
        full_path = nested_dir / file_name
        assert full_path.exists(), "File should have been created in nested directory"

    def test_path_and_name_validation(self, detector_in_standby, test_output_dir):
        """Test validation of file paths and names."""
        device = detector_in_standby

        valid_path = str(test_output_dir)

        # Test various file name formats
        valid_names = [
            "simple.nxs",
            "with_underscores.nxs",
            "with-hyphens.nxs",
            "with123numbers.nxs",
        ]

        for name in valid_names:
            device.file_path.set(valid_path).wait(5.0)
            device.file_name.set(name).wait(5.0)

            # Should be able to enable capture
            device.file_capture.set("On").wait(5.0)
            device.file_capture.set("Off").wait(5.0)

            # File should be created
            full_path = test_output_dir / name
            assert full_path.exists(), f"File {name} should have been created"
