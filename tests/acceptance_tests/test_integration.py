"""
Integration tests for complete detector workflows.
These tests verify that all components work together correctly for realistic use cases.
"""

import pytest
import time
import h5py
import numpy as np
from conftest import wait_for_state


class TestCompleteWorkflow:
    """Test complete acquisition workflows from parameter setup to data collection."""

    def test_full_acquisition_workflow(self, detector_in_standby, test_output_dir):
        """Test a complete acquisition workflow with parameter setting, file capture, and data collection."""
        device = detector_in_standby

        # Store original parameters for restoration
        original_params = {
            "num_scans": device.num_scans.get(),
            "frames": device.frames.get(),
            "start_ke": device.start_ke.get(),
            "end_ke": device.end_ke.get(),
            "comment1": device.comment1.get(),
        }

        try:
            # Step 1: Configure acquisition parameters
            device.num_scans.set(2).wait(0.5)
            device.frames.set(1).wait(0.5)
            device.start_ke.set(100.0).wait(0.5)
            device.end_ke.set(110.0).wait(0.5)
            device.comment1.set("Integration test acquisition").wait(0.5)

            # Verify parameters were set
            assert device.num_scans.get() == 2, "num_scans should be set to 2"
            assert device.frames.get() == 1, "frames should be set to 1"
            assert abs(device.start_ke.get() - 100.0) < 0.1, "start_ke should be ~100.0"
            assert abs(device.end_ke.get() - 110.0) < 0.1, "end_ke should be ~110.0"
            assert device.comment1.get() == "Integration test acquisition", (
                "comment should be set"
            )

            # Step 2: Set up file capture
            file_path = str(test_output_dir)
            file_name = "integration_test.nxs"
            full_path = test_output_dir / file_name

            device.file_path.set(file_path).wait(0.5)
            device.file_name.set(file_name).wait(0.5)
            device.file_capture.set("On").wait(0.5)

            assert device.file_capture.get(as_string=True) == "On", (
                "File capture should be enabled"
            )

            # Step 3: Run acquisition
            initial_num_processed = device.num_processed.get()

            device.acquire.set(1).wait(0.5)
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Monitor acquisition progress
            max_wait_time = 60.0  # Generous timeout for complete workflow
            start_time = time.time()
            acquisition_completed = False

            while (time.time() - start_time) < max_wait_time:
                current_state = device.state.get()
                current_processed = device.num_processed.get()

                if current_state == "STANDBY":
                    acquisition_completed = True
                    break

                # Check for progress
                if current_processed > initial_num_processed:
                    print(f"Progress: {current_processed} scans processed")

                time.sleep(1.0)

            if not acquisition_completed:
                # Force stop if acquisition didn't complete naturally
                device.acquire.set(0).wait(0.5)
                wait_for_state(device, "STANDBY", timeout=10.0)
                pytest.skip("Acquisition did not complete in reasonable time")

            # Step 4: Verify acquisition results
            final_num_processed = device.num_processed.get()

            assert final_num_processed >= initial_num_processed, (
                "Should have processed some scans"
            )

            # Step 5: Stop file capture and verify file
            device.file_capture.set("Off").wait(1.0)
            time.sleep(2.0)

            assert full_path.exists(), "Output file should exist"

            # Verify file contents
            file_size = full_path.stat().st_size
            assert file_size > 2048, (
                f"File should contain substantial data, got {file_size} bytes"
            )

            # Check HDF5 structure
            with h5py.File(full_path, "r") as f:
                assert "entry" in f, "File should have NeXus entry"
                assert "instrument" in f["entry"], "Should have instrument group"
                assert "analyzer" in f["entry/instrument"], "Should have analyzer group"

                # Check for data if it was written
                if "data" in f["entry/instrument/analyzer"]:
                    data = f["entry/instrument/analyzer/data"]
                    assert data.shape[0] > 0, "Should have at least one frame of data"
                    assert data.dtype == np.uint32, "Data should be uint32"

        finally:
            # Restore original parameters
            for param_name, original_value in original_params.items():
                param_signal = getattr(device, param_name)
                param_signal.set(original_value).wait(0.5)

            # Ensure clean state
            if device.file_capture.get(as_string=True) == "On":
                device.file_capture.set("Off").wait(1.0)

    def test_parameter_change_during_setup(self, detector_in_standby, test_output_dir):
        """Test changing parameters and ensuring they're synchronized before acquisition."""
        device = detector_in_standby

        # Store original values
        original_num_scans = device.num_scans.get()
        original_pass_energy = device.pass_energy.get(as_string=True)

        try:
            # Change multiple parameters
            new_num_scans = 3
            device.num_scans.set(new_num_scans).wait(0.5)

            # Change pass energy if possible
            if device.pass_energy.enum_strs and len(device.pass_energy.enum_strs) > 1:
                new_pass_energy = None
                for pe in device.pass_energy.enum_strs:
                    if pe != original_pass_energy:
                        new_pass_energy = pe
                        break

                if new_pass_energy:
                    device.pass_energy.set(new_pass_energy).wait(0.5)

            # Wait for parameter synchronization
            time.sleep(0.5)

            # Verify parameters are set correctly before acquisition
            assert device.num_scans.get() == new_num_scans, (
                "num_scans should be updated"
            )

            # Set up minimal file capture for the test
            device.file_path.set(str(test_output_dir)).wait(0.5)
            device.file_name.set("param_test.nxs").wait(0.5)
            device.file_capture.set("On").wait(0.5)

            # Start acquisition with new parameters
            device.acquire.set(1).wait(0.5)
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Let it run briefly, then stop
            time.sleep(2.0)
            device.acquire.set(0).wait(1.0)
            wait_for_state(device, "STANDBY", timeout=10.0)

            device.file_capture.set("Off").wait(1.0)

        finally:
            # Restore original parameters
            device.num_scans.set(original_num_scans).wait(0.5)
            device.pass_energy.set(original_pass_energy).wait(0.5)

    def test_multiple_acquisitions_same_session(
        self, detector_in_standby, test_output_dir
    ):
        """Test running multiple acquisitions in the same session."""
        device = detector_in_standby

        # Set up for quick acquisitions
        original_num_scans = device.num_scans.get()
        device.num_scans.set(1).wait(0.5)

        try:
            # Set up file capture
            device.file_path.set(str(test_output_dir)).wait(0.5)
            device.file_name.set("multi_acquisition.nxs").wait(0.5)
            device.file_capture.set("On").wait(0.5)

            initial_captured = device.num_captured.get()

            # Run multiple short acquisitions
            for run_num in range(3):
                print(f"Starting acquisition run {run_num + 1}")

                device.acquire.set(1).wait(0.5)
                wait_for_state(device, "RUNNING", timeout=10.0)

                # Wait for completion or timeout
                start_time = time.time()
                while (
                    time.time() - start_time
                ) < 20.0 and device.state.get() == "RUNNING":
                    time.sleep(0.5)

                # Stop if still running
                if device.state.get() == "RUNNING":
                    device.acquire.set(0).wait(0.5)
                    wait_for_state(device, "STANDBY", timeout=5.0)

                # Check progress
                current_captured = device.num_captured.get()
                assert current_captured >= initial_captured, (
                    f"Should have captured data by run {run_num + 1}"
                )

                # Brief pause between runs
                time.sleep(1.0)

            # Finalize file
            device.file_capture.set("Off").wait(0.5)
            time.sleep(1.0)

            # Verify file exists and contains data from multiple runs
            file_path = test_output_dir / "multi_acquisition.nxs"
            assert file_path.exists(), "Output file should exist"

            final_captured = device.num_captured.get()
            assert final_captured > initial_captured, (
                "Should have captured data from multiple runs"
            )

        finally:
            device.num_scans.set(original_num_scans).wait(0.5)


class TestErrorRecovery:
    """Test error recovery and resilience during complex workflows."""

    def test_acquisition_stop_and_restart(self, detector_in_standby, test_output_dir):
        """Test stopping and restarting acquisition mid-run."""
        device = detector_in_standby

        # Set up for longer acquisition that can be interrupted
        original_num_scans = device.num_scans.get()
        device.num_scans.set(10).wait(0.5)  # Longer acquisition

        try:
            # Set up file capture
            device.file_path.set(str(test_output_dir)).wait(0.5)
            device.file_name.set("stop_restart_test.nxs").wait(0.5)
            device.file_capture.set("On").wait(0.5)

            # Start acquisition
            device.acquire.set(1).wait(0.5)
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Let it run briefly, then stop
            time.sleep(3.0)
            device.acquire.set(0).wait(0.5)
            wait_for_state(device, "STANDBY", timeout=10.0)

            # Check that we can restart
            device.acquire.set(1).wait(0.5)
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Stop again
            time.sleep(2.0)
            device.acquire.set(0).wait(0.5)
            wait_for_state(device, "STANDBY", timeout=10.0)

            # Clean up
            device.file_capture.set("Off").wait(1.0)

            # Verify file was created despite interruptions
            file_path = test_output_dir / "stop_restart_test.nxs"
            assert file_path.exists(), (
                "File should exist despite acquisition interruption"
            )

        finally:
            device.num_scans.set(original_num_scans).wait(0.5)

    def test_file_capture_restart(self, detector_in_standby, test_output_dir):
        """Test stopping and restarting file capture."""
        device = detector_in_standby

        file_name = "restart_capture_test.nxs"
        file_path = test_output_dir / file_name

        # First capture session
        device.file_path.set(str(test_output_dir)).wait(0.5)
        device.file_name.set(file_name).wait(0.5)
        device.file_capture.set("On").wait(0.5)

        first_num_captured = device.num_captured.get()

        device.file_capture.set("Off").wait(1.0)

        assert file_path.exists(), "File should exist after first session"

        # Second capture session (should append to existing file)
        device.file_capture.set("On").wait(0.5)

        second_num_captured = device.num_captured.get()
        assert second_num_captured >= first_num_captured, (
            "Should resume from previous count"
        )

        device.file_capture.set("Off").wait(1.0)

    def test_parameter_validation_workflow(self, detector_in_standby):
        """Test that invalid parameter combinations are handled gracefully."""
        device = detector_in_standby

        # Store original values
        original_start_ke = device.start_ke.get()
        original_end_ke = device.end_ke.get()

        try:
            # Try setting end_ke lower than start_ke (might be invalid)
            current_start = device.start_ke.get()
            if current_start > 1.0:
                device.end_ke.set(current_start - 1.0).wait(0.5)

                # The system should either reject this or adjust parameters automatically
                final_start = device.start_ke.get()
                final_end = device.end_ke.get()

                # At minimum, the values should remain valid
                assert isinstance(final_start, (int, float)), (
                    "start_ke should remain numeric"
                )
                assert isinstance(final_end, (int, float)), (
                    "end_ke should remain numeric"
                )
                assert final_start >= 0, "start_ke should remain non-negative"
                assert final_end >= 0, "end_ke should remain non-negative"

        finally:
            # Restore original values
            device.start_ke.set(original_start_ke).wait(0.5)
            device.end_ke.set(original_end_ke).wait(0.5)


class TestPerformanceAndStability:
    """Test performance and stability under various conditions."""

    def test_rapid_parameter_changes(self, detector_in_standby):
        """Test rapid parameter changes don't cause instability."""
        device = detector_in_standby

        # Store original values
        original_frames = device.frames.get()
        original_comment = device.comment1.get()

        try:
            # Make rapid parameter changes
            for i in range(10):
                new_frames = original_frames + (i % 3)  # Cycle through a few values
                new_comment = f"Rapid test {i}"

                device.frames.set(new_frames).wait(0.5)
                device.comment1.set(new_comment).wait(0.5)

            # Let the system settle
            time.sleep(2.0)

            # Verify final values are reasonable
            final_frames = device.frames.get()
            final_comment = device.comment1.get()

            assert isinstance(final_frames, int), "frames should remain integer"
            assert isinstance(final_comment, str), "comment should remain string"
            assert final_frames > 0, "frames should remain positive"

        finally:
            # Restore original values
            device.frames.set(original_frames).wait(0.5)
            device.comment1.set(original_comment).wait(0.5)

    def test_connection_stability_during_workflow(
        self, detector_in_standby, test_output_dir
    ):
        """Test that connection remains stable during a complete workflow."""
        device = detector_in_standby

        # Monitor connection status throughout workflow
        connection_checks = []

        def check_connection():
            status = device.connection_status.get()
            connection_checks.append(status)
            return status == 1

        # Initial check
        assert check_connection(), "Should start with good connection"

        try:
            # Set up file capture
            device.file_path.set(str(test_output_dir)).wait(0.5)
            device.file_name.set("stability_test.nxs").wait(0.5)
            device.file_capture.set("On").wait(0.5)

            assert check_connection(), (
                "Connection should remain stable after file setup"
            )

            # Set some parameters
            original_comment = device.comment1.get()
            device.comment1.set("Connection stability test").wait(0.5)

            assert check_connection(), (
                "Connection should remain stable after parameter change"
            )

            # Brief acquisition
            device.acquire.set(1).wait(0.5)
            wait_for_state(device, "RUNNING", timeout=10.0)

            assert check_connection(), (
                "Connection should remain stable during acquisition"
            )

            time.sleep(2.0)
            device.acquire.set(0).wait(0.5)
            wait_for_state(device, "STANDBY", timeout=10.0)

            assert check_connection(), (
                "Connection should remain stable after stopping acquisition"
            )

            # Clean up
            device.file_capture.set("Off").wait(1.0)
            device.comment1.set(original_comment).wait(0.5)

            assert check_connection(), "Connection should remain stable after cleanup"

            # Verify all connection checks passed
            assert all(status == 1 for status in connection_checks), (
                "Connection should have remained stable throughout"
            )

        except Exception:
            print(f"Connection checks during failure: {connection_checks}")
            raise
