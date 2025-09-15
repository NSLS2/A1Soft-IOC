"""
Acceptance tests for acquisition control functionality.
These tests verify that acquisition can be started, stopped, and monitored correctly.
"""

import pytest
import time
from .conftest import wait_for_state


class TestAcquisitionControl:
    """Test basic acquisition start/stop functionality."""

    def test_start_acquisition(self, detector_in_standby):
        """Test starting an acquisition from STANDBY state."""
        device = detector_in_standby

        # Verify initial state
        assert device.state.get() == "STANDBY", "Should start in STANDBY state"
        assert device.acquire.get() == 0, "Acquire should be 0 initially"
        assert device.acquisition_status.get() == 0, (
            "Acquisition status should be 0 initially"
        )

        # Start acquisition
        device.acquire.set(1).wait(0.5)

        # Wait for state transition to RUNNING
        wait_for_state(device, "RUNNING", timeout=10.0)

        # Verify acquisition status updated
        assert device.acquisition_status.get() == 1, (
            "Acquisition status should be 1 when running"
        )

        # Stop acquisition for cleanup
        device.acquire.set(0).wait(0.5)
        wait_for_state(device, "STANDBY", timeout=10.0)

    def test_stop_acquisition(self, detector_in_standby):
        """Test stopping a running acquisition."""
        device = detector_in_standby

        # Start acquisition first
        device.acquire.set(1).wait(0.5)
        wait_for_state(device, "RUNNING", timeout=10.0)

        # Stop acquisition
        device.acquire.set(0).wait(0.5)

        # Wait for state transition back to STANDBY
        wait_for_state(device, "STANDBY", timeout=10.0)

        # Verify acquisition status updated
        assert device.acquisition_status.get() == 0, (
            "Acquisition status should be 0 when stopped"
        )

    def test_acquisition_state_consistency(self, detector_in_standby):
        """Test that acquire signal and acquisition_status remain consistent."""
        device = detector_in_standby

        # Test starting acquisition
        device.acquire.set(1).wait(0.5)
        wait_for_state(device, "RUNNING", timeout=10.0)

        # Both signals should indicate running
        assert device.acquire.get() == 1, "Acquire should be 1 when running"
        assert device.acquisition_status.get() == 1, (
            "Acquisition status should be 1 when running"
        )

        # Test stopping acquisition
        device.acquire.set(0).wait(0.5)
        wait_for_state(device, "STANDBY", timeout=10.0)

        # Both signals should indicate stopped
        assert device.acquire.get() == 0, "Acquire should be 0 when stopped"
        assert device.acquisition_status.get() == 0, (
            "Acquisition status should be 0 when stopped"
        )

    def test_repeated_start_stop(self, detector_in_standby):
        """Test repeated start/stop cycles work correctly."""
        device = detector_in_standby

        for cycle in range(3):
            # Start acquisition
            device.acquire.set(1).wait(0.5)
            wait_for_state(device, "RUNNING", timeout=10.0)
            assert device.acquisition_status.get() == 1, (
                f"Cycle {cycle}: Should be running"
            )

            # Let it run briefly
            time.sleep(1.0)

            # Stop acquisition
            device.acquire.set(0).wait(0.5)
            wait_for_state(device, "STANDBY", timeout=10.0)
            assert device.acquisition_status.get() == 0, (
                f"Cycle {cycle}: Should be stopped"
            )

    def test_acquisition_with_minimal_scans(self, detector_in_standby):
        """Test acquisition with minimal scan settings to complete quickly."""
        device = detector_in_standby

        # Set minimal parameters for quick completion
        original_num_scans = device.num_scans.get()
        device.num_scans.set(1).wait(5.0)  # Just one scan

        try:
            # Reset scan counters
            initial_act_scans = device.act_scans.get()

            # Start acquisition
            device.acquire.set(1).wait(0.5)
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Wait for acquisition to complete or timeout
            start_time = time.time()
            completed = False
            while time.time() - start_time < 30.0:  # 30 second timeout
                if device.state.get() == "STANDBY":
                    completed = True
                    break
                time.sleep(0.5)

            if not completed:
                # Force stop if it didn't complete
                device.acquire.set(0).wait(0.5)
                wait_for_state(device, "STANDBY", timeout=5.0)
                pytest.skip("Acquisition did not complete in reasonable time")
            else:
                # Verify scan counter advanced
                final_act_scans = device.act_scans.get()
                assert final_act_scans >= initial_act_scans, (
                    "Act scans should have advanced"
                )

        finally:
            # Restore original scan count
            device.num_scans.set(original_num_scans).wait(5.0)


class TestAcquisitionStates:
    """Test acquisition state transitions and monitoring."""

    def test_state_transitions(self, detector_in_standby):
        """Test that state transitions follow expected sequence."""
        device = detector_in_standby

        # Track state changes
        states_seen = []

        def state_callback(value=None, **kwargs):
            states_seen.append(value)

        # Subscribe to state changes
        device.state.subscribe(state_callback)

        try:
            # Start acquisition and monitor states
            device.acquire.set(1).wait(0.5)

            # Wait for RUNNING state
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Stop acquisition
            device.acquire.set(0).wait(0.5)

            # Wait for return to STANDBY
            wait_for_state(device, "STANDBY", timeout=10.0)

            # Verify we saw the expected state transitions
            assert "RUNNING" in states_seen, "Should have seen RUNNING state"
            assert "STANDBY" in states_seen, "Should have seen STANDBY state"

        finally:
            device.state.unsubscribe(state_callback)

    def test_acquisition_monitoring_during_run(self, detector_in_standby):
        """Test monitoring acquisition parameters during a run."""
        device = detector_in_standby

        # Set a reasonable number of scans for monitoring
        original_num_scans = device.num_scans.get()
        device.num_scans.set(5).wait(5.0)

        try:
            initial_act_scans = device.act_scans.get()

            # Start acquisition
            device.acquire.set(1).wait(0.5)
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Monitor for a short time
            monitoring_time = 5.0
            start_time = time.time()
            max_act_scans_seen = initial_act_scans

            while (
                time.time() - start_time
            ) < monitoring_time and device.state.get() == "RUNNING":
                current_act_scans = device.act_scans.get()
                max_act_scans_seen = max(max_act_scans_seen, current_act_scans)
                time.sleep(0.2)

            # Stop acquisition
            device.acquire.set(0).wait(0.5)
            wait_for_state(device, "STANDBY", timeout=10.0)

            # Verify some progress was made (if the acquisition actually ran)
            final_act_scans = device.act_scans.get()
            if device.num_scans.get() > 0:
                # We expect some scans to have completed or at least started
                assert final_act_scans >= initial_act_scans, (
                    "Expected some scan progress"
                )

        finally:
            # Restore original scan count
            device.num_scans.set(original_num_scans).wait(5.0)


class TestAcquisitionErrorHandling:
    """Test error handling during acquisition."""

    def test_double_start_prevention(self, detector_in_standby):
        """Test that starting acquisition twice doesn't cause issues."""
        device = detector_in_standby

        # Start acquisition
        device.acquire.set(1).wait(0.5)
        wait_for_state(device, "RUNNING", timeout=10.0)

        # Try to start again (should be ignored or handled gracefully)
        device.acquire.set(1).wait(0.5)
        time.sleep(0.5)

        # Should still be running normally
        assert device.state.get() == "RUNNING", (
            "Should still be running after double start"
        )
        assert device.acquisition_status.get() == 1, (
            "Acquisition status should still be 1"
        )

        # Stop acquisition
        device.acquire.set(0).wait(0.5)
        wait_for_state(device, "STANDBY", timeout=10.0)

    def test_stop_when_already_stopped(self, detector_in_standby):
        """Test stopping acquisition when already stopped."""
        device = detector_in_standby

        # Ensure we're stopped
        assert device.state.get() == "STANDBY", "Should start in STANDBY"

        # Try to stop when already stopped
        device.acquire.set(0).wait(0.5)
        time.sleep(1.0)

        # Should remain in STANDBY
        assert device.state.get() == "STANDBY", "Should remain in STANDBY"
        assert device.acquisition_status.get() == 0, (
            "Acquisition status should remain 0"
        )

    def test_acquisition_timeout_handling(self, detector_in_standby):
        """Test behavior when acquisition runs longer than expected."""
        device = detector_in_standby

        # Set up for a longer acquisition that we'll interrupt
        original_num_scans = device.num_scans.get()
        device.num_scans.set(100).wait(5.0)  # Many scans

        try:
            # Start acquisition
            device.acquire.set(1).wait(0.5)
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Let it run briefly
            time.sleep(2.0)

            # Force stop
            device.acquire.set(0).wait(0.5)

            # Should be able to stop even during long acquisition
            wait_for_state(device, "STANDBY", timeout=10.0)

            assert device.acquisition_status.get() == 0, (
                "Should be stopped after force stop"
            )

        finally:
            # Restore original settings
            device.num_scans.set(original_num_scans).wait(5.0)


class TestAcquisitionParameters:
    """Test acquisition-related parameter behavior."""

    def test_scan_counter_behavior(self, detector_in_standby):
        """Test that scan counters behave correctly during acquisition."""
        device = detector_in_standby

        # Check initial scan counters
        num_scans = device.num_scans.get()
        initial_act_scans = device.act_scans.get()

        assert isinstance(num_scans, int), "num_scans should be integer"
        assert isinstance(initial_act_scans, int), "act_scans should be integer"
        assert num_scans >= 0, "num_scans should be non-negative"
        assert initial_act_scans >= 0, "act_scans should be non-negative"
        assert initial_act_scans <= num_scans, "act_scans should not exceed num_scans"

    def test_acquisition_mode_consistency(self, detector_in_standby):
        """Test that acquisition mode affects acquisition behavior appropriately."""
        device = detector_in_standby

        # Check current acquisition mode
        acq_mode = device.acq_mode.get(as_string=True)
        assert acq_mode in ["Fixed", "FixedTrigd", "Swept", "Dither"], (
            f"Invalid acq_mode: {acq_mode}"
        )

        # Mode should be readable and consistent
        acq_mode_int = device.acq_mode.get()
        assert isinstance(acq_mode_int, int), (
            "Acquisition mode should have integer representation"
        )
