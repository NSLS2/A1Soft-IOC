"""
Acceptance tests for acquisition control functionality.
These tests verify that acquisition can be started, stopped, and monitored correctly.
"""

import time
from .conftest import wait_for_state


class TestAcquisitionControl:
    """Test basic acquisition start/stop functionality."""

    def test_acquisition_state_consistency(self, detector_in_standby):
        """Test that acquire signal and acquisition_status remain consistent."""
        device = detector_in_standby

        # Verify initial state
        assert device.state.get() == "STANDBY", "Should start in STANDBY state"
        assert device.acquire.get() == 0, "Acquire should be 0 initially"
        assert device.acquisition_status.get() == 0, (
            "Acquisition status should be 0 initially"
        )

        # Test starting acquisition
        device.acquire.set(1).wait(5.0)
        wait_for_state(device, "RUNNING", timeout=10.0)

        # Both signals should indicate running
        assert device.acquire.get() == 1, "Acquire should be 1 when running"
        assert device.acquisition_status.get() == 1, (
            "Acquisition status should be 1 when running"
        )

        # Test stopping acquisition
        device.acquire.set(0).wait(5.0)
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
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)
            assert device.acquisition_status.get() == 1, (
                f"Cycle {cycle}: Should be running"
            )

            # Let it run briefly
            time.sleep(1.0)

            # Stop acquisition
            device.acquire.set(0).wait(5.0)
            wait_for_state(device, "STANDBY", timeout=10.0)
            assert device.acquisition_status.get() == 0, (
                f"Cycle {cycle}: Should be stopped"
            )

    def test_acquisition_with_minimal_scans(self, detector_in_standby):
        """Test acquisition with minimal scan settings to complete quickly."""
        device = detector_in_standby

        device.num_scans.set(1).wait(5.0)  # Just one scan

        # Start acquisition
        device.acquire.set(1).wait(5.0)
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
            raise RuntimeError("Acquisition did not complete in reasonable time")

        # Verify scan counter advanced
        final_act_scans = device.act_scans.get()
        assert final_act_scans > 0, (
            "Act scans should have advanced"
        )


class TestAcquisitionStates:
    """Test acquisition state transitions and monitoring."""

    def test_state_transitions(self, detector_in_standby):
        """Test that state transitions follow expected sequence."""
        device = detector_in_standby

        # Track state changes
        transitions_seen = []

        def state_callback(value=None, old_value=None, **kwargs):
            if value != old_value:
                transitions_seen.append((old_value, value))

        # Subscribe to state changes
        device.state.subscribe(state_callback, run=False)

        try:
            # Start acquisition and monitor states
            device.acquire.set(1).wait(5.0)

            # Wait for RUNNING state
            wait_for_state(device, "RUNNING", timeout=10.0)

            # Stop acquisition
            device.acquire.set(0).wait(5.0)

            # Wait for return to STANDBY
            wait_for_state(device, "STANDBY", timeout=10.0)

            # Verify we saw the expected state transitions
            assert transitions_seen == [("STANDBY", "RUNNING"), ("RUNNING", "STANDBY")], (
                "Should have seen STANDBY, RUNNING, STANDBY transitions"
            )

        finally:
            device.state.unsubscribe(state_callback)

    def test_acquisition_monitoring_during_run(self, detector_in_standby):
        """Test monitoring acquisition parameters during a run."""
        device = detector_in_standby

        device.num_scans.set(5).wait(5.0)

        act_scans_transitions = []
        def act_scans_callback(value=None, old_value=None, **kwargs):
            if value != old_value:
                act_scans_transitions.append((old_value, value))

        device.act_scans.subscribe(
            act_scans_callback,
            run=False,
        )

        try:
            # Start acquisition and wait for completion
            device.acquire.set(1).wait(5.0)
            wait_for_state(device, "RUNNING", timeout=10.0)
            wait_for_state(device, "STANDBY", timeout=60.0)

            assert act_scans_transitions == [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)], (
                "Act scans should be (0, 1), (1, 2), (2, 3), (3, 4), (4, 5)"
            )

        finally:
            device.act_scans.unsubscribe(act_scans_callback)


class TestAcquisitionErrorHandling:
    """Test error handling during acquisition."""

    def test_double_start_prevention(self, detector_in_standby):
        """Test that starting acquisition twice doesn't cause issues."""
        device = detector_in_standby

        # Start acquisition
        device.acquire.set(1).wait(5.0)
        wait_for_state(device, "RUNNING", timeout=10.0)

        # Try to start again (should be ignored or handled gracefully)
        device.acquire.set(1).wait(5.0)

        # Should still be running normally
        assert device.state.get() == "RUNNING", (
            "Should still be running after double start"
        )
        assert device.acquisition_status.get() == 1, (
            "Acquisition status should still be 1"
        )

        # Stop acquisition
        device.acquire.set(0).wait(5.0)
        wait_for_state(device, "STANDBY", timeout=10.0)

    def test_stop_when_already_stopped(self, detector_in_standby):
        """Test stopping acquisition when already stopped."""
        device = detector_in_standby

        # Ensure we're stopped
        assert device.state.get() == "STANDBY", "Should start in STANDBY"

        # Try to stop when already stopped
        device.acquire.set(0).wait(5.0)

        # Should remain in STANDBY
        assert device.state.get() == "STANDBY", "Should remain in STANDBY"
        assert device.acquisition_status.get() == 0, (
            "Acquisition status should remain 0"
        )

    def test_acquisition_timeout_handling(self, detector_in_standby):
        """Test behavior when acquisition runs longer than expected."""
        device = detector_in_standby

        device.num_scans.set(100).wait(5.0)  # Many scans

        # Start acquisition
        device.acquire.set(1).wait(5.0)
        wait_for_state(device, "RUNNING", timeout=10.0)

        # Let it run briefly
        time.sleep(2.0)

        # Force stop
        device.acquire.set(0).wait(5.0)

        # Should be able to stop even during long acquisition
        wait_for_state(device, "STANDBY", timeout=10.0)

        assert device.acquisition_status.get() == 0, (
            "Should be stopped after force stop"
        )
