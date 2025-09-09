"""
Acceptance tests for basic IOC connectivity and status monitoring.
These tests verify that the IOC is running and responding to basic queries.
"""

import pytest
import time
from conftest import wait_for_condition


class TestBasicConnectivity:
    """Test basic connection and communication with the IOC."""

    def test_device_connection(self, analyzer_device):
        """Test that the device can connect to all PVs."""
        device = analyzer_device
        
        # Verify connection to key PVs
        assert device.connected, "Device should be connected to IOC"
        assert device.connection_status.get() == 1, "IOC should be connected to hardware"
        
        # Test that we can read basic status
        state = device.state.get()
        assert state in ["STANDBY", "RUNNING", "MOVING"], f"Invalid state: {state}"

    def test_sync_functionality(self, analyzer_device):
        """Test the parameter synchronization functionality."""
        device = analyzer_device
        
        # Check initial sync status
        sync_status = device.sync.get(as_string=True)
        assert sync_status in ["OFF", "ON"], f"Invalid sync status: {sync_status}"
        
        # Get last sync time before enabling sync
        last_sync_before = device.last_sync.get()
        
        # Enable sync and wait for it to happen
        device.sync.set("ON").wait(1.0)
        time.sleep(2.0)  # Wait for at least one sync cycle
        
        # Verify sync occurred
        last_sync_after = device.last_sync.get()
        assert last_sync_after != last_sync_before, "Sync should have updated last_sync timestamp"

    def test_parameter_reading(self, analyzer_device):
        """Test reading of basic detector parameters."""
        device = analyzer_device
        
        # Test reading various parameter types
        parameters_to_test = [
            ('num_scans', int),
            ('num_steps', int),
            ('start_ke', (int, float)),
            ('end_ke', (int, float)),
            ('pass_energy', str),
            ('lens_mode', str),
            ('acq_mode', str),
        ]
        
        for param_name, expected_type in parameters_to_test:
            param_signal = getattr(device, param_name)
            
            if expected_type == str:
                value = param_signal.get(as_string=True)
            else:
                value = param_signal.get()
            
            assert isinstance(value, expected_type), f"{param_name} should be {expected_type}, got {type(value)}"

    def test_file_status_monitoring(self, analyzer_device):
        """Test file-related status monitoring."""
        device = analyzer_device
        
        # Test file capture status
        file_capture = device.file_capture.get(as_string=True)
        assert file_capture in ["Off", "On"], f"Invalid file_capture status: {file_capture}"
        
        # Test file status string
        file_status = device.file_status.get()
        assert isinstance(file_status, str), "File status should be a string"
        
        # Test counters
        num_captured = device.num_captured.get()
        num_processed = device.num_processed.get()
        assert isinstance(num_captured, int), "num_captured should be integer"
        assert isinstance(num_processed, int), "num_processed should be integer"
        assert num_captured >= 0, "num_captured should be non-negative"
        assert num_processed >= 0, "num_processed should be non-negative"


class TestStatusMonitoring:
    """Test status monitoring and state changes."""

    def test_acquisition_status_consistency(self, detector_in_standby):
        """Test that acquisition status is consistent with acquire signal."""
        device = detector_in_standby
        
        # Initially should be stopped
        assert device.acquire.get() == 0, "Acquire should be 0 initially"
        assert device.acquisition_status.get() == 0, "Acquisition status should be 0 initially"
        
        # Test state consistency
        state = device.state.get()
        if state == "STANDBY":
            assert device.acquisition_status.get() == 0, "Status should be 0 when in STANDBY"

    def test_connection_status_stability(self, analyzer_device):
        """Test that connection status remains stable during normal operation."""
        device = analyzer_device
        
        # Check connection status multiple times
        for i in range(5):
            status = device.connection_status.get()
            assert status == 1, f"Connection status should remain 1, got {status} on check {i}"
            time.sleep(0.5)

    def test_parameter_consistency(self, analyzer_device):
        """Test that parameter values remain consistent when read multiple times."""
        device = analyzer_device
        
        # Test that read-only parameters are stable
        stable_params = [
            device.endX,
            device.startY,
            device.num_slice,
            device.endY,
            device.startX,
        ]
        
        for param in stable_params:
            values = []
            for i in range(3):
                values.append(param.get())
                time.sleep(0.1)
            
            # All values should be the same (stable parameters)
            assert all(v == values[0] for v in values), f"Parameter {param.name} should be stable" 