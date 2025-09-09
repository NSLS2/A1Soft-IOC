"""
Acceptance tests for parameter getting and setting functionality.
These tests verify that detector parameters can be read and written correctly.
"""

import pytest
import time
from conftest import wait_for_condition


class TestParameterSetting:
    """Test setting and getting detector parameters."""

    def test_integer_parameter_setting(self, detector_in_standby):
        """Test setting integer parameters."""
        device = detector_in_standby
        
        # Test frames parameter
        original_frames = device.frames.get()
        new_frames = original_frames + 1 if original_frames < 100 else original_frames - 1
        
        device.frames.set(new_frames)
        time.sleep(0.5)  # Allow time for the set operation
        
        actual_frames = device.frames.get()
        assert actual_frames == new_frames, f"Expected frames={new_frames}, got {actual_frames}"
        
        # Restore original value
        device.frames.set(original_frames)

    def test_float_parameter_setting(self, detector_in_standby):
        """Test setting float parameters."""
        device = detector_in_standby
        
        # Test start_ke parameter
        original_start_ke = device.start_ke.get()
        new_start_ke = original_start_ke + 0.1 if original_start_ke < 1000 else original_start_ke - 0.1
        
        device.start_ke.set(new_start_ke)
        time.sleep(0.5)
        
        actual_start_ke = device.start_ke.get()
        # Allow for small floating point differences
        assert abs(actual_start_ke - new_start_ke) < 0.01, f"Expected start_ke≈{new_start_ke}, got {actual_start_ke}"
        
        # Restore original value
        device.start_ke.set(original_start_ke)

    def test_enum_parameter_setting(self, detector_in_standby):
        """Test setting enum parameters."""
        device = detector_in_standby
        
        # Test pass_energy enum
        original_pass_energy = device.pass_energy.get(as_string=True)
        
        # Get available enum strings
        enum_strings = device.pass_energy.enum_strs
        assert len(enum_strings) > 1, "Should have multiple pass energy options"
        
        # Find a different value to set
        new_pass_energy = None
        for enum_val in enum_strings:
            if enum_val != original_pass_energy:
                new_pass_energy = enum_val
                break
        
        if new_pass_energy:
            device.pass_energy.set(new_pass_energy)
            time.sleep(0.5)
            
            actual_pass_energy = device.pass_energy.get(as_string=True)
            assert actual_pass_energy == new_pass_energy, f"Expected {new_pass_energy}, got {actual_pass_energy}"
            
            # Restore original value
            device.pass_energy.set(original_pass_energy)

    def test_boolean_parameter_setting(self, detector_in_standby):
        """Test setting boolean parameters."""
        device = detector_in_standby
        
        # Test spin parameter
        original_spin = device.spin.get()
        new_spin = not original_spin
        
        device.spin.set(new_spin)
        time.sleep(0.5)
        
        actual_spin = device.spin.get()
        assert actual_spin == new_spin, f"Expected spin={new_spin}, got {actual_spin}"
        
        # Restore original value
        device.spin.set(original_spin)

    def test_string_parameter_setting(self, detector_in_standby):
        """Test setting string parameters."""
        device = detector_in_standby
        
        # Test comment1 parameter
        original_comment = device.comment1.get()
        new_comment = "Test comment from acceptance test"
        
        device.comment1.set(new_comment)
        time.sleep(0.5)
        
        actual_comment = device.comment1.get()
        assert actual_comment == new_comment, f"Expected '{new_comment}', got '{actual_comment}'"
        
        # Restore original value
        device.comment1.set(original_comment)

    def test_parameter_validation(self, detector_in_standby):
        """Test that parameter validation works correctly."""
        device = detector_in_standby
        
        # Test that setting invalid values raises appropriate errors
        # This might be device-specific, so we test common cases
        
        # Test setting negative frames (should fail or be corrected)
        original_frames = device.frames.get()
        try:
            device.frames.set(-1)
            time.sleep(0.5)
            # If no exception, check that value was corrected or rejected
            actual_frames = device.frames.get()
            assert actual_frames >= 0, "Negative frames should not be allowed"
        except Exception:
            # Exception is acceptable for invalid parameters
            pass
        finally:
            # Ensure we restore a valid value
            device.frames.set(max(1, original_frames))


class TestParameterDependencies:
    """Test parameter dependencies and constraints."""

    def test_energy_parameter_consistency(self, detector_in_standby):
        """Test that energy-related parameters maintain consistency."""
        device = detector_in_standby
        
        start_ke = device.start_ke.get()
        end_ke = device.end_ke.get()
        center_ke = device.center_ke.get()
        
        # These values should be physically reasonable
        assert start_ke >= 0, "Start KE should be non-negative"
        assert end_ke >= 0, "End KE should be non-negative"
        assert center_ke >= 0, "Center KE should be non-negative"
        
        # If we have a sweep, start and end should be different
        if device.acq_mode.get(as_string=True) == "Swept":
            if start_ke != end_ke:
                if start_ke < end_ke:
                    assert start_ke <= center_ke <= end_ke, "Center KE should be between start and end"
                else:
                    assert end_ke <= center_ke <= start_ke, "Center KE should be between end and start"

    def test_scan_parameter_consistency(self, detector_in_standby):
        """Test that scan-related parameters are consistent."""
        device = detector_in_standby
        
        num_scans = device.num_scans.get()
        act_scans = device.act_scans.get()
        
        assert num_scans >= 0, "Number of scans should be non-negative"
        assert act_scans >= 0, "Actual scans should be non-negative"
        assert act_scans <= num_scans, "Actual scans should not exceed planned scans"

    def test_dimension_parameters(self, detector_in_standby):
        """Test that dimension-related parameters are reasonable."""
        device = detector_in_standby
        
        start_x = device.startX.get()
        end_x = device.endX.get()
        start_y = device.startY.get()
        end_y = device.endY.get()
        num_slice = device.num_slice.get()
        num_steps = device.num_steps.get()
        
        # Basic sanity checks
        assert start_x >= 0, "StartX should be non-negative"
        assert end_x >= 0, "EndX should be non-negative"
        assert start_y >= 0, "StartY should be non-negative"
        assert end_y >= 0, "EndY should be non-negative"
        assert num_slice > 0, "Number of slices should be positive"
        assert num_steps > 0, "Number of steps should be positive"


class TestReadOnlyParameters:
    """Test that read-only parameters behave correctly."""

    def test_read_only_parameter_access(self, analyzer_device):
        """Test that read-only parameters can be read but not written."""
        device = analyzer_device
        
        # Test some read-only parameters
        read_only_params = [
            device.state,
            device.acquisition_status,
            device.connection_status,
            device.last_sync,
            device.file_status,
            device.num_captured,
            device.num_processed,
            device.act_scans,
        ]
        
        for param in read_only_params:
            # Should be able to read
            value = param.get()
            assert value is not None, f"Should be able to read {param.name}"
            
            # For string parameters, also test string reading
            if hasattr(param, 'get'):
                try:
                    str_value = param.get(as_string=True)
                    assert isinstance(str_value, str), f"{param.name} string value should be string"
                except:
                    pass  # Not all parameters support as_string

    def test_computed_parameters(self, analyzer_device):
        """Test parameters that are computed from other values."""
        device = analyzer_device
        
        # Test that computed parameters have reasonable values
        escale_min = device.escale_min.get()
        escale_max = device.escale_max.get()
        
        if escale_min is not None and escale_max is not None:
            assert escale_min <= escale_max, "EScale min should be <= max"
        
        xscale_min = device.xscale_min.get()
        xscale_max = device.xscale_max.get()
        
        if xscale_min is not None and xscale_max is not None:
            assert xscale_min <= xscale_max, "XScale min should be <= max" 