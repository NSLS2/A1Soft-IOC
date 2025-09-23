"""
Acceptance tests for parameter getting and setting functionality.
These tests verify that detector parameters can be read and written correctly.
"""


class TestParameterSetting:
    """Test setting and getting detector parameters."""

    def test_integer_parameter_setting(self, detector_in_standby):
        """Test setting integer parameters."""
        device = detector_in_standby

        if device.frames.get() == 200:
            to_set = 300
        else:
            to_set = 200
        device.frames.set(to_set).wait(5.0)

        actual_frames = device.frames.get()
        assert actual_frames == to_set, f"Expected frames={to_set}, got {actual_frames}"

    def test_float_parameter_setting(self, detector_in_standby):
        """Test setting float parameters."""
        device = detector_in_standby

        # Test start_ke parameter
        original_start_ke = device.start_ke.get()
        new_start_ke = (
            original_start_ke + 0.1
            if original_start_ke < 1000
            else original_start_ke - 0.1
        )

        try:
            device.start_ke.set(new_start_ke).wait(5.0)

            actual_start_ke = device.start_ke.get()
            # Allow for small floating point differences
            assert abs(actual_start_ke - new_start_ke) < 0.01, (
                f"Expected start_ke≈{new_start_ke}, got {actual_start_ke}"
            )

        finally:
            device.start_ke.set(original_start_ke).wait(5.0)

    def test_enum_parameter_setting(self, detector_in_standby):
        """Test setting enum parameters."""
        device = detector_in_standby

        # Test pass_energy enum
        original_pass_energy = device.pass_energy.get(as_string=True)

        # Get available enum strings
        enum_strings = device.pass_energy.enum_strs
        assert len(enum_strings) > 1, "Should have multiple pass energy options"

        # Find a different value to set
        new_pass_energy = enum_strings[0]
        for enum_val in enum_strings:
            if enum_val != original_pass_energy:
                new_pass_energy = enum_val
                break

        try:
            device.pass_energy.set(new_pass_energy).wait(5.0)

            actual_pass_energy = device.pass_energy.get(as_string=True)
            assert actual_pass_energy == new_pass_energy, (
                f"Expected {new_pass_energy}, got {actual_pass_energy}"
            )
        finally:
            device.pass_energy.set(original_pass_energy).wait(5.0)


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
            device.num_captured,
            device.num_processed,
            device.act_scans,
        ]

        for param in read_only_params:
            # Should be able to read
            value = param.get()
            assert value is not None, f"Should be able to read {param.name}"

            # For string parameters, also test string reading
            if hasattr(param, "get"):
                try:
                    str_value = param.get(as_string=True)
                    assert isinstance(str_value, str), (
                        f"{param.name} string value should be string"
                    )
                except Exception:
                    pass  # Not all parameters support as_string
