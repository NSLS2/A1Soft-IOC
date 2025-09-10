"""
Acceptance tests for device interface functionality.
Tests the SpectrumAnalyzer device both with and without bluesky integration.
"""

import pytest
import time

from bluesky import RunEngine
from bluesky.plans import count, trigger_and_read
from bluesky.utils import SyncOrAsyncIterator
import bluesky.plan_stubs as bps
from ophyd.status import Status
from ophyd import Staged

from .conftest import wait_for_state


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
        except:
            pass

    def test_device_in_simple_count_plan(self, detector_in_standby, run_engine, test_output_dir):
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
        assert 'start' in doc_types, "Should generate start document"
        assert 'descriptor' in doc_types, "Should generate descriptor document"
        assert 'event' in doc_types, "Should generate event document"
        assert 'stop' in doc_types, "Should generate stop document"
        
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
            assert signal_name in description, f"Missing signal {signal_name} in description"
            assert 'source' in description[signal_name], f"Missing source for {signal_name}"
            assert 'dtype' in description[signal_name], f"Missing dtype for {signal_name}"

        # Test read method  
        reading = device.read()
        assert isinstance(reading, dict), "read() should return dict"
        
        for signal_name in expected_signals:
            if signal_name in reading:  # Some signals may not be in reading
                assert 'value' in reading[signal_name], f"Missing value for {signal_name}"
                assert 'timestamp' in reading[signal_name], f"Missing timestamp for {signal_name}"

    def test_device_stream_assets_protocol(self, detector_in_standby, test_output_dir):
        """Test WritesStreamAssets protocol implementation."""
        device = detector_in_standby
        
        # Configure device for file writing
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("test_stream.h5").wait(1.0)
        device.num_scans.set(1).wait(1.0)
        
        # Stage device (required for stream assets)
        device.stage()
        
        try:
            # Check describe includes stream asset
            description = device.describe()
            stream_key = f"{device.name}_image"
            assert stream_key in description, f"Missing stream asset key {stream_key}"
            
            stream_desc = description[stream_key]
            assert stream_desc.get('external') == 'STREAM:', "Should be marked as external stream"
            assert 'shape' in stream_desc, "Stream asset should have shape"
            assert 'dtype' in stream_desc, "Stream asset should have dtype"
            
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
                assert 'stream_resource' in doc_names, "Should generate stream_resource doc"
                if len(asset_docs) > 1:
                    assert 'stream_datum' in doc_names, "Should generate stream_datum doc"
                    
        finally:
            device.unstage()

    def test_device_staging_with_bluesky(self, detector_in_standby, test_output_dir):
        """Test device staging behavior within bluesky context."""
        device = detector_in_standby
        
        # Configure device
        device.file_path.set(str(test_output_dir)).wait(1.0) 
        device.file_name.set("test_staging.h5").wait(1.0)
        
        # Initially unstaged
        assert device._staged == Staged.no, "Should start unstaged"
        
        # Stage device
        staged_signals = device.stage()
        assert device._staged == Staged.yes, "Should be staged"
        assert device.file_capture.get(as_string=True) == "On", "File capture should be on when staged"
        
        # Verify staging signals are set correctly
        assert len(staged_signals) > 0, "Should return list of staged signals"
        
        # Test triggering when staged
        status = device.trigger()
        assert isinstance(status, Status), "Should return Status when staged"
        
        # Unstage
        device.unstage()
        assert device._staged == Staged.no, "Should be unstaged"
        assert device.file_capture.get(as_string=True) == "Off", "File capture should be off when unstaged"

    def test_device_in_trigger_and_read_plan(self, detector_in_standby, run_engine, test_output_dir):
        """Test device with trigger_and_read plan."""
        device = detector_in_standby
        RE = run_engine
        
        # Configure device
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("test_trigger_read.h5").wait(1.0)
        device.num_scans.set(1).wait(1.0)
        
        documents = []
        def collect_docs(name, doc):
            documents.append((name, doc))
        
        RE.subscribe(collect_docs)
        
        # Use trigger_and_read plan
        RE(trigger_and_read([device]))
        
        # Verify proper document sequence
        doc_types = [name for name, doc in documents]
        assert 'start' in doc_types
        assert 'descriptor' in doc_types  
        assert 'event' in doc_types
        assert 'stop' in doc_types

    def test_device_with_multiple_triggers(self, detector_in_standby, run_engine, test_output_dir):
        """Test device can handle multiple triggers in sequence."""
        device = detector_in_standby  
        RE = run_engine
        
        # Configure device
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("test_multi_trigger.h5").wait(1.0)
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
        events = [doc for name, doc in documents if name == 'event']
        assert len(events) >= 3, "Should have at least 3 event documents"


class TestDeviceWithoutBluesky:
    """Test device interface using direct ophyd methods without bluesky."""

    def test_direct_staging_and_triggering(self, detector_in_standby, test_output_dir):
        """Test direct staging and triggering without RunEngine."""
        device = detector_in_standby
        
        # Configure device
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("test_direct.h5").wait(1.0)  
        device.num_scans.set(1).wait(1.0)
        
        # Direct staging
        assert device._staged == Staged.no, "Should start unstaged"
        staged_signals = device.stage()
        
        try:
            assert device._staged == Staged.yes, "Should be staged"
            assert device.file_capture.get(as_string=True) == "On", "File capture should be on"
            
            # Direct triggering
            status = device.trigger()
            assert isinstance(status, Status), "Should return Status object"
            
            # Wait for completion
            status.wait(timeout=30.0)
            assert status.done, "Status should be done after waiting"
            assert status.success, "Status should be successful"
            
        finally:
            device.unstage()
            assert device._staged == Staged.no, "Should be unstaged"

    def test_manual_parameter_control(self, detector_in_standby):
        """Test manual parameter setting and reading without bluesky coordination."""
        device = detector_in_standby
        
        # Test setting and reading various parameters
        original_num_scans = device.num_scans.get()
        
        try:
            # Set and verify numeric parameter
            test_scans = 5
            device.num_scans.set(test_scans).wait(1.0)
            assert device.num_scans.get() == test_scans, "num_scans should be updated"
            
            # Test energy parameters
            original_start_ke = device.start_ke.get()
            test_start_ke = 100.0
            device.start_ke.set(test_start_ke).wait(1.0)
            assert abs(device.start_ke.get() - test_start_ke) < 0.1, "start_ke should be updated"
            
            # Test string parameters
            original_pass_energy = device.pass_energy.get(as_string=True)
            available_pass_energies = ["3", "5", "10", "15", "20", "50", "100", "150", "200"]
            if original_pass_energy in available_pass_energies:
                # Try setting a different pass energy
                new_pass_energy = "10" if original_pass_energy != "10" else "20" 
                device.pass_energy.set(new_pass_energy).wait(1.0)
                assert device.pass_energy.get(as_string=True) == new_pass_energy
                
                # Restore original
                device.pass_energy.set(original_pass_energy).wait(1.0)
            
            # Restore original values
            device.start_ke.set(original_start_ke).wait(1.0)
            
        finally:
            device.num_scans.set(original_num_scans).wait(1.0)

    def test_direct_acquisition_control(self, detector_in_standby):
        """Test direct acquisition control without bluesky."""
        device = detector_in_standby
        
        # Verify initial state
        assert device.state.get() == "STANDBY", "Should start in STANDBY"
        assert device.acquire.get() == 0, "Acquire should be 0"
        
        # Manually start acquisition
        device.acquire.set(1).wait(0.5)
        
        # Wait for state change
        wait_for_state(device, "RUNNING", timeout=10.0)
        assert device.acquisition_status.get() == 1, "Acquisition status should be 1"
        
        # Let it run briefly
        time.sleep(2.0)
        
        # Manually stop acquisition
        device.acquire.set(0).wait(0.5)
        wait_for_state(device, "STANDBY", timeout=10.0)
        assert device.acquisition_status.get() == 0, "Acquisition status should be 0"

    def test_direct_file_control(self, detector_in_standby, test_output_dir):
        """Test direct file capture control without bluesky."""
        device = detector_in_standby
        
        # Configure file settings
        test_file = test_output_dir / "direct_test.h5"
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("direct_test.h5").wait(1.0)
        
        # Verify initial file capture state
        assert device.file_capture.get(as_string=True) == "Off", "File capture should be off initially"
        
        # Manually enable file capture
        device.file_capture.set("On").wait(1.0)
        assert device.file_capture.get(as_string=True) == "On", "File capture should be on"
        
        # Check file counters
        initial_captured = device.num_captured.get()
        initial_processed = device.num_processed.get()
        
        assert isinstance(initial_captured, int), "num_captured should be integer"
        assert isinstance(initial_processed, int), "num_processed should be integer"
        
        # Disable file capture
        device.file_capture.set("Off").wait(1.0)
        assert device.file_capture.get(as_string=True) == "Off", "File capture should be off"

    def test_direct_detector_parameters(self, detector_in_standby):
        """Test direct detector parameter access without bluesky."""
        device = detector_in_standby
        
        # Test detector max count parameters
        max_count = device.det_max_count.get()
        assert isinstance(max_count, (int, float)), "det_max_count should be numeric"
        assert max_count >= 0, "det_max_count should be non-negative"
        
        # Test threshold setting
        original_threshold = device.det_max_count_threshold.get()
        try:
            test_threshold = max(1000, original_threshold)
            device.det_max_count_threshold.set(test_threshold).wait(1.0)
            assert device.det_max_count_threshold.get() == test_threshold, "Threshold should be updated"
        finally:
            device.det_max_count_threshold.set(original_threshold).wait(1.0)
        
        # Test detector off control
        original_det_off = device.det_off.get()
        device.det_off.set(0).wait(0.5)  # Ensure detector is on
        assert device.det_off.get() == 0, "Detector should be on"

    def test_status_monitoring_without_bluesky(self, detector_in_standby):
        """Test status monitoring capabilities without bluesky."""
        device = detector_in_standby
        
        # Test connection monitoring
        assert device.connection_status.get() == 1, "Should be connected"
        
        # Test sync functionality
        original_sync = device.sync.get(as_string=True)
        original_last_sync = device.last_sync.get()
        
        try:
            # Enable sync  
            device.sync.set("ON").wait(1.0)
            time.sleep(2.0)  # Wait for sync cycle
            
            # Verify sync occurred
            new_last_sync = device.last_sync.get()
            assert new_last_sync != original_last_sync, "Last sync should be updated"
            
        finally:
            device.sync.set(original_sync).wait(1.0)

    def test_error_handling_without_bluesky(self, detector_in_standby, test_output_dir):
        """Test error handling in direct usage mode."""
        device = detector_in_standby
        
        # Test triggering without staging should raise error
        with pytest.raises(RuntimeError, match="not ready to trigger"):
            device.trigger()
        
        # Test staging with file capture already on should raise error
        device.file_capture.set("On").wait(1.0)
        try:
            with pytest.raises(RuntimeError, match="File capture must be off"):
                device.stage()
        finally:
            device.file_capture.set("Off").wait(1.0)


class TestDeviceProtocolCompliance:
    """Test that device properly implements required protocols."""

    def test_readable_protocol_completeness(self, detector_in_standby):
        """Test complete Readable protocol implementation."""
        device = detector_in_standby
        
        # Test required methods exist
        assert hasattr(device, 'describe'), "Should have describe method"
        assert hasattr(device, 'read'), "Should have read method"
        assert callable(device.describe), "describe should be callable"
        assert callable(device.read), "read should be callable"
        
        # Test describe returns proper structure
        desc = device.describe()
        assert isinstance(desc, dict), "describe should return dict"
        
        for key, value in desc.items():
            assert isinstance(key, str), "Keys should be strings"
            assert isinstance(value, dict), "Values should be dicts"
            required_keys = ['source', 'dtype', 'shape']
            for req_key in required_keys:
                if req_key not in value:
                    # Some keys might be optional, but at least source and dtype should exist
                    assert 'source' in value, f"Missing source in {key}"
                    assert 'dtype' in value, f"Missing dtype in {key}"

    def test_stream_assets_protocol_completeness(self, detector_in_standby, test_output_dir):
        """Test complete WritesStreamAssets protocol implementation."""
        device = detector_in_standby
        
        # Configure and stage device
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("protocol_test.h5").wait(1.0)
        device.num_scans.set(1).wait(1.0)
        
        device.stage()
        try:
            # Test required methods exist
            assert hasattr(device, 'collect_asset_docs'), "Should have collect_asset_docs method"
            assert callable(device.collect_asset_docs), "collect_asset_docs should be callable"
            
            # Test method signature and return type
            asset_docs = device.collect_asset_docs()
            assert isinstance(asset_docs, SyncOrAsyncIterator), "Should return SyncOrAsyncIterator"
            
            # Convert to list to test contents
            asset_list = list(asset_docs)
            # May be empty if no acquisition has happened yet, but should be iterable
            assert isinstance(asset_list, list), "Should be convertible to list"
            
        finally:
            device.unstage()

    def test_device_integration_both_modes(self, detector_in_standby, run_engine, test_output_dir):
        """Test device works correctly switching between bluesky and direct modes."""
        device = detector_in_standby
        RE = run_engine
        
        # Configure device
        device.file_path.set(str(test_output_dir)).wait(1.0)
        device.file_name.set("integration_test.h5").wait(1.0)
        device.num_scans.set(1).wait(1.0)
        
        # First use directly
        device.stage()
        try:
            status = device.trigger()
            status.wait(timeout=30.0)
            assert status.success, "Direct trigger should succeed"
        finally:
            device.unstage()
        
        # Then use with bluesky
        documents = []
        RE.subscribe(lambda name, doc: documents.append((name, doc)))
        
        RE(count([device], num=1))
        
        # Verify both modes worked
        assert len([d for n, d in documents if n == 'event']) >= 1, "Bluesky mode should generate events"
        assert device._staged == Staged.no, "Should be properly unstaged after bluesky"
