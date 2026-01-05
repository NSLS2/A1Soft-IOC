"""Unit tests for DetectorWriter class."""

import asyncio
from unittest.mock import MagicMock, patch
import numpy as np
import pytest

from a1soft.ioc import DetectorWriter


@pytest.fixture
def writer():
    """Create a fresh DetectorWriter instance."""
    return DetectorWriter()


@pytest.fixture
def sample_frame_data():
    """Sample frame data matching expected detector output format."""
    return {
        "cur_height": 100,
        "cur_width": 200,
        "channel_2_data": np.random.randint(0, 1000, (100, 200), dtype=np.uint32),
        "deflX": 1.5,
    }


@pytest.fixture
def mock_nxopen():
    """Mock nxopen context manager."""
    mock_file = MagicMock()
    mock_file.entry.instrument.analyzer = MagicMock()
    mock_file.__enter__ = MagicMock(return_value=mock_file)
    mock_file.__exit__ = MagicMock(return_value=False)

    with patch("a1soft.ioc.nxopen", return_value=mock_file) as mock:
        yield mock, mock_file


class TestDetectorWriterOpen:
    """Tests for open() method."""

    @pytest.mark.asyncio
    async def test_open_creates_file_path(self, writer, tmp_path):
        """Verifies open() sets up file path, queue, and starts writer task."""
        with patch("a1soft.ioc.nxopen"):
            name = await writer.open(str(tmp_path), "test")

        assert name == "test_0001.nxs"
        assert writer._full_file_path == tmp_path / "test_0001.nxs"
        assert writer._image_queue is not None
        assert writer._image_writer_task is not None
        assert writer._first_write is True

        # Cleanup
        await writer.close()

    @pytest.mark.asyncio
    async def test_open_increments_file_number(self, writer, tmp_path):
        """Verifies file numbering increments based on existing files."""
        # Create existing files
        (tmp_path / "test_0001.nxs").touch()
        (tmp_path / "test_0002.nxs").touch()

        with patch("a1soft.ioc.nxopen"):
            name = await writer.open(str(tmp_path), "test")

        assert name == "test_0003.nxs"
        await writer.close()

    @pytest.mark.asyncio
    async def test_open_creates_directory(self, writer, tmp_path):
        """Verifies open() creates directory if it doesn't exist."""
        new_dir = tmp_path / "subdir" / "nested"

        with patch("a1soft.ioc.nxopen"):
            await writer.open(str(new_dir), "test")

        assert new_dir.exists()
        await writer.close()


class TestDetectorWriterQueue:
    """Tests for queue management."""

    @pytest.mark.asyncio
    async def test_write_image_queues_data(self, writer, tmp_path, sample_frame_data):
        """Verifies write_image() adds frames to queue."""
        with patch("a1soft.ioc.nxopen"):
            await writer.open(str(tmp_path), "test")

        await writer.write_image(0, sample_frame_data)

        assert writer._image_queue.qsize() == 1
        await writer.close()

    @pytest.mark.asyncio
    async def test_write_image_rejects_empty_data(self, writer, tmp_path, caplog):
        """Verifies write_image() logs error for empty data."""
        with patch("a1soft.ioc.nxopen"):
            await writer.open(str(tmp_path), "test")

        await writer.write_image(0, {})

        assert writer._image_queue.qsize() == 0
        assert "Failed to get current frame" in caplog.text
        await writer.close()

    def test_write_field_queues_metadata(self, writer):
        """Verifies write_field() stores pending fields."""
        test_array = np.array([1.0, 2.0, 3.0])

        writer.write_field("entry/data", test_array, "test_field", "eV", attr="value")

        assert len(writer._pending_fields) == 1
        path, array, name, units, kwargs = writer._pending_fields[0]
        assert path == "entry/data"
        assert name == "test_field"
        assert units == "eV"
        np.testing.assert_array_equal(array, test_array)


class TestTryWriteFrames:
    """Tests for _try_write_frames() method."""

    def test_try_write_frames_success(
        self, writer, tmp_path, sample_frame_data, mock_nxopen
    ):
        """Verifies frames written to file on success."""
        mock_open, mock_file = mock_nxopen
        writer._full_file_path = tmp_path / "test.nxs"
        writer._first_write = True

        # Setup mock detector with no existing datasets
        mock_detector = MagicMock()
        mock_detector.__contains__ = MagicMock(return_value=False)
        mock_file.entry.instrument.analyzer = mock_detector

        frames = [(0, sample_frame_data)]
        result = writer._try_write_frames(frames)

        assert result is True
        assert writer._first_write is False
        mock_open.assert_called_once()

    def test_try_write_frames_file_locked(self, writer, tmp_path, sample_frame_data):
        """Verifies returns False when file is locked."""
        writer._full_file_path = tmp_path / "test.nxs"
        writer._first_write = True

        with patch("a1soft.ioc.nxopen", side_effect=PermissionError("File locked")):
            frames = [(0, sample_frame_data)]
            result = writer._try_write_frames(frames)

        assert result is False
        # first_write should remain True since write failed
        assert writer._first_write is True

    def test_try_write_frames_empty_list(self, writer):
        """Verifies empty frame list returns True without file access."""
        result = writer._try_write_frames([])
        assert result is True

    def test_try_write_frames_writes_pending_fields(
        self, writer, tmp_path, sample_frame_data, mock_nxopen
    ):
        """Verifies pending metadata fields are written."""
        mock_open, mock_file = mock_nxopen
        writer._full_file_path = tmp_path / "test.nxs"
        writer._first_write = True

        # Setup mock
        mock_detector = MagicMock()
        mock_detector.__contains__ = MagicMock(return_value=False)
        mock_file.entry.instrument.analyzer = mock_detector
        mock_file.__getitem__ = MagicMock(return_value=MagicMock())

        # Queue a metadata field
        writer.write_field("entry/data", np.array([1.0]), "test", "eV")

        frames = [(0, sample_frame_data)]
        writer._try_write_frames(frames)

        # Pending fields should be cleared after write
        assert len(writer._pending_fields) == 0


class TestImageWriterTask:
    """Tests for _image_writer() background task."""

    @pytest.mark.asyncio
    async def test_image_writer_retries_on_lock(
        self, writer, tmp_path, sample_frame_data
    ):
        """Verifies writer retries when _try_write_frames returns False."""
        writer._full_file_path = tmp_path / "test.nxs"
        writer._image_queue = asyncio.Queue(maxsize=100)
        writer._first_write = True

        call_count = 0

        def mock_try_write(frames):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return False  # Simulate lock
            return True

        with patch.object(writer, "_try_write_frames", side_effect=mock_try_write):
            # Queue a frame and shutdown signal
            await writer._image_queue.put((0, sample_frame_data))
            await writer._image_queue.put(None)

            # Run writer
            await writer._image_writer()

        assert call_count == 3  # Should have retried twice before success

    @pytest.mark.asyncio
    async def test_image_writer_batches_frames(
        self, writer, tmp_path, sample_frame_data
    ):
        """Verifies multiple queued frames written in single batch."""
        writer._full_file_path = tmp_path / "test.nxs"
        writer._image_queue = asyncio.Queue(maxsize=100)
        writer._first_write = True

        written_batches = []

        def mock_try_write(frames):
            written_batches.append(len(frames))
            return True

        with patch.object(writer, "_try_write_frames", side_effect=mock_try_write):
            # Queue multiple frames before writer processes
            await writer._image_queue.put((0, sample_frame_data))
            await writer._image_queue.put((1, sample_frame_data))
            await writer._image_queue.put((2, sample_frame_data))
            await writer._image_queue.put(None)

            await writer._image_writer()

        # All frames should be batched in single write (or close to it)
        total_frames = sum(written_batches)
        assert total_frames == 3


class TestDetectorWriterClose:
    """Tests for close() method."""

    @pytest.mark.asyncio
    async def test_close_flushes_pending(self, writer, tmp_path, sample_frame_data):
        """Verifies shutdown signal causes pending frames to flush."""
        written_frames = []

        def mock_try_write(frames):
            written_frames.extend(frames)
            return True

        with patch("a1soft.ioc.nxopen"):
            await writer.open(str(tmp_path), "test")

        with patch.object(writer, "_try_write_frames", side_effect=mock_try_write):
            await writer.write_image(0, sample_frame_data)
            await writer.write_image(1, sample_frame_data)

            # Small delay to let writer potentially start
            await asyncio.sleep(0.01)

            with patch("a1soft.ioc.nxopen"):
                await writer.close()

        # Both frames should have been written
        assert len(written_frames) == 2

    @pytest.mark.asyncio
    async def test_close_finalizes_with_links(
        self, writer, tmp_path, sample_frame_data, mock_nxopen
    ):
        """Verifies _link_results called on close."""
        mock_open, mock_file = mock_nxopen

        await writer.open(str(tmp_path), "test")

        # Create the file so close() tries to finalize it
        writer._full_file_path.touch()

        with patch.object(writer, "_link_results") as mock_link:
            await writer.close()
            mock_link.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_clears_state(self, writer, tmp_path):
        """Verifies close() resets all state."""
        with patch("a1soft.ioc.nxopen"):
            await writer.open(str(tmp_path), "test")
            await writer.close()

        assert writer._full_file_path is None
        assert writer._image_queue is None
        assert writer._image_writer_task is None
        assert writer._first_write is True
        assert len(writer._pending_fields) == 0

    @pytest.mark.asyncio
    async def test_close_retries_finalization_on_lock(self, writer, tmp_path):
        """Verifies close() retries file finalization if locked."""
        with patch("a1soft.ioc.nxopen"):
            await writer.open(str(tmp_path), "test")

        # Create the file
        writer._full_file_path.touch()

        call_count = 0
        original_retry_delay = writer.RETRY_DELAY
        writer.RETRY_DELAY = 0.01  # Speed up test

        def mock_nxopen_with_lock(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise PermissionError("File locked")
            mock = MagicMock()
            mock.__enter__ = MagicMock(return_value=mock)
            mock.__exit__ = MagicMock(return_value=False)
            return mock

        with patch("a1soft.ioc.nxopen", side_effect=mock_nxopen_with_lock):
            await writer.close()

        writer.RETRY_DELAY = original_retry_delay
        assert call_count == 3  # Retried twice before success


class TestLinkResults:
    """Tests for _link_results() method."""

    def test_link_results_skips_uninitialized(self, writer, caplog):
        """Verifies _link_results skips if file structure missing."""
        mock_file = MagicMock()
        mock_file.__contains__ = MagicMock(return_value=False)

        writer._link_results(mock_file)

        assert "File was never initialized" in caplog.text

    def test_link_results_creates_nxdata(self, writer):
        """Verifies _link_results creates NXdata with all required fields."""
        mock_file = MagicMock()
        mock_analyzer = MagicMock()

        # Setup contains checks
        mock_file.__contains__ = MagicMock(return_value=True)
        mock_file.entry.__contains__ = MagicMock(return_value=True)
        mock_file.entry.instrument.__contains__ = MagicMock(return_value=True)
        mock_analyzer.__contains__ = MagicMock(return_value=True)
        mock_file.entry.instrument.analyzer = mock_analyzer

        with (
            patch("a1soft.ioc.NXdata") as mock_nxdata,
            patch("a1soft.ioc.NXlink") as mock_nxlink,
        ):
            writer._link_results(mock_file)

            # Should create NXdata with links
            mock_nxdata.assert_called_once()
            assert mock_nxlink.call_count == 4  # data, deflector_x, angles, energies
