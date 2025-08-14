import cProfile
import pstats
import io
import functools
import time
import os
import gzip
import bz2
import lzma
import threading
from queue import Queue
from typing import Union, IO, Optional, Generator, Iterable


def debug_method(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        if getattr(self, 'debug', False):
            print(f"[DEBUG] Call: {method.__name__} args={args} kwargs={kwargs}")
            start = time.time()
            result = method(self, *args, **kwargs)
            elapsed = (time.time() - start) * 1000
            print(f"[DEBUG] End: {method.__name__} ({elapsed:.2f} ms)")
            return result
        else:
            return method(self, *args, **kwargs)
    return wrapper


class BufferedLineReader:
    """
    High-performance line reader for large text files (txt/gz/bz2/xz/lzma).
    - Safely handles chunk boundaries for any encoding (UTF-8/Unicode, etc.).
    - Uses background thread to buffer chunks for normal files; supports compressed files.
    - Iterator interface: for line in BufferedLineReader(...).
    """

    COMPRESSION_NONE = "none"
    COMPRESSION_GZIP = "gz"
    COMPRESSION_BZIP2 = "bz2"
    COMPRESSION_XZ = "xz"
    COMPRESSION_LZMA = "lzma"

    DEFAULT_CHUNK_SIZES = {
        COMPRESSION_NONE: 128 * 1024 * 1024,    # 128MB for normal files
        COMPRESSION_GZIP: 32 * 1024 * 1024,
        COMPRESSION_BZIP2: 16 * 1024 * 1024,
        COMPRESSION_XZ: 32 * 1024 * 1024,
        COMPRESSION_LZMA: 32 * 1024 * 1024,
    }

    def __init__(
        self,
        file_or_path: Union[str, IO[bytes]],
        chunk_size: Optional[int] = None,
        encoding: str = 'utf-8',
        errors: str = 'replace',
        debug: bool = False
    ) -> None:
        self.user_chunk_size = chunk_size
        self.encoding = encoding
        self.errors = errors
        self.debug = debug
        self.closed = False

        self._line_count = 0
        self._bytes_processed = 0
        self._start_time = None
        self._total_time = 0.0
        self._mode = None

        self._initialize_file_properties(file_or_path)
        self._compression_type = self._detect_compression_type()

    def _initialize_file_properties(self, file_or_path: Union[str, IO[bytes]]) -> None:
        if isinstance(file_or_path, str):
            self.file_path = file_or_path
            self.file_obj: Optional[IO[bytes]] = None
        else:
            self.file_obj = file_or_path
            self.file_path = getattr(file_or_path, 'name', '<unknown>')
            if hasattr(self.file_obj, 'mode') and 'b' not in getattr(self.file_obj, 'mode', ''):
                raise ValueError("file_obj must be opened in binary mode ('rb')")

    def _detect_compression_type(self) -> str:
        compression_map = {
            '.gz': self.COMPRESSION_GZIP,
            '.bz2': self.COMPRESSION_BZIP2,
            '.xz': self.COMPRESSION_XZ,
            '.lzma': self.COMPRESSION_LZMA,
        }
        for ext, comp_type in compression_map.items():
            if isinstance(self.file_path, str) and self.file_path.endswith(ext):
                return comp_type
        return self.COMPRESSION_NONE

    def _get_file_size(self) -> int:
        try:
            if self.file_obj and hasattr(self.file_obj, 'fileno'):
                return os.fstat(self.file_obj.fileno()).st_size
            elif isinstance(self.file_path, str):
                return os.path.getsize(self.file_path)
        except Exception:
            pass
        return 0

    def _get_chunk_size(self) -> int:
        if self.user_chunk_size:
            return self.user_chunk_size
        return self.DEFAULT_CHUNK_SIZES.get(self._compression_type, 16 * 1024 * 1024)

    def _track_line(self, line: str) -> None:
        self._line_count += 1

    @debug_method
    def __iter__(self) -> Generator[str, None, None]:
        self._start_time = time.time()
        _ = self._get_file_size()
        chunk_size = self._get_chunk_size()

        if self._compression_type != self.COMPRESSION_NONE:
            mode = "compressed"
            chunk_iterator: Iterable[bytes] = self._compressed_chunk_iterator(chunk_size)
        else:
            mode = "buffered"
            chunk_iterator = self._buffered_chunk_iterator(chunk_size)

        self._mode = mode

        if self.debug:
            pr = cProfile.Profile()
            pr.enable()
            try:
                yield from self._read_lines_from_chunks(chunk_iterator)
            finally:
                pr.disable()
                s = io.StringIO()
                sortby = pstats.SortKey.CUMULATIVE
                ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
                ps.print_stats(20)
                print("[PROFILE RESULT]\n" + s.getvalue())
                self._total_time = time.time() - self._start_time
                self.close()
        else:
            try:
                yield from self._read_lines_from_chunks(chunk_iterator)
            finally:
                self._total_time = time.time() - self._start_time
                self.close()

    def _compressed_chunk_iterator(self, chunk_size: int) -> Generator[bytes, None, None]:
        opener = {
            self.COMPRESSION_GZIP: gzip.open,
            self.COMPRESSION_BZIP2: bz2.open,
            self.COMPRESSION_XZ: lzma.open,
            self.COMPRESSION_LZMA: lzma.open,
        }.get(self._compression_type)
        if opener is None:
            raise ValueError(f"Unsupported compression type: {self._compression_type}")
        if not isinstance(self.file_path, str):
            raise ValueError("Compressed reading requires a file path string.")
        with opener(self.file_path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def _buffered_chunk_iterator(self, chunk_size: int) -> Generator[bytes, None, None]:
        self._ensure_file_open()
        queue_size = 8
        chunk_queue: "Queue[Optional[bytes]]" = Queue(maxsize=queue_size)

        def chunk_reader():
            try:
                while True:
                    chunk = self.file_obj.read(chunk_size)  # type: ignore[union-attr]
                    if not chunk:
                        chunk_queue.put(None)
                        break
                    chunk_queue.put(chunk)
            except Exception as e:
                chunk_queue.put(e)  # type: ignore[arg-type]

        reader_thread = threading.Thread(target=chunk_reader, daemon=True)
        reader_thread.start()

        while True:
            chunk = chunk_queue.get()
            if isinstance(chunk, Exception):
                raise chunk
            if chunk is None:
                break
            yield chunk
        reader_thread.join(timeout=0.01)

    def _read_lines_from_chunks(self, chunk_iterator: Iterable[bytes]) -> Generator[str, None, None]:
        text_buffer = ""
        for chunk in chunk_iterator:
            if not chunk:
                continue
            self._bytes_processed += len(chunk)
            text = chunk.decode(self.encoding, errors=self.errors)
            text_buffer += text
            if '\n' in text_buffer:
                parts = text_buffer.split('\n')
                for line in parts[:-1]:
                    self._track_line(line)
                    yield line
                text_buffer = parts[-1]
        if text_buffer:
            self._track_line(text_buffer)
            yield text_buffer

    def _ensure_file_open(self) -> None:
        if getattr(self, 'file_obj', None) is None:
            if not isinstance(self.file_path, str):
                raise ValueError("file_or_path must be a path string when no file object is provided.")
            self.file_obj = open(self.file_path, 'rb')

    def close(self) -> None:
        if hasattr(self, 'file_obj') and self.file_obj and not getattr(self.file_obj, 'closed', True):
            try:
                self.file_obj.close()
            except Exception:
                pass
        self.closed = True

    def get_stats(self) -> dict:
        mb = self._bytes_processed / (1024 * 1024)
        lines_per_sec = int(self._line_count / self._total_time) if self._total_time > 0 else 0
        mb_per_sec = mb / self._total_time if self._total_time > 0 else 0.0
        return {
            "file": os.path.basename(self.file_path) if isinstance(self.file_path, str) else "<stream>",
            "mode": self._mode,
            "lines": self._line_count,
            "bytes": self._bytes_processed,
            "time": round(self._total_time, 2),
            "lines/s": lines_per_sec,
            "MB/s": round(mb_per_sec, 2)
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
