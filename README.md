
# BufferedLineReader

A **high-performance** line reader for large text files, with support for compressed formats (`.gz`, `.bz2`, `.xz`, `.lzma`). Designed to read quickly in **chunks** and safely split lines for UTF‑8/Unicode.

## Highlights
- Reads in **large chunks** (default 128MB for normal files) to reduce I/O calls.
- **Background thread** queues data for normal files, overlapping I/O with line splitting.
- Supports multiple compression formats: `gzip`, `bzip2`, `xz/lzma`.
- Safely handles line boundaries across chunks.
- **Debug mode** with `cProfile` to measure timing & function call stats.
- Iterator-style API: `for line in BufferedLineReader(...):`.

## Installation
No third-party dependencies. Requires Python 3.8+.
Copy `buffered_line_reader.py` into your project.

## Quick Usage

```python
from buffered_line_reader import BufferedLineReader

# Read a normal file
with BufferedLineReader("data.txt", debug=False) as reader:
    for i, line in enumerate(reader):
        pass
    print(reader.get_stats())  # Performance stats

# Read a gzip file
with BufferedLineReader("logs.gz", encoding="utf-8") as reader:
    for line in reader:
        pass
```

### Key Options
- `chunk_size: int | None` — chunk size in bytes. Defaults:
  - Normal files: 128MB
  - `.gz`/`.xz`/`.lzma`: 32MB
  - `.bz2`: 16MB
- `encoding: str` (default `utf-8`), `errors: str` (default `replace`).
- `debug: bool` — when `True`, prints function call logs and runs `cProfile` (top 20 functions).

## Full Example

```python
from buffered_line_reader import BufferedLineReader

reader = BufferedLineReader("huge.txt", chunk_size=64*1024*1024, debug=True)
for line in reader:
    pass

stats = reader.get_stats()
print("Stats:", stats)
```

## Design & Performance Notes
- **Single split per chunk** using `str.split('\n')` (C-optimized) and retaining incomplete last line.
- **Single decode per chunk** using chosen `encoding`.
- For compressed files, speed depends heavily on compression algorithm and CPU.
- For normal files, background-thread buffering can improve throughput on fast disks/FS.
- Increasing `chunk_size` may reduce overhead but use more temporary RAM.

## Limitations
- Splits on `\n` only. If file has `\r\n`, Python split still works but may leave `\r`; you can `rstrip('\r')` if needed.
- Sequential reading only — no random seek by position.
- If passing a `file_obj`, it must be **binary mode** (`'rb'`).

## API

| Method | Description |
|---|---|
| `__iter__()` | Returns iterator of lines (without newline char). |
| `get_stats()` | Returns dict with: file, mode (`buffered`/`compressed`), lines, bytes, time, lines/s, MB/s. |
| `close()` | Closes the file if open. |
| Context manager | Supports `with ... as ...:` auto-closing. |

## Debug & Profiling

```python
with BufferedLineReader("data.txt", debug=True) as r:
    for _ in r:
        pass
# Console shows [DEBUG] ... and [PROFILE RESULT] (top 20 functions by CUMULATIVE time)
```

## License
You may choose an appropriate license (MIT/Apache-2.0, etc.). Default: no license included — please add one if needed.
