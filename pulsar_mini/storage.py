
import os
import struct
from pathlib import Path

DATA_DIR = Path("data")
SEGMENT_MAX_SIZE = 1024 * 1024  # 1MB

def get_topic_dir(topic: str) -> Path:
    """Returns the directory for a given topic, creating it if it doesn't exist."""
    topic_dir = DATA_DIR / topic
    topic_dir.mkdir(parents=True, exist_ok=True)
    return topic_dir

def get_cursor_file(topic: str, consumer_id: str) -> Path:
    """Returns the path to the cursor file for a consumer."""
    topic_dir = get_topic_dir(topic)
    return topic_dir / f"{consumer_id}.cursor"

def get_latest_segment(topic_dir: Path) -> tuple[Path, Path, int]:
    """Gets the latest segment, index, and starting message ID."""
    log_files = sorted(topic_dir.glob("*.log"))
    if not log_files:
        start_id = 1
        log_file = topic_dir / f"{start_id:08d}.log"
        index_file = topic_dir / f"{start_id:08d}.index"
        return log_file, index_file, start_id

    latest_log = log_files[-1]
    start_id = int(latest_log.stem)
    index_file = latest_log.with_suffix(".index")
    return latest_log, index_file, start_id

def write_message(topic: str, data: bytes) -> int:
    """Writes a message to the latest segment and returns the message ID."""
    topic_dir = get_topic_dir(topic)
    log_file, index_file, start_id = get_latest_segment(topic_dir)

    if log_file.exists() and log_file.stat().st_size > SEGMENT_MAX_SIZE:
        start_id += len(list(index_file.open('rb'))) // 8
        log_file = topic_dir / f"{start_id:08d}.log"
        index_file = topic_dir / f"{start_id:08d}.index"

    with log_file.open("ab") as f, index_file.open("ab") as idx:
        offset = f.tell()
        length = len(data)
        f.write(struct.pack(">I", length))
        f.write(data)

        message_id = start_id + (offset // 8 if offset > 0 else 0)
        idx.write(struct.pack(">II", message_id, offset))
        return message_id

def find_segment_for_message(topic_dir: Path, message_id: int) -> tuple[Path, Path] | None:
    """Finds the segment containing a given message ID."""
    log_files = sorted(topic_dir.glob("*.log"))
    for log_file in reversed(log_files):
        start_id = int(log_file.stem)
        if message_id >= start_id:
            return log_file, log_file.with_suffix(".index")
    return None

def read_next_message(topic: str, consumer_id: str) -> tuple[int, bytes] | None:
    """Reads the next message for a consumer."""
    topic_dir = get_topic_dir(topic)
    cursor_file = get_cursor_file(topic, consumer_id)

    last_read_id = 0
    if cursor_file.exists():
        last_read_id = int(cursor_file.read_text())
    
    next_message_id = last_read_id + 1

    segment_info = find_segment_for_message(topic_dir, next_message_id)
    if not segment_info:
        return None

    log_file, index_file = segment_info
    
    with index_file.open('rb') as idx, log_file.open('rb') as f:
        while True:
            entry = idx.read(8)
            if not entry:
                break
            msg_id, offset = struct.unpack(">II", entry)
            if msg_id == next_message_id:
                f.seek(offset)
                length_data = f.read(4)
                if not length_data: break
                length = struct.unpack(">I", length_data)[0]
                return msg_id, f.read(length)

    # Check next segment if available
    log_files = sorted(topic_dir.glob("*.log"))
    current_segment_index = log_files.index(log_file)
    if current_segment_index + 1 < len(log_files):
        next_log_file = log_files[current_segment_index + 1]
        next_index_file = next_log_file.with_suffix(".index")
        with next_index_file.open('rb') as idx, next_log_file.open('rb') as f:
            entry = idx.read(8)
            if not entry: return None
            msg_id, offset = struct.unpack(">II", entry)
            f.seek(offset)
            length_data = f.read(4)
            if not length_data: return None
            length = struct.unpack(">I", length_data)[0]
            return msg_id, f.read(length)

    return None


def acknowledge_message(topic: str, consumer_id: str, message_id: int):
    """Updates the consumer's cursor to the given message ID."""
    cursor_file = get_cursor_file(topic, consumer_id)
    cursor_file.write_text(str(message_id))
