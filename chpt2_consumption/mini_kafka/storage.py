
import os
import struct
from pathlib import Path

DATA_DIR = Path("data")
SEGMENT_MAX_SIZE = 1024 * 1024  # 1MB

def get_partition_dir(topic: str, partition: int) -> Path:
    """Returns the directory for a given topic partition, creating it if it doesn't exist."""
    partition_dir = DATA_DIR / topic / str(partition)
    partition_dir.mkdir(parents=True, exist_ok=True)
    return partition_dir

def get_cursor_file(topic: str, partition: int, group_id: str) -> Path:
    """Returns the path to the cursor file for a consumer group."""
    partition_dir = get_partition_dir(topic, partition)
    return partition_dir / f"{group_id}.cursor"

def get_latest_segment(partition_dir: Path) -> tuple[Path, Path, int]:
    """Gets the latest segment, index, and starting message ID for a partition."""
    log_files = sorted(partition_dir.glob("*.log"))
    if not log_files:
        start_id = 1
        log_file = partition_dir / f"{start_id:08d}.log"
        index_file = partition_dir / f"{start_id:08d}.index"
        return log_file, index_file, start_id

    latest_log = log_files[-1]
    start_id = int(latest_log.stem)
    index_file = latest_log.with_suffix(".index")
    return latest_log, index_file, start_id

def write_message(topic: str, partition: int, data: bytes) -> int:
    """Writes a message to the latest segment of a partition and returns the message ID."""
    partition_dir = get_partition_dir(topic, partition)
    log_file, index_file, start_id = get_latest_segment(partition_dir)

    # Create a new segment if the current one is full
    if log_file.exists() and log_file.stat().st_size > SEGMENT_MAX_SIZE:
        with index_file.open('rb') as idx:
            num_messages_in_segment = len(idx.read()) // 8
        start_id += num_messages_in_segment
        log_file = partition_dir / f"{start_id:08d}.log"
        index_file = partition_dir / f"{start_id:08d}.index"

    with log_file.open("ab") as f, index_file.open("ab") as idx:
        offset = f.tell()
        length = len(data)
        f.write(struct.pack(">I", length))
        f.write(data)

        with index_file.open('rb') as current_idx:
            num_messages = len(current_idx.read()) // 8
        message_id = start_id + num_messages

        idx.write(struct.pack(">II", message_id, offset))
        return message_id

def find_segment_for_message(partition_dir: Path, message_id: int) -> tuple[Path, Path] | None:
    """Finds the segment containing a given message ID."""
    log_files = sorted(partition_dir.glob("*.log"))
    for log_file in reversed(log_files):
        start_id = int(log_file.stem)
        if message_id >= start_id:
            index_file = log_file.with_suffix(".index")
            with index_file.open('rb') as idx:
                content = idx.read()
                num_messages = len(content) // 8
                if message_id < start_id + num_messages:
                    return log_file, index_file
    return None

def read_next_message(topic: str, partition: int, group_id: str) -> tuple[int, bytes] | None:
    """Reads the next message for a consumer group from a specific partition."""
    partition_dir = get_partition_dir(topic, partition)
    cursor_file = get_cursor_file(topic, partition, group_id)

    last_read_id = 0
    if cursor_file.exists():
        last_read_id = int(cursor_file.read_text())
    
    next_message_id = last_read_id + 1

    segment_info = find_segment_for_message(partition_dir, next_message_id)
    if not segment_info:
        return None

    log_file, index_file = segment_info
    start_id = int(log_file.stem)

    with index_file.open('rb') as idx:
        entry_position = (next_message_id - start_id) * 8
        idx.seek(entry_position)
        entry = idx.read(8)
        if not entry or len(entry) < 8:
            return None
        
        msg_id, offset = struct.unpack(">II", entry)

    with log_file.open('rb') as f:
        f.seek(offset)
        length_data = f.read(4)
        if not length_data:
            return None
        length = struct.unpack(">I", length_data)[0]
        return msg_id, f.read(length)

def acknowledge_message(topic: str, partition: int, group_id: str, message_id: int):
    """Updates the consumer group's cursor to the given message ID for a partition."""
    cursor_file = get_cursor_file(topic, partition, group_id)
    cursor_file.write_text(str(message_id))
