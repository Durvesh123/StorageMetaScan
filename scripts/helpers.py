# helpers.py
# Utility helpers for partial & full hashing and safe file stat.

import os
import hashlib
import logging
from typing import Optional

log = logging.getLogger("storage_monitor.helpers")

def safe_stat(path: str):
    """Return os.stat result or None on error."""
    try:
        return os.stat(path, follow_symlinks=False)
    except Exception as e:
        log.debug("stat failed for %s: %s", path, e)
        return None

def partial_hash(path: str, sample_bytes: int = 4096) -> Optional[str]:
    """
    Compute md5 of first sample_bytes + last sample_bytes.
    Returns hex digest or None on failure.
    """
    try:
        size = os.path.getsize(path)
        if size == 0:
            return hashlib.md5(b"").hexdigest()
        with open(path, "rb") as f:
            head = f.read(sample_bytes)
            if size <= sample_bytes * 2:
                # file small enough, hash full content
                f.seek(0)
                full = f.read()
                return hashlib.md5(full).hexdigest()
            # read last sample_bytes
            f.seek(max(size - sample_bytes, 0))
            tail = f.read(sample_bytes)
        h = hashlib.md5()
        h.update(head)
        h.update(tail)
        return h.hexdigest()
    except Exception as e:
        log.debug("partial_hash failed for %s: %s", path, e)
        return None

def full_hash(path: str, chunk_size: int = 4 * 1024 * 1024) -> Optional[str]:
    """
    Compute sha256 of full file content in streaming fashion.
    Returns hex digest or None on failure.
    """
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        log.debug("full_hash failed for %s: %s", path, e)
        return None

