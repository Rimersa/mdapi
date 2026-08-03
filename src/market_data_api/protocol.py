from __future__ import annotations

import json
import struct

BUNDLE_MAGIC = b"MDPB0001"
HEADER_SIZE = 4
MAX_PART_HEADER = 1024 * 1024


def encode_part_header(payload: dict) -> bytes:
    raw = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(raw) > MAX_PART_HEADER:
        raise ValueError("bundle part header 过大")
    return struct.pack("!I", len(raw)) + raw


def encode_bundle_end() -> bytes:
    return struct.pack("!I", 0)


def decode_part_header(raw: bytes) -> dict:
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("bundle part header 必须是对象")
    return value
