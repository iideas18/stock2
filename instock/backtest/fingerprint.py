"""compute_fingerprint: SHA-256 of all input artifact contents + config.

Order-invariant (files sorted by SHA before hashing).

Input file SHA uses file bytes (pyarrow-compatible Parquet files are
byte-stable for identical data + pyarrow version).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable


def _file_sha256(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(path)
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def compute_fingerprint(
    input_files: Iterable[Path],
    config_dict: dict,
) -> str:
    file_shas = sorted(_file_sha256(Path(p)) for p in input_files)
    cfg_blob = json.dumps(config_dict, sort_keys=True, default=str)
    cfg_sha = hashlib.sha256(cfg_blob.encode("utf-8")).hexdigest()
    combined = hashlib.sha256()
    for s in file_shas:
        combined.update(s.encode("ascii"))
    combined.update(cfg_sha.encode("ascii"))
    return combined.hexdigest()
