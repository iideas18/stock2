"""InStock monitoring package — status checks, alert state, webhooks."""
from __future__ import annotations

import os
from pathlib import Path

__all__ = ["get_monitoring_root"]


def get_monitoring_root() -> Path:
    return Path(os.environ.get("INSTOCK_MONITORING_ROOT", "data/monitoring"))
