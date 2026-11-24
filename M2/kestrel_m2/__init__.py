"""Kestrel Milestone 2: contracts, warehouse, analytical storage, Kafka streaming, Spark.

M2 builds on the Milestone 1 cleaning package (``kestrel_m1``), which lives in ``../M1``.
When this package is imported from a checkout, ``../M1`` is put on ``sys.path`` so that
``import kestrel_m1`` works without installing anything; in Docker the image copies M1
next to M2 with the same layout.
"""

from __future__ import annotations

import sys
from pathlib import Path

_M1 = Path(__file__).resolve().parents[2] / "M1"
if _M1.is_dir() and str(_M1) not in sys.path:
    sys.path.insert(0, str(_M1))
