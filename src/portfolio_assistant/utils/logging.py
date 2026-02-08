"""Central logging configuration."""

from __future__ import annotations

import logging


DEFAULT_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format=DEFAULT_FORMAT)
