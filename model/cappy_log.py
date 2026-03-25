"""
Echo Cappy diagnostics to **stdout** and the logger so bare runtimes (Domino, WSGI) show URLs
without tuning ``logging.ini`` levels or handlers.
"""

from __future__ import annotations

import logging
from typing import Any


def cappy_echo_info(logger: logging.Logger, fmt: str, *args: Any) -> None:
    text = fmt % args if args else fmt
    logger.info(text)
    print(text, flush=True)


def cappy_echo_warning(logger: logging.Logger, fmt: str, *args: Any) -> None:
    text = fmt % args if args else fmt
    logger.warning(text)
    print(text, flush=True)


def cappy_echo_error(logger: logging.Logger, fmt: str, *args: Any) -> None:
    text = fmt % args if args else fmt
    logger.error(text)
    print(text, flush=True)
