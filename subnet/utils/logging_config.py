from __future__ import annotations

import atexit
import copy
import logging
from logging.handlers import QueueHandler, QueueListener
import os
import queue
import sys
from typing import TextIO

DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

RESET_COLOR = "\033[0m"
LEVEL_COLORS = {
    logging.DEBUG: "\033[2;36m",
    logging.INFO: "\033[34m",
    logging.WARNING: "\033[38;5;208m",
    logging.ERROR: "\033[31m",
    logging.CRITICAL: "\033[1;35m",
}

_queue_listener: QueueListener | None = None


class ColoredFormatter(logging.Formatter):
    def __init__(self, fmt: str = DEFAULT_LOG_FORMAT, *, use_color: bool = True) -> None:
        super().__init__(fmt=fmt)
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        if not self.use_color:
            return super().format(record)

        color = LEVEL_COLORS.get(record.levelno)
        if color is None:
            return super().format(record)

        colored_record = copy.copy(record)
        colored_record.levelname = f"{color}{record.levelname}{RESET_COLOR}"
        return super().format(colored_record)


def configure_logging(
    level: int = logging.INFO,
    fmt: str = DEFAULT_LOG_FORMAT,
    *,
    stream: TextIO | None = None,
    force: bool = False,
    use_color: bool | None = None,
    use_queue: bool = False,
    queue_maxsize: int = 0,
) -> None:
    stream = stream or sys.stderr
    if use_color is None:
        use_color = _should_use_color(stream)

    handler = logging.StreamHandler(stream)
    handler.setFormatter(ColoredFormatter(fmt, use_color=use_color))

    root_logger = logging.getLogger()
    if use_queue:
        _configure_queue_logging(
            root_logger=root_logger,
            handler=handler,
            level=level,
            force=force,
            queue_maxsize=queue_maxsize,
        )
        return

    if force or not root_logger.handlers:
        stop_logging_queue_listener()
        logging.basicConfig(level=level, handlers=[handler], force=force)
        return

    stop_logging_queue_listener()
    root_logger.setLevel(level)
    updated_console_handler = False
    for existing_handler in root_logger.handlers:
        if not _is_console_handler(existing_handler):
            continue

        existing_stream = getattr(existing_handler, "stream", stream)
        existing_use_color = use_color if use_color is not None else _should_use_color(existing_stream)
        existing_handler.setFormatter(ColoredFormatter(fmt, use_color=existing_use_color))
        updated_console_handler = True

    if not updated_console_handler:
        root_logger.addHandler(handler)


def _should_use_color(stream: TextIO) -> bool:
    if os.getenv("FORCE_COLOR"):
        return True
    if os.getenv("NO_COLOR"):
        return False
    isatty = getattr(stream, "isatty", None)
    return bool(isatty and isatty())


def _is_console_handler(handler: logging.Handler) -> bool:
    return isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler)


def _configure_queue_logging(
    *,
    root_logger: logging.Logger,
    handler: logging.Handler,
    level: int,
    force: bool,
    queue_maxsize: int,
) -> None:
    stop_logging_queue_listener()

    log_queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=max(0, queue_maxsize))
    queue_handler = QueueHandler(log_queue)
    listener = QueueListener(log_queue, handler, respect_handler_level=True)
    listener.start()
    _set_queue_listener(listener)

    root_logger.setLevel(level)
    if force:
        for existing_handler in tuple(root_logger.handlers):
            root_logger.removeHandler(existing_handler)
            existing_handler.close()
        root_logger.addHandler(queue_handler)
        return

    for existing_handler in tuple(root_logger.handlers):
        if isinstance(existing_handler, QueueHandler) or _is_console_handler(existing_handler):
            root_logger.removeHandler(existing_handler)
            existing_handler.close()

    root_logger.addHandler(queue_handler)


def _set_queue_listener(listener: QueueListener | None) -> None:
    global _queue_listener
    _queue_listener = listener


def stop_logging_queue_listener() -> None:
    global _queue_listener
    listener = _queue_listener
    _queue_listener = None
    if listener is not None:
        listener.stop()


atexit.register(stop_logging_queue_listener)
