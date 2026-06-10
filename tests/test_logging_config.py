import io
import logging
from logging.handlers import QueueHandler

from subnet.utils.logging_config import (
    ColoredFormatter,
    _should_use_color,
    configure_logging,
    stop_logging_queue_listener,
)


def test_colored_formatter_colors_info_and_warning_level_names() -> None:
    formatter = ColoredFormatter("%(levelname)s:%(message)s", use_color=True)

    info_record = logging.LogRecord("test", logging.INFO, __file__, 1, "hello", (), None)
    warning_record = logging.LogRecord("test", logging.WARNING, __file__, 1, "careful", (), None)

    assert formatter.format(info_record) == "\033[34mINFO\033[0m:hello"
    assert formatter.format(warning_record) == "\033[38;5;208mWARNING\033[0m:careful"
    assert info_record.levelname == "INFO"
    assert warning_record.levelname == "WARNING"


def test_colored_formatter_can_disable_colors() -> None:
    formatter = ColoredFormatter("%(levelname)s:%(message)s", use_color=False)
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "plain", (), None)

    assert formatter.format(record) == "INFO:plain"


def test_configure_logging_updates_existing_console_handler() -> None:
    root_logger = logging.getLogger()
    previous_handlers = list(root_logger.handlers)
    previous_level = root_logger.level
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))

    for previous_handler in previous_handlers:
        root_logger.removeHandler(previous_handler)
    root_logger.addHandler(handler)

    try:
        configure_logging(
            level=logging.INFO,
            fmt="%(levelname)s:%(message)s",
            stream=stream,
            use_color=True,
        )
        logging.getLogger("test.configure_logging").warning("careful")

        assert root_logger.handlers == [handler]
        assert stream.getvalue().strip() == "\033[38;5;208mWARNING\033[0m:careful"
    finally:
        root_logger.removeHandler(handler)
        for previous_handler in previous_handlers:
            root_logger.addHandler(previous_handler)
        root_logger.setLevel(previous_level)


def test_force_color_env_overrides_no_color(monkeypatch) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    monkeypatch.setenv("FORCE_COLOR", "1")

    assert _should_use_color(io.StringIO())


def test_configure_logging_can_use_queue_handler() -> None:
    root_logger = logging.getLogger()
    previous_handlers = list(root_logger.handlers)
    previous_level = root_logger.level
    stream = io.StringIO()

    for previous_handler in previous_handlers:
        root_logger.removeHandler(previous_handler)

    try:
        configure_logging(
            level=logging.INFO,
            fmt="%(levelname)s:%(message)s",
            stream=stream,
            force=True,
            use_color=True,
            use_queue=True,
        )
        logging.getLogger("test.configure_logging.queue").warning("queued")
        stop_logging_queue_listener()

        assert len(root_logger.handlers) == 1
        assert isinstance(root_logger.handlers[0], QueueHandler)
        assert stream.getvalue().strip() == "\033[38;5;208mWARNING\033[0m:queued"
    finally:
        stop_logging_queue_listener()
        for handler in tuple(root_logger.handlers):
            root_logger.removeHandler(handler)
            handler.close()
        for previous_handler in previous_handlers:
            root_logger.addHandler(previous_handler)
        root_logger.setLevel(previous_level)
