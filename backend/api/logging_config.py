import logging
import sys
from typing import Any

import structlog


def add_logger_name(
    logger: logging.Logger,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    event_dict["logger"] = logger.name
    return event_dict


def render_json_log(
    logger: logging.Logger,
    method_name: str,
    event_dict: dict[str, Any],
) -> str:
    return structlog.processors.JSONRenderer(ensure_ascii=False)(
        logger,
        method_name,
        event_dict,
    )


def configure_structured_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
        force=True,
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            add_logger_name,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            render_json_log,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
