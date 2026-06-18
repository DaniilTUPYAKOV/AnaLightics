import json
import logging

from backend.api.logging_config import render_json_log


def test_render_json_log_renders_event_and_extra_fields():
    """Structlog renderer should output event dictionaries as JSON objects."""
    payload = json.loads(
        render_json_log(
            logging.getLogger("backend.api.test"),
            "info",
            {
                "event": "kafka_send_succeeded",
                "request_id": "request-123",
                "project_id": "project-123",
                "duration_ms": 12.5,
            },
        )
    )

    assert payload["event"] == "kafka_send_succeeded"
    assert payload["level"] == "info"
    assert payload["logger"] == "backend.api.test"
    assert payload["request_id"] == "request-123"
    assert payload["project_id"] == "project-123"
    assert payload["duration_ms"] == 12.5
