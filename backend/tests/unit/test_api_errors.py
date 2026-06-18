from fastapi import status

from backend.api.exceptions import (
    ApiError,
    ForbiddenError,
    ServiceUnavailableError,
    build_error_response,
)


def test_build_error_response_uses_api_error_contract():
    """API errors should be rendered with a stable public contract."""
    error = ApiError(
        status_code=status.HTTP_418_IM_A_TEAPOT,
        code="TEST_ERROR",
        message="Test error",
        details={"field": "value"},
    )

    assert build_error_response(error, "request-123") == {
        "error": {
            "code": "TEST_ERROR",
            "message": "Test error",
            "request_id": "request-123",
            "details": {"field": "value"},
        }
    }


def test_forbidden_error_has_stable_code_and_message():
    """Forbidden auth failures should not expose internal implementation details."""
    error = ForbiddenError()

    assert error.status_code == status.HTTP_403_FORBIDDEN
    assert error.code == "API_KEY_INVALID"
    assert error.message == "Invalid or inactive API key"


def test_service_unavailable_error_can_override_public_message():
    """Service unavailable errors should keep their code while allowing safe messages."""
    error = ServiceUnavailableError("Rate limiter unavailable")

    assert error.status_code == status.HTTP_503_SERVICE_UNAVAILABLE
    assert error.code == "SERVICE_UNAVAILABLE"
    assert error.message == "Rate limiter unavailable"
