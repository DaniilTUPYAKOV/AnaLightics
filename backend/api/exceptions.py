from typing import Any

from fastapi import status


class ApiError(Exception):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    code = "INTERNAL_ERROR"
    message = "Internal server error"

    def __init__(
        self,
        message: str | None = None,
        *,
        status_code: int | None = None,
        code: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        self.status_code = status_code or self.status_code
        self.code = code or self.code
        self.message = message or self.message
        self.details = details or {}
        super().__init__(self.message)


class UnauthorizedError(ApiError):
    status_code = status.HTTP_401_UNAUTHORIZED
    code = "AUTH_REQUIRED"
    message = "API key is required"


class ForbiddenError(ApiError):
    status_code = status.HTTP_403_FORBIDDEN
    code = "API_KEY_INVALID"
    message = "Invalid or inactive API key"


class NotFoundError(ApiError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "NOT_FOUND"
    message = "Resource not found"


class TooManyRequestsError(ApiError):
    status_code = status.HTTP_429_TOO_MANY_REQUESTS
    code = "RATE_LIMIT_EXCEEDED"
    message = "Too many events"


class ServiceUnavailableError(ApiError):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    code = "SERVICE_UNAVAILABLE"
    message = "Service temporarily unavailable"


class KafkaUnavailableError(ServiceUnavailableError):
    code = "KAFKA_UNAVAILABLE"
    message = "Event ingestion is temporarily unavailable"


class GatewayTimeoutError(ApiError):
    status_code = status.HTTP_504_GATEWAY_TIMEOUT
    code = "UPSTREAM_TIMEOUT"
    message = "Upstream request timed out"


class KafkaTimeoutError(GatewayTimeoutError):
    code = "KAFKA_TIMEOUT"
    message = "Event ingestion timed out"


class InternalKafkaError(ApiError):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    code = "KAFKA_INTERNAL_ERROR"
    message = "Internal error while ingesting event"


def build_error_response(error: ApiError, request_id: str | None) -> dict[str, Any]:
    return {
        "error": {
            "code": error.code,
            "message": error.message,
            "request_id": request_id,
            "details": error.details,
        }
    }
