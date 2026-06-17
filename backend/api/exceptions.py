from fastapi import HTTPException
from fastapi import status


class UnauthorizedError(HTTPException):
    def __init__(self, detail: str = "Unauthorized"):
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)


class ForbiddenError(HTTPException):
    def __init__(self, detail: str = "Forbidden"):
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail=detail)


class NotFoundError(HTTPException):
    def __init__(self, detail: str = "Not found"):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class TooManyRequestsError(HTTPException):
    def __init__(self, detail: str = "Too many requests"):
        super().__init__(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=detail,
        )


class ServiceUnavailableError(HTTPException):
    def __init__(self, detail: str = "Service unavailable"):
        super().__init__(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=detail,
        )


class GatewayTimeoutError(HTTPException):
    def __init__(self, detail: str = "Gateway timeout"):
        super().__init__(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=detail,
        )


class InternalKafkaError(HTTPException):
    def __init__(self, detail: str = "Internal Kafka error"):
        super().__init__(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        )
