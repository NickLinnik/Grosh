"""Translation layer: domain exceptions → HTTP responses.

Routes raise nothing HTTP-specific; services raise domain exceptions.
This module registers FastAPI exception handlers that map each domain
class to a status code and user-facing message.
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from grosh_api.services.auth_service import (
    InvalidAccessTokenError,
    InvalidCredentialsError,
    SessionExpiredError,
)
from grosh_api.services.user_service import (
    CannotDeleteSelfError,
    UserAlreadyExistsError,
    UserNotFoundError,
)


def _json_error(status_code: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail})


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(InvalidCredentialsError)
    async def _invalid_credentials(
        _request: Request, _exc: InvalidCredentialsError
    ) -> JSONResponse:
        return _json_error(401, "Invalid email or password.")

    @app.exception_handler(InvalidAccessTokenError)
    async def _invalid_access_token(
        _request: Request, _exc: InvalidAccessTokenError
    ) -> JSONResponse:
        return _json_error(401, "Not authenticated.")

    @app.exception_handler(SessionExpiredError)
    async def _session_expired(
        _request: Request, _exc: SessionExpiredError
    ) -> JSONResponse:
        return _json_error(401, "Session expired. Please log in again.")

    @app.exception_handler(UserAlreadyExistsError)
    async def _user_already_exists(
        _request: Request, _exc: UserAlreadyExistsError
    ) -> JSONResponse:
        return _json_error(409, "A user with this email already exists.")

    @app.exception_handler(UserNotFoundError)
    async def _user_not_found(
        _request: Request, _exc: UserNotFoundError
    ) -> JSONResponse:
        return _json_error(404, "User not found.")

    @app.exception_handler(CannotDeleteSelfError)
    async def _cannot_delete_self(
        _request: Request, _exc: CannotDeleteSelfError
    ) -> JSONResponse:
        return _json_error(400, "Cannot delete your own account.")
