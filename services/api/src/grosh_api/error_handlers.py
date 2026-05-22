"""Error handler registration for the main API service.

Domain exceptions that need HTTP mapping are registered here alongside
the three cross-cutting handlers (HTTPException, RequestValidationError,
catch-all) supplied by grosh_shared.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from grosh_shared.http.errors import (
    ErrorCode,
    ProblemDetail,
    _default_title,
    register_error_handlers,
)

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

logger = logging.getLogger(__name__)


def _problem_response(
    status_code: int, code: ErrorCode, detail: str, instance: str = ""
) -> JSONResponse:
    problem = ProblemDetail(
        type=f"https://docs.grosh.app/errors/{code.value.lower().replace('_', '-')}",
        title=_default_title(code),
        status=status_code,
        code=code,
        detail=detail,
        instance=instance,
    )
    return JSONResponse(
        status_code=status_code, content=problem.model_dump(mode="json")
    )


def register_all_error_handlers(app: FastAPI) -> None:
    register_error_handlers(app)

    @app.exception_handler(InvalidCredentialsError)
    async def _invalid_credentials(
        request: Request, _exc: InvalidCredentialsError
    ) -> JSONResponse:
        return _problem_response(
            401,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "Invalid email or password.",
            request.url.path,
        )

    @app.exception_handler(InvalidAccessTokenError)
    async def _invalid_access_token(
        request: Request, _exc: InvalidAccessTokenError
    ) -> JSONResponse:
        return _problem_response(
            401,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "Not authenticated.",
            request.url.path,
        )

    @app.exception_handler(SessionExpiredError)
    async def _session_expired(
        request: Request, _exc: SessionExpiredError
    ) -> JSONResponse:
        return _problem_response(
            401,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "Session expired. Please log in again.",
            request.url.path,
        )

    @app.exception_handler(UserAlreadyExistsError)
    async def _user_already_exists(
        request: Request, _exc: UserAlreadyExistsError
    ) -> JSONResponse:
        return _problem_response(
            409,
            ErrorCode.VALIDATION_ERROR,
            "A user with this email already exists.",
            request.url.path,
        )

    @app.exception_handler(UserNotFoundError)
    async def _user_not_found(
        request: Request, _exc: UserNotFoundError
    ) -> JSONResponse:
        return _problem_response(
            404,
            ErrorCode.USER_NOT_FOUND,
            "User not found.",
            request.url.path,
        )

    @app.exception_handler(CannotDeleteSelfError)
    async def _cannot_delete_self(
        request: Request, _exc: CannotDeleteSelfError
    ) -> JSONResponse:
        return _problem_response(
            400,
            ErrorCode.VALIDATION_ERROR,
            "Cannot delete your own account.",
            request.url.path,
        )
