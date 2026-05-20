"""RFC 7807 Problem Details error contract shared across all Grosh HTTP services."""

import logging
from enum import StrEnum
from typing import Any, NoReturn

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ErrorCode(StrEnum):
    VALIDATION_ERROR = "VALIDATION_ERROR"
    AUTHENTICATION_REQUIRED = "AUTHENTICATION_REQUIRED"
    INSUFFICIENT_PERMISSIONS = "INSUFFICIENT_PERMISSIONS"
    ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND"
    INTEGRATION_NOT_FOUND = "INTEGRATION_NOT_FOUND"
    USER_NOT_FOUND = "USER_NOT_FOUND"
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    RATE_NOT_FOUND = "RATE_NOT_FOUND"
    INVALID_CURSOR = "INVALID_CURSOR"
    INVALID_DATE_RANGE = "INVALID_DATE_RANGE"
    REPROCESS_LOCKED = "REPROCESS_LOCKED"
    BACKFILL_WINDOW_TOO_LARGE = "BACKFILL_WINDOW_TOO_LARGE"
    MONOBANK_TOKEN_INVALID = "MONOBANK_TOKEN_INVALID"
    MONOBANK_API_UNAVAILABLE = "MONOBANK_API_UNAVAILABLE"
    INTEGRATION_ALREADY_LINKED = "INTEGRATION_ALREADY_LINKED"
    JOB_STATUS_UNAVAILABLE = "JOB_STATUS_UNAVAILABLE"
    JOB_SUBMISSION_FAILED = "JOB_SUBMISSION_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


_TITLES: dict[ErrorCode, str] = {
    ErrorCode.VALIDATION_ERROR: "Validation Error",
    ErrorCode.AUTHENTICATION_REQUIRED: "Authentication Required",
    ErrorCode.INSUFFICIENT_PERMISSIONS: "Insufficient Permissions",
    ErrorCode.ACCOUNT_NOT_FOUND: "Account Not Found",
    ErrorCode.INTEGRATION_NOT_FOUND: "Integration Not Found",
    ErrorCode.USER_NOT_FOUND: "User Not Found",
    ErrorCode.JOB_NOT_FOUND: "Job Not Found",
    ErrorCode.RATE_NOT_FOUND: "Rate Not Found",
    ErrorCode.INVALID_CURSOR: "Invalid Cursor",
    ErrorCode.INVALID_DATE_RANGE: "Invalid Date Range",
    ErrorCode.REPROCESS_LOCKED: "Reprocess Already In Progress",
    ErrorCode.BACKFILL_WINDOW_TOO_LARGE: "Backfill Window Too Large",
    ErrorCode.MONOBANK_TOKEN_INVALID: "Monobank Token Invalid",
    ErrorCode.MONOBANK_API_UNAVAILABLE: "Monobank API Unavailable",
    ErrorCode.INTEGRATION_ALREADY_LINKED: "Integration Already Linked",
    ErrorCode.JOB_STATUS_UNAVAILABLE: "Job Status Unavailable",
    ErrorCode.JOB_SUBMISSION_FAILED: "Job Submission Failed",
    ErrorCode.INTERNAL_ERROR: "Internal Server Error",
}


def _default_title(code: ErrorCode) -> str:
    return _TITLES[code]


class ProblemDetail(BaseModel):
    type: str
    title: str
    status: int
    code: ErrorCode
    detail: str
    instance: str
    validation_errors: list[dict[str, Any]] | None = None


def raise_problem(
    status_code: int,
    code: ErrorCode,
    detail: str,
    *,
    instance: str = "",
    title: str | None = None,
    validation_errors: list[dict[str, Any]] | None = None,
) -> NoReturn:
    """Raise an HTTPException whose body conforms to the RFC 7807 envelope.

    The registered exception handler in each service (see error_handlers.py)
    serializes the body, sets the JSON content-type, and includes the
    validation_errors list on 422 responses only.
    """
    problem = ProblemDetail(
        type=f"https://docs.grosh.app/errors/{code.value.lower().replace('_', '-')}",
        title=title or _default_title(code),
        status=status_code,
        code=code,
        detail=detail,
        instance=instance,
        validation_errors=validation_errors,
    )
    raise HTTPException(status_code=status_code, detail=problem.model_dump(mode="json"))


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def _http_exception_handler(
        request: Request, exc: HTTPException
    ) -> JSONResponse:
        if isinstance(exc.detail, dict):
            return JSONResponse(status_code=exc.status_code, content=exc.detail)
        problem = ProblemDetail(
            type="https://docs.grosh.app/errors/internal-error",
            title=_default_title(ErrorCode.INTERNAL_ERROR),
            status=exc.status_code,
            code=ErrorCode.INTERNAL_ERROR,
            detail=str(exc.detail),
            instance=request.url.path,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=problem.model_dump(mode="json"),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        validation_errors = [
            {
                "loc": list(err["loc"]),
                "msg": err["msg"],
                "type": err["type"],
            }
            for err in exc.errors()
        ]
        problem = ProblemDetail(
            type="https://docs.grosh.app/errors/validation-error",
            title=_default_title(ErrorCode.VALIDATION_ERROR),
            status=422,
            code=ErrorCode.VALIDATION_ERROR,
            detail="Validation failed",
            instance=request.url.path,
            validation_errors=validation_errors,
        )
        return JSONResponse(
            status_code=422,
            content=problem.model_dump(mode="json"),
        )

    @app.exception_handler(Exception)
    async def _catch_all_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error(
            "Unhandled exception for %s %s",
            request.method,
            request.url.path,
            exc_info=True,
        )
        problem = ProblemDetail(
            type="https://docs.grosh.app/errors/internal-error",
            title=_default_title(ErrorCode.INTERNAL_ERROR),
            status=500,
            code=ErrorCode.INTERNAL_ERROR,
            detail="An unexpected error occurred",
            instance=request.url.path,
        )
        return JSONResponse(
            status_code=500,
            content=problem.model_dump(mode="json"),
        )
