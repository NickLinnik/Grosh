"""Error handler registration for the ingestion service.

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

from grosh_ingestion.errors import (
    AccountAlreadyExistsError,
    AccountNotManualError,
    AccountNotOwnedError,
    BackfillAlreadyRunningError,
    IntegrationAlreadyExistsError,
    InvalidRateSourceError,
    K8sDispatchError,
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

    @app.exception_handler(AccountAlreadyExistsError)
    async def _account_already_exists(
        request: Request, exc: AccountAlreadyExistsError
    ) -> JSONResponse:
        return _problem_response(
            409,
            ErrorCode.VALIDATION_ERROR,
            str(exc),
            request.url.path,
        )

    @app.exception_handler(AccountNotOwnedError)
    async def _account_not_owned(
        request: Request, exc: AccountNotOwnedError
    ) -> JSONResponse:
        return _problem_response(
            403,
            ErrorCode.INSUFFICIENT_PERMISSIONS,
            str(exc),
            request.url.path,
        )

    @app.exception_handler(AccountNotManualError)
    async def _account_not_manual(
        request: Request, exc: AccountNotManualError
    ) -> JSONResponse:
        return _problem_response(
            403,
            ErrorCode.INSUFFICIENT_PERMISSIONS,
            str(exc),
            request.url.path,
        )

    @app.exception_handler(InvalidRateSourceError)
    async def _invalid_rate_source(
        request: Request, exc: InvalidRateSourceError
    ) -> JSONResponse:
        return _problem_response(
            422,
            ErrorCode.VALIDATION_ERROR,
            str(exc),
            request.url.path,
        )

    @app.exception_handler(IntegrationAlreadyExistsError)
    async def _integration_already_exists(
        request: Request, exc: IntegrationAlreadyExistsError
    ) -> JSONResponse:
        return _problem_response(
            409,
            ErrorCode.INTEGRATION_ALREADY_LINKED,
            str(exc),
            request.url.path,
        )

    @app.exception_handler(BackfillAlreadyRunningError)
    async def _backfill_already_running(
        request: Request, exc: BackfillAlreadyRunningError
    ) -> JSONResponse:
        return _problem_response(
            409,
            ErrorCode.REPROCESS_LOCKED,
            str(exc),
            request.url.path,
        )

    @app.exception_handler(K8sDispatchError)
    async def _k8s_dispatch_error(
        request: Request, exc: K8sDispatchError
    ) -> JSONResponse:
        logger.error("K8s dispatch error in request handler: %s", exc)
        return _problem_response(
            502,
            ErrorCode.JOB_SUBMISSION_FAILED,
            "K8s API unavailable",
            request.url.path,
        )
