"""Translation layer: domain exceptions → HTTP responses."""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from grosh_ingestion.errors import (
    AccountAlreadyExistsError,
    AccountNotOwnedError,
    BackfillAlreadyRunningError,
    IntegrationAlreadyExistsError,
    InvalidRateSourceError,
    K8sDispatchError,
)

logger = logging.getLogger(__name__)


def _json_error(status_code: int, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"detail": detail})


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(AccountAlreadyExistsError)
    async def _account_already_exists(
        _request: Request, exc: AccountAlreadyExistsError
    ) -> JSONResponse:
        return _json_error(409, str(exc))

    @app.exception_handler(AccountNotOwnedError)
    async def _account_not_owned(
        _request: Request, exc: AccountNotOwnedError
    ) -> JSONResponse:
        return _json_error(403, str(exc))

    @app.exception_handler(InvalidRateSourceError)
    async def _invalid_rate_source(
        _request: Request, exc: InvalidRateSourceError
    ) -> JSONResponse:
        return _json_error(422, str(exc))

    @app.exception_handler(IntegrationAlreadyExistsError)
    async def _integration_already_exists(
        _request: Request, exc: IntegrationAlreadyExistsError
    ) -> JSONResponse:
        return _json_error(409, str(exc))

    @app.exception_handler(BackfillAlreadyRunningError)
    async def _backfill_already_running(
        _request: Request, exc: BackfillAlreadyRunningError
    ) -> JSONResponse:
        return _json_error(409, str(exc))

    @app.exception_handler(K8sDispatchError)
    async def _k8s_dispatch_error(
        _request: Request, exc: K8sDispatchError
    ) -> JSONResponse:
        logger.error("K8s dispatch error in request handler: %s", exc)
        return _json_error(503, "K8s API unavailable")
