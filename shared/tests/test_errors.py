"""Unit tests for grosh_shared.errors — RFC 7807 envelope module."""

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from pydantic import BaseModel

from grosh_shared.errors import (
    ErrorCode,
    ProblemDetail,
    _default_title,
    raise_problem,
    register_error_handlers,
)

_ALL_CODES = {
    "VALIDATION_ERROR",
    "AUTHENTICATION_REQUIRED",
    "INSUFFICIENT_PERMISSIONS",
    "ACCOUNT_NOT_FOUND",
    "INTEGRATION_NOT_FOUND",
    "USER_NOT_FOUND",
    "JOB_NOT_FOUND",
    "RATE_NOT_FOUND",
    "INVALID_CURSOR",
    "INVALID_DATE_RANGE",
    "REPROCESS_LOCKED",
    "BACKFILL_WINDOW_TOO_LARGE",
    "MONOBANK_TOKEN_INVALID",
    "MONOBANK_API_UNAVAILABLE",
    "INTEGRATION_ALREADY_LINKED",
    "JOB_STATUS_UNAVAILABLE",
    "JOB_SUBMISSION_FAILED",
    "INTERNAL_ERROR",
}


def test_error_code_enum_has_exactly_18_members() -> None:
    assert set(ErrorCode) == {ErrorCode(v) for v in _ALL_CODES}
    assert len(ErrorCode) == 18


def test_error_code_enum_values_match_names() -> None:
    for code in ErrorCode:
        assert code.value == code.name


def test_problem_detail_round_trips_json() -> None:
    original = ProblemDetail(
        type="https://docs.grosh.app/errors/account-not-found",
        title="Account Not Found",
        status=404,
        code=ErrorCode.ACCOUNT_NOT_FOUND,
        detail="No account with that ID exists for this user.",
        instance="/accounts/abc-123",
        validation_errors=None,
    )
    dumped = original.model_dump(mode="json")
    restored = ProblemDetail.model_validate(dumped)

    assert restored.type == original.type
    assert restored.title == original.title
    assert restored.status == original.status
    assert restored.code == original.code
    assert restored.detail == original.detail
    assert restored.instance == original.instance
    assert restored.validation_errors is None


def test_problem_detail_round_trips_with_validation_errors() -> None:
    errors = [{"loc": ["body", "email"], "msg": "field required", "type": "missing"}]
    original = ProblemDetail(
        type="https://docs.grosh.app/errors/validation-error",
        title="Validation Error",
        status=422,
        code=ErrorCode.VALIDATION_ERROR,
        detail="Validation failed",
        instance="/auth/login",
        validation_errors=errors,
    )
    dumped = original.model_dump(mode="json")
    restored = ProblemDetail.model_validate(dumped)

    assert restored.validation_errors == errors


def test_raise_problem_raises_http_exception() -> None:
    with pytest.raises(HTTPException) as exc_info:
        raise_problem(
            404,
            ErrorCode.ACCOUNT_NOT_FOUND,
            "Account not found.",
            instance="/accounts/abc",
        )

    exc = exc_info.value
    assert exc.status_code == 404
    assert isinstance(exc.detail, dict)
    assert exc.detail["code"] == "ACCOUNT_NOT_FOUND"
    assert exc.detail["status"] == 404
    assert exc.detail["detail"] == "Account not found."
    assert exc.detail["instance"] == "/accounts/abc"
    assert exc.detail["title"] == "Account Not Found"
    assert "https://docs.grosh.app/errors/account-not-found" == exc.detail["type"]


def test_raise_problem_custom_title_overrides_default() -> None:
    with pytest.raises(HTTPException) as exc_info:
        raise_problem(
            401,
            ErrorCode.AUTHENTICATION_REQUIRED,
            "Token missing.",
            title="Please log in",
        )

    assert exc_info.value.detail["title"] == "Please log in"


def test_raise_problem_422_includes_validation_errors() -> None:
    errors = [{"loc": ["body", "token"], "msg": "field required", "type": "missing"}]
    with pytest.raises(HTTPException) as exc_info:
        raise_problem(
            422,
            ErrorCode.VALIDATION_ERROR,
            "Validation failed",
            validation_errors=errors,
        )

    assert exc_info.value.detail["validation_errors"] == errors


def test_raise_problem_non_422_defaults_validation_errors_to_none() -> None:
    with pytest.raises(HTTPException) as exc_info:
        raise_problem(404, ErrorCode.USER_NOT_FOUND, "Not found.")

    assert exc_info.value.detail["validation_errors"] is None


def test_default_title_returns_non_empty_for_all_codes() -> None:
    for code in ErrorCode:
        title = _default_title(code)
        assert isinstance(title, str)
        assert len(title) > 0, f"Empty title for {code}"


def test_register_error_handlers_returns_none() -> None:
    test_app = FastAPI()
    result = register_error_handlers(test_app)
    assert result is None


def _build_test_client_app() -> TestClient:
    app = FastAPI()
    register_error_handlers(app)

    class LoginBody(BaseModel):
        email: str
        password: str

    @app.get("/raise-problem")
    def _raise_problem_route() -> None:
        raise_problem(
            404,
            ErrorCode.ACCOUNT_NOT_FOUND,
            "Account not found.",
        )

    @app.get("/raise-legacy")
    def _raise_legacy_route() -> None:
        raise HTTPException(status_code=404, detail="legacy string detail")

    @app.post("/validate")
    def _validate_route(_body: LoginBody) -> dict[str, str]:
        return {"ok": "true"}

    @app.get("/boom")
    def _boom_route() -> None:
        raise ValueError("secret-prod-token-12345")

    return TestClient(app, raise_server_exceptions=False)


def test_http_exception_handler_emits_envelope_when_detail_is_dict() -> None:
    client = _build_test_client_app()

    response = client.get("/raise-problem")

    assert response.status_code == 404
    body = response.json()
    assert body["code"] == "ACCOUNT_NOT_FOUND"
    assert body["status"] == 404
    assert body["detail"] == "Account not found."
    assert body["title"] == "Account Not Found"
    assert body["type"].endswith("/account-not-found")
    assert body["validation_errors"] is None


def test_http_exception_handler_wraps_legacy_string_detail() -> None:
    client = _build_test_client_app()

    response = client.get("/raise-legacy")

    assert response.status_code == 404
    body = response.json()
    assert body["code"] == "INTERNAL_ERROR"
    assert body["status"] == 404
    assert body["detail"] == "legacy string detail"
    assert body["instance"] == "/raise-legacy"


def test_request_validation_error_handler_converts_loc_tuple_to_list() -> None:
    client = _build_test_client_app()

    response = client.post("/validate", json={})

    assert response.status_code == 422
    body = response.json()
    assert body["code"] == "VALIDATION_ERROR"
    assert body["status"] == 422
    assert body["title"] == "Validation Error"
    assert body["instance"] == "/validate"
    assert isinstance(body["validation_errors"], list)
    assert len(body["validation_errors"]) >= 1
    for err in body["validation_errors"]:
        assert set(err.keys()) == {"loc", "msg", "type"}
        assert isinstance(err["loc"], list)


def test_catch_all_handler_returns_500_without_leaking_exception_text() -> None:
    client = _build_test_client_app()

    response = client.get("/boom")

    assert response.status_code == 500
    body = response.json()
    assert body["code"] == "INTERNAL_ERROR"
    assert body["status"] == 500
    assert body["detail"] == "An unexpected error occurred"
    assert body["instance"] == "/boom"
    serialized = response.text
    assert "secret-prod-token-12345" not in serialized
    assert "ValueError" not in serialized
