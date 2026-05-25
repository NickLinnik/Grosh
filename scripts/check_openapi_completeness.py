"""OpenAPI completeness check.

Imports grosh_api.main:app and grosh_ingestion.main:app in-process, calls
.openapi() on each, and asserts that every non-204 response that has a content
block also carries a substantive schema (not an empty {} or bare
{"additionalProperties": true}).

Required env vars (set to stubs — no real DB or Kafka needed):
    DATABASE_URL          e.g. postgresql://stub/stub
    JWT_SECRET            any non-empty string
    ADMIN_EMAIL           any e-mail address
    ADMIN_PASSWORD        any string

These are read inside the lifespan function, not at import time, so the app
objects can be constructed and .openapi() invoked without a live database.
"""

import os
import sys

# ---------------------------------------------------------------------------
# Stub env vars required to import the FastAPI apps without a real database.
# All are consumed inside the lifespan context manager (not at module level),
# so setting them here is sufficient for .openapi() to succeed.
# ---------------------------------------------------------------------------
_STUBS: dict[str, str] = {
    "DATABASE_URL": "postgresql://stub/stub",
    "JWT_SECRET": "stub-secret",
    "ADMIN_EMAIL": "stub@stub.com",
    "ADMIN_PASSWORD": "stub-password",
}
for _key, _val in _STUBS.items():
    os.environ.setdefault(_key, _val)

# Insert both service source trees so imports resolve without an editable
# install (works both locally and in CI after `uv sync --all-packages`).
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "services", "api", "src")
)
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), "..", "services", "ingestion", "src")
)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared", "src"))

from grosh_api.main import app as api_app  # noqa: E402
from grosh_ingestion.main import app as ingestion_app  # noqa: E402


# ---------------------------------------------------------------------------
# Schema analysis helpers
# ---------------------------------------------------------------------------

_SKIP_CODES = {"204"}


def _is_substantive(schema: dict) -> bool:
    """Return True if the schema carries enough structural information.

    Substantive schemas are ones that describe a concrete shape:
    - $ref pointing to a named component
    - properties / items / allOf / anyOf / oneOf
    - additionalProperties whose value is a typed schema (not bare True)

    Not substantive:
    - {}  (empty — FastAPI emits this for -> None at status 200)
    - {"additionalProperties": true}  (untyped bare-dict return)
    """
    if not schema:
        return False

    if schema.get("$ref"):
        return True

    for keyword in ("properties", "items", "allOf", "anyOf", "oneOf"):
        if keyword in schema:
            return True

    additional = schema.get("additionalProperties")
    if additional is not None:
        # {"additionalProperties": true} is NOT substantive
        # {"additionalProperties": {"type": "string"}} IS substantive
        return additional is not True and isinstance(additional, dict)

    return False


# ---------------------------------------------------------------------------
# Walk the OpenAPI document
# ---------------------------------------------------------------------------


def check_app(service_name: str, app) -> list[str]:
    """Return a list of violation strings for the given FastAPI app."""
    doc: dict = app.openapi()
    paths: dict = doc.get("paths", {})
    violations: list[str] = []

    for path, methods in paths.items():
        for method, operation in methods.items():
            if method.lower() in ("head", "options"):
                continue

            responses: dict = operation.get("responses", {})
            for code, response_obj in responses.items():
                if code in _SKIP_CODES:
                    continue

                content: dict = response_obj.get("content", {})

                # No content block at all — response_class=Response or similar.
                # This is intentional (body-less 200/201/etc.) and acceptable.
                if not content:
                    continue

                for media_type, media_obj in content.items():
                    schema: dict = media_obj.get("schema", {})

                    if not _is_substantive(schema):
                        violations.append(
                            f"  [{service_name}] {method.upper()} {path}"
                            f" → {code} ({media_type}): "
                            f"schema is missing or unsubstantive: {schema!r}"
                        )

    return violations


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    services = [
        ("grosh_api", api_app),
        ("grosh_ingestion", ingestion_app),
    ]

    all_violations: list[str] = []
    for name, app in services:
        violations = check_app(name, app)
        all_violations.extend(violations)

    if all_violations:
        print("OpenAPI completeness check FAILED.")
        print(
            f"{len(all_violations)} operation(s) lack a substantive response schema:\n"
        )
        for line in all_violations:
            print(line)
        print(
            "\nFix: add response_model=<YourPydanticModel> to the @router.get/post/... "
            "decorator, or use response_class=Response for intentionally body-less "
            "endpoints."
        )
        sys.exit(1)

    print(
        f"OpenAPI completeness check passed. "
        f"All operations across {len(services)} service(s) have substantive response schemas."
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
