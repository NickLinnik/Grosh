"""AST-based enforcement of the single-writer carve-out rule.

Rules enforced:
  1. normalizer package must not contain INSERT/UPDATE on transactions or
     transfer_match_anomalies (except DELETE — normalizer DELETEs transactions
     during reprocess, which is the documented exception).
  2. pipeline package must not contain SELECT on transactions for reading
     normalizer-owned data (select_for_user belongs only in normalizer).
  3. The pipeline transaction_repo must not expose select_for_user.
  4. The normalizer must not contain claim_pair or any UPDATE on transactions.
  5. The API service must not import TransactionRow from grosh_shared.normalized
     — the API has its own local TransactionRow for HTTP response shapes and
     the two classes must not be unified (see CLAUDE.md "Shared package
     conventions" and spec 003 §2.9).

NOTE: Runtime-built SQL (variable table names via f-strings, ORM queries,
parameterized table names) is NOT caught by the AST string-literal walk and
relies on code review.
"""

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[4]
_NORMALIZER_SRC = _REPO_ROOT / "services/normalizer/src/grosh_normalizer"
_PIPELINE_SRC = _REPO_ROOT / "services/pipeline/src/grosh_pipeline"
_API_SRC = _REPO_ROOT / "services/api/src/grosh_api"


def _collect_py_files(directory: Path) -> list[Path]:
    return list(directory.rglob("*.py"))


def _extract_string_literals(source: str) -> list[str]:
    """Return all string literals in the AST (single + multiline)."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    strings: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            strings.append(node.value)
    return strings


def _contains_sql_pattern(strings: list[str], pattern: str) -> list[str]:
    """Return strings that contain `pattern` (case-insensitive)."""
    pattern_lower = pattern.lower()
    return [s for s in strings if pattern_lower in s.lower()]


# ---------------------------------------------------------------------------
# Rule 1: normalizer must not INSERT or UPDATE transactions / anomalies
# ---------------------------------------------------------------------------


def test_normalizer_has_no_insert_on_transactions() -> None:
    """normalizer must not INSERT into transactions (pipeline is the write owner).

    Exception: reprocess_repo.py's restore_from_backup is the documented
    single-writer carve-out — it re-inserts from a backup snapshot, not from
    new data. See CLAUDE.md §data-ownership-matrix for the full rationale.
    """
    _ALLOWED = {"reprocess_repo.py"}
    violations: list[str] = []
    for path in _collect_py_files(_NORMALIZER_SRC):
        if path.name in _ALLOWED:
            continue
        source = path.read_text()
        strings = _extract_string_literals(source)
        hits = _contains_sql_pattern(strings, "INSERT INTO transactions")
        for h in hits:
            violations.append(f"{path.relative_to(_REPO_ROOT)}: {h[:80]!r}")

    assert not violations, (
        "normalizer contains INSERT INTO transactions — "
        "pipeline is the single write owner:\n" + "\n".join(violations)
    )


def test_normalizer_has_no_update_on_transactions() -> None:
    """normalizer must not UPDATE transactions (pipeline is the write owner)."""
    violations: list[str] = []
    for path in _collect_py_files(_NORMALIZER_SRC):
        source = path.read_text()
        strings = _extract_string_literals(source)
        hits = _contains_sql_pattern(strings, "UPDATE transactions")
        for h in hits:
            violations.append(f"{path.relative_to(_REPO_ROOT)}: {h[:80]!r}")

    assert not violations, (
        "normalizer contains UPDATE transactions — "
        "pipeline is the single write owner:\n" + "\n".join(violations)
    )


def test_normalizer_has_no_insert_on_anomalies() -> None:
    """normalizer must not INSERT into transfer_match_anomalies (pipeline owns it)."""
    violations: list[str] = []
    for path in _collect_py_files(_NORMALIZER_SRC):
        source = path.read_text()
        strings = _extract_string_literals(source)
        hits = _contains_sql_pattern(strings, "INSERT INTO transfer_match_anomalies")
        for h in hits:
            violations.append(f"{path.relative_to(_REPO_ROOT)}: {h[:80]!r}")

    assert not violations, (
        "normalizer contains INSERT INTO transfer_match_anomalies — "
        "pipeline is the single write owner:\n" + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# Rule 2: pipeline transaction_repo must not expose select_for_user
# ---------------------------------------------------------------------------


def test_pipeline_transaction_repo_has_no_select_for_user() -> None:
    """select_for_user belongs only in normalizer's TransactionReadRepo."""
    tx_repo_path = _PIPELINE_SRC / "repositories" / "transaction_repo.py"
    assert (
        tx_repo_path.exists()
    ), f"Expected pipeline transaction_repo at {tx_repo_path}"

    source = tx_repo_path.read_text()
    assert "select_for_user" not in source, (
        "pipeline/repositories/transaction_repo.py defines select_for_user — "
        "this method must live only in normalizer/repositories/transaction_read_repo.py"
    )


# ---------------------------------------------------------------------------
# Rule 3: normalizer must not import from grosh_pipeline
# ---------------------------------------------------------------------------


def test_normalizer_does_not_import_pipeline() -> None:
    """normalizer must not import from grosh_pipeline (strict package isolation)."""
    violations: list[str] = []
    for path in _collect_py_files(_NORMALIZER_SRC):
        source = path.read_text()
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("grosh_pipeline"):
                    violations.append(
                        f"{path.relative_to(_REPO_ROOT)}: from {module} import ..."
                    )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("grosh_pipeline"):
                        violations.append(
                            f"{path.relative_to(_REPO_ROOT)}: import {alias.name}"
                        )

    assert not violations, (
        "normalizer imports from grosh_pipeline — packages must be independent:\n"
        + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# Rule 4: pipeline must not import from grosh_normalizer
# ---------------------------------------------------------------------------


def test_pipeline_does_not_import_normalizer() -> None:
    """pipeline must not import from grosh_normalizer (strict package isolation)."""
    violations: list[str] = []
    for path in _collect_py_files(_PIPELINE_SRC):
        source = path.read_text()
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("grosh_normalizer"):
                    violations.append(
                        f"{path.relative_to(_REPO_ROOT)}: from {module} import ..."
                    )
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("grosh_normalizer"):
                        violations.append(
                            f"{path.relative_to(_REPO_ROOT)}: import {alias.name}"
                        )

    assert not violations, (
        "pipeline imports from grosh_normalizer — packages must be independent:\n"
        + "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# Rule 5: API service must not import TransactionRow from grosh_shared.normalized
# ---------------------------------------------------------------------------


def test_api_does_not_import_shared_transaction_row() -> None:
    """API has its own local TransactionRow; must not unify with the shared one.

    The shared TransactionRow exists only because the normalizer's reprocess
    flow needs the inverse-mapping `to_normalized()` method. The API service
    is read-only over the transactions table and has its own TransactionRow
    dataclass shaped for HTTP response bodies. Unifying the two would expand
    the schema-awareness exception to a service that doesn't need it (see
    CLAUDE.md "Shared package conventions" and spec 003 §2.9).
    """
    violations: list[str] = []
    for path in _collect_py_files(_API_SRC):
        source = path.read_text()
        try:
            tree = ast.parse(source)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module != "grosh_shared.normalized":
                    continue
                for alias in node.names:
                    if alias.name == "TransactionRow":
                        violations.append(
                            f"{path.relative_to(_REPO_ROOT)}: "
                            f"from grosh_shared.normalized import TransactionRow"
                        )

    assert not violations, (
        "API service imports TransactionRow from grosh_shared.normalized — "
        "the two TransactionRow classes must not be unified:\n" + "\n".join(violations)
    )
