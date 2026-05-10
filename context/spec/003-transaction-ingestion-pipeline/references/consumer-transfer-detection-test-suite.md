# Transfer Detection — Test Specification

Implementation instructions for the unit and integration test suites covering
`MonobankTransferDetection` (the per-source strategy) and the surrounding
modules: row-flag computation, IBAN classification, the count-and-decide
branch, metadata block builders, anomaly builders, the description guard,
and the universal-fetch + claim repository.

Reference documents (consume in this order):

1. `references/adr-transfer-detection-v2.md` — the algorithm design and
   the source of truth for behavior (universal candidate fetch,
   directional transitive rule, bucket-locked principle, metadata block
   schema, anomaly enum). When a test asserts "evidence is bilateral" or
   "the canary fires," the ADR defines what that means.
2. `context/spec/003-transaction-ingestion-pipeline/technical-considerations.md`
   — the implementation contract. Section "Transfer detection
   (Monobank strategy)" specifies the module layout under
   `sources/monobank/transfer/`, the `TransferResult.metadata_block`
   field, the orchestrator merge contract (`metadata.layer.transfer ⇔
   mcc == '4829'`), and the file structure entries this test suite
   targets. When a test imports `flags.compute_row_flags` or asserts on
   `Decision` discriminated-union variants, the tech spec is what pins
   those names.

The current document is authoritative for test structure, fixtures, and
assertions. The ADR pins behavior; the tech spec pins module shape; this
doc pins what gets verified and how.

---

## 1. Test infrastructure

### 1.1 File layout

```
services/consumer/tests/
  conftest.py                                                 # existing — pool/conn fixture
  helpers.py                                                  # existing — make_event()
  unit/
    conftest.py                                               # extended — pure-fn fixtures + RowFlags factory
    test_description_guard.py                                 # description parsing + pair validation + multi-hop helper
    test_transfer_flags.py                                    # compute_row_flags — directional transitive rule, all cp_iban_status cells
    test_transfer_iban_classifier.py                          # is_consistent + classify_pair_evidence — full matrix
    test_transfer_metadata.py                                 # build_row_block + build_pair_block — JSON shape snapshots
    test_transfer_anomalies.py                                # AnomalyRecord builders — reason_detail formatting
    test_transfer_decision.py                                 # decide() — count-and-decide + bucket-locked principle
    test_transfer_result.py                                   # TransferResult dataclass shape + metadata_block field
  integration/
    conftest.py                                               # existing — extend repo fixtures + insert helpers
    test_transfer_universal_fetch_integration.py              # universal SELECT … FOR UPDATE SKIP LOCKED behavior
    test_transfer_consistency_filter_integration.py           # post-fetch hard IBAN consistency filter
    test_transfer_decision_integration.py                     # count==0/1/>1 branches end-to-end against real DB
    test_transfer_metadata_block_integration.py               # metadata.layer.transfer presence/shape invariant
    test_transfer_directional_transitive_integration.py       # multi-hop expense IBAN suppressed; income IBAN honored; 4-leg chain
    test_transfer_external_short_circuit_integration.py       # cp_iban_status=external skips fetch; vocabulary-drift signal
    test_transfer_amount_asymmetry_integration.py             # FOP↔FOP cross-currency: two-clause predicate finds partner regardless of arrival order
    test_transfer_anomaly_integration.py                      # anomaly UNIQUE constraint, CASCADE, auto-resolve
    test_transfer_claim_locks_integration.py                  # FOR UPDATE SKIP LOCKED + advisory locks
    test_transfer_pipeline_integration.py                     # PipelineOrchestrator end-to-end
```

The unit suite is a clean-slate rewrite — none of the v1 tier-specific test
files survive. The integration suite is a restructure: tier-specific files
collapse into algorithm-step files, the existing fixture infrastructure
(insert helpers, repo fixtures, conn fixture) carries over with minor
extensions. Counts: ~150 unit cases + ~80 integration cases.

### 1.2 Unit-test fixtures (`tests/unit/conftest.py`)

The v2 design pushes most logic into pure functions (`flags`,
`iban_classifier`, `decision`, `metadata`, `anomalies`). These take plain
dataclasses in and return plain dataclasses out — **no fakes, no in-memory
repos, no `conn` parameter, no async**. Most unit tests are 5-line
parametrize cells.

**Helper factories** (small, named, no clever metaprogramming):

- `make_normalized_tx(**overrides) -> NormalizedTransaction` — factory for
  `NormalizedTransaction` with sensible MCC-4829 defaults; override only
  the fields each test cares about. Defaults: `mcc='4829'`, `direction='expense'`,
  `time=T`, `amount_cents=10_000`, `operation_amount_cents=10_000`,
  `counterparty_iban=None`, `description=None`, `source='monobank'`.
- `make_row_flags(**overrides) -> RowFlags` — factory for `RowFlags`.
  Defaults: `description_matched=False`, `multi_hop_description=False`,
  `cp_iban_status='null'`.
- `make_candidate(**overrides) -> CandidateRow` — factory for the
  `CandidateRow` dataclass used by the decision module. Defaults match an
  unclaimed unilateral candidate.
- `make_account_props(**overrides) -> AccountProps` — `(type, currency_code, iban)`
  dataclass used wherever description validation needs context.

**Account-IBAN lookup stubs** (for `compute_row_flags`):

- `iban_lookup_returning(account_id_or_none)` — returns a callable
  `Callable[[str], UUID | None]` that always returns the given value.
  Sufficient for testing because `compute_row_flags` only branches on
  "is the IBAN known to belong to one of this user's own accounts."
- `iban_lookup_from_dict(mapping: dict[str, UUID])` — returns a callable
  that resolves IBANs through the dict. Used in tests that exercise
  multiple IBANs in one scenario.

There are **no `InMemory*Repo` classes** in the unit suite. The detector
itself is exercised end-to-end only in integration tests; unit tests
exercise its constituent pure functions directly.

### 1.3 Integration fixtures (`tests/integration/conftest.py`)

Preserved from today (no changes needed):

- `pool` (session-scoped) — asyncpg pool against the throwaway test DB.
- `conn` (function-scoped) — connection wrapped in a rolled-back
  transaction.
- `rate_repo`, `anomaly_repo`, `account_property_repo` — direct
  instantiations of the production repos.

Updated:

- `transfer_repo` fixture — instantiates the rebuilt
  `TransferQueryRepo` (v2 methods: `exists`, `find_universal_candidates`,
  `claim_pair`).
- `transfer_service` fixture — wires `MonobankTransferDetection` from
  `sources/monobank/transfer/detector.py` (not the old single-file
  `transfer.py`) with the three repos via DI.

Insert/query helpers (extend the existing set; signatures unchanged):

- `insert_account(conn, *, user_id, type, currency_code, iban=None, source="monobank", **extras) -> UUID`
- `insert_transaction(conn, *, id=None, user_id, account_id, time, amount_cents, operation_amount_cents=None, mcc='4829', direction, counterparty_iban=None, description=None, related_transaction_id=None, special_category=None, metadata=None, source="monobank", **extras) -> UUID`
- `get_transaction(conn, tx_id) -> Row | None`
- `get_anomaly(conn, transaction_id) -> Row | None`
- `count_anomalies(conn, user_id) -> int`

New helper:

- `get_transfer_metadata(conn, tx_id) -> dict | None` — safely walks the
  metadata JSONB tree and returns `metadata.layer.transfer` or `None` if
  any intermediate key is missing. Implementation:
  `(row['metadata'] or {}).get('layer', {}).get('transfer')`. Returning
  `None` covers all three "absent" cases — `metadata IS NULL`, no
  `layer` sub-tree, or no `transfer` sub-tree — so test assertions can
  use `assert get_transfer_metadata(...) is None` regardless of which
  level is missing. Used by metadata-invariant tests so they don't
  repeat the JSONB navigation.

### 1.4 Test constants

Used by both suites:

```python
from datetime import UTC, datetime, timedelta
from uuid import uuid4

T              = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
T_PLUS_1S      = T + timedelta(seconds=1)
T_MINUS_1S     = T - timedelta(seconds=1)
T_PLUS_2S      = T + timedelta(seconds=2)
T_MINUS_2S     = T - timedelta(seconds=2)
T_PLUS_2S_1MS  = T + timedelta(seconds=2, milliseconds=1)   # boundary just outside
T_PLUS_3S      = T + timedelta(seconds=3)                   # outside window

WINDOW_SECONDS = 2
MCC_TRANSFER   = '4829'    # MccCode.WIRE_TRANSFER.code
MCC_GROCERY    = '5411'

USER_ID        = uuid4()

# Account fixtures (reused across tests)
UAH_FOP   = AccountProps(type="fop",   currency_code="UAH", iban="UA111000000000000000000111")
USD_FOP   = AccountProps(type="fop",   currency_code="USD", iban="UA222000000000000000000222")
EUR_FOP   = AccountProps(type="fop",   currency_code="EUR", iban="UA333000000000000000000333")
UAH_BLACK = AccountProps(type="black", currency_code="UAH", iban="UA444000000000000000000444")
UAH_WHITE = AccountProps(type="white", currency_code="UAH", iban="UA555000000000000000000555")
EUR_CARD  = AccountProps(type="black", currency_code="EUR", iban="UA666000000000000000000666")
EXTERNAL  = "UA999000000000000000000999"     # never owned by USER_ID
```

---

## 2. Unit tests — description guard (`tests/unit/test_description_guard.py`)

Pure functions over the description maps. The v2 detector consumes
`parse_description`, `is_transfer_description`, `is_multi_hop_description`,
and `validate_pair_descriptions`. All four are tested here.

### 2.1 `parse_description` — known income phrases

1. `"З гривневого рахунку ФОП"` → `(type="fop", currency="UAH")`
2. `"З доларового рахунку ФОП"` → `(type="fop", currency="USD")`
3. `"З єврового рахунку ФОП"` → `(type="fop", currency="EUR")`
4. `"З доларового рахунку ФОП для переказу на картку"` → `(type="fop", currency="USD")` — multi-hop variant, same constraint.
5. `"З єврового рахунку ФОП для переказу на картку"` → `(type="fop", currency="EUR")`
6. `"З гривневого рахунку ФОП для переказу на картку"` → `(type="fop", currency="UAH")`
7. `"З Чорної картки"` → `(type="black", currency=None)`
8. `"З Білої картки"` → `(type="white", currency=None)`
9. `"З Платинової картки"` → `(type="platinum", currency=None)`
10. `"З Залізної картки"` → `(type="iron", currency=None)`
11. `"З Жовтої картки"` → `(type="yellow", currency=None)`
12. `"З доларової картки"` → `(type=None, currency="USD")` — currency-only, any card type.
13. `"З єврової картки"` → `(type=None, currency="EUR")`

### 2.2 `parse_description` — known expense phrases

14. `"Переказ на картку"` → `(type=None, currency=None)` — generic, no constraint.
15. `"На чорну картку"` → `(type="black", currency=None)`
16. `"На білу картку"` → `(type="white", currency=None)`
17. `"На платинову картку"` → `(type="platinum", currency=None)`
18. `"На залізну картку"` → `(type="iron", currency=None)`
19. `"На жовту картку"` → `(type="yellow", currency=None)`
20. `"На гривневий рахунок ФОП"` → `(type="fop", currency="UAH")`
21. `"На доларовий рахунок ФОП"` → `(type="fop", currency="USD")`
22. `"На євровий рахунок ФОП"` → `(type="fop", currency="EUR")`
23. `"На гривневий рахунок ФОП для переказу на картку"` → `(type="fop", currency="UAH")`
24. `"На доларовий рахунок ФОП для переказу на картку"` → `(type="fop", currency="USD")`
25. `"На євровий рахунок ФОП для переказу на картку"` → `(type="fop", currency="EUR")`

### 2.3 `parse_description` — unknown / non-transfer

26. `"Олена К."` → `None` — person name.
27. `"516874****1234"` → `None` — masked card number.
28. `"Випуск іменної картки"` → `None` — bank fee.
29. `""` → `None`
30. `None` → `None`

### 2.4 `is_transfer_description` — prefix detection

The function does NOT require the description to be in the known set — it
only checks the prefix family. Used by the detector to decide whether a
non-matched description deserves an `unpaired_*_description` anomaly
(vocabulary-drift signal).

31. `"З Чорної картки"` → `True` (known + "З " prefix)
32. `"З невідомого рахунку"` → `True` ("З " prefix, not in known set)
33. `"На білу картку"` → `True` (known + "На " prefix)
34. `"На невідому картку"` → `True` ("На " prefix, not in known set)
35. `"Переказ на картку"` → `True` (explicit allowed phrase)
36. `"Олена К."` → `False`
37. `""` → `False`
38. `None` → `False`

### 2.5 `is_multi_hop_description` — `для переказу на` family detection

The new helper. Returns `True` when the description is in the multi-hop
family (subset of `description_matched`). Drives the directional transitive
rule in `compute_row_flags`.

39. `"З доларового рахунку ФОП для переказу на картку"` → `True`
40. `"З єврового рахунку ФОП для переказу на картку"` → `True`
41. `"З гривневого рахунку ФОП для переказу на картку"` → `True`
42. `"На гривневий рахунок ФОП для переказу на картку"` → `True`
43. `"На доларовий рахунок ФОП для переказу на картку"` → `True`
44. `"На євровий рахунок ФОП для переказу на картку"` → `True`
45. `"З гривневого рахунку ФОП"` → `False` (FOP transfer without multi-hop suffix)
46. `"З Чорної картки"` → `False`
47. `"Переказ на картку"` → `False`
48. `"Олена К."` → `False`
49. `""` → `False`
50. `None` → `False`

### 2.6 `validate_pair_descriptions` — both sides together

The detector calls this in two paths: as a canary on count==1 (mismatch
records `description_consistency_mismatch`, claim proceeds), and as a hard
filter on count>1 (mismatch removes the candidate). The function itself is
the same in both cases.

Signature: `validate_pair_descriptions(income_desc, expense_desc, income_acc_props, expense_acc_props) -> bool`.
The income's "З ..." names the SOURCE (expense leg's account); the
expense's "На ..." names the TARGET (income leg's account).

51. **Card→card consistent pair**: income="З Чорної картки", expense="Переказ на картку";
    expense leg on UAH_BLACK, income leg on UAH_WHITE → income constraint
    `(type=black)` checked against expense account UAH_BLACK → True;
    expense constraint `None` (generic) → True. Both pass.
52. **FOP→card consistent pair**: income="З гривневого рахунку ФОП",
    expense="На чорну картку"; expense leg on UAH_FOP, income leg on
    UAH_BLACK → income constraint `(type=fop, currency=UAH)` vs expense
    account UAH_FOP → True; expense constraint `(type=black)` vs income
    account UAH_BLACK → True.
53. **Mismatch — wrong card color**: income="З Білої картки",
    expense="Переказ на картку"; expense leg on UAH_BLACK → income
    constraint `(type=white)` vs expense account UAH_BLACK → False.
    Returns False.
54. **Mismatch — wrong currency**: income="З гривневого рахунку ФОП" but
    expense leg on USD_FOP → False (UAH constraint, USD account).
55. **Mismatch — wrong type**: income="З Чорної картки" but expense leg on
    UAH_FOP → False.
56. **One side generic, other validates**: expense="Переказ на картку"
    (no constraint), income="З Чорної картки", expense leg on UAH_BLACK →
    True (generic side imposes no constraint, specific side passes).
57. **Both unknown → vacuously valid**: both descriptions outside the
    known set → both `parse_description` calls return None → no
    constraints → True. (Validation is a soft signal; it can't punish a
    pair just because we don't recognize the words.)
58. **Multi-hop variant validates same as base**: income="З доларового
    рахунку ФОП для переказу на картку" + expense leg on USD_FOP → True
    (the multi-hop suffix doesn't change the constraint).
59. **Both wrong → False** (expense fails, income fails — either failure
    is sufficient).
60. **`None` description handled**: either side `None` → that side imposes
    no constraint → result depends only on the other side.

---

## 3. Unit tests — `compute_row_flags` (`tests/unit/test_transfer_flags.py`)

The v2 strategy's first-class branch point. `compute_row_flags(tx, account_iban_lookup) -> RowFlags`
returns `(description_matched, multi_hop_description, cp_iban_status)`.

`account_iban_lookup` is a callable `Callable[[str], UUID | None]` that
returns the account UUID owning a given IBAN, or `None` if the IBAN
doesn't belong to any of the user's own accounts. The unit tests stub
this with `iban_lookup_returning(...)` or `iban_lookup_from_dict(...)`.

### 3.1 `description_matched` and `multi_hop_description`

61. Description in `_INCOME_MAP` (e.g. "З Чорної картки") →
    `description_matched=True`, `multi_hop_description=False`.
62. Description in `_EXPENSE_MAP` (e.g. "На чорну картку") →
    `description_matched=True`, `multi_hop_description=False`.
63. Description in multi-hop family (e.g. "На доларовий рахунок ФОП для
    переказу на картку") → `description_matched=True, multi_hop_description=True`.
64. Description outside known set ("Олена К.") → both `False`.
65. Description with prefix but not in known set ("З невідомого рахунку")
    → `description_matched=False, multi_hop_description=False`.
66. Description = `None` → both `False`.
67. Description = `""` → both `False`.

### 3.2 `cp_iban_status` — directional transitive rule (the load-bearing branch)

The Monobank quirk: a multi-hop expense's `counterparty_iban` points at
the chain's final destination, not the immediate partner. `compute_row_flags`
encodes this in one line: `direction == "expense" AND multi_hop_description
→ cp_iban_status = "transitive"`. Income-side IBANs are honest under the
same multi-hop description.

Test the 4 cells of (`direction`, `multi_hop_description`) crossed with
the IBAN-presence axis:

68. `direction="expense"`, `multi_hop_description=True`, `cp_iban` set,
    IBAN belongs to own account → `cp_iban_status="transitive"` (suppressed
    even though it resolves; the rule is evidence-suppression, not
    look-up failure).
69. `direction="expense"`, `multi_hop_description=True`, `cp_iban` set,
    IBAN does NOT belong to own account → `cp_iban_status="transitive"`
    (suppressed; the rule fires before the lookup matters).
70. `direction="expense"`, `multi_hop_description=False`, `cp_iban` set,
    IBAN owns own account → `cp_iban_status="honest"`.
71. `direction="expense"`, `multi_hop_description=False`, `cp_iban` set,
    IBAN doesn't resolve → `cp_iban_status="external"`.
72. `direction="income"`, `multi_hop_description=True`, `cp_iban` set,
    IBAN owns own account → `cp_iban_status="honest"` (income side honest
    even with multi-hop description — the directional rule applies only
    to expense).
73. `direction="income"`, `multi_hop_description=True`, `cp_iban` set,
    IBAN doesn't resolve → `cp_iban_status="external"`.
74. `direction="income"`, `multi_hop_description=False`, `cp_iban` set,
    IBAN owns own account → `cp_iban_status="honest"`.
75. `direction="income"`, `multi_hop_description=False`, `cp_iban` set,
    IBAN doesn't resolve → `cp_iban_status="external"`.

### 3.3 `cp_iban_status` — null IBAN

76. `cp_iban=None`, any direction, any description →
    `cp_iban_status="null"` (the multi-hop / lookup branches are skipped
    entirely when there's no IBAN to classify).

### 3.4 Composition

77. Realistic multi-hop expense (USD FOP expense, desc "З гривневого
    рахунку ФОП для переказу на картку" — wait, that's an income
    description; use "На гривневий рахунок ФОП для переказу на картку")
    → `description_matched=True, multi_hop_description=True,
    cp_iban_status="transitive"`. Captures the full directional rule in
    one assertion.
78. Realistic card→card without IBAN (card expense, desc "Переказ на
    картку") → `description_matched=True, multi_hop_description=False,
    cp_iban_status="null"`.
79. Realistic external (expense to friend's bank, desc "Олена К.",
    `cp_iban` set to non-own IBAN) → `description_matched=False,
    multi_hop_description=False, cp_iban_status="external"`.

---

## 4. Unit tests — IBAN classifier (`tests/unit/test_transfer_iban_classifier.py`)

Two pure functions: `is_consistent` (the hard filter) and
`classify_pair_evidence` (the level assignment).

### 4.1 `is_consistent` — hard filter on candidate set

The full predicate (per ADR §4):

- Drop if `incoming.cp_iban_status == 'honest'` AND incoming's `cp_iban`
  does NOT resolve to candidate's `account_id`.
- Drop if `candidate.cp_iban_status IN ('honest', 'external')` AND
  candidate's `cp_iban` does NOT resolve to incoming's `account_id`.
- Otherwise keep.

`null` and `transitive` impose NO constraint on either side. `external`
on the candidate side is always dropped (its `cp_iban` definitionally
doesn't resolve to any own account).

Use the AccountProps fixtures: incoming on UAH_FOP, candidate on UAH_BLACK.

80. **Both `null`** → consistent (no claim made by either side).
81. **Both `honest`, both pointing correctly at each other** → consistent.
82. **Incoming `honest` pointing at candidate's account, candidate `null`**
    → consistent (only one side claims; the other imposes no constraint).
83. **Candidate `honest` pointing at incoming's account, incoming `null`**
    → consistent.
84. **Incoming `honest` pointing at WRONG account (not candidate's)** →
    inconsistent (drop). Critical case — incoming's claim is concrete
    enough to disqualify a candidate that doesn't match it.
85. **Candidate `honest` pointing at WRONG account (not incoming's)** →
    inconsistent (drop).
86. **Incoming `transitive`, candidate `honest` pointing at incoming** →
    consistent (incoming's transitive IBAN is suppressed, so it can't
    contradict; candidate's honest claim is satisfied).
87. **Incoming `transitive`, candidate `honest` pointing at WRONG account**
    → inconsistent (the candidate's honest claim is binding regardless of
    incoming's status).
88. **Candidate `transitive`, incoming `honest` pointing at candidate** →
    consistent (candidate's transitive IBAN is suppressed; incoming's
    honest claim is satisfied).
89. **Both `transitive`** → consistent.
90. **Incoming `null`, candidate `external`** → inconsistent. The
    candidate has explicitly claimed its partner is outside the platform;
    honor that.
91. **Incoming `transitive`, candidate `external`** → inconsistent.
92. **Incoming `honest` pointing at candidate, candidate `external`** →
    inconsistent (candidate's external claim wins; if candidate's cp_iban
    doesn't resolve to ANY own account, it definitionally doesn't resolve
    to incoming's account).
93. **Candidate `null`, incoming `null`/`transitive`/`honest` regardless of
    pointing** → consistent (candidate makes no claim).

The full 4×4 `cp_iban_status` matrix (with pointing-correctly vs
pointing-wrongly variations on the `honest` cells) yields ~16 explicit
parametrize rows; cases 80–93 cover the load-bearing combinations and the
parametrize table fills in the rest.

### 4.2 `classify_pair_evidence` — level assignment

After consistency-filtering, surviving candidates are tagged. The function
returns one of `"bilateral"`, `"unilateral"`, `"none"`. (Also test that
the function never returns `"unknown"` or `None` — the level is always
defined for a surviving pair.)

94. **Both `honest`, both pointing at each other** → `"bilateral"`.
95. **Incoming `honest` pointing at candidate, candidate `null`** →
    `"unilateral"`.
96. **Incoming `honest` pointing at candidate, candidate `transitive`** →
    `"unilateral"` (transitive doesn't contribute, but honest one side is
    enough).
97. **Incoming `null`, candidate `honest` pointing at incoming** →
    `"unilateral"`.
98. **Incoming `transitive`, candidate `honest` pointing at incoming** →
    `"unilateral"`.
99. **Both `null`** → `"none"`.
100. **Both `transitive`** → `"none"`.
101. **One `null`, one `transitive`** → `"none"`.

(The function is only called on consistency-filter survivors, so cases
with `external` on either side are out of scope — they were already
dropped.)

---

## 5. Unit tests — metadata builders (`tests/unit/test_transfer_metadata.py`)

Pure JSON-shape builders. The whole reason they live in their own module
is that the JSON shape of `metadata.layer.transfer.{row, pair}` can change
without touching the detector. Tests are snapshot comparisons against the
ADR §2 schema.

### 5.1 `build_row_block(flags) -> dict`

102. Default flags → `{"description_matched": False, "multi_hop_description": False, "cp_iban_status": "null"}`.
103. All-true flags + `cp_iban_status="honest"` → `{"description_matched": True, "multi_hop_description": True, "cp_iban_status": "honest"}`.
104. Each `cp_iban_status` value (`"null"`, `"transitive"`, `"external"`,
    `"honest"`) round-trips into the JSON unchanged. Parametrize.
105. The output dict has **exactly** three keys (catch accidental field
    additions or removals via `set(block.keys()) == {"description_matched",
    "multi_hop_description", "cp_iban_status"}`).
106. The output dict is JSON-serializable (`json.dumps` round-trip equals
    the input).

### 5.2 `build_pair_block(iban_evidence, description_decisive) -> dict`

107. `("bilateral", False)` → `{"iban_evidence": "bilateral", "description_decisive": False}`.
108. `("unilateral", True)` → `{"iban_evidence": "unilateral", "description_decisive": True}`.
109. `("none", False)` → `{"iban_evidence": "none", "description_decisive": False}`.
110. `("none", True)` → `{"iban_evidence": "none", "description_decisive": True}` (rare but valid combination — none-evidence claim with description tiebreaker doesn't happen, but the builder doesn't reject it; the decision module is responsible for not constructing that combination).
111. The output dict has **exactly** two keys.
112. JSON round-trip identity.

---

## 6. Unit tests — anomaly builders (`tests/unit/test_transfer_anomalies.py`)

Each builder returns an `AnomalyRecord` with the right `reason_code` and a
formatted `reason_detail` string carrying the inputs. These tests don't
read the detail too literally — they assert the code is correct, the
candidate IDs are passed through, and the detail mentions the inputs that
should be human-debuggable.

### 6.1 `unpaired_from_description` builder

113. Returns `reason_code="unpaired_from_description"`,
     `candidate_ids=[]`, and `reason_detail` mentions the description.
114. Idempotent: same inputs → equal `AnomalyRecord` (frozen dataclass).

### 6.2 `unpaired_to_description` builder

115. `reason_code="unpaired_to_description"`, `candidate_ids=[]`, detail
     mentions description.

### 6.3 `description_consistency_mismatch` builder (canary)

116. `reason_code="description_consistency_mismatch"`,
     `candidate_ids=[partner_id]`, detail mentions both descriptions and
     both account property summaries (so a debugger can diff them).
117. Builder accepts the partner_id (the pair was claimed before this
     anomaly was constructed — confirms the slice's "anomaly recorded
     after claim" timing).

### 6.4 `description_account_mismatch` builder

118. `reason_code="description_account_mismatch"`, `candidate_ids` lists
     all rejected candidates from the bucket, detail mentions the
     incoming description and the bucket's evidence level.

### 6.5 `ambiguous_pair_match` builder

119. `reason_code="ambiguous_pair_match"`, `candidate_ids` lists the
     pre-filter bucket members, detail mentions the bucket's evidence
     level (`bilateral` / `unilateral` / `none`) and the surviving count
     after description filter.
120. The builder distinguishes "0 survivors with evidence ≥unilateral"
     from ">1 survivors" in the detail string (so an operator can tell
     them apart without re-querying).

---

## 7. Unit tests — `decide()` (`tests/unit/test_transfer_decision.py`)

The count-and-decide branch + bucket-locked principle. Pure function
`decide(candidates, incoming_flags, incoming_acc_props, account_props_by_id) -> Decision`
returning a `Claim` / `Anomaly` / `Skip` discriminated union.

Use the helper factories from §1.2 to build candidates and account props.
Validation calls into `validate_pair_descriptions` from `descriptions.py`
— don't mock it; let the real function run on the descriptions you set up.

### 7.1 `count == 0`

121. No candidates, incoming description has no transfer prefix → `Skip`,
     no anomaly, no claim.
122. No candidates, incoming description has "З " prefix →
     `Anomaly(unpaired_from_description)`.
123. No candidates, incoming description has "На " prefix →
     `Anomaly(unpaired_to_description)`.
124. No candidates, incoming description = "Переказ на картку" → `Skip`
     (per ADR §6, count==0 emits unpaired only on prefix; the explicit
     allowed phrase doesn't trigger).

### 7.2 `count == 1` (canary path)

125. Single candidate, descriptions consistent → `Claim(partner_id,
     iban_evidence=<the candidate's level>, description_decisive=False)`.
126. Single candidate, descriptions inconsistent → `Claim(...)` with the
     same partner AND a `description_consistency_mismatch` anomaly
     attached. Pair still claims (canary semantics).
127. Single candidate with `iban_evidence="bilateral"` → claim, evidence
     surfaces in the Claim's pair_evidence field.
128. Single candidate with `iban_evidence="unilateral"` → claim.
129. Single candidate with `iban_evidence="none"` → claim. (The detector
     doesn't reject `none`-evidence claims at count==1; description has
     already validated.) Reasoning: a single survivor of consistency
     filtering with valid descriptions is enough.
129b. Single candidate with `iban_evidence="none"` AND descriptions
     INCONSISTENT → `Claim(partner_id, iban_evidence="none",
     description_decisive=False)` AND
     `description_consistency_mismatch` canary attached. The combination
     is the riskiest claim profile (no IBAN signal, descriptions
     disagree) but the count==1 path still proceeds — the spec is
     deliberate about this: count==1 means the universal fetch + hard
     consistency filter narrowed the candidate set to one row, which is
     a strong enough signal on its own. The canary is the operator's
     drift indicator for this combination — sustained spikes in
     `none`-evidence canaries would suggest the algorithm needs a
     stronger gate.

### 7.3 `count > 1` — bucket selection (the bucket-locked principle)

The principle: partition by evidence level; commit to the highest
non-empty bucket; never fall through to a weaker bucket if the
description filter on the higher bucket fails.

130. Two candidates, both `bilateral`, both descriptions valid → after
     description filter still 2 → `Anomaly(ambiguous_pair_match,
     candidate_ids=[both], detail mentions "bilateral")`. No claim. **Do
     not fall through to unilateral or none.**
131. Two candidates, both `bilateral`, description filter eliminates one
     → `Claim(survivor_id, iban_evidence="bilateral",
     description_decisive=True)`.
132. Two candidates, both `bilateral`, description filter eliminates BOTH
     → `Anomaly(ambiguous_pair_match, ...)` with detail noting evidence
     level "bilateral" and "description filter eliminated all". **Do
     not fall through.**
133. Mixed bucket: 1 `bilateral`, 2 `unilateral`. Bucket-locked picks
     `bilateral`. Single bilateral candidate → branches to count==1 path
     → `Claim(bilateral_id, iban_evidence="bilateral",
     description_decisive=False)`. The two unilateral candidates are
     irrelevant — they were never considered.
133b. **All three buckets non-empty**: 1 `bilateral`, 2 `unilateral`, 3
     `none` candidates. Bucket-locked picks `bilateral` and claims it
     (count==1 in the chosen bucket). The 5 candidates in the lower
     buckets are NEVER passed to `validate_pair_descriptions` and NEVER
     considered for the claim. This pins the priority order
     `bilateral > unilateral > none` AND verifies the implementation
     short-circuits past lower buckets after picking the highest
     non-empty one.
134. Mixed bucket: 0 `bilateral`, 2 `unilateral`. Bucket-locked picks
     `unilateral`. Description filter eliminates one → `Claim(survivor,
     iban_evidence="unilateral", description_decisive=True)`.
135. Mixed bucket: 0 `bilateral`, 0 `unilateral`, 3 `none`. Bucket-locked
     picks `none`. Description filter eliminates all 3 →
     `Anomaly(description_account_mismatch, ...)` (special-cased: with
     `none` evidence, "all candidates failed description" maps to
     description_account_mismatch, not ambiguous_pair_match).
136. Mixed bucket: 0 `bilateral`, 0 `unilateral`, 2 `none`. Description
     filter survives both → `Anomaly(ambiguous_pair_match, ...)` with
     detail noting evidence "none". (The bucket-locked principle still
     applies: 2 surviving none-evidence candidates is still ambiguous.)
137. Mixed bucket: 0 `bilateral`, 0 `unilateral`, 1 `none`. → `Claim`
     with `iban_evidence="none"`.
138. **Bucket-locked invariant under description failure**: 1
     `bilateral`, 1 `unilateral`. Bucket-locked picks `bilateral`.
     Description filter eliminates the bilateral candidate →
     `Anomaly(ambiguous_pair_match)`. **Do not** then look at the
     unilateral candidate. The unilateral candidate's description-
     compatibility is irrelevant — the algorithm committed to
     `bilateral`.

### 7.4 `Decision` discriminated union shape

139. `Claim` carries `partner_id: UUID`, `iban_evidence: Literal[...]`,
     `description_decisive: bool`, optional `canary_anomaly: AnomalyRecord | None`.
140. `Anomaly` carries `record: AnomalyRecord` (single) — there's never a
     count==1 path that returns `Anomaly` (count==1 always claims, with
     optional canary). `Anomaly` only appears for count==0 with prefix or
     count>1 with empty/multi survivors.
141. `Skip` is a singleton sentinel; no fields. Returned only for
     count==0 without prefix.
142. The discriminated union is a frozen dataclass / sealed class — adding
     a fourth variant is a deliberate decision, not an accidental tuple.

---

## 8. Unit tests — `TransferResult` (`tests/unit/test_transfer_result.py`)

The strategy's return type. Frozen dataclass.

143. Default `TransferResult()` has `special_category=None`,
     `related_transaction_id=None`, `anomalies=[]`, `metadata_block=None`.
144. Successful claim: `TransferResult(special_category="transfer",
     related_transaction_id=UUID, anomalies=[], metadata_block={"row":
     {...}, "pair": {...}})`.
145. Successful claim with canary: same as 144 but `anomalies=[<canary>]`.
146. Anomaly only (no claim): `TransferResult(special_category=None,
     related_transaction_id=None, anomalies=[<anomaly>],
     metadata_block={"row": {...}})`.
147. Skip on non-MCC-4829: `TransferResult(special_category=None,
     related_transaction_id=None, anomalies=[], metadata_block=None)`
     (block is `None`, not `{}`, so the orchestrator can use `is None` to
     decide whether to write `metadata.layer.transfer` at all).
148. Frozen — attempting to mutate any field raises
     `dataclasses.FrozenInstanceError`.
149. Equality is structural (two `TransferResult` instances with the same
     fields compare equal).

---

## 9. Integration tests — universal fetch (`tests/integration/test_transfer_universal_fetch_integration.py`)

`transfer_repo.find_universal_candidates` against real Postgres. Verifies
the SQL filter list works as advertised: user scope, opposite direction,
MCC 4829, account exclusion, unclaimed, time window, two-clause amount
predicate. Locking semantics are tested in the dedicated claim-locks file
(§14).

### 9.1 Filter behavior

150. **Returns 0 when no row matches**: insert nothing on the partner
     side. `find_universal_candidates` returns `[]`.
151. **Returns 1 when one row matches all predicates**: insert a single
     unclaimed opposite-direction MCC-4829 row in window with matching
     amount → returns 1 row.
152. **Returns >1 when multiple rows match**: insert two qualifying
     candidates → returns 2.
153. **Excludes claimed rows**: insert a row with
     `related_transaction_id IS NOT NULL` → not returned.
154. **Excludes wrong direction**: insert opposite candidate but with
     same direction as incoming → not returned.
155. **Excludes wrong MCC**: insert candidate with MCC 5411 → not
     returned.
156. **Excludes incoming's own account**: insert candidate on same
     account as incoming (`account_id != self`) → not returned.
157. **Excludes wrong user**: insert candidate under different `user_id`
     → not returned.

### 9.2 Time window boundaries

158. Candidate at exactly `T + 2s` → returned.
159. Candidate at exactly `T - 2s` → returned.
160. Candidate at `T + 2.001s` → not returned.
161. Candidate at `T - 2.001s` → not returned.
162. Candidate at `T + 3s` → not returned.

### 9.3 Two-clause amount predicate (the asymmetry fix)

The predicate is `cand.amount_cents = incoming.op_amount OR
cand.operation_amount_cents = incoming.amount`. Tests verify both clauses
contribute and that the OR doesn't accidentally widen to unrelated rows.

163. **Same-currency identity** (incoming `amount=op_amount=5000`): both
     clauses become `cand.amount = 5000 OR cand.op_amount = 5000` —
     candidate with `amount=5000, op_amount=5000` matches via clause 1
     (and harmlessly via clause 2 too).
164. **Asymmetric pair, expense side incoming** (incoming USD FOP expense
     `amount=50000` USD-cents, `op_amount=2190000` UAH-cents). Candidate
     UAH FOP income `amount=2190000`, `op_amount=2190000`. Clause 1
     matches (`cand.amount=2190000 = incoming.op_amount=2190000`).
165. **Asymmetric pair, income side incoming** (incoming UAH FOP income
     `amount=2190000`, `op_amount=2190000`). Candidate USD FOP expense
     `amount=50000`, `op_amount=2190000`. Clause 1 misses
     (`cand.amount=50000 ≠ incoming.op_amount=2190000`); **clause 2
     matches** (`cand.op_amount=2190000 = incoming.amount=2190000`).
     This is the case that requires the second clause.
166. **No match either clause** → returned set excludes the row even if
     time/MCC/direction qualify.
167. **`incoming.operation_amount_cents IS NULL`** (the detector falls
     back to `incoming.amount_cents` per ADR §3 SQL): match still works.
     Insert candidate with `amount_cents = incoming.amount_cents`. Returned.
167b. **End-to-end claim under NULL `operation_amount_cents`**: extend
     case 167 through the full strategy. Run `detect_and_pair` on the
     incoming row; verify the pair claims, both legs get
     `special_category='transfer'`, and `metadata.layer.transfer.pair`
     is written with the right `iban_evidence` level. Also verify the
     candidate's own `operation_amount_cents` (which may also be NULL)
     doesn't break the claim or the metadata block. This ensures the
     fallback handling holds across the full algorithm pass, not just
     the SQL fetch.

### 9.4 Returned row shape

168. The returned `CandidateRow` records carry exactly the columns the
     decision module needs: `id`, `account_id`, `direction`,
     `counterparty_iban`, `description`, `amount_cents`,
     `operation_amount_cents`, `time`. (Other columns like `metadata`,
     `cashback_amount_cents` are not selected — the candidate processing
     doesn't need them.)

---

## 10. Integration tests — consistency filter (`tests/integration/test_transfer_consistency_filter_integration.py`)

Full strategy run end-to-end via `transfer_service.detect_and_pair`.
Verifies the consistency filter drops the right candidates after the
universal fetch.

For each scenario: seed accounts, insert the candidate row(s), run the
strategy on the incoming `NormalizedTransaction`, assert the resulting
`TransferResult` (claim happened or not, which partner if any).

169. **Both `null`** → universal fetch returns the candidate; consistency
     filter passes vacuously; if descriptions consistent → claim with
     `iban_evidence="none"`.
170. **Both `honest` and pointing at each other** → consistency passes;
     `iban_evidence="bilateral"`; claim.
171. **Incoming `honest` pointing at candidate; candidate `null`** →
     consistency passes; `iban_evidence="unilateral"`; claim.
172. **Incoming `honest` pointing at WRONG account** → consistency drops
     the candidate; no claim; `Skip` outcome (no anomaly, since
     description prefix logic still applies independently).
173. **Candidate `honest` pointing at WRONG account** → consistency drops;
     no claim.
174. **Candidate `external`** (cp_iban set, doesn't resolve to any own
     account) → ALWAYS dropped, regardless of incoming's status.
     Parametrize over incoming `null` / `transitive` / `honest` to cover
     the "always" claim explicitly. (Cases 174a, 174b, 174c.)
175. **Incoming `transitive` (multi-hop expense), candidate `honest`
     pointing at incoming** → consistency passes; the suppressed
     incoming IBAN doesn't contradict; `iban_evidence="unilateral"`;
     claim. This is the multi-hop chain case.

---

## 11. Integration tests — decision (`tests/integration/test_transfer_decision_integration.py`)

End-to-end strategy runs covering the count-and-decide branch against
real DB. Mirrors §7 (unit) but at the DB level so the actual SQL claim
+ partner UPDATE happens.

### 11.1 `count == 0`

176. No candidates, description "Олена К." → no claim, no anomaly,
     incoming row inserted normally (handled by orchestrator, not the
     strategy itself, but verified through the pipeline integration test
     in §16).
177. No candidates, description "З Чорної картки" →
     `unpaired_from_description` anomaly recorded.
178. No candidates, description "На білу картку" →
     `unpaired_to_description` anomaly recorded.

### 11.2 `count == 1`

179. Single candidate, descriptions consistent, `iban_evidence="bilateral"`
     → `claim_pair` issued; partner row's `related_transaction_id` set;
     `special_category='transfer'` on partner; `metadata.layer.transfer.pair`
     written on partner row with `iban_evidence="bilateral"`,
     `description_decisive=False`.
180. Single candidate, descriptions inconsistent → claim still issued;
     `description_consistency_mismatch` anomaly recorded with the
     partner's ID.
181. Single candidate, `iban_evidence="unilateral"` → claim; pair block
     reflects the level.
182. Single candidate, `iban_evidence="none"` → claim; pair block
     reflects level.

### 11.3 `count > 1` — bucket-locked behavior

183. Two `bilateral` candidates, both descriptions valid → no claim;
     `ambiguous_pair_match` anomaly recorded with both IDs and
     `iban_evidence: bilateral` in `reason_detail`.
184. Two `bilateral` candidates, description filter narrows to 1 → claim
     the survivor; `description_decisive=True` in the pair block;
     additional canary on the survivor still runs.
185. Two `bilateral` candidates, description filter narrows to 0 →
     `ambiguous_pair_match` anomaly with both IDs and detail mentioning
     "description filter eliminated all".
186. 1 `bilateral` + 2 `unilateral` candidates → claim the bilateral
     (bucket-locked); the two unilaterals are not even considered.
187. 0 `bilateral` + 2 `unilateral`, description narrows to 1 → claim
     with `iban_evidence="unilateral"`, `description_decisive=True`.
188. 0 `bilateral` + 2 `unilateral`, description narrows to 0 →
     `ambiguous_pair_match` (≥unilateral evidence + zero survivors).
189. 0 `bilateral` + 0 `unilateral` + 2 `none` candidates, description
     narrows to 1 → claim with `iban_evidence="none"`,
     `description_decisive=True`.
190. 0 `bilateral` + 0 `unilateral` + 3 `none`, description narrows to 0
     → `description_account_mismatch` (special case: `none`-evidence with
     full description rejection).
191. 0 `bilateral` + 0 `unilateral` + 2 `none`, both pass description →
     `ambiguous_pair_match` (still ambiguous even at `none` level).

---

## 12. Integration tests — metadata block invariant (`tests/integration/test_transfer_metadata_block_integration.py`)

The load-bearing invariant: `metadata.layer.transfer exists ⇔ mcc == '4829'`.
Plus the `pair` sub-block being present iff a claim happened, and
identical on both legs.

192. **Non-MCC-4829 row** (e.g. MCC 5411 grocery): run the orchestrator;
     verify `get_transfer_metadata(conn, tx.id) is None`. The
     `metadata.layer.transfer` key must be absent (not present-with-null).
193. **MCC 4829 unpaired row** (no candidates found, no transfer prefix
     in description): metadata block exists with `row` only;
     `block["pair"]` raises `KeyError` (or `block.get("pair") is None`).
194. **MCC 4829 paired row, both legs**: claim a pair end-to-end. Query
     both rows. Both have `row` (with their own per-leg flags — they
     differ between income and expense leg) and identical `pair` blocks
     (byte-equal `iban_evidence` and `description_decisive`). Use
     `json.loads` round-trip for the equality so whitespace and key order
     don't matter.
195. **MCC 4829 anomaly row** (claim rejected by ambiguity or description
     mismatch): metadata block exists with `row` only; no `pair`
     sub-block.
196. **External short-circuit row** (`cp_iban_status=external`, known
     description): metadata block exists with `row`; the `row` carries
     `cp_iban_status="external"`. No `pair`. (The block is still written
     as required by the invariant.)

### 12.1 Per-leg `row` content

197. **Income and expense legs of a paired transfer have different `row`
     content**: income leg has `description_matched` / `cp_iban_status`
     reflecting the income's description and IBAN; expense leg has
     values for its own. (The `row` content is per-tx; only `pair` is
     per-pair.)

### 12.2 Defensive: `jsonb_set` against missing intermediate keys

The orchestrator and `claim_pair` both write into `metadata.layer.transfer.*`.
Postgres `jsonb_set` does not auto-create missing intermediate keys
unless `create_missing => true` is passed (default true in standard
overload but worth pinning). These cases verify the SQL path is
defensive against partial metadata.

197b. **Partner row `metadata IS NULL` at claim time**: insert a partner
     row with `metadata = NULL` (use explicit NULL in INSERT, not omitted
     column). Run the strategy on the incoming pair so `claim_pair`
     fires its UPDATE on the partner. Verify the partner row's
     `metadata` is now a well-formed object containing
     `metadata.layer.transfer.pair = {...}` (no SQL exception, no
     truncated structure). The `claim_pair` SQL must use
     `jsonb_set(COALESCE(metadata, '{}'::jsonb), ...)` or equivalent.
197c. **Partner row `metadata = '{}'` (empty object) at claim time**:
     same as 197b but with empty object instead of NULL. After claim,
     `metadata.layer.transfer.pair = {...}` is present.
197d. **Partner row `metadata = {"source": {...}}` at claim time**
     (has `source` namespace but no `layer` key): `claim_pair` must
     create the `layer.transfer.pair` path WITHOUT clobbering
     `metadata.source`. Verify post-claim: `metadata.source` is
     byte-identical to pre-claim, `metadata.layer.transfer.pair` is
     present.

---

## 13. Integration tests — directional transitive rule (`tests/integration/test_transfer_directional_transitive_integration.py`)

The Monobank quirk: multi-hop expense IBANs point at the chain end, not
the immediate partner. The classifier suppresses them as `transitive`.
Multi-hop income IBANs are honest. Verifies the rule fires correctly
under realistic chains.

### 13.1 Directional rule on individual rows

198. **Multi-hop expense with chain-end IBAN** (USD FOP expense, desc "На
     гривневий рахунок ФОП для переказу на картку", `cp_iban` set to
     UAH_BLACK.iban — the final destination, not the immediate UAH FOP
     partner): `metadata.layer.transfer.row.cp_iban_status = "transitive"`.
199. **Multi-hop income with honest IBAN** (UAH FOP income, desc "З
     доларового рахунку ФОП для переказу на картку", `cp_iban` set to
     USD_FOP.iban — the immediate partner): `cp_iban_status = "honest"`.
200. **Non-multi-hop expense with honest IBAN** (FOP UAH expense, desc
     "На чорну картку", `cp_iban` set to UAH_BLACK.iban — direct, not
     multi-hop): `cp_iban_status = "honest"`. The multi-hop rule does
     NOT fire.
201. **Non-multi-hop income with honest IBAN**: `cp_iban_status = "honest"`.

### 13.2 4-leg multi-hop chain (USD FOP → UAH FOP → UAH card)

The realistic chain has 4 transactions: USD FOP expense, UAH FOP income,
UAH FOP expense, UAH card income. Two pairs: (USD FOP exp, UAH FOP inc)
and (UAH FOP exp, card inc).

202. **Process all 4 in order** (1, 2, 3, 4):
     - After leg 2: legs 1+2 paired. Pair 1 evidence: `unilateral`
       (income side honest, expense side transitive — its IBAN points at
       the card endpoint, not at UAH FOP).
     - After leg 4: legs 3+4 paired. Pair 2 evidence: `bilateral` if
       both have IBANs, else `unilateral` depending on whether card
       income carries a cp_iban.
     - Verify all 4 transactions have `special_category='transfer'` and
       cross-referencing `related_transaction_id`.
     - Verify two distinct pairs (4 transactions, 2 pair groupings).

203. **Process in reverse order (4, 3, 2, 1)**: same final state. Late
     arrivals find earlier rows. Auto-resolve fires on any
     `unpaired_*_description` from earlier insertions.

204. **Process out of order (2, 4, 1, 3)**: stress the auto-resolve and
     find-via-late-arrival paths. Final state: 2 pairs claimed, 0
     anomalies remaining.

---

## 14. Integration tests — external short-circuit (`tests/integration/test_transfer_external_short_circuit_integration.py`)

`cp_iban_status == 'external'` skips the universal fetch entirely (per
ADR §3a). Three sub-branches based on description.

205. **External + known description** (P2P expense to a friend's bank,
     desc "На білу картку" but `cp_iban` is an external IBAN): no
     candidate fetch issued (assert by inspecting query log or by
     observing that no row in the DB could have been claimed even if
     present); `metadata.layer.transfer.row` written with
     `cp_iban_status="external"`; **no anomaly**. The user has
     implicitly opted out by not linking the destination account.
206. **External + prefix-but-unknown description** ("З нового банку",
     `cp_iban` is external): `unpaired_from_description` anomaly
     recorded (vocabulary-drift signal). The external status doesn't
     suppress this signal — it's exactly the case we want to surface.
207. **External + no transfer prefix** ("Олена К.", `cp_iban` is
     external): no anomaly, no claim, metadata block written with
     `cp_iban_status="external"` for traceability.
208. **External + multi-hop description** ("На гривневий рахунок ФОП для
     переказу на картку", `cp_iban` is external — wouldn't normally
     happen but defensive case): the directional transitive rule fires
     FIRST (`cp_iban_status="transitive"`, NOT `"external"`), because
     the rule check `direction=expense AND multi_hop_description`
     happens before the lookup branch. So this case lands in the
     non-external path. Verify by inspecting the row's
     `cp_iban_status` after processing.

---

## 15. Integration tests — amount asymmetry (`tests/integration/test_transfer_amount_asymmetry_integration.py`)

The two-clause amount predicate. Webhook arrival order can put either
leg first. With the two-clause OR, both arrival orders find their
partner.

209. **USD FOP expense arrives first; UAH FOP income arrives second**:
     - USD FOP expense `amount=50000` (USD-cents), `op_amount=2190000`
       (UAH-cents). Inserted unpaired (no UAH FOP yet).
     - UAH FOP income `amount=2190000`, `op_amount=2190000`. Universal
       fetch with two-clause predicate finds the USD FOP expense via
       clause 2 (`cand.op_amount = incoming.amount = 2190000`). Pair
       claims.
210. **UAH FOP income arrives first; USD FOP expense arrives second**:
     - UAH FOP income inserted unpaired.
     - USD FOP expense arrives. Fetch finds UAH FOP income via clause 1
       (`cand.amount = incoming.op_amount = 2190000`). Pair claims.
211. **Same-currency direct pair** (control case, no asymmetry): both
     legs have `amount = op_amount = 5000`. Either arrival order
     pairs successfully. Confirms the asymmetry fix doesn't break
     the simple case.
212. **Asymmetric pair under bucket-locked**: 227 white-card-income rows
     in the user's history exhibit the asymmetric pattern AND have
     unilateral IBAN evidence (income side IBAN points at white card).
     Construct a scenario: incoming UAH FOP income has 2 candidates
     under the two-clause predicate — its UAH FOP expense partner
     (unilateral, IBAN points correctly) AND its USD FOP expense
     sibling (transitive, IBAN suppressed). Bucket-locked picks the
     unilateral. Claim succeeds with the right partner. Critical
     regression test for the asymmetry fix not introducing false pairs.

---

## 16. Integration tests — anomaly lifecycle (`tests/integration/test_transfer_anomaly_integration.py`)

Cascade behavior, UNIQUE constraint, auto-resolve.

### 16.1 Auto-resolve on successful claim

213. **`unpaired_from_description` deleted on partner arrival**: insert
     leg A with prefix, no partner → anomaly recorded. Insert leg B
     that pairs with A. After claim: A's anomaly is gone.
214. **`unpaired_to_description` deleted on partner arrival**:
     symmetric.
215. **Both legs had unpaired anomalies → both deleted on pair**:
215b. **Auto-resolve + canary in the same claim**: insert leg A with
     prefix description (`unpaired_from_description` recorded). Insert
     leg B with prefix description (`unpaired_to_description` recorded).
     Then run the strategy on a third event whose payload re-presents
     leg B (e.g. via Kafka redelivery without prior idempotency dedup —
     for this test, simulate by deleting the anomaly only and replaying
     B) such that the strategy now finds A as the partner with
     description-mismatched validation. Verify final state: both
     unpaired anomalies on A and B are deleted, AND a single
     `description_consistency_mismatch` canary anomaly is recorded
     against the incoming event (the leg that triggered the claim).
     Pinning the order: auto-resolve runs after `claim_pair` succeeds;
     the canary is recorded as part of the same `TransferResult`. So
     the final anomaly count for the user is exactly 1 (the canary), not
     0 (only deletes happened) and not 3 (deletes + canary + duplicate).
216. **Terminal anomaly persists across re-runs**: insert tx + record
     `ambiguous_pair_match`. Re-process unrelated events. Verify the
     anomaly row still exists.
217. **`description_consistency_mismatch` is terminal** (canary; pair
     was claimed, anomaly is a drift signal — it stays). Verify
     across re-runs.

### 16.2 Schema constraints

218. **UNIQUE on transaction_id**: insert anomaly for tx A; attempt to
     insert another for the same tx → constraint violation (loud).
     Confirms the production code's `ON CONFLICT (transaction_id) DO
     NOTHING` is the right idempotent path.
219. **Anomaly CASCADE on transaction DELETE**: insert tx + anomaly,
     DELETE tx → anomaly row also gone (`ON DELETE CASCADE`).

### 16.3 Pair break behavior

220. **Deleting a paired transaction nulls the partner's
     related_transaction_id but preserves `special_category='transfer'`
     and `metadata.layer.transfer.pair`**. (SET NULL on FK column only.)
     Verify partner becomes a candidate again for any future fetch
     (`related_transaction_id IS NULL`).

---

## 17. Integration tests — claim locks (`tests/integration/test_transfer_claim_locks_integration.py`)

Real `FOR UPDATE SKIP LOCKED` semantics. Use two `asyncpg` connections
on separate transactions to simulate concurrency.

221. **Single connection claims partner successfully**: standard
     happy-path lock acquire + UPDATE + COMMIT.
222. **Two connections race for the same partner**: conn1 begins,
     `find_universal_candidates` returns the partner (locks it), holds
     the transaction open (no commit yet). conn2 calls
     `find_universal_candidates` → returns 0 rows (SKIP LOCKED). conn1
     commits → pair claimed. conn2 has already moved on to insert
     unpaired (or, depending on implementation, will retry on the next
     event).
223. **Locked row released on rollback → next query finds it**: conn1
     locks then ROLLBACK. conn2's next fetch returns the row.
224. **Advisory lock interplay** (Slice 16 prerequisite): conn1
     simulates the pipeline consumer holding `pg_advisory_xact_lock`
     during a transaction. conn2 simulates the reprocess job — its
     `pg_advisory_lock` blocks until conn1 commits.

---

## 18. Integration tests — full pipeline (`tests/integration/test_transfer_pipeline_integration.py`)

End-to-end runs through `PipelineOrchestrator.run()` against real DB.
Confirms the strategy plus orchestrator merge contract plus persistence
all work together.

### 18.1 Successful claim + currency conversion + persistence + metadata wiring

225. **FOP→card transfer, both legs through the pipeline**: seed
     accounts, currency rates. Process FOP expense; then process card
     income. Query DB: both rows have `special_category='transfer'`,
     cross-referencing `related_transaction_id`, `amount_uah_cents`
     populated, `metadata.layer.rate.uah` set (currency conversion ran),
     `metadata.layer.transfer.row` set on both, `metadata.layer.transfer.pair`
     identical on both (verifying the orchestrator merge wrote the block
     correctly AND `claim_pair` wrote it on the partner UPDATE).

### 18.2 Non-claim paths still produce correct rows

226. **External P2P expense** (person name, no IBAN, no candidates):
     persisted as plain expense; no `related_transaction_id`; no
     anomaly; `metadata.layer.transfer.row` set (because MCC 4829);
     no `pair` sub-block.
227. **Non-MCC-4829 transaction** (grocery, MCC 5411): persisted
     normally; `metadata.layer.transfer` key entirely absent (the
     invariant). Transfer detection effectively a no-op.

### 18.3 Idempotency through the full pipeline

228. **Duplicate event delivered twice (same `id`)**: first run claims
     and persists; second run is a no-op (pipeline-level
     `ON CONFLICT (id) DO NOTHING` + strategy-level `exists()` guard).
     Only one row in DB; no duplicate anomaly; no double-claim.
229. **Re-delivery after pair claim → no double-claim**: A and B
     paired. Re-deliver A. Strategy's `exists()` guard returns early.
     No-op.
229b. **Reprocess re-publishes a previously-claimed event** (Slice 16
     prerequisite scenario): A and B were paired in steady-state. A
     reprocess job runs: it deletes A and B (CASCADE clears the
     anomalies, SET NULL clears `related_transaction_id`), then
     re-publishes A and B to `normalized_transactions` for fresh
     pipeline processing. Verify: when A is re-processed (it has the
     same deterministic UUID5 id), the orchestrator's
     `ON CONFLICT (id) DO NOTHING` does NOT fire (the row was deleted
     and is no longer in the DB), so the strategy runs. The strategy's
     `exists()` guard correctly returns False (row was deleted). The
     strategy proceeds, finds B (also deleted-then-re-published) as a
     candidate when B's event arrives. Pair is re-claimed. Final state:
     A and B paired again (possibly with the same or a different
     `description_decisive` flag, depending on whether the candidate
     set has changed under the user — both outcomes are documented as
     "current state, not historical" per ADR §2). No duplicate anomaly,
     no orphan state. This pins the strategy's idempotency contract:
     `exists()` reflects DB state at strategy-call time, not "did this
     event ever exist."

### 18.4 Source dispatch

230. **Monobank source**: `tx.source="monobank"` → dispatches to
     `MonobankTransferDetection`. Strategy runs.
231. **Manual source**: `tx.source="manual"` → no strategy registered
     → strategy step is a no-op. Row persisted with
     `special_category=NULL` and **no** `metadata.layer.transfer` block
     (the orchestrator only writes the block when the strategy returns
     a non-None `metadata_block`).
232. **Unknown source**: `tx.source="revolut"` → no strategy in
     registry → INFO log, pipeline continues, no error.

---

## 19. Test hygiene

- Every async test uses `pytest.mark.asyncio` (or module-level
  `pytestmark = pytest.mark.asyncio`).
- Integration tests never share state — per-test transaction rollback.
- Use `caplog` for log assertions. Never parse stdout.
- UUID comparisons use `==`, not string conversion.
- Parametrize when it compresses (description tables in §2, the 4×4
  consistency matrix in §4.1, the cp_iban_status × multi_hop cells in
  §3.2). Otherwise keep tests explicit.
- Pure-function unit tests (sections 2–8) take plain dataclasses and
  return plain dataclasses — **no `AsyncMock`, no `MagicMock`, no fakes
  for these layers**. Mock only at the strategy boundary (`detect_and_pair`
  in pipeline tests if the test focuses on orchestrator behavior, not
  strategy behavior).
- Snapshot-style assertions (sections 5, 6) compare full dicts/dataclasses
  with `==`, not field-by-field, so accidental field additions or
  removals fail loudly.
- All Cyrillic test descriptions live in the test file as-is (UTF-8
  source). No escape sequences.

---

## 20. Coverage target

```
pytest --cov=grosh_consumer.sources.monobank.transfer \
       --cov=grosh_consumer.sources.monobank.descriptions \
       --cov=grosh_consumer.services.transfer_detection \
       --cov=grosh_consumer.repositories.transfer_repo \
       --cov=grosh_consumer.repositories.anomaly_repo \
       --cov=grosh_consumer.repositories.account_property_repo \
       --cov-branch
```

Targets: ≥ 95% line, ≥ 90% branch on `sources/monobank/transfer/*` and
`repositories/transfer_repo.py`. Uncovered lines explained in PR
description.

The pure-function modules (`flags`, `iban_classifier`, `decision`,
`metadata`, `anomalies`) should hit 100% line + branch — they have no
IO, no try/except surface, and the unit tests parametrize the full
input domain.
