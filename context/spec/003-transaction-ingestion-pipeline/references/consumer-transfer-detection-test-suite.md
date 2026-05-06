# Transfer Detection — Test Specification

Implementation instructions for unit and integration tests covering
`MonobankTransferDetection` strategy, description guard validation,
anomaly recording, and pipeline integration.

Reference `adr-transfer-detection.md` for behavioral context. This
document is authoritative for test structure and assertions.

## 1. Test infrastructure

### 1.1 File layout

```
services/consumer/tests/
  conftest.py                                          # existing — DB pool, conn fixture
  helpers.py                                           # existing — make_event()
  unit/
    conftest.py                                        # existing — extend with transfer helpers
    test_description_guard.py                          # description parsing + account validation
    test_transfer_result.py                            # TransferResult model
    test_transfer_service_tier_a.py                    # Tier A with in-memory repos
    test_transfer_service_tier_b.py                    # Tier B with in-memory repos
    test_transfer_service_tier_c.py                    # Tier C with in-memory repos
    test_transfer_service_anomalies.py                 # anomaly recording across tiers
    test_transfer_service_idempotency.py               # duplicate/re-delivery handling
    test_transfer_service_multi_hop.py                 # multi-hop chain scenarios
    test_transfer_service_claim_locks.py               # concurrent claim simulation
  integration/
    conftest.py                                        # existing — extend with account/tx helpers
    test_transfer_tier_a_integration.py                # Tier A against real Postgres
    test_transfer_tier_b_integration.py                # Tier B against real Postgres
    test_transfer_tier_c_integration.py                # Tier C against real Postgres
    test_transfer_anomaly_integration.py               # anomaly recording + auto-resolution
    test_transfer_claim_locks_integration.py           # FOR UPDATE SKIP LOCKED behavior
    test_transfer_pipeline_integration.py              # full pipeline orchestrator with transfer detection
```

### 1.2 Shared fixtures — unit tests (`tests/unit/conftest.py`)

**`InMemoryTransactionRepo`** — stores transactions as a list of dicts.
Does not touch `conn`. `conn` accepted for interface compatibility and
ignored.

Methods match production query semantics:

- `exists(conn, tx_id)` → `bool`: check if `id` already in store.
- `find_unclaimed_partner_tier_a(conn, user_id, target_account_id, opposite_type, time, window_seconds)` → list of tx dicts:
  - `mcc = 4829`, `related_transaction_id IS NULL`, `account_id = target_account_id`, `direction = opposite_type`, `|time - tx.time| <= window_seconds`.
  - Returns list (caller checks length for ambiguity).
- `find_unclaimed_partner_tier_b(conn, user_id, account_iban, opposite_type, time, window_seconds)` → list of tx dicts:
  - `mcc = 4829`, `related_transaction_id IS NULL`, `counterparty_iban = account_iban`, `direction = opposite_type`, `|time - tx.time| <= window_seconds`.
- `find_unclaimed_partner_tier_c(conn, user_id, operation_amount_cents, opposite_type, time, window_seconds)` → list of tx dicts:
  - `mcc = 4829`, `counterparty_iban IS NULL`, `related_transaction_id IS NULL`, `direction = opposite_type`, `operation_amount_cents = amount_cents` (the incoming tx's operation_amount matches candidate's amount), `|time - tx.time| <= window_seconds`, `account_id != incoming_account_id`.
- `claim_pair(conn, tx_id_a, tx_id_b)`: set `related_transaction_id` on both, set `special_category = 'transfer'` on both.
- `insert(conn, tx_dict)`: add to store.

Seed helpers:
- `add_transaction(**fields)` — insert with sensible defaults.

**`InMemoryAccountRepo`** — stores accounts as a list of dicts.

Methods:
- `find_by_iban(conn, iban, user_id)` → `UUID | None`
- `get_account(conn, account_id)` → account dict (with `type`, `currency_code`, `iban`)
- `get_user_accounts(conn, user_id)` → list of account dicts

Seed helpers:
- `add_account(id, user_id, type, currency_code, iban=None, **extras)`

**`InMemoryAnomalyRepo`** — stores anomalies as a list of dicts.

Methods:
- `record_anomaly(conn, transaction_id, candidate_ids, reason_code, reason_detail)`
- `delete_unpaired_anomalies_for_transactions(conn, tx_ids)` — remove only `unpaired_*` anomalies where `transaction_id IN tx_ids`; terminal anomalies are preserved
- `get_anomaly_for_transaction(conn, transaction_id)` → anomaly dict or None

### 1.3 Shared fixtures — integration tests (`tests/integration/conftest.py`)

Extend existing fixtures with:

- `insert_account(conn, *, user_id, type, currency_code, iban=None, source="monobank", **extras)` → UUID. INSERT into `accounts`, returns `id`.
- `insert_transaction(conn, *, id=None, user_id, account_id, time, amount_cents, operation_amount_cents=None, mcc=4829, direction, counterparty_iban=None, description=None, related_transaction_id=None, source="monobank", **extras)` → UUID. INSERT into `transactions`, returns `id`. Populates all required columns with sensible defaults.
- `get_transaction(conn, tx_id)` → Row. SELECT *, return full row.
- `get_anomaly(conn, transaction_id)` → Row or None. SELECT from `transfer_match_anomalies`.
- `count_anomalies(conn, user_id)` → int.

### 1.4 Test constants

```python
from datetime import UTC, datetime, timedelta

T = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
T_PLUS_1S = T + timedelta(seconds=1)
T_MINUS_1S = T - timedelta(seconds=1)
T_PLUS_2S = T + timedelta(seconds=2)
T_MINUS_2S = T - timedelta(seconds=2)
T_PLUS_3S = T + timedelta(seconds=3)  # outside window

WINDOW_SECONDS = 2
MCC_TRANSFER = 4829
MCC_GROCERY = 5411
```

Account fixtures (reused across tests):

```python
USER_ID = uuid4()

# Monobank accounts for the test user
UAH_FOP  = Account(id=uuid4(), user_id=USER_ID, type="fop",   currency_code="UAH", iban="UA111...")
USD_FOP  = Account(id=uuid4(), user_id=USER_ID, type="fop",   currency_code="USD", iban="UA222...")
EUR_FOP  = Account(id=uuid4(), user_id=USER_ID, type="fop",   currency_code="EUR", iban="UA333...")
UAH_BLACK = Account(id=uuid4(), user_id=USER_ID, type="black", currency_code="UAH", iban="UA444...")
UAH_WHITE = Account(id=uuid4(), user_id=USER_ID, type="white", currency_code="UAH", iban="UA555...")
EUR_CARD  = Account(id=uuid4(), user_id=USER_ID, type="black", currency_code="EUR", iban="UA666...")
```

---

## 2. Unit tests — description guard (`tests/unit/test_description_guard.py`)

Tests for the description parsing and account validation functions.
These are pure functions — no repos, no DB, no async.

### 2.1 Income description parsing

1. **"З гривневого рахунку ФОП" → `(type=fop, currency=UAH)`**
2. **"З доларового рахунку ФОП" → `(type=fop, currency=USD)`**
3. **"З єврового рахунку ФОП" → `(type=fop, currency=EUR)`**
4. **"З доларового рахунку ФОП для переказу на картку" → `(type=fop, currency=USD)`**
   — suffix stripped, same constraint as without suffix.
5. **"З єврового рахунку ФОП для переказу на картку" → `(type=fop, currency=EUR)`**
6. **"З Чорної картки" → `(type=black)`**
7. **"З Білої картки" → `(type=white)`**
8. **"З Платинової картки" → `(type=platinum)`**
9. **"З Залізної картки" → `(type=iron)`**
10. **"З Жовтої картки" → `(type=yellow)`**
11. **"З доларової картки" → `(currency=USD)`** — any card type.
12. **"З єврової картки" → `(currency=EUR)`** — any card type.

### 2.2 Expense description parsing

13. **"Переказ на картку" → no constraint** (generic transfer).
14. **"На чорну картку" → `(type=black)`**
15. **"На білу картку" → `(type=white)`**
16. **"На платинову картку" → `(type=platinum)`**
17. **"На залізну картку" → `(type=iron)`**
18. **"На жовту картку" → `(type=yellow)`**
19. **"На гривневий рахунок ФОП" → `(type=fop, currency=UAH)`**
20. **"На доларовий рахунок ФОП" → `(type=fop, currency=USD)`**
21. **"На євровий рахунок ФОП" → `(type=fop, currency=EUR)`**
22. **"На гривневий рахунок ФОП для переказу на картку" → `(type=fop, currency=UAH)`**
23. **"На доларовий рахунок ФОП для переказу на картку" → `(type=fop, currency=USD)`**
24. **"На євровий рахунок ФОП для переказу на картку" → `(type=fop, currency=EUR)`**

### 2.3 Unrecognized descriptions

25. **"Олена К." → None** — not a transfer description, not in allowed set.
26. **"516874****1234" → None** — masked card number.
27. **"Випуск іменної картки" → None** — bank fee.
28. **"" (empty string) → None**
29. **None → None**

### 2.4 Transfer-like prefix detection

30. **"З невідомого рахунку" → recognized as "З " prefix, but not in allowed set** — function returns parsed prefix but constraint is None (unrecognized pattern). Caller decides to record `unpaired_from_description`.
31. **"На невідому картку" → recognized as "На " prefix, not in allowed set** — same pattern.

### 2.5 Account validation

32. **Income "З Чорної картки" validates against `type=black` account → True**
33. **Income "З Чорної картки" validates against `type=white` account → False**
34. **Income "З гривневого рахунку ФОП" validates against `type=fop, currency=UAH` → True**
35. **Income "З гривневого рахунку ФОП" validates against `type=fop, currency=USD` → False**
36. **Income "З гривневого рахунку ФОП" validates against `type=black, currency=UAH` → False** — type mismatch.
37. **Income "З доларової картки" validates against `type=black, currency=USD` → True** — currency-only constraint.
38. **Income "З доларової картки" validates against `type=white, currency=USD` → True** — any card type.
39. **Income "З доларової картки" validates against `type=black, currency=EUR` → False** — currency mismatch.
40. **Expense "Переказ на картку" validates against any account → True** — no constraint.
41. **Expense "На чорну картку" validates against `type=black` → True**
42. **Expense "На чорну картку" validates against `type=fop` → False**
43. **Expense "На гривневий рахунок ФОП" validates against `type=fop, currency=UAH` → True**
44. **Expense "На гривневий рахунок ФОП" validates against `type=fop, currency=EUR` → False**

### 2.6 Cross-pair validation (both sides together)

45. **Card→card: income "З Чорної картки" + expense "Переказ на картку"**
   — income validates against expense's account (black card) → True.
   — expense validates against income's account (any) → True. Both pass.

46. **FOP→card: income "З гривневого рахунку ФОП" + expense "На чорну картку"**
   — income validates against expense's account (must be FOP UAH) → check expense is on FOP UAH.
   — expense validates against income's account (must be black) → check income is on black card.
   Wait — per the validation rule: income "З ..." names the SOURCE, validated against the EXPENSE leg's account. Expense "На ..." names the TARGET, validated against the INCOME leg's account.

   Scenario: FOP UAH expense → black card income.
   Income desc: "З гривневого рахунку ФОП" → constraint `(type=fop, currency=UAH)` → validate against the **expense** leg's account = FOP UAH → True.
   Expense desc: "На чорну картку" → constraint `(type=black)` → validate against the **income** leg's account = black card → True.
   Both pass → pair proceeds.

47. **Mismatch: income "З Білої картки" but expense is on FOP, not white card**
   — income constraint `(type=white)` checked against expense account `(type=fop)` → False.
   — pair rejected at Tier C. Record `description_account_mismatch`.

48. **One side generic, other side validates**
   — Expense "Переказ на картку" (no constraint) + income "З Чорної картки" (`type=black`) validated against expense account (black card) → True. Pair proceeds.

---

## 3. Unit tests — TransferResult model (`tests/unit/test_transfer_result.py`)

49. **Default TransferResult has special_category=None, no related_id, empty anomalies**
50. **TransferResult with transfer pair carries special_category='transfer' and related_transaction_id**
51. **TransferResult with anomaly carries special_category=None and anomaly list**
52. **TransferResult is immutable (frozen)**

---

## 4. Unit tests — Tier A (`tests/unit/test_transfer_service_tier_a.py`)

All tests use `InMemoryTransactionRepo`, `InMemoryAccountRepo`, `InMemoryAnomalyRepo`.
Default time `T`. MCC 4829 on all relevant transactions.

### 4.1 Basic IBAN matching

53. **Expense with counterparty_iban → own account, partner exists on target account**
   - UAH FOP expense at T, `counterparty_iban = UAH_BLACK.iban`.
   - Existing income on UAH_BLACK at T+1s, MCC 4829, unclaimed.
   - Result: both paired, `special_category = 'transfer'`, `related_transaction_id` set on both.

54. **Expense with counterparty_iban → own account, no partner at any tier**
   - UAH FOP expense at T, `counterparty_iban = UAH_BLACK.iban`.
   - No income on UAH_BLACK within ±2s (Tier A = 0).
   - No unclaimed tx with `counterparty_iban = UAH_FOP.iban` within ±2s (Tier B = 0).
   - No Tier C match (description guard or amount).
   - Result: insert normally as expense. No anomaly.

54b. **Tier A→B fallthrough: counterparty_iban points to non-immediate hop, Tier B finds partner**
   - USD FOP expense at T, `counterparty_iban = UAH_BLACK.iban` (final destination, not immediate hop).
   - Tier A: UAH_BLACK is an own account, but 0 unclaimed income on UAH_BLACK within ±2s → falls through.
   - Tier B: USD FOP has IBAN. Existing UAH FOP income at T-1s has `counterparty_iban = USD_FOP.iban` → 1 match.
   - Result: paired via Tier B. `special_category = 'transfer'`, `related_transaction_id` set on both.

55. **Income with counterparty_iban → own account, partner exists**
   - USD FOP income at T, `counterparty_iban = UAH_FOP.iban`.
   - Existing expense on UAH_FOP at T-1s, MCC 4829, unclaimed.
   - Result: paired.

56. **counterparty_iban → external account (not in user's accounts)**
   - Expense with `counterparty_iban = "UA999..."` (not any own IBAN).
   - Result: skip Tier A entirely. Falls through to Tier B/C.

### 4.2 Time window enforcement

57. **Partner exactly at +2s → paired**
   - Expense at T. Partner at T+2s. Within window. Paired.

58. **Partner exactly at -2s → paired**
   - Expense at T. Partner at T-2s. Within window. Paired.

59. **Partner at +3s → no match**
   - Expense at T. Partner at T+3s. Outside window. Not paired.

60. **Partner at -3s → no match**
   - Expense at T. Partner at T-3s. Outside window. Not paired.

### 4.3 Type matching

61. **Expense seeks income partner (opposite type)**
   - Expense on FOP at T. Two candidates on target account: one income at T+1s, one expense at T+1s.
   - Only income is returned as candidate. Paired.

62. **Income seeks expense partner**
   - Symmetric to above.

### 4.4 MCC filtering

63. **Partner has MCC 5411 (grocery) → not a candidate**
   - Expense MCC 4829 at T, partner income at T+1s on target account but MCC 5411.
   - No match found.

64. **Incoming tx has MCC != 4829 → transfer detection skipped entirely**
   - Transaction with MCC 5411. Transfer detection returns immediately with no changes.

### 4.5 Claim exclusion

65. **Partner already claimed (related_transaction_id != NULL) → not a candidate**
   - Expense at T. Partner income at T+1s, but `related_transaction_id` already set.
   - No unclaimed candidates found.

### 4.6 Ambiguity

66. **Two unclaimed partners on target account → ambiguous_iban_match**
   - Expense at T. Two income candidates at T+1s on target account, both unclaimed.
   - Result: no claim. Anomaly `ambiguous_iban_match` with both candidate IDs.

67. **Two candidates but one outside time window → exactly 1 valid → paired**
   - Expense at T. One income at T+1s, one income at T+10s.
   - Only the T+1s candidate is in window. Paired.

### 4.7 Description validation (canary on Tier A)

68. **Descriptions consistent → pair claimed, no anomaly**
   - FOP UAH expense at T, desc "На чорну картку", `counterparty_iban = UAH_BLACK.iban`.
   - Partner: income on UAH_BLACK, desc "З гривневого рахунку ФОП".
   - Income validates against expense account (FOP UAH) → True.
   - Expense validates against income account (black) → True.
   - Paired. Zero anomalies.

69. **Descriptions inconsistent → pair claimed + description_consistency_mismatch anomaly**
   - FOP UAH expense at T, desc "На білу картку", `counterparty_iban = UAH_BLACK.iban`.
   - Partner: income on UAH_BLACK, desc "З гривневого рахунку ФОП".
   - Expense says "white" but income account is black → description mismatch.
   - Pair still claimed (IBAN is deterministic). Anomaly `description_consistency_mismatch`.

70. **Income description wrong, expense OK → canary anomaly**
   - Partner income desc "З Білої картки" but expense account is FOP.
   - Income constraint `(type=white)` vs FOP → mismatch.
   - Paired (Tier A), anomaly recorded.

71. **Both descriptions wrong → single canary anomaly per transaction**
   - Both sides mismatch. Still paired. One anomaly record per the incoming transaction.

---

## 5. Unit tests — Tier B (`tests/unit/test_transfer_service_tier_b.py`)

### 5.1 Reverse IBAN lookup

72. **Incoming tx has no counterparty_iban; existing unclaimed tx points at my account IBAN**
   - Card income at T on UAH_BLACK (no `counterparty_iban`).
   - Existing FOP expense at T-1s with `counterparty_iban = UAH_BLACK.iban`, unclaimed.
   - Tier A skipped (no IBAN on incoming). Tier B finds the FOP expense.
   - Paired.

73. **Multiple existing txs point at my IBAN → ambiguous_reverse_iban**
   - Card income at T. Two unclaimed expenses from different accounts both have `counterparty_iban = UAH_BLACK.iban`.
   - Anomaly `ambiguous_reverse_iban` with both candidate IDs.

74. **Existing tx has counterparty_iban pointing at my IBAN but is already claimed → skip**
   - Card income at T. Existing expense with `counterparty_iban = UAH_BLACK.iban` but already claimed.
   - No candidate found via Tier B. Falls through to Tier C.

75. **Existing tx has counterparty_iban pointing at my IBAN but wrong type → not a candidate**
   - Card income at T. Existing tx is also income (same type) with `counterparty_iban = UAH_BLACK.iban`.
   - Tier B requires opposite type. No match.

76. **Existing tx has counterparty_iban pointing at my IBAN but outside time window → no match**
   - Card income at T. Existing expense at T-5s with `counterparty_iban = UAH_BLACK.iban`.
   - Outside ±2s. No match.

### 5.2 Description validation (canary on Tier B)

77. **Tier B pair, descriptions consistent → paired, no anomaly**
78. **Tier B pair, descriptions inconsistent → paired + description_consistency_mismatch**
   — Same logic as Tier A canary: IBAN-based pair proceeds regardless.

---

## 6. Unit tests — Tier C (`tests/unit/test_transfer_service_tier_c.py`)

### 6.1 Operation amount cross-match

79. **Card→card same currency: expense.operation_amount = income.amount_cents**
   - Black card expense at T: `amount_cents=5000`, `operation_amount_cents=5000`, desc "Переказ на картку", no IBAN.
   - White card income at T+1s: `amount_cents=5000`, `operation_amount_cents=5000`, desc "З Чорної картки", no IBAN.
   - Tier C: `expense.operation_amount_cents (5000) = income.amount_cents (5000)` → match.
   - Description validation passes → paired.

80. **Card→card FX: UAH expense → EUR income**
   - UAH card expense at T: `amount_cents=78400` (UAH), `operation_amount_cents=1750` (EUR value), desc "Переказ на картку".
   - EUR card income at T+1s: `amount_cents=1750` (EUR), `operation_amount_cents=78400` (UAH value), desc "З Чорної картки".
   - Cross-match: `expense.operation_amount_cents (1750) = income.amount_cents (1750)` → match.
   - Also check reverse: `income.operation_amount_cents (78400) = expense.amount_cents (78400)`.
   - Description guard: income "З Чорної картки" (`type=black`) validates against expense account (UAH black card) → True.
   - Paired.

81. **No IBAN, amount matches but descriptions don't validate → description_account_mismatch**
   - Black card expense, desc "Переказ на картку". EUR card income, desc "З Білої картки".
   - Amount matches within ±2s.
   - Income desc expects `type=white`, but expense account is `type=black` → validation fails.
   - Anomaly `description_account_mismatch`. Not paired.

82. **No IBAN, amount matches, descriptions validate, but >1 valid candidate → ambiguous_amount_match**
   - Black card expense at T. Two white cards (different accounts) both have matching income at T+1s, both with desc "З Чорної картки".
   - Both validate. Ambiguity → anomaly `ambiguous_amount_match`. Not paired.

83. **No IBAN, no amount match → no candidate. Description has "З " prefix → unpaired_from_description**
   - Card income at T, desc "З Чорної картки". No matching expense.
   - Record `unpaired_from_description`.

84. **No IBAN, no amount match, description has "На " prefix → unpaired_to_description**
   - Card expense at T, desc "На білу картку". No matching income.
   - Record `unpaired_to_description`.

### 6.2 Same-account exclusion

85. **Tier C excludes candidates on the same account**
   - Two transactions on the same account (UAH FOP), opposite types, amounts match.
   - Tier C requires `different account_id`. No candidate.

### 6.3 Description not in allowed set

86. **Both IBANs NULL, description is a person name ("Олена К.") → not a transfer candidate**
   - Expense desc "Олена К." is not in the allowed description set.
   - Tier C skipped (description guard fails at entry). No anomaly (not a "З "/"На " prefix).

87. **Both IBANs NULL, description starts with "З " but not in allowed set → unpaired_from_description**
   - Income desc "З невідомого рахунку". Starts with "З " but not recognized.
   - No Tier C attempt. Anomaly `unpaired_from_description`.

### 6.4 Tier C time window

88. **Partner exactly at ±2s boundary → matched**
89. **Partner at ±3s → no match**

---

## 7. Unit tests — anomaly recording (`tests/unit/test_transfer_service_anomalies.py`)

### 7.1 Auto-resolution on successful claim

90. **Unpaired anomaly auto-deleted when partner arrives**
   - Transaction A inserted, no partner → anomaly `unpaired_from_description`.
   - Transaction B arrives, pairs with A.
   - After pairing: anomaly for A is deleted.

91. **Both legs had unpaired anomalies → both deleted on claim**
   - A inserted → `unpaired_from_description`.
   - B inserted → `unpaired_to_description`.
   - Then A's handler runs again (backfill re-run? no — idempotency guard prevents this).
   - Actually: B's handler finds A, pairs them. Both anomalies deleted.

92. **Terminal anomaly (ambiguous_iban_match) persists — auto-delete only fires for unpaired_* anomalies**
   - Transaction A recorded with `ambiguous_iban_match` (Tier A saw >1 candidate).
   - A is still unclaimed (`related_transaction_id IS NULL`).
   - Auto-delete targets `unpaired_from_description` / `unpaired_to_description` only — these are the only anomaly types where the transaction was awaiting a partner. Terminal anomalies (`ambiguous_*`, `description_account_mismatch`) represent rejected decisions that persist until manual investigation.
   - Verify: A's anomaly row still exists after subsequent events are processed.
   - Note: `description_consistency_mismatch` is also terminal (pair was claimed, anomaly is a drift canary — it stays).

### 7.2 Anomaly content

93. **ambiguous_iban_match includes all candidate IDs**
   - 3 candidates on target account → `candidate_ids = [id1, id2, id3]`.

94. **description_account_mismatch includes the single candidate that failed validation**
   - `candidate_ids = [failed_candidate_id]`.

95. **unpaired_from_description has empty candidate_ids**
   - `candidate_ids = []` (no candidates found at all).

96. **description_consistency_mismatch includes the partner that WAS claimed**
   - `candidate_ids = [partner_id]` — the partner that was successfully paired.

97. **reason_detail includes human-readable context**
   - For `description_account_mismatch`: detail contains expected type/currency and actual type/currency.
   - For `description_consistency_mismatch`: detail describes which description disagreed.

---

## 8. Unit tests — idempotency guard (`tests/unit/test_transfer_service_idempotency.py`)

98. **Transaction ID already exists in DB → return immediately, no side effects**
   - Insert tx A into the in-memory store.
   - Call `detect_and_pair()` with a NormalizedTransaction whose `id = A.id`.
   - No partner search, no anomaly recording, no modifications.
   - Return `TransferResult` with `special_category=None`, no `related_transaction_id`.

99. **Transaction ID does not exist → proceed with detection**
   - Call `detect_and_pair()` with a new ID.
   - Normal tier resolution proceeds.

100. **Idempotency guard runs before any tier logic**
   - Mock the transaction repo. Set `exists()` to return True.
   - Verify none of `find_unclaimed_partner_tier_a/b/c` were called.

---

## 9. Unit tests — multi-hop chains (`tests/unit/test_transfer_service_multi_hop.py`)

### 9.1 USD FOP → UAH FOP → UAH card (4 legs, 2 pairs)

101. **All 4 legs arrive in order → 2 independent pairs**
   - Leg 1: USD FOP expense, `counterparty_iban = UAH_FOP.iban`, desc "На гривневий рахунок ФОП для переказу на картку".
   - Leg 2: UAH FOP income, `counterparty_iban = USD_FOP.iban`, desc "З доларового рахунку ФОП для переказу на картку".
   - Leg 3: UAH FOP expense, `counterparty_iban = UAH_BLACK.iban`, desc "На чорну картку".
   - Leg 4: UAH card income, no IBAN, desc "З гривневого рахунку ФОП".
   - Processing order: 1, 2, 3, 4.
   - After leg 2: legs 1 and 2 paired (Tier A on both).
   - After leg 4: leg 3 already inserted. Leg 4 triggers Tier B (leg 3's IBAN points at UAH_BLACK). Legs 3 and 4 paired.
   - Verify: 4 transactions, all `special_category = 'transfer'`. Two distinct pairs.

102. **Intermediate FOP legs (income + expense) on same account don't cross-pair**
   - UAH FOP has both an income and expense at the same timestamp.
   - They're on the same `account_id`. Tier C requires different accounts.
   - Each pairs with its correct partner on a different account.

103. **Leg 4 arrives before leg 3 → leg 4 inserts unpaired, leg 3 pairs via Tier A**
   - Leg 4 (card income, no IBAN) arrives first. No partner found. If desc starts with "З " → `unpaired_from_description` anomaly.
   - Leg 3 (FOP expense, `counterparty_iban = UAH_BLACK.iban`) arrives. Finds leg 4 on UAH_BLACK via Tier A. Paired.
   - Anomaly for leg 4 auto-deleted.

### 9.2 Direct FOP → card (2 legs, asymmetric tiers)

104. **FOP expense (Tier A) + card income (Tier B)**
   - FOP expense: `counterparty_iban = UAH_BLACK.iban`. Finds income on black card at T+1s. Paired via Tier A.
   - If card income arrives first: no IBAN on card, Tier B checks if any unclaimed tx has `counterparty_iban = UAH_BLACK.iban`. FOP expense not yet inserted → no match.
   - FOP expense arrives second: Tier A finds the card income. Paired.

### 9.3 FOP → FOP (bidirectional IBAN, both Tier A)

105. **Both FOP legs have counterparty_iban → both can trigger Tier A**
   - USD FOP expense at T, `counterparty_iban = UAH_FOP.iban`.
   - UAH FOP income at T+1s, `counterparty_iban = USD_FOP.iban`.
   - Whichever arrives second finds the first via Tier A and pairs.
   - The first leg inserts normally (no partner yet).

---

## 10. Unit tests — claim lock simulation (`tests/unit/test_transfer_service_claim_locks.py`)

These tests verify the logical behavior. Real `FOR UPDATE SKIP LOCKED`
is tested in integration tests.

106. **Single candidate, unclaimed → claim succeeds**
107. **Candidate already being claimed (simulated as already claimed mid-query) → no match**
   - In-memory repo returns empty list (simulating SKIP LOCKED behavior).
   - Transaction inserts normally, no pair.

108. **Two concurrent handlers for two legs of the same pair**
   - Handler 1 processes leg A: finds leg B unclaimed, claims pair.
   - Handler 2 processes leg B: leg B already claimed by handler 1.
   - Handler 2's idempotency guard triggers (leg B already in DB with `related_transaction_id` set).
   - No duplicate claim.

---

## 11. Integration tests — Tier A (`tests/integration/test_transfer_tier_a_integration.py`)

All tests use `conn` fixture with rolled-back transaction.

### 11.1 Basic pairing

109. **FOP→card: IBAN match, single partner, descriptions consistent**
   - Insert accounts: UAH FOP, UAH black card.
   - Insert existing income on black card (T+1s, MCC 4829, unclaimed).
   - Process FOP expense (T, `counterparty_iban = black.iban`).
   - Verify: both transactions have `special_category = 'transfer'` and cross-referencing `related_transaction_id`.
   - Verify: zero anomalies.

110. **IBAN matches own account but no partner within ±2s**
   - Insert account, no partner transaction.
   - Process expense. Verify: inserted as expense, no pairing, no anomaly.

111. **IBAN matches own account, partner at exactly +2s → paired**
   - Boundary test. Verify time window is inclusive at 2s.

112. **IBAN matches own account, partner at +2.001s → not paired**
   - Validates sub-second precision: exactly 2s is inclusive, 2.001s is exclusive.
   - Uses `T + timedelta(seconds=2, milliseconds=1)` to confirm the boundary is strict.

### 11.2 Ambiguity

113. **Two unclaimed partners → ambiguous_iban_match anomaly**
   - Insert two incomes on black card at T+1s (different source_ids).
   - Process FOP expense. No pairing. Anomaly recorded.
   - Verify anomaly: `reason_code = 'ambiguous_iban_match'`, `candidate_ids` contains both IDs.

### 11.3 Description canary

114. **Tier A pair with consistent descriptions → no anomaly**
115. **Tier A pair with inconsistent descriptions → paired + description_consistency_mismatch anomaly**
   - FOP expense desc "На білу картку" but IBAN points to black card.
   - Pair claimed (IBAN is deterministic). Anomaly recorded with the partner's ID.

### 11.4 Index utilization

116. **Only MCC 4829 transactions are candidates**
   - Insert partner with MCC 5411 on target account. Not found.

117. **Already-claimed transactions are excluded**
   - Insert partner with `related_transaction_id` already set. Not found.

---

## 12. Integration tests — Tier B (`tests/integration/test_transfer_tier_b_integration.py`)

118. **Card income, existing FOP expense has counterparty_iban pointing at card → paired**
   - Insert FOP expense with `counterparty_iban = UAH_BLACK.iban`.
   - Process card income (no IBAN). Tier B finds FOP expense. Paired.

119. **Multiple FOP expenses point at card IBAN → ambiguous_reverse_iban**
   - Two FOP expenses from different FOP accounts, both `counterparty_iban = UAH_BLACK.iban`.
   - Process card income. Ambiguity → anomaly.

120. **FOP expense pointing at card IBAN but outside time window → no Tier B match**

121. **Tier B with consistent descriptions → no anomaly**
122. **Tier B with inconsistent descriptions → paired + canary anomaly**

---

## 13. Integration tests — Tier C (`tests/integration/test_transfer_tier_c_integration.py`)

### 13.1 Same-currency card→card

123. **Black card expense + white card income, amounts match, descriptions validate**
   - Black expense: `operation_amount_cents=5000`, desc "Переказ на картку", no IBAN.
   - White income: `amount_cents=5000`, desc "З Чорної картки", no IBAN.
   - Tier C: amount cross-match + description validates (`type=black` against expense account). Paired.

124. **Amounts match but description validation fails → description_account_mismatch**
   - White income desc "З Білої картки" but expense is on black card.
   - `type=white` doesn't match `type=black`. Not paired. Anomaly.

125. **Amounts match, descriptions validate, but 2 valid candidates → ambiguous_amount_match**
   - Black expense at T. Two matching incomes on different white-type accounts (UAH_WHITE and another white card).
   - Both validate. Anomaly `ambiguous_amount_match`.

### 13.2 FX card→card

126. **UAH→EUR transfer: operation_amount cross-match**
   - UAH card expense: `amount_cents=78400`, `operation_amount_cents=1750`, desc "Переказ на картку".
   - EUR card income: `amount_cents=1750`, `operation_amount_cents=78400`, desc "З Чорної картки".
   - Cross-match: `expense.operation_amount (1750) = income.amount (1750)`.
   - Paired.

127. **FX amounts swapped: income.operation_amount = expense.amount also holds**
   - Verify the cross-match works symmetrically.

### 13.3 Same-account exclusion

128. **Two transactions on the same account cannot pair via Tier C**
   - Same account_id. Even if amounts/time/descriptions match → excluded.

### 13.4 Edge cases

129. **Tier C only reached when both IBANs are NULL**
   - If incoming tx has NULL IBAN but candidate has non-NULL IBAN → candidate excluded from Tier C index (partial index `WHERE counterparty_iban IS NULL`).

130. **External P2P: person name description → no Tier C attempt, no anomaly**
   - Expense desc "Олена К." → not in allowed set. No "З "/"На " prefix. No anomaly.

131. **Unrecognized "З " prefix → unpaired_from_description**
   - Income desc "З нового рахунку" — "З " prefix but not in known patterns.
   - No Tier C attempt. Anomaly recorded.

132. **Unrecognized "На " prefix → unpaired_to_description**
   - Expense desc "На невідому картку" — "На " prefix but not in known patterns.
   - No Tier C attempt. Anomaly `unpaired_to_description` recorded.

133. **Amount match within window but partner on a different user's account → no match**
   - Tier C filters by `user_id`. Different user → excluded.

### 13.5 Zero-amount transactions

134. **MCC 4829 + amount_cents=0 (card verification hold) → does not false-match via Tier C**
   - Two zero-amount check transactions on different accounts, both MCC 4829, within ±2s, both IBANs NULL.
   - `direction = 'check'` (neither income nor expense). Tier C requires opposite type (income vs expense) — check doesn't qualify.
   - Both insert normally. Neither paired. No anomaly (descriptions are bank-generated verification text, not "З "/"На " prefix).

---

## 14. Integration tests — anomaly lifecycle (`tests/integration/test_transfer_anomaly_integration.py`)

### 14.1 Auto-resolution

135. **Unpaired anomaly deleted when partner arrives and pair succeeds**
   - Insert tx A, no partner → anomaly `unpaired_from_description`.
   - Insert tx B that pairs with A.
   - Query anomalies: A's anomaly deleted.

136. **Both legs had unpaired anomalies → both deleted on pair**
   - A arrives → `unpaired_from_description`.
   - B arrives → `unpaired_to_description`.
   - B's handler pairs with A → both anomalies deleted.

137. **Anomaly UNIQUE constraint: one anomaly per transaction**
   - Insert anomaly for tx A. Attempt to insert another for same tx.
   - Constraint violation (not silently ignored — loud failure).

### 14.2 Cascade

138. **Deleting a transaction cascades to its anomaly**
   - Insert tx, record anomaly. DELETE tx.
   - Verify: `SELECT COUNT(*) FROM transfer_match_anomalies WHERE transaction_id = tx.id` returns 0.

139. **Deleting a paired transaction sets related_transaction_id to NULL on partner**
   - A and B paired (`related_transaction_id` cross-referencing). DELETE A.
   - Verify: B's `related_transaction_id IS NULL`.
   - Verify: B's `special_category` remains `'transfer'` (SET NULL only affects the FK column, not the category).
   - Verify: B is now visible to unclaimed-partner queries (`related_transaction_id IS NULL`).

---

## 15. Integration tests — claim locks (`tests/integration/test_transfer_claim_locks_integration.py`)

### 15.1 FOR UPDATE SKIP LOCKED behavior

140. **Single connection claims partner successfully**
   - Connection 1: begin transaction, query unclaimed partner with `FOR UPDATE SKIP LOCKED`, returns partner row. Claim both.
   - Verify pair.

141. **Two connections race for the same partner: one wins, one gets empty result**
   - Connection 1: begin transaction, `SELECT ... FOR UPDATE SKIP LOCKED` → gets partner row. Does NOT commit yet.
   - Connection 2: same query → `SKIP LOCKED` returns empty (row locked by conn 1).
   - Connection 1 commits (pair claimed).
   - Connection 2: no candidate found, inserts normally (or retries later).

142. **Locked row released on rollback → available to next query**
   - Connection 1: begin, lock row, rollback.
   - Connection 2: same query → row now available.

### 15.2 Advisory lock integration

143. **Pipeline consumer holds advisory lock → reprocess job blocks**
   - Connection 1 (consumer): `SELECT pg_advisory_xact_lock(hashtext('reprocess:' || user_id))`.
   - Connection 2 (reprocess): same advisory lock → blocks.
   - Connection 1 commits → connection 2 unblocks.

---

## 16. Integration tests — full pipeline (`tests/integration/test_transfer_pipeline_integration.py`)

End-to-end tests that run `PipelineOrchestrator.run()` with real DB.

### 16.1 Transfer detection + currency conversion + persistence

144. **FOP→card transfer: both legs persisted with type='transfer', currency conversion applied**
   - Seed: accounts, currency rates.
   - Process FOP expense (Tier A matchable). Then process card income.
   - Query DB: both transactions have `special_category = 'transfer'`, `related_transaction_id` cross-referencing, `amount_uah_cents` populated.

145. **External P2P: persisted as expense, no transfer detection side effects**
   - Expense with person name description, MCC 4829, no IBAN.
   - Persisted as `expense`. No anomaly. No `related_transaction_id`.

146. **Non-MCC-4829 transaction: transfer detection skipped entirely**
   - MCC 5411 grocery. Persisted as `expense`. Transfer detection not invoked.

### 16.2 Idempotency through the full pipeline

147. **Duplicate event (same ID) → second invocation is a no-op**
   - Process event A. Process event A again.
   - `ON CONFLICT (id) DO NOTHING` on the INSERT. Idempotency guard in transfer detection prevents spurious anomalies.
   - Only one row in DB.

148. **Re-delivery after successful pair → no duplicate anomaly or double-claim**
   - A and B paired. Re-deliver A. Idempotency guard sees A already exists. No-op.

### 16.3 Source dispatch

149. **Monobank source → MonobankTransferDetection strategy dispatched**
   - `tx.source = "monobank"` → uses Monobank strategy.

150. **Manual source → transfer detection skipped (no strategy registered)**
   - `tx.source = "manual"` → no transfer detection strategy. Transaction persisted with `special_category = NULL`.

151. **Unknown source → transfer detection skipped, no error**
   - `tx.source = "revolut"` → no strategy in registry. Logged as info. Pipeline continues.

---

## 17. Test hygiene

- Every async test uses `pytest.mark.asyncio` (or module-level `pytestmark = pytest.mark.asyncio`).
- Integration tests never share state — per-test transaction rollback.
- Use `caplog` for log assertions. Never parse stdout.
- UUID comparisons use `==`, not string conversion.
- Parametrize when it compresses (description tables in §2). Otherwise keep explicit.
- Mock-based tests use `unittest.mock.AsyncMock` for repo methods.
- All test descriptions are human-readable as MCC 4829, ±2s, etc.

---

## 18. Coverage target

```
pytest --cov=grosh_consumer.sources.monobank.transfer \
       --cov=grosh_consumer.sources.monobank.descriptions \
       --cov=grosh_consumer.services.transfer_detection \
       --cov=grosh_consumer.repositories.anomaly_repo \
       --cov-branch
```

Targets: ≥ 95% line, ≥ 90% branch. Uncovered lines explained in PR.
