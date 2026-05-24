"""Top-level e2e conftest — intentionally minimal.

Subdirs (`integration/`, `unit/`, `services/`) own their lifecycle independently
because they have different infrastructure needs:
- integration/  → throwaway test DB, no application containers
- unit/         → no infra
- services/     → throwaway test DB + reconfigured application containers
"""
