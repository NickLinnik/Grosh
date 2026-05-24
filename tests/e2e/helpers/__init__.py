"""E2E test helpers.

Exports `make_event` for backwards compat with existing integration tests.
New tests should import from helpers.factories, helpers.http, helpers.wait.
"""

from helpers.events import make_event

__all__ = ["make_event"]
