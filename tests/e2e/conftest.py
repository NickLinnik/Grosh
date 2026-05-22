"""Shared fixtures for the cross-service e2e suite."""

import pytest

from helpers import make_event


@pytest.fixture
def make_event_fixture():
    return make_event
