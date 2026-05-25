"""Shared fixtures for grosh-enrichment tests."""

import pytest

from tests.helpers import make_event


@pytest.fixture
def make_event_fixture():
    return make_event
