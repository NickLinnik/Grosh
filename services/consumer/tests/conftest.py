"""Shared fixtures for grosh-consumer tests."""

import pytest

from tests.helpers import make_event


@pytest.fixture
def make_event_fixture():
    return make_event
