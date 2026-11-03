"""Shared pytest fixtures."""

import time

import pytest


def _set_tz(monkeypatch, name: str):
    monkeypatch.setenv("TZ", name)
    time.tzset()


@pytest.fixture(autouse=True)
def utc_local_time(monkeypatch):
    """Run every test with a UTC local time zone, so naive datetimes give the same result on any machine."""
    _set_tz(monkeypatch, "UTC")
    yield
    monkeypatch.undo()
    time.tzset()


@pytest.fixture
def non_utc_local_time(monkeypatch):
    """Switch the local time zone to Europe/Berlin (UTC+1 in winter) for one test."""
    _set_tz(monkeypatch, "Europe/Berlin")
