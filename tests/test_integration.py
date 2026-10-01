"""Opt-in integration tests against real AWS infrastructure.

These are skipped unless ``DYNAMOCACHE_INTEGRATION=1`` is set. They require AWS
credentials and a writable region. Configure the target with environment
variables:

* ``DYNAMOCACHE_INTEGRATION`` — set to ``1`` to enable.
* ``DYNAMOCACHE_TABLE`` — table name to use (default ``dynamocache-itest``).
* ``DYNAMOCACHE_DAX_ENDPOINT`` — optional DAX cluster endpoint. When set, the
  data plane goes through DAX (only reachable from inside the cluster's VPC);
  when unset, the test runs in direct DynamoDB mode.

Run with::

    DYNAMOCACHE_INTEGRATION=1 pytest -m integration
"""

import os
import time

import pytest

from dynamocache import Dynamocache

pytestmark = pytest.mark.integration

_ENABLED = os.environ.get("DYNAMOCACHE_INTEGRATION") == "1"


@pytest.fixture(scope="module")
def cache():
    if not _ENABLED:
        pytest.skip("set DYNAMOCACHE_INTEGRATION=1 to run integration tests")
    table = os.environ.get("DYNAMOCACHE_TABLE", "dynamocache-itest")
    endpoint = os.environ.get("DYNAMOCACHE_DAX_ENDPOINT") or None
    c = Dynamocache(table, endpoint=endpoint)
    c.create_table()
    return c


def test_memoize_round_trip(cache):
    calls = {"n": 0}

    @cache.memoize(ttl_seconds=30)
    def expensive(x):
        calls["n"] += 1
        return {"result": x * 2}

    assert expensive(21) == {"result": 42}
    assert expensive(21) == {"result": 42}
    assert calls["n"] == 1


def test_expiry(cache):
    calls = {"n": 0}

    @cache.memoize(ttl_seconds=1)
    def f():
        calls["n"] += 1
        return calls["n"]

    assert f() == 1
    time.sleep(2)
    assert f() == 2
