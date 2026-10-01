"""Unit tests for the Dynamocache caching behavior.

These run entirely offline against an in-memory backend; DAX cannot be
emulated locally, so the real backend is exercised only by the opt-in
integration suite.
"""

import pytest

from dynamocache import Dynamocache, InMemoryBackend, JSONSerializer
from dynamocache.backend import CacheEntry


class Clock:
    """Controllable time source for deterministic TTL tests."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


def make_cache(**kwargs):
    backend = kwargs.pop("backend", None) or InMemoryBackend()
    clock = kwargs.pop("clock", None) or Clock()
    cache = Dynamocache("test-cache", backend=backend, time_fn=clock, **kwargs)
    return cache, backend, clock


def test_hit_avoids_recomputation():
    cache, _, _ = make_cache()
    calls = {"n": 0}

    @cache.memoize(ttl_seconds=60)
    def f():
        calls["n"] += 1
        return 42

    assert f() == 42
    assert f() == 42
    assert calls["n"] == 1


def test_distinct_args_are_cached_separately():
    cache, _, _ = make_cache()
    calls = {"n": 0}

    @cache.memoize(ttl_seconds=60)
    def square(x):
        calls["n"] += 1
        return x * x

    assert square(2) == 4
    assert square(3) == 9
    assert square(2) == 4
    assert calls["n"] == 2


def test_expiry_triggers_recomputation():
    cache, _, clock = make_cache()
    calls = {"n": 0}

    @cache.memoize(ttl_seconds=10)
    def f():
        calls["n"] += 1
        return calls["n"]

    assert f() == 1
    clock.now += 5  # still fresh
    assert f() == 1
    clock.now += 6  # now expired (11s elapsed > 10s ttl)
    assert f() == 2
    assert calls["n"] == 2


def test_value_written_with_correct_expiry():
    cache, backend, clock = make_cache()

    @cache.memoize(ttl_seconds=30)
    def f():
        return "v"

    f()
    # Exactly one entry stored, expiring 30s from "now".
    (entry,) = list(backend._store.values())
    assert isinstance(entry, CacheEntry)
    assert entry.ttl == int(clock.now) + 30


def test_json_serializer_round_trips_through_cache():
    cache, _, _ = make_cache(serializer=JSONSerializer())

    @cache.memoize(ttl_seconds=60)
    def f():
        return {"hello": "world"}

    assert f() == {"hello": "world"}
    assert f() == {"hello": "world"}


# -- failure policy ----------------------------------------------------------


class BoomBackend(InMemoryBackend):
    def __init__(self, fail_get=False, fail_put=False):
        super().__init__()
        self.fail_get = fail_get
        self.fail_put = fail_put

    def get(self, key):
        if self.fail_get:
            raise RuntimeError("get exploded")
        return super().get(key)

    def put(self, key, value, ttl):
        if self.fail_put:
            raise RuntimeError("put exploded")
        return super().put(key, value, ttl)


def test_read_failure_degrades_to_calling_function():
    cache, _, _ = make_cache(backend=BoomBackend(fail_get=True))
    calls = {"n": 0}

    @cache.memoize(ttl_seconds=60)
    def f():
        calls["n"] += 1
        return 7

    assert f() == 7  # does not raise, just recomputes
    assert calls["n"] == 1


def test_write_failure_still_returns_result():
    cache, _, _ = make_cache(backend=BoomBackend(fail_put=True))

    @cache.memoize(ttl_seconds=60)
    def f():
        return 7

    assert f() == 7  # write blows up internally but result is returned


def test_strict_mode_propagates_read_failure():
    cache, _, _ = make_cache(backend=BoomBackend(fail_get=True), strict=True)

    @cache.memoize(ttl_seconds=60)
    def f():
        return 7

    with pytest.raises(RuntimeError, match="get exploded"):
        f()


def test_strict_mode_propagates_write_failure():
    cache, _, _ = make_cache(backend=BoomBackend(fail_put=True), strict=True)

    @cache.memoize(ttl_seconds=60)
    def f():
        return 7

    with pytest.raises(RuntimeError, match="put exploded"):
        f()


# -- keys / methods ----------------------------------------------------------


def test_same_named_functions_do_not_collide():
    cache, _, _ = make_cache()

    def build():
        @cache.memoize(ttl_seconds=60)
        def get():
            return "inner"

        return get

    class A:
        @cache.memoize(ttl_seconds=60)
        def get(self):
            return "A"

    class B:
        @cache.memoize(ttl_seconds=60)
        def get(self):
            return "B"

    assert A().get() == "A"
    assert B().get() == "B"  # would be "A" if bare-name keys collided


def test_custom_key_fn():
    cache, backend, _ = make_cache()

    @cache.memoize(ttl_seconds=60, key_fn=lambda func, args, kwargs: "fixed-key")
    def f(x):
        return x

    f(1)
    f(2)  # same key -> returns cached first value
    assert list(backend._store.keys()) == ["fixed-key"]
    assert f(2) == 1


def test_include_self_distinguishes_instances():
    cache, _, _ = make_cache()

    class Counter:
        def __init__(self, base):
            self.base = base

        @cache.memoize(ttl_seconds=60, include_self=True)
        def value(self):
            return self.base

    # Without include_self these would collide; with it they must not.
    assert Counter(1).value() == 1
    assert Counter(2).value() == 2


# -- table setup -------------------------------------------------------------


class RecordingBackend(InMemoryBackend):
    def __init__(self):
        super().__init__()
        self.created = False
        self.ttl_enabled = False

    def create_table(self, *, wait=True):
        self.created = True

    def enable_ttl(self):
        self.ttl_enabled = True


def test_create_table_enables_ttl_by_default():
    backend = RecordingBackend()
    cache = Dynamocache("t", backend=backend)
    cache.create_table()
    assert backend.created is True
    assert backend.ttl_enabled is True


def test_create_table_can_skip_ttl():
    backend = RecordingBackend()
    cache = Dynamocache("t", backend=backend)
    cache.create_table(enable_ttl=False)
    assert backend.created is True
    assert backend.ttl_enabled is False
