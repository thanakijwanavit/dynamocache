"""Unit tests for cache-key construction."""

from dynamocache.keys import first_param_is_self, make_key


def func_a(x):
    return x


def func_b(x):
    return x


def test_same_call_same_key():
    assert make_key(func_a, (1,), {}) == make_key(func_a, (1,), {})


def test_different_args_differ():
    assert make_key(func_a, (1,), {}) != make_key(func_a, (2,), {})


def test_kwarg_order_does_not_matter():
    k1 = make_key(func_a, (), {"a": 1, "b": 2})
    k2 = make_key(func_a, (), {"b": 2, "a": 1})
    assert k1 == k2


def test_same_bare_name_different_module_or_qualname_do_not_collide():
    # func_a and func_b are distinct callables; their qualnames differ.
    assert make_key(func_a, (1,), {}) != make_key(func_b, (1,), {})


def test_key_is_bounded_length():
    huge = "x" * 100_000
    key = make_key(func_a, (huge,), {})
    # module:qualname: + 64 hex chars — nowhere near DynamoDB's 2KB limit.
    assert len(key) < 200


class Thing:
    def method(self, x):
        return x


def test_self_excluded_by_default():
    a = Thing()
    b = Thing()
    key_a = make_key(Thing.method, (a, 5), {})
    key_b = make_key(Thing.method, (b, 5), {})
    # Two different instances, same argument -> same key by default.
    assert key_a == key_b


def test_include_self_distinguishes_instances():
    a = Thing()
    b = Thing()
    key_a = make_key(Thing.method, (a, 5), {}, include_self=True)
    key_b = make_key(Thing.method, (b, 5), {}, include_self=True)
    assert key_a != key_b


def test_first_param_is_self_detection():
    assert first_param_is_self(Thing.method) is True
    assert first_param_is_self(func_a) is False
