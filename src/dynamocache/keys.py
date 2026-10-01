"""Cache-key construction.

The cache key uniquely identifies a memoized call. It is derived from the
*identity* of the wrapped callable (its module and qualified name) combined with
a stable digest of the call arguments. Using the qualified name means two
functions that merely share a bare name — for example ``get`` methods on two
different classes — never collide, and hashing the arguments keeps keys within
DynamoDB's 2 KB partition-key limit regardless of argument size.

Instance methods get an explicit, predictable answer: if the wrapped callable's
first parameter is named ``self``, that first positional argument is excluded
from the key by default. This avoids keying on an instance's memory address
(which changes every process, so a naive key would never hit across a Lambda
cold start) while keeping behavior obvious. Callers who need instance identity,
or who want to ignore or transform particular arguments, can supply their own
``key_fn``.
"""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Mapping, Sequence
from typing import Any, Callable

__all__ = ["make_key", "first_param_is_self"]


def first_param_is_self(func: Callable[..., Any]) -> bool:
    """Return ``True`` if ``func``'s first parameter is named ``self``.

    Used to decide whether to drop the bound instance from the default cache
    key. Falls back to ``False`` for callables whose signature cannot be
    introspected (some builtins and C-implemented callables).
    """
    try:
        params = list(inspect.signature(func).parameters)
    except (ValueError, TypeError):
        return False
    return bool(params) and params[0] == "self"


def _canonical(value: Any) -> str:
    """Render ``value`` to a stable string for hashing.

    Mappings are rendered with sorted keys so that keyword-argument ordering
    does not affect the resulting cache key. Everything else falls back to
    ``repr``; callers passing objects with an unstable ``repr`` (e.g. the
    default ``object`` repr, which embeds a memory address) should provide a
    custom ``key_fn``.
    """
    if isinstance(value, Mapping):
        items = sorted((str(k), _canonical(v)) for k, v in value.items())
        return "{" + ",".join(f"{k}:{v}" for k, v in items) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canonical(v) for v in value) + "]"
    return repr(value)


def make_key(
    func: Callable[..., Any],
    args: Sequence[Any],
    kwargs: Mapping[str, Any],
    *,
    include_self: bool = False,
) -> str:
    """Build the cache key for a call to ``func`` with ``args``/``kwargs``.

    :param func: The wrapped callable.
    :param args: Positional arguments the call was made with.
    :param kwargs: Keyword arguments the call was made with.
    :param include_self: If ``True``, keep the leading ``self`` argument (when
        present) in the key. Defaults to ``False``.
    :returns: A key of the form ``"module:qualname:<sha256 hex>"``.
    """
    key_args: Sequence[Any] = args
    if not include_self and args and first_param_is_self(func):
        key_args = args[1:]

    payload = _canonical([list(key_args), dict(kwargs)])
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    module = func.__module__ or "?"
    qualname = getattr(func, "__qualname__", func.__name__)
    return f"{module}:{qualname}:{digest}"
