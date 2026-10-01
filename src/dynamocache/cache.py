"""The public :class:`Dynamocache` entry point."""

from __future__ import annotations

import functools
import logging
import time
from typing import Any, Callable, TypeVar

from .backend import CacheBackend, DynamoCacheBackend
from .keys import make_key
from .serialization import PickleSerializer, Serializer

__all__ = ["Dynamocache"]

logger = logging.getLogger("dynamocache")

F = TypeVar("F", bound=Callable[..., Any])


class Dynamocache:
    """Cache Python function results in DynamoDB, accelerated by DAX.

    Typical use::

        cache = Dynamocache("my-cache", endpoint="dax://my-cluster.abc123.dax.apse1.cache.amazonaws.com")
        cache.create_table()  # one-time setup

        @cache.memoize(ttl_seconds=300)
        def expensive(user_id: str) -> dict:
            ...

    :param table_name: Name of the DynamoDB table used for the cache.
    :param endpoint: DAX cluster endpoint(s). This is the primary, intended
        configuration. When omitted, the cache talks directly to DynamoDB — an
        explicit "direct mode" useful for local development and testing, since a
        DAX cluster is only reachable from inside its VPC.
    :param region_name: AWS region. When ``None`` (the default), the region is
        resolved by the AWS SDK in the usual way (environment variables, shared
        config, instance/task role) rather than being hard-coded.
    :param access_key: Optional explicit AWS access key id.
    :param secret_key: Optional explicit AWS secret access key.
    :param session_token: Optional explicit AWS session token.
    :param serializer: Strategy used to (de)serialize cached values. Defaults to
        :class:`~dynamocache.serialization.PickleSerializer`. See its security
        note before caching across a trust boundary.
    :param strict: Failure policy. When ``False`` (the default), backend errors
        are logged and the cache degrades gracefully — a read failure falls
        through to calling the wrapped function, and a write failure is ignored
        after the result is computed. When ``True``, backend errors propagate.
    :param backend: Advanced/testing hook — a preconstructed
        :class:`~dynamocache.backend.CacheBackend`. When omitted, a
        :class:`~dynamocache.backend.DynamoCacheBackend` is built from the
        connection arguments above.
    :param time_fn: Advanced/testing hook — a callable returning the current
        Unix time in seconds. Defaults to :func:`time.time`.
    """

    def __init__(
        self,
        table_name: str,
        *,
        endpoint: Any | None = None,
        region_name: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
        serializer: Serializer | None = None,
        strict: bool = False,
        backend: CacheBackend | None = None,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self.table_name = table_name
        self.serializer: Serializer = serializer or PickleSerializer()
        self.strict = strict
        self._time = time_fn
        self._backend: CacheBackend = backend or DynamoCacheBackend(
            table_name,
            endpoint=endpoint,
            region_name=region_name,
            access_key=access_key,
            secret_key=secret_key,
            session_token=session_token,
        )

    @property
    def backend(self) -> CacheBackend:
        """The underlying storage backend."""
        return self._backend

    def create_table(self, *, wait: bool = True, enable_ttl: bool = True) -> None:
        """Create the cache table if needed and enable native TTL.

        Idempotent: if the table already exists this is a no-op. When
        ``enable_ttl`` is ``True`` (the default), DynamoDB's native time-to-live
        is enabled on the ``ttl`` attribute so expired items are reclaimed by
        DynamoDB itself rather than merely being ignored at read time.
        """
        self._backend.create_table(wait=wait)
        if enable_ttl:
            self._backend.enable_ttl()

    def memoize(
        self,
        ttl_seconds: int = 60,
        *,
        key_fn: Callable[..., str] | None = None,
        include_self: bool = False,
    ) -> Callable[[F], F]:
        """Return a decorator that memoizes a function's results in the cache.

        :param ttl_seconds: How long, in seconds, a cached result stays valid.
        :param key_fn: Optional custom key builder, called as
            ``key_fn(func, args, kwargs)`` and expected to return a ``str``. Use
            this to include instance identity, ignore particular arguments, or
            otherwise control cache keys. When omitted, a key is derived from the
            function's module and qualified name plus a digest of its arguments
            (with a leading ``self`` argument excluded — see ``include_self``).
        :param include_self: When using the default key builder, set to ``True``
            to keep the leading ``self`` argument in the key. Ignored when
            ``key_fn`` is supplied.
        """

        def decorator(func: F) -> F:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                if key_fn is not None:
                    key = key_fn(func, args, kwargs)
                else:
                    key = make_key(func, args, kwargs, include_self=include_self)

                cached = self._read(key)
                if cached is not None:
                    return cached.value

                result = func(*args, **kwargs)
                self._write(key, result, ttl_seconds)
                return result

            return wrapper  # type: ignore[return-value]

        return decorator

    # -- internal read/write with the configured failure policy --------------

    class _Hit:
        __slots__ = ("value",)

        def __init__(self, value: Any) -> None:
            self.value = value

    def _read(self, key: str) -> Dynamocache._Hit | None:
        try:
            entry = self._backend.get(key)
        except Exception:
            if self.strict:
                raise
            logger.warning("cache read failed for key %s; recomputing", key, exc_info=True)
            return None

        if entry is None:
            return None
        if entry.ttl <= int(self._time()):
            return None
        try:
            return self._Hit(self.serializer.loads(entry.value))
        except Exception:
            if self.strict:
                raise
            logger.warning(
                "cache value for key %s could not be deserialized; recomputing",
                key,
                exc_info=True,
            )
            return None

    def _write(self, key: str, result: Any, ttl_seconds: int) -> None:
        try:
            payload = self.serializer.dumps(result)
            expiry = int(self._time()) + ttl_seconds
            self._backend.put(key, payload, expiry)
        except Exception:
            if self.strict:
                raise
            logger.warning("cache write failed for key %s; result not cached", key, exc_info=True)
