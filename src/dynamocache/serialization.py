"""Serialization strategies for cache values.

A serializer converts an arbitrary Python object to ``bytes`` for storage in
DynamoDB and back again. ``Dynamocache`` uses :class:`PickleSerializer` by
default, but any object implementing the :class:`Serializer` protocol can be
supplied instead.

.. warning::
    The default :class:`PickleSerializer` uses :mod:`pickle`, which executes
    arbitrary code on deserialization. Only ever point a cache backed by pickle
    at a table that is written to exclusively by code you trust. If the cache
    table can be written to across a trust boundary, use :class:`JSONSerializer`
    (or a custom serializer) instead.
"""

from __future__ import annotations

import json
import pickle
from typing import Any, Protocol, runtime_checkable

__all__ = ["Serializer", "PickleSerializer", "JSONSerializer"]


@runtime_checkable
class Serializer(Protocol):
    """Protocol for converting cache values to and from ``bytes``."""

    def dumps(self, obj: Any) -> bytes:
        """Serialize ``obj`` to ``bytes``."""
        ...

    def loads(self, data: bytes) -> Any:
        """Deserialize ``bytes`` produced by :meth:`dumps` back to an object."""
        ...


class PickleSerializer:
    """Default serializer, backed by :mod:`pickle`.

    Handles nearly any Python object, at the cost of producing an opaque,
    Python-specific payload. See the module-level security warning.
    """

    def __init__(self, protocol: int = pickle.HIGHEST_PROTOCOL) -> None:
        self._protocol = protocol

    def dumps(self, obj: Any) -> bytes:
        return pickle.dumps(obj, protocol=self._protocol)

    def loads(self, data: bytes) -> Any:
        return pickle.loads(data)


class JSONSerializer:
    """Serializer that stores values as UTF-8 encoded JSON.

    Safe across trust boundaries, but only supports JSON-compatible values
    (``dict``, ``list``, ``str``, ``int``, ``float``, ``bool``, ``None``).
    """

    def __init__(self, *, ensure_ascii: bool = False) -> None:
        self._ensure_ascii = ensure_ascii

    def dumps(self, obj: Any) -> bytes:
        return json.dumps(obj, ensure_ascii=self._ensure_ascii).encode("utf-8")

    def loads(self, data: bytes) -> Any:
        return json.loads(data.decode("utf-8"))
