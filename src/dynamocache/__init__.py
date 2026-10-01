"""Dynamocache — cache Python function results in DynamoDB, accelerated by DAX.

See :class:`Dynamocache` for the main entry point.
"""

from __future__ import annotations

from .backend import CacheBackend, CacheEntry, DynamoCacheBackend, InMemoryBackend
from .cache import Dynamocache
from .serialization import JSONSerializer, PickleSerializer, Serializer

__all__ = [
    "Dynamocache",
    "Serializer",
    "PickleSerializer",
    "JSONSerializer",
    "CacheBackend",
    "CacheEntry",
    "DynamoCacheBackend",
    "InMemoryBackend",
]

__version__ = "0.1.0"
