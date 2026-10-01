"""Storage backends for the cache.

``Dynamocache`` talks to a small :class:`CacheBackend` protocol rather than to
DynamoDB directly. This keeps the caching logic independent of the transport and
makes it trivial to substitute an in-memory fake in tests (DAX cannot be
emulated locally, so unit tests mock at this boundary).

The production backend, :class:`DynamoCacheBackend`, uses two clients:

* a **data-plane** client for ``get``/``put`` — Amazon DAX when a cluster
  endpoint is configured (the primary, intended deployment), or plain DynamoDB
  when it is not (an explicitly opt-in "direct mode" for local development and
  testing, since a DAX cluster is only reachable from inside its VPC); and
* a **management** client (always plain DynamoDB) for the control-plane
  operations ``create_table`` and ``update_time_to_live``, which DAX does not
  serve.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

__all__ = ["CacheEntry", "CacheBackend", "DynamoCacheBackend", "InMemoryBackend"]

# Attribute names used in the DynamoDB item.
_KEY_ATTR = "key"
_VALUE_ATTR = "value"
_TTL_ATTR = "ttl"


@dataclass(frozen=True)
class CacheEntry:
    """A stored cache value and its absolute expiry time.

    :param value: The serialized payload.
    :param ttl: Absolute expiry as a Unix timestamp (seconds).
    """

    value: bytes
    ttl: int


class CacheBackend(Protocol):
    """Protocol implemented by cache storage backends."""

    def get(self, key: str) -> CacheEntry | None:
        """Return the stored entry for ``key``, or ``None`` if absent."""
        ...

    def put(self, key: str, value: bytes, ttl: int) -> None:
        """Store ``value`` under ``key`` with absolute expiry ``ttl``."""
        ...

    def create_table(self, *, wait: bool = True) -> None:
        """Create the backing table if it does not already exist."""
        ...

    def enable_ttl(self) -> None:
        """Enable DynamoDB's native time-to-live on the ``ttl`` attribute."""
        ...


class DynamoCacheBackend:
    """Cache backend backed by Amazon DAX and/or DynamoDB.

    :param table_name: Name of the DynamoDB table used for the cache.
    :param endpoint: DAX cluster endpoint(s). Accepts a single endpoint string
        or a list of them. When ``None``, the backend operates in direct
        DynamoDB mode (see the module docstring).
    :param region_name: AWS region. When ``None``, the region is resolved by the
        AWS SDK in the usual way (environment, shared config, instance role).
    :param access_key: Optional explicit AWS access key id.
    :param secret_key: Optional explicit AWS secret access key.
    :param session_token: Optional explicit AWS session token.
    :param data_client: Advanced/testing hook — a preconstructed low-level
        data-plane client to use instead of building one.
    :param management_client: Advanced/testing hook — a preconstructed
        DynamoDB management client to use instead of building one.
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
        data_client: Any | None = None,
        management_client: Any | None = None,
    ) -> None:
        self.table_name = table_name
        self._endpoint = endpoint
        self._region_name = region_name
        self._credentials = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "aws_session_token": session_token,
        }
        self._data_client = data_client
        self._management_client = management_client

    # -- client construction -------------------------------------------------

    def _client_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            k: v for k, v in self._credentials.items() if v is not None
        }
        if self._region_name is not None:
            kwargs["region_name"] = self._region_name
        return kwargs

    @property
    def data(self) -> Any:
        """The lazily constructed data-plane client (DAX or DynamoDB)."""
        if self._data_client is None:
            self._data_client = self._build_data_client()
        return self._data_client

    @property
    def management(self) -> Any:
        """The lazily constructed DynamoDB management client."""
        if self._management_client is None:
            import boto3

            self._management_client = boto3.client("dynamodb", **self._client_kwargs())
        return self._management_client

    def _build_data_client(self) -> Any:
        if self._endpoint is None:
            import boto3

            return boto3.client("dynamodb", **self._client_kwargs())

        endpoints = (
            [self._endpoint] if isinstance(self._endpoint, str) else list(self._endpoint)
        )
        try:
            from amazon_dax_client import AmazonDaxClient
        except ImportError as exc:  # pragma: no cover - exercised via message only
            raise ImportError(
                "A DAX endpoint was configured but the 'amazon-dax-client' package "
                "is not installed. Install the DAX extra with "
                "'pip install dynamocache[dax]' (DAX mode requires it), or omit the "
                "endpoint to use direct DynamoDB mode."
            ) from exc
        return AmazonDaxClient(endpoints=endpoints, **self._client_kwargs())

    # -- data-plane ----------------------------------------------------------

    def get(self, key: str) -> CacheEntry | None:
        response = self.data.get_item(
            TableName=self.table_name,
            Key={_KEY_ATTR: {"S": key}},
            ConsistentRead=False,
        )
        item = response.get("Item")
        if not item or _VALUE_ATTR not in item or _TTL_ATTR not in item:
            return None
        return CacheEntry(
            value=bytes(item[_VALUE_ATTR]["B"]),
            ttl=int(item[_TTL_ATTR]["N"]),
        )

    def put(self, key: str, value: bytes, ttl: int) -> None:
        self.data.put_item(
            TableName=self.table_name,
            Item={
                _KEY_ATTR: {"S": key},
                _VALUE_ATTR: {"B": value},
                _TTL_ATTR: {"N": str(ttl)},
            },
        )

    # -- control-plane (always plain DynamoDB) -------------------------------

    def create_table(self, *, wait: bool = True) -> None:
        client = self.management
        try:
            client.create_table(
                TableName=self.table_name,
                AttributeDefinitions=[
                    {"AttributeName": _KEY_ATTR, "AttributeType": "S"}
                ],
                KeySchema=[{"AttributeName": _KEY_ATTR, "KeyType": "HASH"}],
                BillingMode="PAY_PER_REQUEST",
            )
        except client.exceptions.ResourceInUseException:
            # Table already exists — treat as success and continue.
            return
        if wait:
            client.get_waiter("table_exists").wait(TableName=self.table_name)

    def enable_ttl(self) -> None:
        client = self.management
        try:
            client.update_time_to_live(
                TableName=self.table_name,
                TimeToLiveSpecification={"Enabled": True, "AttributeName": _TTL_ATTR},
            )
        except client.exceptions.ClientError as exc:  # pragma: no cover
            # TTL is already enabled with the same attribute — idempotent success.
            message = str(exc)
            if "TimeToLive is already enabled" not in message:
                raise


class InMemoryBackend:
    """A simple in-memory :class:`CacheBackend`, primarily for tests.

    Stores entries in a plain dict. ``create_table`` and ``enable_ttl`` are
    no-ops.
    """

    def __init__(self) -> None:
        self._store: dict[str, CacheEntry] = {}

    def get(self, key: str) -> CacheEntry | None:
        return self._store.get(key)

    def put(self, key: str, value: bytes, ttl: int) -> None:
        self._store[key] = CacheEntry(value=value, ttl=ttl)

    def create_table(self, *, wait: bool = True) -> None:
        return None

    def enable_ttl(self) -> None:
        return None
