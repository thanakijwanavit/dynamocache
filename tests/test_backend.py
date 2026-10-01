"""Unit tests for DynamoCacheBackend marshalling, using a fake client.

These verify that the backend builds the correct DynamoDB item shapes and
control-plane calls without touching AWS. A fake low-level client stands in for
both the DAX/DynamoDB data client and the DynamoDB management client.
"""

import pytest

from dynamocache.backend import CacheEntry, DynamoCacheBackend


class FakeExceptions:
    class ResourceInUseException(Exception):
        pass

    class ClientError(Exception):
        pass


class FakeClient:
    def __init__(self):
        self.items = {}
        self.get_calls = []
        self.put_calls = []
        self.created = None
        self.ttl_spec = None
        self.raise_in_use = False
        self.exceptions = FakeExceptions()

    def get_item(self, TableName, Key, ConsistentRead=False):
        self.get_calls.append((TableName, Key, ConsistentRead))
        key = Key["key"]["S"]
        if key in self.items:
            return {"Item": self.items[key]}
        return {}

    def put_item(self, TableName, Item):
        self.put_calls.append((TableName, Item))
        self.items[Item["key"]["S"]] = Item

    def create_table(self, **kwargs):
        if self.raise_in_use:
            raise self.exceptions.ResourceInUseException("exists")
        self.created = kwargs

    def get_waiter(self, name):
        class _W:
            def wait(self_inner, **kwargs):
                return None

        return _W()

    def update_time_to_live(self, TableName, TimeToLiveSpecification):
        self.ttl_spec = TimeToLiveSpecification


@pytest.fixture
def backend():
    client = FakeClient()
    be = DynamoCacheBackend("t", data_client=client, management_client=client)
    return be, client


def test_put_then_get_round_trip(backend):
    be, client = backend
    be.put("k", b"payload", 12345)
    # Item shape is correct.
    item = client.items["k"]
    assert item["value"]["B"] == b"payload"
    assert item["ttl"]["N"] == "12345"
    assert item["key"]["S"] == "k"
    entry = be.get("k")
    assert entry == CacheEntry(value=b"payload", ttl=12345)


def test_get_missing_returns_none(backend):
    be, _ = backend
    assert be.get("absent") is None


def test_get_uses_eventually_consistent_read(backend):
    be, client = backend
    be.put("k", b"v", 1)
    be.get("k")
    _, _, consistent = client.get_calls[-1]
    assert consistent is False


def test_create_table_shape_and_ttl(backend):
    be, client = backend
    be.create_table()
    assert client.created["TableName"] == "t"
    assert client.created["KeySchema"][0]["AttributeName"] == "key"
    assert client.created["BillingMode"] == "PAY_PER_REQUEST"
    be.enable_ttl()
    assert client.ttl_spec == {"Enabled": True, "AttributeName": "ttl"}


def test_create_table_ignores_already_exists(backend):
    be, client = backend
    client.raise_in_use = True
    be.create_table()  # must not raise


def test_missing_dax_dependency_message():
    be = DynamoCacheBackend("t", endpoint="dax://cluster")
    # amazon-dax-client is intentionally not installed in the unit environment.
    with pytest.raises(ImportError, match="amazon-dax-client"):
        _ = be.data
