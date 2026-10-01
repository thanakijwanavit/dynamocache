"""Unit tests for serializers."""

import pytest

from dynamocache.serialization import JSONSerializer, PickleSerializer, Serializer


@pytest.mark.parametrize("serializer", [PickleSerializer(), JSONSerializer()])
def test_round_trip_json_compatible(serializer):
    value = {"a": 1, "b": [1, 2, 3], "c": "text", "d": None}
    assert serializer.loads(serializer.dumps(value)) == value


def test_pickle_handles_arbitrary_objects():
    value = {("tuple", "key"): {1, 2, 3}}
    serializer = PickleSerializer()
    assert serializer.loads(serializer.dumps(value)) == value


def test_dumps_returns_bytes():
    assert isinstance(PickleSerializer().dumps(1), bytes)
    assert isinstance(JSONSerializer().dumps(1), bytes)


def test_serializers_satisfy_protocol():
    assert isinstance(PickleSerializer(), Serializer)
    assert isinstance(JSONSerializer(), Serializer)
