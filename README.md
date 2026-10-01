# Dynamocache

Cache Python function results in DynamoDB, accelerated by [Amazon DAX](https://aws.amazon.com/dynamodb/dax/).

Dynamocache turns any DynamoDB table into a durable, fully-managed function cache. Decorate a slow function, and its results are memoized in DynamoDB with a per-entry TTL. Point it at a DAX cluster and reads come back with microsecond latency — ideal for hot paths in AWS Lambda and other in-VPC compute.

> **Status:** 0.1.0 (alpha). The API is still settling; expect changes before 1.0.

## Install

```bash
pip install dynamocache          # direct DynamoDB mode
pip install "dynamocache[dax]"   # adds the DAX data-plane client
```

`boto3` is the only required dependency. DAX mode additionally needs AWS's
`amazon-dax-client`, pulled in by the `[dax]` extra (kept optional so a plain
install always succeeds even where that client has no matching wheel).

## Quickstart

```python
from dynamocache import Dynamocache

cache = Dynamocache(
    "my-cache",
    endpoint="dax://my-cluster.abc123.dax.apse1.cache.amazonaws.com",
)

# One-time setup: creates the table if needed and enables native TTL.
cache.create_table()

@cache.memoize(ttl_seconds=300)
def get_profile(user_id: str) -> dict:
    ...  # an expensive database or API call

get_profile("u-42")  # computed and cached
get_profile("u-42")  # served from the cache
```

## DAX and the VPC requirement

DAX is the intended, primary way to run Dynamocache: pass your cluster endpoint
as `endpoint=`. A DAX cluster is only reachable from **inside its VPC**, so DAX
mode works from Lambda functions, ECS/EC2, and other in-VPC compute — but not
from a laptop.

For local development and tests, omit `endpoint` to use **direct DynamoDB mode**,
which talks to DynamoDB over the public API. It shares the exact same behavior
and table format; only the read latency (and the VPC requirement) differ. Direct
mode is a development convenience, not the recommended production configuration.

```python
# Local development — no DAX cluster needed.
cache = Dynamocache("my-cache")  # region resolved from your AWS config
```

Region and credentials follow standard AWS resolution (environment variables,
shared config, instance/task role). You can also pass `region_name`,
`access_key`, `secret_key`, and `session_token` explicitly.

## Cache keys

By default a key is built from the wrapped function's module and qualified name
plus a SHA-256 digest of its arguments. Two functions that merely share a bare
name never collide, and keys stay well within DynamoDB's 2 KB partition-key
limit no matter how large the arguments are.

For methods, the leading `self` argument is **excluded by default**, so the cache
keys on the arguments rather than on an instance's (per-process) memory address.
Adjust this when you need to:

```python
@cache.memoize(ttl_seconds=60, include_self=True)   # key on the instance too
def value(self): ...

@cache.memoize(ttl_seconds=60, key_fn=lambda func, args, kwargs: f"user:{args[0]}")
def by_user(user_id): ...   # full control over the key
```

If you pass custom objects as arguments, make sure they have a stable `repr`, or
supply a `key_fn` — the default keying uses `repr` for values it doesn't
otherwise recognize.

## Expiry

`ttl_seconds` sets how long an entry stays valid. Freshness is checked at read
time (giving sub-minute precision), and `create_table()` also enables DynamoDB's
**native TTL** so expired items are reclaimed by DynamoDB itself — the table does
not grow without bound.

## Failure policy

By default the cache **degrades gracefully**: if the backend is unreachable, a
read falls through to calling your function and a write failure is logged and
ignored. A cache outage slows you down; it does not take you down. Pass
`strict=True` to make backend errors propagate instead.

Dynamocache logs through the standard library under the `dynamocache` logger.

## Serialization

Values are serialized with `pickle` by default, which handles almost any Python
object. **Pickle executes code on load** — only point a pickle-backed cache at a
table written to exclusively by code you trust. For untrusted or cross-language
tables, use the JSON serializer (or your own):

```python
from dynamocache import Dynamocache, JSONSerializer

cache = Dynamocache("my-cache", serializer=JSONSerializer())
```

## Development

```bash
pip install -e ".[dev]"
make test        # unit tests (offline, no AWS)
make lint        # ruff
make typecheck   # mypy --strict
make integration # opt-in; hits real AWS/DAX, needs credentials
```

Unit tests run entirely offline against an in-memory backend (DAX cannot be
emulated locally). The integration suite is opt-in — see
`tests/test_integration.py` for the environment variables it reads.

## JavaScript / TypeScript SDK

A sibling SDK lives in [`js/`](js/README.md) and is published to npm as
[`daxcache`](js/README.md) (the `dynamocache` name was already taken on npm). It
mirrors this package's design — a `wrap()`-based API, the same DAX-first/direct
fallback model, and the same failure policy — using AWS's `@amazon-dax-sdk/lib-dax`
data-plane client. The two SDKs are independent and are not intended to share a
cache table (this package defaults to pickle; `daxcache` uses JSON).

## License

MIT — see [LICENSE](LICENSE).
