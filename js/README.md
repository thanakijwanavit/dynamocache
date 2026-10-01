# daxcache

Cache function results in DynamoDB, accelerated by [Amazon DAX](https://aws.amazon.com/dynamodb/dax/) — the JavaScript/TypeScript sibling of the Python [`dynamocache`](../README.md) package.

Wrap an async function and its results are memoized in DynamoDB with a per-entry TTL. Point it at a DAX cluster for microsecond-latency reads from in-VPC compute such as AWS Lambda.

> **Status:** 0.1.0 (alpha). The API is still settling.

## Install

```bash
npm install daxcache
```

`daxcache` uses the AWS SDK for JavaScript v3. DAX mode goes through
[`@amazon-dax-sdk/lib-dax`](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DAX.client.run-application-nodejs-3.html),
AWS's DAX data-plane client for SDK v3.

## Quickstart

```ts
import { DynamoCache } from "daxcache";

const cache = new DynamoCache({
  tableName: "my-cache",
  endpoints: ["dax://my-cluster.abc123.dax.apse1.cache.amazonaws.com"],
});

// One-time setup: create the table if needed and enable native TTL.
await cache.createTable();

const getProfile = cache.wrap(
  async (userId: string) => fetchProfile(userId), // an expensive call
  { ttlSeconds: 300 },
);

await getProfile("u-42"); // computed and cached
await getProfile("u-42"); // served from the cache
```

`cache.wrap` returns an **async** function regardless of whether the wrapped
function was async.

## DAX and the VPC requirement

DAX is the intended production path: pass your cluster `endpoints`. A DAX cluster
is reachable only from **inside its VPC**, so DAX mode works from Lambda,
ECS/EC2, and other in-VPC compute — but not from a laptop.

Omit `endpoints` for **direct DynamoDB mode**, which shares the same behavior and
table format and is meant for local development and tests:

```ts
const cache = new DynamoCache({ tableName: "my-cache", region: "ap-southeast-1" });
```

Note that `@amazon-dax-sdk/lib-dax` supports only the aggregated document-client
style (no bare-bones `.send(Command)` calls); `daxcache` uses it accordingly.

## Cache keys

A key is built from a stable function name plus a SHA-256 digest of the
arguments, so keys stay within DynamoDB's 2 KB partition-key limit. Because
`fn.name` can be empty or mangled by minifiers, pass an explicit `name` when it
matters, or a full `keyFn`:

```ts
cache.wrap(fn, { ttlSeconds: 60, name: "getProfile" });
cache.wrap(fn, { ttlSeconds: 60, keyFn: (args) => `user:${args[0]}` });
```

The receiver (`this`) is preserved on the wrapped function but is **not** part of
the default key — use `keyFn` if you need instance identity.

## Failure policy

By default the cache **degrades gracefully**: backend errors are logged to
`console.warn` and a read failure falls through to calling your function. Pass
`strict: true` to make backend errors throw instead.

## Cross-language note

`daxcache` stores values as JSON, while the Python `dynamocache` package uses
pickle by default. The two SDKs are independent and are not intended to share a
cache table. If you deliberately want one, configure a JSON serializer on the
Python side and use matching table/attribute conventions.

## Development

```bash
npm install
npm run typecheck
npm test
npm run build
```

Unit tests run offline against an in-memory backend (DAX cannot be emulated
locally). The AWS SDK clients are imported dynamically, so tests need no AWS
credentials.

## License

MIT
