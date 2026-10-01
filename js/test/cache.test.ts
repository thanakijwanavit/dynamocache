import { beforeEach, describe, expect, it, vi } from "vitest";

import { CacheBackend, InMemoryBackend } from "../src/backend.js";
import { DynamoCache } from "../src/cache.js";

class Clock {
  constructor(public t = 1000) {}
  now = () => this.t;
}

function makeCache(overrides: Partial<ConstructorParameters<typeof DynamoCache>[0]> = {}) {
  const backend = overrides.backend ?? new InMemoryBackend();
  const clock = new Clock();
  const cache = new DynamoCache({
    tableName: "test-cache",
    backend,
    now: clock.now,
    ...overrides,
  });
  return { cache, backend: backend as InMemoryBackend, clock };
}

describe("DynamoCache.wrap", () => {
  it("avoids recomputation on a hit", async () => {
    const { cache } = makeCache();
    let calls = 0;
    const f = cache.wrap(
      async () => {
        calls += 1;
        return 42;
      },
      { ttlSeconds: 60, name: "f" },
    );
    expect(await f()).toBe(42);
    expect(await f()).toBe(42);
    expect(calls).toBe(1);
  });

  it("caches distinct arguments separately", async () => {
    const { cache } = makeCache();
    let calls = 0;
    const square = cache.wrap(
      async (x: number) => {
        calls += 1;
        return x * x;
      },
      { ttlSeconds: 60, name: "square" },
    );
    expect(await square(2)).toBe(4);
    expect(await square(3)).toBe(9);
    expect(await square(2)).toBe(4);
    expect(calls).toBe(2);
  });

  it("recomputes after expiry", async () => {
    const { cache, clock } = makeCache();
    let n = 0;
    const f = cache.wrap(
      async () => {
        n += 1;
        return n;
      },
      { ttlSeconds: 10, name: "f" },
    );
    expect(await f()).toBe(1);
    clock.t += 5;
    expect(await f()).toBe(1);
    clock.t += 6; // 11s elapsed > 10s ttl
    expect(await f()).toBe(2);
  });

  it("writes the entry with the correct expiry", async () => {
    const { cache, backend, clock } = makeCache();
    const f = cache.wrap(async () => "v", { ttlSeconds: 30, name: "f" });
    await f();
    const [entry] = [...backend.store.values()];
    expect(entry?.ttl).toBe(clock.t + 30);
  });

  it("preserves `this` on the wrapped function", async () => {
    const { cache } = makeCache();
    const obj = {
      base: 5,
      compute: cache.wrap(
        async function (this: { base: number }, x: number) {
          return this.base + x;
        },
        { ttlSeconds: 60, name: "compute" },
      ),
    };
    expect(await obj.compute(2)).toBe(7);
  });

  it("supports a custom keyFn", async () => {
    const { cache, backend } = makeCache();
    const f = cache.wrap(async (x: number) => x, {
      ttlSeconds: 60,
      keyFn: () => "fixed",
    });
    await f(1);
    expect(await f(2)).toBe(1); // same key -> cached first value
    expect([...backend.store.keys()]).toEqual(["fixed"]);
  });
});

describe("failure policy", () => {
  function boom(kind: "get" | "put"): CacheBackend {
    const inner = new InMemoryBackend();
    return {
      get: kind === "get" ? () => Promise.reject(new Error("get boom")) : inner.get.bind(inner),
      put: kind === "put" ? () => Promise.reject(new Error("put boom")) : inner.put.bind(inner),
      createTable: inner.createTable.bind(inner),
      enableTtl: inner.enableTtl.bind(inner),
    };
  }

  beforeEach(() => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  it("degrades to calling the function on read failure", async () => {
    const { cache } = makeCache({ backend: boom("get") });
    let calls = 0;
    const f = cache.wrap(
      async () => {
        calls += 1;
        return 7;
      },
      { ttlSeconds: 60, name: "f" },
    );
    expect(await f()).toBe(7);
    expect(calls).toBe(1);
  });

  it("still returns the result on write failure", async () => {
    const { cache } = makeCache({ backend: boom("put") });
    const f = cache.wrap(async () => 7, { ttlSeconds: 60, name: "f" });
    expect(await f()).toBe(7);
  });

  it("propagates read failure in strict mode", async () => {
    const { cache } = makeCache({ backend: boom("get"), strict: true });
    const f = cache.wrap(async () => 7, { ttlSeconds: 60, name: "f" });
    await expect(f()).rejects.toThrow("get boom");
  });

  it("propagates write failure in strict mode", async () => {
    const { cache } = makeCache({ backend: boom("put"), strict: true });
    const f = cache.wrap(async () => 7, { ttlSeconds: 60, name: "f" });
    await expect(f()).rejects.toThrow("put boom");
  });
});
