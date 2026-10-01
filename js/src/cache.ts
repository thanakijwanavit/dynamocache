/** The public {@link DynamoCache} entry point. */

import { CacheBackend, DaxBackend } from "./backend.js";
import { makeKey } from "./keys.js";
import { JsonSerializer, Serializer } from "./serialization.js";

/** A function whose results can be cached. */
export type AnyFn = (...args: any[]) => any;

export interface DynamoCacheOptions {
  tableName: string;
  /**
   * DAX cluster endpoint(s) — the intended production configuration. Omit for
   * direct DynamoDB mode (local development/testing; DAX is VPC-only).
   */
  endpoints?: string[];
  region?: string;
  /** Value (de)serialization strategy. Defaults to JSON. */
  serializer?: Serializer;
  /**
   * Failure policy. When `false` (default), backend errors are logged and the
   * cache degrades gracefully; when `true`, they propagate.
   */
  strict?: boolean;
  /** Advanced/testing hook: a preconstructed backend. */
  backend?: CacheBackend;
  /** Advanced/testing hook: current time in Unix seconds. Defaults to Date.now/1000. */
  now?: () => number;
}

export interface WrapOptions {
  ttlSeconds: number;
  /** Stable name identifying the function in cache keys. Defaults to `fn.name`. */
  name?: string;
  /** Custom key builder. Receives the call arguments; returns a string key. */
  keyFn?: (args: readonly unknown[]) => string;
}

/** A wrapped function always returns a promise of the original result. */
export type Wrapped<F extends AnyFn> = (
  ...args: Parameters<F>
) => Promise<Awaited<ReturnType<F>>>;

/**
 * Cache function results in DynamoDB, accelerated by DAX.
 *
 * @example
 * ```ts
 * const cache = new DynamoCache({
 *   tableName: "my-cache",
 *   endpoints: ["dax://my-cluster.abc123.dax.apse1.cache.amazonaws.com"],
 * });
 * await cache.createTable();
 *
 * const getProfile = cache.wrap(
 *   async (userId: string) => fetchProfile(userId),
 *   { ttlSeconds: 300 },
 * );
 * await getProfile("u-42"); // computed and cached
 * await getProfile("u-42"); // served from the cache
 * ```
 */
export class DynamoCache {
  private readonly serializer: Serializer;
  private readonly strict: boolean;
  private readonly now: () => number;
  private readonly backend: CacheBackend;

  constructor(options: DynamoCacheOptions) {
    this.serializer = options.serializer ?? new JsonSerializer();
    this.strict = options.strict ?? false;
    this.now = options.now ?? (() => Math.floor(Date.now() / 1000));
    this.backend =
      options.backend ??
      new DaxBackend({
        tableName: options.tableName,
        endpoints: options.endpoints,
        region: options.region,
      });
  }

  /** Create the cache table if needed and enable native TTL. Idempotent. */
  async createTable(options: { enableTtl?: boolean } = {}): Promise<void> {
    await this.backend.createTable();
    if (options.enableTtl ?? true) {
      await this.backend.enableTtl();
    }
  }

  /**
   * Wrap `fn` so its results are memoized in the cache. The returned function is
   * always async. The receiver (`this`) is preserved but is not part of the
   * default cache key; use `keyFn` if you need it.
   */
  wrap<F extends AnyFn>(fn: F, options: WrapOptions): Wrapped<F> {
    const name = options.name ?? fn.name ?? "anonymous";
    const ttlSeconds = options.ttlSeconds;
    const keyFn = options.keyFn;
    const self = this;

    return async function (
      this: unknown,
      ...args: Parameters<F>
    ): Promise<Awaited<ReturnType<F>>> {
      const key = keyFn ? keyFn(args) : makeKey(name, args);

      const hit = await self.read(key);
      if (hit.found) {
        return hit.value as Awaited<ReturnType<F>>;
      }

      const result = await fn.apply(this, args);
      await self.write(key, result, ttlSeconds);
      return result;
    };
  }

  private async read(key: string): Promise<{ found: boolean; value?: unknown }> {
    let entry;
    try {
      entry = await this.backend.get(key);
    } catch (err) {
      if (this.strict) throw err;
      console.warn(`[daxcache] read failed for key ${key}; recomputing`, err);
      return { found: false };
    }
    if (!entry) return { found: false };
    if (entry.ttl <= this.now()) return { found: false };
    try {
      return { found: true, value: this.serializer.deserialize(entry.value) };
    } catch (err) {
      if (this.strict) throw err;
      console.warn(`[daxcache] could not deserialize key ${key}; recomputing`, err);
      return { found: false };
    }
  }

  private async write(key: string, result: unknown, ttlSeconds: number): Promise<void> {
    try {
      const payload = this.serializer.serialize(result);
      await this.backend.put(key, payload, this.now() + ttlSeconds);
    } catch (err) {
      if (this.strict) throw err;
      console.warn(`[daxcache] write failed for key ${key}; result not cached`, err);
    }
  }
}
