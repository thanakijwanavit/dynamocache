/**
 * daxcache — cache function results in DynamoDB, accelerated by DAX.
 *
 * See {@link DynamoCache} for the main entry point.
 */

export { DynamoCache } from "./cache.js";
export type {
  DynamoCacheOptions,
  WrapOptions,
  Wrapped,
  AnyFn,
} from "./cache.js";
export { JsonSerializer } from "./serialization.js";
export type { Serializer } from "./serialization.js";
export {
  DaxBackend,
  InMemoryBackend,
} from "./backend.js";
export type {
  CacheBackend,
  CacheEntry,
  DaxBackendOptions,
} from "./backend.js";
export { makeKey, canonical } from "./keys.js";
