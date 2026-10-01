/**
 * Cache-key construction.
 *
 * A key is built from a stable *name* for the wrapped function plus a SHA-256
 * digest of a canonical rendering of its arguments. Canonicalization sorts
 * object keys so that property ordering never affects the key, and hashing keeps
 * keys within DynamoDB's 2 KB partition-key limit regardless of argument size.
 *
 * JavaScript has no reliable module/qualified-name for a function, and `fn.name`
 * can be empty or mangled by minifiers, so callers are encouraged to pass an
 * explicit `name` when wrapping. The receiver (`this`) is never part of the
 * default key — supply a custom `keyFn` if you need instance identity.
 */

import { createHash } from "node:crypto";

/** Render `value` to a stable string for hashing. */
export function canonical(value: unknown): string {
  if (value === undefined) return "undefined";
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) ?? String(value);
  }
  if (Array.isArray(value)) {
    return "[" + value.map(canonical).join(",") + "]";
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return "{" + keys.map((k) => JSON.stringify(k) + ":" + canonical(obj[k])).join(",") + "}";
}

/**
 * Build the cache key for a call.
 *
 * @param name - A stable name identifying the wrapped function.
 * @param args - The arguments the call was made with.
 * @returns A key of the form `"<name>:<sha256 hex>"`.
 */
export function makeKey(name: string, args: readonly unknown[]): string {
  const digest = createHash("sha256").update(canonical(args)).digest("hex");
  return `${name}:${digest}`;
}
