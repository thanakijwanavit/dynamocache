import { describe, expect, it } from "vitest";

import { canonical, makeKey } from "../src/keys.js";

describe("canonical", () => {
  it("is stable regardless of object key order", () => {
    expect(canonical({ a: 1, b: 2 })).toBe(canonical({ b: 2, a: 1 }));
  });

  it("distinguishes different values", () => {
    expect(canonical([1])).not.toBe(canonical([2]));
  });

  it("handles nested structures and null/undefined", () => {
    expect(canonical({ x: [1, null], y: undefined })).toBe(
      canonical({ y: undefined, x: [1, null] }),
    );
  });
});

describe("makeKey", () => {
  it("is deterministic for the same call", () => {
    expect(makeKey("f", [1, 2])).toBe(makeKey("f", [1, 2]));
  });

  it("differs by function name", () => {
    expect(makeKey("f", [1])).not.toBe(makeKey("g", [1]));
  });

  it("differs by arguments", () => {
    expect(makeKey("f", [1])).not.toBe(makeKey("f", [2]));
  });

  it("stays bounded for large arguments", () => {
    const key = makeKey("f", ["x".repeat(100_000)]);
    expect(key.length).toBeLessThan(200);
  });
});
