import { describe, expect, it } from "vitest";

import { JsonSerializer } from "../src/serialization.js";

describe("JsonSerializer", () => {
  const s = new JsonSerializer();

  it("round-trips JSON-compatible values", () => {
    const value = { a: 1, b: [1, 2, 3], c: "text", d: null };
    expect(s.deserialize(s.serialize(value))).toEqual(value);
  });

  it("produces a string", () => {
    expect(typeof s.serialize({ x: 1 })).toBe("string");
  });
});
