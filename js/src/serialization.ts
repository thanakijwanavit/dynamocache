/**
 * Serialization strategies for cache values.
 *
 * A serializer converts an arbitrary value to a string for storage in DynamoDB
 * and back again. The default {@link JsonSerializer} is safe and human-readable;
 * supply your own by implementing {@link Serializer}.
 */

export interface Serializer {
  /** Serialize `value` to a string. */
  serialize(value: unknown): string;
  /** Deserialize a string produced by {@link serialize} back to a value. */
  deserialize(raw: string): unknown;
}

/** Default serializer: stores values as JSON. */
export class JsonSerializer implements Serializer {
  serialize(value: unknown): string {
    return JSON.stringify(value);
  }

  deserialize(raw: string): unknown {
    return JSON.parse(raw);
  }
}
