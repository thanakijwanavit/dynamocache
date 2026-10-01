/**
 * Storage backends for the cache.
 *
 * {@link DynamoCache} talks to the small {@link CacheBackend} interface rather
 * than to DynamoDB directly, which makes it trivial to substitute an in-memory
 * fake in tests (DAX cannot be emulated locally, so unit tests mock here).
 *
 * The production backend, {@link DaxBackend}, uses two clients:
 *
 * - a **data-plane** document client for get/put — Amazon DAX via
 *   `@amazon-dax-sdk/lib-dax` when endpoints are configured (the intended
 *   deployment), or `@aws-sdk/lib-dynamodb` when they are not (direct mode for
 *   local development, since DAX is only reachable inside its VPC); and
 * - a **management** client (`@aws-sdk/client-dynamodb`) for the control-plane
 *   operations `createTable` and `updateTimeToLive`, which DAX does not serve.
 *
 * The AWS SDK packages are imported dynamically so this module — and the
 * in-memory backend — can be used without them installed.
 */

/** A stored cache value and its absolute expiry (Unix seconds). */
export interface CacheEntry {
  value: string;
  ttl: number;
}

/** Interface implemented by cache storage backends. */
export interface CacheBackend {
  get(key: string): Promise<CacheEntry | null>;
  put(key: string, value: string, ttl: number): Promise<void>;
  createTable(): Promise<void>;
  enableTtl(): Promise<void>;
}

const KEY_ATTR = "key";
const VALUE_ATTR = "value";
const TTL_ATTR = "ttl";

export interface DaxBackendOptions {
  tableName: string;
  /** DAX cluster endpoint(s). Omit for direct DynamoDB mode. */
  endpoints?: string[];
  region?: string;
  /** Advanced/testing hooks: preconstructed document/management clients. */
  dataClient?: DocumentLike;
  managementClient?: ManagementLike;
}

/** Minimal shape of an AWS document client (DAX or DynamoDB). */
export interface DocumentLike {
  get(params: unknown): Promise<{ Item?: Record<string, unknown> }>;
  put(params: unknown): Promise<unknown>;
}

/** Minimal shape of the DynamoDB management client. */
export interface ManagementLike {
  createTable(params: unknown): Promise<unknown>;
  updateTimeToLive(params: unknown): Promise<unknown>;
  waitUntilExists?(name: string): Promise<unknown>;
}

/** Cache backend backed by Amazon DAX and/or DynamoDB. */
export class DaxBackend implements CacheBackend {
  private readonly tableName: string;
  private readonly endpoints?: string[];
  private readonly region?: string;
  private dataClient?: DocumentLike;
  private managementClient?: ManagementLike;

  constructor(options: DaxBackendOptions) {
    this.tableName = options.tableName;
    this.endpoints = options.endpoints;
    this.region = options.region;
    this.dataClient = options.dataClient;
    this.managementClient = options.managementClient;
  }

  private async data(): Promise<DocumentLike> {
    if (this.dataClient) return this.dataClient;
    if (this.endpoints && this.endpoints.length > 0) {
      let DaxDocument: new (config: unknown) => DocumentLike;
      try {
        ({ DaxDocument } = (await import("@amazon-dax-sdk/lib-dax")) as unknown as {
          DaxDocument: new (config: unknown) => DocumentLike;
        });
      } catch (err) {
        throw new Error(
          "DAX endpoints were configured but '@amazon-dax-sdk/lib-dax' is not " +
            "installed. Install it, or omit endpoints to use direct DynamoDB mode. " +
            `(${(err as Error).message})`,
        );
      }
      this.dataClient = new DaxDocument({ endpoints: this.endpoints, region: this.region });
    } else {
      const { DynamoDBClient } = await import("@aws-sdk/client-dynamodb");
      const { DynamoDBDocument } = await import("@aws-sdk/lib-dynamodb");
      this.dataClient = DynamoDBDocument.from(
        new DynamoDBClient({ region: this.region }),
      ) as unknown as DocumentLike;
    }
    return this.dataClient;
  }

  private async management(): Promise<ManagementLike> {
    if (this.managementClient) return this.managementClient;
    const { DynamoDBClient } = await import("@aws-sdk/client-dynamodb");
    this.managementClient = new DynamoDBClient({
      region: this.region,
    }) as unknown as ManagementLike;
    return this.managementClient;
  }

  async get(key: string): Promise<CacheEntry | null> {
    const client = await this.data();
    const res = await client.get({
      TableName: this.tableName,
      Key: { [KEY_ATTR]: key },
    });
    const item = res.Item;
    if (!item || item[VALUE_ATTR] === undefined || item[TTL_ATTR] === undefined) {
      return null;
    }
    return { value: String(item[VALUE_ATTR]), ttl: Number(item[TTL_ATTR]) };
  }

  async put(key: string, value: string, ttl: number): Promise<void> {
    const client = await this.data();
    await client.put({
      TableName: this.tableName,
      Item: { [KEY_ATTR]: key, [VALUE_ATTR]: value, [TTL_ATTR]: ttl },
    });
  }

  async createTable(): Promise<void> {
    const client = await this.management();
    const { CreateTableCommand } = await import("@aws-sdk/client-dynamodb");
    try {
      await (client as unknown as { send(cmd: unknown): Promise<unknown> }).send(
        new CreateTableCommand({
          TableName: this.tableName,
          AttributeDefinitions: [{ AttributeName: KEY_ATTR, AttributeType: "S" }],
          KeySchema: [{ AttributeName: KEY_ATTR, KeyType: "HASH" }],
          BillingMode: "PAY_PER_REQUEST",
        }),
      );
    } catch (err) {
      if ((err as { name?: string }).name !== "ResourceInUseException") throw err;
      // Table already exists — treat as success.
    }
  }

  async enableTtl(): Promise<void> {
    const client = await this.management();
    const { UpdateTimeToLiveCommand } = await import("@aws-sdk/client-dynamodb");
    try {
      await (client as unknown as { send(cmd: unknown): Promise<unknown> }).send(
        new UpdateTimeToLiveCommand({
          TableName: this.tableName,
          TimeToLiveSpecification: { Enabled: true, AttributeName: TTL_ATTR },
        }),
      );
    } catch (err) {
      const message = (err as Error).message ?? "";
      if (!message.includes("TimeToLive is already enabled")) throw err;
    }
  }
}

/** A simple in-memory {@link CacheBackend}, primarily for tests. */
export class InMemoryBackend implements CacheBackend {
  readonly store = new Map<string, CacheEntry>();

  async get(key: string): Promise<CacheEntry | null> {
    return this.store.get(key) ?? null;
  }

  async put(key: string, value: string, ttl: number): Promise<void> {
    this.store.set(key, { value, ttl });
  }

  async createTable(): Promise<void> {
    /* no-op */
  }

  async enableTtl(): Promise<void> {
    /* no-op */
  }
}
