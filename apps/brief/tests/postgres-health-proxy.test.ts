import { describe, expect, it } from "vitest";
import { createHealthContext } from "../src/health.js";
import { createPostgresHealthProxy } from "../src/postgres-health-proxy.js";

interface ExampleStore {
  load(id: string): Promise<string>;
  persist(value: string): Promise<string>;
  ping(): Promise<string>;
}

class ExampleStoreAdapter implements ExampleStore {
  constructor(
    private readonly prefix: string,
    private readonly failLoad: boolean = false,
    private readonly failPing: boolean = false
  ) {}

  async load(id: string): Promise<string> {
    if (this.failLoad) {
      throw new Error("load failed");
    }
    return `${this.prefix}:${id}`;
  }

  async persist(value: string): Promise<string> {
    return `${this.prefix}:${value}`;
  }

  async ping(): Promise<string> {
    if (this.failPing) {
      throw new Error("ping failed");
    }
    return `${this.prefix}:pong`;
  }
}

describe("createPostgresHealthProxy", () => {
  it("marks postgres healthy when tracked methods succeed", async () => {
    const healthContext = createHealthContext();
    healthContext.postgresHealthy = false;

    const proxy = createPostgresHealthProxy(
      new ExampleStoreAdapter("ok"),
      healthContext,
      ["load", "persist"]
    );

    await expect(proxy.load("abc")).resolves.toBe("ok:abc");
    await expect(proxy.persist("value")).resolves.toBe("ok:value");
    expect(healthContext.postgresHealthy).toBe(true);
  });

  it("marks postgres unhealthy when a tracked method fails", async () => {
    const healthContext = createHealthContext();
    healthContext.postgresHealthy = true;

    const proxy = createPostgresHealthProxy(
      new ExampleStoreAdapter("broken", true),
      healthContext,
      ["load", "persist"]
    );

    await expect(proxy.load("abc")).rejects.toThrow("load failed");
    expect(healthContext.postgresHealthy).toBe(false);
  });

  it("does not change postgres health for untracked method failures", async () => {
    const healthContext = createHealthContext();
    healthContext.postgresHealthy = true;

    const proxy = createPostgresHealthProxy(
      new ExampleStoreAdapter("partial", false, true),
      healthContext,
      ["load", "persist"]
    );

    await expect(proxy.ping()).rejects.toThrow("ping failed");
    expect(healthContext.postgresHealthy).toBe(true);
  });
});
