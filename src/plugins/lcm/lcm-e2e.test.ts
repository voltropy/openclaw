import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, afterEach } from "vitest";
import type { LcmConfig } from "./db/config.js";
import { closeLcmConnection } from "./db/connection.js";
import { LcmContextEngine } from "./engine.js";

function tmpDbPath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "lcm-e2e-"));
  return path.join(dir, "test-lcm.db");
}

function createTestConfig(dbPath: string): LcmConfig {
  return {
    enabled: true,
    databasePath: dbPath,
    contextThreshold: 0.75,
    freshTailCount: 4,
    leafTargetTokens: 600,
    condensedTargetTokens: 900,
    maxExpandTokens: 4000,
    autocompactDisabled: true,
  };
}

describe("LCM E2E: real SQLite", () => {
  let dbPath: string;
  let engine: LcmContextEngine;

  afterEach(async () => {
    await engine?.dispose();
    closeLcmConnection();
    if (dbPath && fs.existsSync(dbPath)) {
      fs.rmSync(path.dirname(dbPath), { recursive: true, force: true });
    }
  });

  it("ingest creates tables and stores messages", async () => {
    dbPath = tmpDbPath();
    engine = new LcmContextEngine(createTestConfig(dbPath));

    // Ingest a user message
    const result = await engine.ingest({
      sessionId: "test-session-1",
      message: { role: "user", content: "Hello world" } as any,
    });
    expect(result.ingested).toBe(true);

    // DB should exist and have tables
    expect(fs.existsSync(dbPath)).toBe(true);

    // Ingest an assistant message
    const result2 = await engine.ingest({
      sessionId: "test-session-1",
      message: {
        role: "assistant",
        content: [{ type: "text", text: "Hi there!" }],
      } as any,
    });
    expect(result2.ingested).toBe(true);
  });

  it("assemble returns ingested messages", async () => {
    dbPath = tmpDbPath();
    engine = new LcmContextEngine(createTestConfig(dbPath));

    // Ingest several messages
    const messages = [
      { role: "user", content: "First message" },
      { role: "assistant", content: [{ type: "text", text: "First reply" }] },
      { role: "user", content: "Second message" },
      { role: "assistant", content: [{ type: "text", text: "Second reply" }] },
    ];

    for (const msg of messages) {
      await engine.ingest({
        sessionId: "test-session-2",
        message: msg as any,
      });
    }

    // Assemble should return messages
    const assembled = await engine.assemble({
      sessionId: "test-session-2",
      messages: [], // empty — LCM should build from its store
      tokenBudget: 128_000,
    });

    expect(assembled.messages.length).toBeGreaterThan(0);
    // Should have 4 messages
    expect(assembled.messages.length).toBe(4);
  });

  it("ingest handles BashExecutionMessage variant", async () => {
    dbPath = tmpDbPath();
    engine = new LcmContextEngine(createTestConfig(dbPath));

    const result = await engine.ingest({
      sessionId: "test-session-3",
      message: {
        role: "assistant",
        command: "ls -la",
        output: "total 42\ndrwxr-xr-x  5 user staff 160 Feb 13 12:00 .",
      } as any,
    });
    expect(result.ingested).toBe(true);
  });

  it("auto-compaction summary can be ingested", async () => {
    dbPath = tmpDbPath();
    engine = new LcmContextEngine(createTestConfig(dbPath));

    // Simulate ingesting an auto-compaction summary
    const summary = "## Summary\nThe conversation covered setting up LCM tools...";
    const result = await engine.ingest({
      sessionId: "test-session-4",
      message: { role: "user", content: summary } as any,
    });
    expect(result.ingested).toBe(true);

    // Should be retrievable via assemble
    const assembled = await engine.assemble({
      sessionId: "test-session-4",
      messages: [],
      tokenBudget: 128_000,
    });
    expect(assembled.messages.length).toBe(1);
  });

  it("retrieval engine is accessible", async () => {
    dbPath = tmpDbPath();
    engine = new LcmContextEngine(createTestConfig(dbPath));

    // Ingest something first to trigger migration
    await engine.ingest({
      sessionId: "test-session-5",
      message: { role: "user", content: "Test message for retrieval" } as any,
    });

    const retrieval = engine.getRetrieval();
    expect(retrieval).toBeDefined();

    // Describe should work (even if it finds nothing for a fake ID)
    const result = await retrieval.describe("sum_nonexistent");
    expect(result).toBeDefined();
  });

  it("compact evaluates correctly when under threshold", async () => {
    dbPath = tmpDbPath();
    engine = new LcmContextEngine(createTestConfig(dbPath));

    // Ingest a small message
    await engine.ingest({
      sessionId: "test-session-6",
      message: { role: "user", content: "Short message" } as any,
    });

    // Compact should say "below threshold"
    const result = await engine.compact({
      sessionId: "test-session-6",
      sessionFile: "/tmp/fake-session.json",
    });
    expect(result.ok).toBe(true);
    expect(result.compacted).toBe(false);
  });
});
