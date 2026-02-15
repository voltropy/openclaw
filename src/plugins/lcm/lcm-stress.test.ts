import type { AgentMessage } from "@mariozechner/pi-agent-core";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, afterEach } from "vitest";
import type { LcmConfig } from "./db/config.js";
import { closeLcmConnection } from "./db/connection.js";
import { LcmContextEngine } from "./engine.js";

// ── Helpers ──────────────────────────────────────────────────────────────────

function tmpDbPath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "lcm-stress-"));
  return path.join(dir, "test-lcm.db");
}

function createStressConfig(dbPath: string): LcmConfig {
  return {
    enabled: true,
    databasePath: dbPath,
    // Low threshold so compaction triggers quickly
    contextThreshold: 0.5,
    freshTailCount: 2,
    leafTargetTokens: 200,
    condensedTargetTokens: 300,
    maxExpandTokens: 4000,
    autocompactDisabled: false,
  };
}

/** Generate a large message (~targetChars characters). */
function makeLargeContent(index: number, targetChars: number = 10_000): string {
  const filler = `deadbeef_msg${index} `.repeat(Math.ceil(targetChars / 20));
  return filler.slice(0, targetChars);
}

/** Rough token estimate matching LCM's internal logic. */
function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

/** Mock summarize function that returns a short deterministic summary. */
function mockSummarize(text: string, aggressive?: boolean): Promise<string> {
  const truncLen = aggressive ? 100 : 200;
  return Promise.resolve(
    `[Summary${aggressive ? " (aggressive)" : ""}] ${text.length} chars compressed. ` +
      text.slice(0, truncLen),
  );
}

// Token budget small enough that a few 10k-char messages (~2500 tokens each) will exceed it
const TOKEN_BUDGET = 8_000;
const SESSION_ID = "stress-test-session";

// ── Tests ────────────────────────────────────────────────────────────────────

describe("LCM Stress: compaction lifecycle", () => {
  let dbPath: string;
  let engine: LcmContextEngine;

  afterEach(async () => {
    await engine?.dispose();
    closeLcmConnection();
    if (dbPath && fs.existsSync(dbPath)) {
      fs.rmSync(path.dirname(dbPath), { recursive: true, force: true });
    }
  });

  it("full lifecycle: ingest → compact → re-fill → compact again → two summaries", async () => {
    dbPath = tmpDbPath();
    engine = new LcmContextEngine(createStressConfig(dbPath));

    // ── Phase 1: Fill context until over threshold ──────────────────────
    // Token budget = 8000, threshold = 0.5 → triggers at 4000 tokens
    // Each 10k-char message ≈ 2500 tokens, so 2 messages should exceed threshold
    const phase1MessageCount = 4; // ~10k tokens total, well over 4k threshold
    for (let i = 0; i < phase1MessageCount; i++) {
      const role = i % 2 === 0 ? "user" : "assistant";
      const content = makeLargeContent(i);
      await engine.ingest({
        sessionId: SESSION_ID,
        message: { role, content } as AgentMessage,
      });
    }

    // Verify messages were ingested — assemble should return them
    const preCompactAssembly = await engine.assemble({
      sessionId: SESSION_ID,
      messages: [],
      tokenBudget: TOKEN_BUDGET,
    });
    expect(preCompactAssembly.messages.length).toBe(phase1MessageCount);
    const preCompactTokens = preCompactAssembly.estimatedTokens;
    expect(preCompactTokens).toBeGreaterThan(TOKEN_BUDGET * 0.5); // over threshold

    // ── Phase 2: First compaction ──────────────────────────────────────
    const compact1 = await engine.compact({
      sessionId: SESSION_ID,
      sessionFile: "/tmp/fake-session.json",
      compactionTarget: "threshold",
      tokenBudget: TOKEN_BUDGET,
      legacyParams: {
        summarize: mockSummarize,
        tokenBudget: TOKEN_BUDGET,
      },
    });

    expect(compact1.ok).toBe(true);
    expect(compact1.compacted).toBe(true);

    // After compaction, assembled context should be smaller
    const postCompact1Assembly = await engine.assemble({
      sessionId: SESSION_ID,
      messages: [],
      tokenBudget: TOKEN_BUDGET,
    });
    expect(postCompact1Assembly.estimatedTokens).toBeLessThan(preCompactTokens);

    // Count summaries in assembled messages (summaries are user messages with summary markers)
    const summaryMessages1 = postCompact1Assembly.messages.filter((m) => {
      const content = typeof m.content === "string" ? m.content : JSON.stringify(m.content);
      return (
        content.includes("[Summary") ||
        content.includes("summary_") ||
        content.includes("## Summary") ||
        content.includes("conversation history")
      );
    });
    expect(summaryMessages1.length).toBeGreaterThanOrEqual(1);

    // ── Phase 3: Fill context again ────────────────────────────────────
    const phase2Start = phase1MessageCount;
    const phase2MessageCount = 4;
    for (let i = 0; i < phase2MessageCount; i++) {
      const idx = phase2Start + i;
      const role = idx % 2 === 0 ? "user" : "assistant";
      const content = makeLargeContent(idx);
      await engine.ingest({
        sessionId: SESSION_ID,
        message: { role, content } as AgentMessage,
      });
    }

    // Verify context is over threshold again
    const preCompact2Assembly = await engine.assemble({
      sessionId: SESSION_ID,
      messages: [],
      tokenBudget: TOKEN_BUDGET,
    });
    expect(preCompact2Assembly.estimatedTokens).toBeGreaterThan(TOKEN_BUDGET * 0.5);

    // ── Phase 4: Second compaction ─────────────────────────────────────
    const compact2 = await engine.compact({
      sessionId: SESSION_ID,
      sessionFile: "/tmp/fake-session.json",
      compactionTarget: "threshold",
      tokenBudget: TOKEN_BUDGET,
      legacyParams: {
        summarize: mockSummarize,
        tokenBudget: TOKEN_BUDGET,
      },
    });

    expect(compact2.ok).toBe(true);
    expect(compact2.compacted).toBe(true);

    // ── Phase 5: Verify two summaries in context ───────────────────────
    const finalAssembly = await engine.assemble({
      sessionId: SESSION_ID,
      messages: [],
      tokenBudget: TOKEN_BUDGET,
    });

    // Should be smaller than pre-compaction
    expect(finalAssembly.estimatedTokens).toBeLessThan(preCompact2Assembly.estimatedTokens);

    // Count summary messages — should have at least 2 from two compaction rounds
    const summaryMessages2 = finalAssembly.messages.filter((m) => {
      const content = typeof m.content === "string" ? m.content : JSON.stringify(m.content);
      return (
        content.includes("[Summary") ||
        content.includes("summary_") ||
        content.includes("## Summary") ||
        content.includes("conversation history")
      );
    });
    expect(summaryMessages2.length).toBeGreaterThanOrEqual(2);

    // Total assembled messages should be reasonable — summaries + fresh tail
    expect(finalAssembly.messages.length).toBeGreaterThanOrEqual(2); // at least summaries
    expect(finalAssembly.messages.length).toBeLessThan(phase1MessageCount + phase2MessageCount); // less than all raw
  });

  it("compact is a no-op when under threshold", async () => {
    dbPath = tmpDbPath();
    engine = new LcmContextEngine(createStressConfig(dbPath));

    // Ingest one small message — well under threshold
    await engine.ingest({
      sessionId: "small-session",
      message: { role: "user", content: "Hello" } as AgentMessage,
    });

    const result = await engine.compact({
      sessionId: "small-session",
      sessionFile: "/tmp/fake.json",
      tokenBudget: TOKEN_BUDGET,
      compactionTarget: "threshold",
      legacyParams: {
        summarize: mockSummarize,
        tokenBudget: TOKEN_BUDGET,
      },
    });

    expect(result.ok).toBe(true);
    expect(result.compacted).toBe(false);
  });

  it("many small messages compact correctly", async () => {
    dbPath = tmpDbPath();
    engine = new LcmContextEngine(createStressConfig(dbPath));

    // Ingest 20 medium messages (~1000 chars each = ~250 tokens = ~5000 total)
    for (let i = 0; i < 20; i++) {
      const role = i % 2 === 0 ? "user" : "assistant";
      const content = `Message ${i}: ${"the quick brown fox jumps over the lazy dog ".repeat(20)}`;
      await engine.ingest({
        sessionId: "many-small",
        message: { role, content } as AgentMessage,
      });
    }

    const result = await engine.compact({
      sessionId: "many-small",
      sessionFile: "/tmp/fake.json",
      tokenBudget: TOKEN_BUDGET,
      compactionTarget: "threshold",
      legacyParams: {
        summarize: mockSummarize,
        tokenBudget: TOKEN_BUDGET,
      },
    });

    expect(result.ok).toBe(true);
    expect(result.compacted).toBe(true);

    // Assembly should include a summary and fresh tail
    const assembly = await engine.assemble({
      sessionId: "many-small",
      messages: [],
      tokenBudget: TOKEN_BUDGET,
    });
    expect(assembly.messages.length).toBeLessThan(20);
    expect(assembly.messages.length).toBeGreaterThan(0);
  });
});
