/**
 * LCM Live E2E Test
 *
 * Spawns a real OpenClaw gateway subprocess, connects via WebSocket with
 * device auth, sends messages through the full pipeline (real LLM calls),
 * and verifies LCM database state (messages ingested, summaries created).
 *
 * NO mocking. Everything is real.
 *
 * Run with:
 *   OPENCLAW_LCM_LIVE_TEST=1 npx vitest run src/plugins/lcm/lcm-e2e-live.test.ts --test-timeout 300000
 */
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { WebSocket } from "ws";
import { buildDeviceAuthPayload } from "../../gateway/device-auth.js";
import {
  loadOrCreateDeviceIdentity,
  publicKeyRawBase64UrlFromPem,
  signDevicePayload,
} from "../../infra/device-identity.js";
import { getDeterministicFreePortBlock } from "../../test-utils/ports.js";
import { getLcmConnection, closeLcmConnection } from "./db/connection.js";

const ENABLED = process.env.OPENCLAW_LCM_LIVE_TEST === "1";
const GATEWAY_TOKEN = "lcm-e2e-test-token-1234567890";

// ── Helpers ──────────────────────────────────────────────────────────────────

function rpcRequest(
  ws: WebSocket,
  method: string,
  params: Record<string, unknown>,
  timeoutMs = 60_000,
): Promise<{ ok: boolean; payload?: unknown; error?: string }> {
  return new Promise((resolve, reject) => {
    const id = randomUUID();
    const timer = setTimeout(() => reject(new Error(`RPC ${method} timed out`)), timeoutMs);
    const handler = (data: Buffer | string) => {
      try {
        const msg = JSON.parse(typeof data === "string" ? data : data.toString("utf-8"));
        if (msg.id === id && msg.type === "res") {
          clearTimeout(timer);
          ws.removeListener("message", handler);
          resolve({ ok: msg.ok !== false, payload: msg.payload, error: msg.error?.message });
        }
      } catch {
        // ignore parse errors
      }
    };
    ws.on("message", handler);
    ws.send(JSON.stringify({ type: "req", id, method, params }));
  });
}

function waitForEvent(
  ws: WebSocket,
  predicate: (event: string, payload: Record<string, unknown>) => boolean,
  timeoutMs = 120_000,
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("waitForEvent timed out")), timeoutMs);
    const handler = (data: Buffer | string) => {
      try {
        const msg = JSON.parse(typeof data === "string" ? data : data.toString("utf-8"));
        if (msg.type === "event" && predicate(msg.event, msg.payload)) {
          clearTimeout(timer);
          ws.removeListener("message", handler);
          resolve(msg.payload);
        }
      } catch {
        // ignore
      }
    };
    ws.on("message", handler);
  });
}

function waitForChatDone(ws: WebSocket, sessionKey: string, timeoutMs = 120_000) {
  return waitForEvent(
    ws,
    (event, payload) =>
      event === "chat" &&
      payload?.sessionKey === sessionKey &&
      (payload?.state === "final" || payload?.state === "error"),
    timeoutMs,
  );
}

async function connectToGateway(
  port: number,
  deviceIdentityPath: string,
  token?: string,
): Promise<WebSocket> {
  const ws = await new Promise<WebSocket>((resolve, reject) => {
    const sock = new WebSocket(`ws://127.0.0.1:${port}`);
    const timer = setTimeout(() => reject(new Error("WS open timed out")), 10_000);
    sock.on("open", () => {
      clearTimeout(timer);
      resolve(sock);
    });
    sock.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });

  const identity = loadOrCreateDeviceIdentity(deviceIdentityPath);
  const signedAtMs = Date.now();
  const clientId = "test";
  const clientMode = "test";
  const role = "operator";
  const scopes = ["operator.admin"];

  const payload = buildDeviceAuthPayload({
    deviceId: identity.deviceId,
    clientId,
    clientMode,
    role,
    scopes,
    signedAtMs,
    token: token ?? null,
  });
  const signature = signDevicePayload(identity.privateKeyPem, payload);

  const connectId = randomUUID();
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("connect handshake timed out")), 10_000);
    const handler = (data: Buffer | string) => {
      try {
        const msg = JSON.parse(typeof data === "string" ? data : data.toString("utf-8"));
        if (msg.type === "res" && msg.id === connectId) {
          clearTimeout(timer);
          ws.removeListener("message", handler);
          if (msg.ok) {
            resolve();
          } else {
            reject(new Error(`connect failed: ${msg.error?.message ?? JSON.stringify(msg)}`));
          }
        }
      } catch {
        // ignore
      }
    };
    ws.on("message", handler);
    ws.send(
      JSON.stringify({
        type: "req",
        id: connectId,
        method: "connect",
        params: {
          minProtocol: 3,
          maxProtocol: 3,
          client: {
            id: clientId,
            version: "0.0.1",
            platform: "test",
            mode: clientMode,
          },
          role,
          scopes,
          caps: [],
          auth: token ? { token } : undefined,
          device: {
            id: identity.deviceId,
            publicKey: publicKeyRawBase64UrlFromPem(identity.publicKeyPem),
            signature,
            signedAt: signedAtMs,
          },
        },
      }),
    );
  });

  return ws;
}

// ── Test Suite ───────────────────────────────────────────────────────────────

describe.skipIf(!ENABLED)("LCM Live E2E", () => {
  let tempDir: string;
  let stateDir: string;
  let lcmDbPath: string;
  let port: number;
  let gatewayProcess: ReturnType<typeof import("node:child_process").spawn> | null = null;
  let gatewayOutput = "";
  let deviceIdentityPath: string;

  beforeAll(async () => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "lcm-live-e2e-"));
    stateDir = path.join(tempDir, ".openclaw");
    fs.mkdirSync(stateDir, { recursive: true });
    fs.mkdirSync(path.join(stateDir, "workspace"), { recursive: true });
    fs.mkdirSync(path.join(stateDir, "identity"), { recursive: true });
    lcmDbPath = path.join(stateDir, "lcm.db");

    port = await getDeterministicFreePortBlock();

    // Write config
    const configPath = path.join(stateDir, "openclaw.json");
    const config = {
      gateway: {
        port,
        bind: "loopback",
        mode: "local",
        auth: { mode: "token", token: "lcm-e2e-test-token-1234567890" },
      },
      plugins: {
        slots: {
          contextEngine: "lcm",
        },
      },
    };
    fs.writeFileSync(configPath, JSON.stringify(config, null, 2));

    // Pre-create device identity (shared between gateway + test client)
    deviceIdentityPath = path.join(stateDir, "identity", "device.json");
    loadOrCreateDeviceIdentity(deviceIdentityPath);

    // Write a bootstrap script that starts the real gateway via tsx
    const bootstrapPath = path.join(tempDir, "lcm-e2e-bootstrap.mjs");
    const serverImplPath = path.resolve("src/gateway/server.impl.ts");
    const runtimePath = path.resolve("src/runtime.ts");
    const runLoopPath = path.resolve("src/cli/gateway-cli/run-loop.ts");

    // Use the run-loop approach to get proper gateway startup with all subsystems
    fs.writeFileSync(
      bootstrapPath,
      [
        `const { runGatewayLoop } = await import(${JSON.stringify(pathToFileURL(runLoopPath).href)});`,
        `const { defaultRuntime } = await import(${JSON.stringify(pathToFileURL(runtimePath).href)});`,
        `const { startGatewayServer } = await import(${JSON.stringify(pathToFileURL(serverImplPath).href)});`,
        ``,
        `await runGatewayLoop({`,
        `  start: async () => {`,
        `    const server = await startGatewayServer(${port});`,
        `    process.stdout.write("READY\\n");`,
        `    return server;`,
        `  },`,
        `  runtime: defaultRuntime,`,
        `});`,
      ].join("\n"),
      "utf8",
    );

    // Spawn gateway subprocess using process.execPath (real node, not mise shim)
    const { spawn } = await import("node:child_process");
    const nodeBin = process.execPath;

    gatewayProcess = spawn(nodeBin, ["--import", "tsx", bootstrapPath], {
      cwd: path.resolve(import.meta.dirname, "../../.."),
      env: {
        ...process.env,
        // Isolation: keep real HOME (for mise/node resolution) but redirect state/config
        OPENCLAW_STATE_DIR: stateDir,
        OPENCLAW_CONFIG_PATH: configPath,
        OPENCLAW_DEVICE_IDENTITY_PATH: deviceIdentityPath,
        // No respawn
        OPENCLAW_NO_RESPAWN: "1",
        OPENCLAW_NODE_OPTIONS_READY: "1",
        // Cheap model
        OPENCLAW_DEFAULT_MODEL: "anthropic/claude-haiku",
        // LCM tuning for small context
        LCM_CONTEXT_THRESHOLD: "0.5",
        LCM_FRESH_TAIL_COUNT: "2",
        LCM_LEAF_TARGET_TOKENS: "200",
        LCM_DATABASE_PATH: lcmDbPath,
        // Skip optional subsystems
        OPENCLAW_SKIP_BROWSER_CONTROL_SERVER: "1",
        OPENCLAW_SKIP_GMAIL_WATCHER: "1",
        OPENCLAW_SKIP_CANVAS_HOST: "1",
        OPENCLAW_SKIP_CHANNELS: "1",
        OPENCLAW_SKIP_CRON: "1",
        OPENCLAW_SKIP_PROVIDERS: "1",
        // Avoid vitest interference
        VITEST: undefined,
        OPENCLAW_TEST_MINIMAL_GATEWAY: undefined,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    gatewayProcess.stdout?.setEncoding("utf8");
    gatewayProcess.stderr?.setEncoding("utf8");
    gatewayProcess.stdout?.on("data", (chunk: string) => {
      gatewayOutput += chunk;
    });
    gatewayProcess.stderr?.on("data", (chunk: string) => {
      gatewayOutput += chunk;
    });

    // Wait for READY signal or WS connectivity
    const maxWaitMs = 60_000;
    const startTime = Date.now();
    let ready = false;

    // First try waiting for READY in stdout
    const readyPromise = new Promise<void>((resolve) => {
      const check = () => {
        if (gatewayOutput.includes("READY")) {
          resolve();
        }
      };
      gatewayProcess!.stdout?.on("data", check);
      gatewayProcess!.stderr?.on("data", check);
    });

    // Also poll WS as backup
    const pollPromise = (async () => {
      while (Date.now() - startTime < maxWaitMs) {
        if (gatewayProcess.exitCode !== null) {
          throw new Error(
            `Gateway exited with code ${gatewayProcess.exitCode}.\nOutput:\n${gatewayOutput.slice(-3000)}`,
          );
        }
        try {
          const testWs = await connectToGateway(port, deviceIdentityPath, GATEWAY_TOKEN);
          testWs.close();
          return;
        } catch {
          await new Promise((r) => setTimeout(r, 500));
        }
      }
      throw new Error(
        `Gateway did not become ready in ${maxWaitMs}ms.\nOutput:\n${gatewayOutput.slice(-3000)}`,
      );
    })();

    await Promise.race([readyPromise, pollPromise]);
    // Give it a moment to fully initialize after READY
    await new Promise((r) => setTimeout(r, 1000));
    ready = true;

    if (!ready) {
      throw new Error("Gateway failed to start");
    }

    console.error(`  Gateway started on port ${port}`);
  }, 90_000);

  afterAll(async () => {
    closeLcmConnection();
    if (gatewayProcess && gatewayProcess.exitCode === null) {
      gatewayProcess.kill("SIGTERM");
      await new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          gatewayProcess?.kill("SIGKILL");
          resolve();
        }, 5_000);
        gatewayProcess!.on("exit", () => {
          clearTimeout(timer);
          resolve();
        });
      });
    }
    if (tempDir && fs.existsSync(tempDir)) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it("ingests messages and produces summaries after compaction", async () => {
    const ws = await connectToGateway(port, deviceIdentityPath, GATEWAY_TOKEN);

    try {
      const sessionKey = "agent:main:main";
      const filler = "deadbeef ".repeat(1250); // ~2500 tokens

      // ── Phase 1: Send 4 messages to build context ───────────────────
      for (let i = 0; i < 4; i++) {
        const donePromise = waitForChatDone(ws, sessionKey);
        const sendRes = await rpcRequest(ws, "chat.send", {
          sessionKey,
          message: `Message ${i}: ${filler}\n\nRespond with just "ok${i}".`,
          idempotencyKey: `fill-${i}-${randomUUID()}`,
        });
        expect(sendRes.ok, `chat.send ${i} failed: ${sendRes.error}`).toBe(true);
        await donePromise;
        console.error(`  ✓ message ${i} done`);
      }

      // ── Verify: messages in LCM DB ──────────────────────────────────
      const db1 = getLcmConnection(lcmDbPath);
      const tables = (
        db1.prepare("SELECT name FROM sqlite_master WHERE type='table'").all() as {
          name: string;
        }[]
      ).map((t) => t.name);
      expect(tables).toContain("messages");
      expect(tables).toContain("conversations");

      const msgCount1 = (
        db1.prepare("SELECT count(*) as cnt FROM messages").get() as { cnt: number }
      ).cnt;
      console.error(`  Messages in DB after phase 1: ${msgCount1}`);
      expect(msgCount1).toBeGreaterThan(0);
      closeLcmConnection(lcmDbPath);

      // ── Phase 2: Trigger compaction via /compact ────────────────────
      const compactDone1 = waitForChatDone(ws, sessionKey);
      const compactRes1 = await rpcRequest(ws, "chat.send", {
        sessionKey,
        message: "/compact",
        idempotencyKey: `compact-1-${randomUUID()}`,
      });
      expect(compactRes1.ok, `compact 1 failed: ${compactRes1.error}`).toBe(true);
      await compactDone1;
      console.error("  ✓ first compaction done");

      // ── Verify: summaries created ───────────────────────────────────
      const db2 = getLcmConnection(lcmDbPath);

      const hasSummariesTable =
        (
          db2
            .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='summaries'")
            .all() as { name: string }[]
        ).length > 0;
      expect(hasSummariesTable, "summaries table should exist after compaction").toBe(true);

      const summaryCount1 = (
        db2.prepare("SELECT count(*) as cnt FROM summaries").get() as { cnt: number }
      ).cnt;
      console.error(`  Summaries after first compaction: ${summaryCount1}`);
      expect(summaryCount1).toBeGreaterThanOrEqual(1);
      closeLcmConnection(lcmDbPath);

      // ── Phase 3: Send 4 more messages ───────────────────────────────
      for (let i = 4; i < 8; i++) {
        const donePromise = waitForChatDone(ws, sessionKey);
        await rpcRequest(ws, "chat.send", {
          sessionKey,
          message: `Message ${i}: ${filler}\n\nRespond with just "ok${i}".`,
          idempotencyKey: `fill-${i}-${randomUUID()}`,
        });
        await donePromise;
        console.error(`  ✓ message ${i} done`);
      }

      // ── Phase 4: Second compaction ──────────────────────────────────
      const compactDone2 = waitForChatDone(ws, sessionKey);
      await rpcRequest(ws, "chat.send", {
        sessionKey,
        message: "/compact",
        idempotencyKey: `compact-2-${randomUUID()}`,
      });
      await compactDone2;
      console.error("  ✓ second compaction done");

      // ── Verify: ≥2 summaries ────────────────────────────────────────
      const db3 = getLcmConnection(lcmDbPath);

      const summaryCount2 = (
        db3.prepare("SELECT count(*) as cnt FROM summaries").get() as { cnt: number }
      ).cnt;
      console.error(`  Summaries after second compaction: ${summaryCount2}`);
      expect(summaryCount2).toBeGreaterThanOrEqual(2);

      // Context tokens < raw message tokens (compaction saved space)
      const totalRawTokens = (
        db3.prepare("SELECT COALESCE(SUM(token_count), 0) as total FROM messages").get() as {
          total: number;
        }
      ).total;
      const totalSummaryTokens = (
        db3.prepare("SELECT COALESCE(SUM(token_count), 0) as total FROM summaries").get() as {
          total: number;
        }
      ).total;
      console.error(
        `  Raw message tokens: ${totalRawTokens}, Summary tokens: ${totalSummaryTokens}`,
      );
      // Summaries should be smaller than the messages they replace
      expect(totalSummaryTokens).toBeLessThan(totalRawTokens);
      closeLcmConnection(lcmDbPath);

      // ── Phase 5: Verify agent still works ───────────────────────────
      const finalDone = waitForChatDone(ws, sessionKey);
      await rpcRequest(ws, "chat.send", {
        sessionKey,
        message: 'What number was the last "ok" response? Reply with just the number.',
        idempotencyKey: `verify-${randomUUID()}`,
      });
      const finalPayload = await finalDone;
      console.error(`  ✓ final verification done: ${JSON.stringify(finalPayload).slice(0, 200)}`);
    } finally {
      ws.close();
    }
  }, 300_000);
});
