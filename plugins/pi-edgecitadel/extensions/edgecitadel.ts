import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createInterface } from "node:readline";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "@sinclair/typebox";

type JsonObject = Record<string, unknown>;

class McpBridge {
  private process: ChildProcessWithoutNullStreams | null = null;
  private nextId = 1;
  private pending = new Map<number, { resolve: (value: JsonObject) => void; reject: (error: Error) => void }>();

  async start(): Promise<void> {
    if (this.process) return;
    this.process = spawn("edgecitadel", ["native-mcp", "--host-type", "pi"], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.process.stderr.pipe(process.stderr);
    createInterface({ input: this.process.stdout }).on("line", (line) => {
      let message: JsonObject;
      try { message = JSON.parse(line) as JsonObject; } catch { return; }
      const id = message.id;
      if (typeof id !== "number") return;
      const waiter = this.pending.get(id);
      if (!waiter) return;
      this.pending.delete(id);
      if (message.error) waiter.reject(new Error(JSON.stringify(message.error)));
      else waiter.resolve((message.result ?? {}) as JsonObject);
    });
    this.process.once("exit", () => {
      for (const waiter of this.pending.values()) waiter.reject(new Error("EdgeCitadel MCP server exited"));
      this.pending.clear();
      this.process = null;
    });
    await this.request("initialize", {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo: { name: "pi-edgecitadel", version: "0.1.0" },
    });
    this.notify("notifications/initialized", {});
  }

  async tool(name: string, args: JsonObject = {}): Promise<JsonObject> {
    await this.start();
    return this.request("tools/call", { name, arguments: args });
  }

  close(): void {
    this.process?.stdin.end();
  }

  private request(method: string, params: JsonObject): Promise<JsonObject> {
    if (!this.process) return Promise.reject(new Error("EdgeCitadel MCP server is unavailable"));
    const id = this.nextId++;
    this.process.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
    return new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
  }

  private notify(method: string, params: JsonObject): void {
    this.process?.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", method, params })}\n`);
  }
}

export default function edgecitadel(pi: ExtensionAPI) {
  const bridge = new McpBridge();
  const execute = (name: string) => async (_id: string, params: JsonObject) => {
    const result = await bridge.tool(name, params);
    const content = Array.isArray(result.content) ? result.content : [{ type: "text", text: JSON.stringify(result) }];
    return { content, details: result.structuredContent ?? result };
  };

  pi.registerTool({ name: "edgecitadel_agents", label: "EdgeCitadel Agents", description: "List available EdgeCitadel Agents.", parameters: Type.Object({}), execute: execute("edgecitadel_agents") });
  pi.registerTool({ name: "edgecitadel_delegate", label: "EdgeCitadel Delegate", description: "Delegate a correlated task to another Agent.", parameters: Type.Object({ recipient_id: Type.String(), request: Type.String({ maxLength: 16384 }), skill_id: Type.Optional(Type.String()), deadline_at_ms: Type.Optional(Type.Integer()) }), execute: execute("edgecitadel_delegate") });
  pi.registerTool({ name: "edgecitadel_inbox", label: "EdgeCitadel Inbox", description: "List pending tasks for this Pi session.", parameters: Type.Object({}), execute: execute("edgecitadel_inbox") });
  pi.registerTool({ name: "edgecitadel_task_status", label: "EdgeCitadel Task", description: "Read one task state.", parameters: Type.Object({ task_id: Type.String() }), execute: execute("edgecitadel_task_status") });
  pi.registerTool({ name: "edgecitadel_task_update", label: "EdgeCitadel Task Update", description: "Record an explicit task lifecycle transition.", parameters: Type.Object({ task_id: Type.String(), state: Type.Union([Type.Literal("accepted"), Type.Literal("running"), Type.Literal("completed"), Type.Literal("failed"), Type.Literal("rejected"), Type.Literal("cancelled")]), reason: Type.Optional(Type.String({ maxLength: 1024 })), result: Type.Optional(Type.Union([Type.String({ maxLength: 65536 }), Type.Record(Type.String(), Type.Unknown())])) }), execute: execute("edgecitadel_task_update") });
  pi.registerTool({ name: "edgecitadel_trace", label: "EdgeCitadel Trace", description: "Inspect metadata-only local traces.", parameters: Type.Object({ trace_id: Type.Optional(Type.String()) }), execute: execute("edgecitadel_trace") });
  pi.registerTool({ name: "edgecitadel_diagnose", label: "EdgeCitadel Diagnose", description: "Check the local EdgeCitadel service and transport.", parameters: Type.Object({}), execute: execute("edgecitadel_diagnose") });

  pi.on("session_start", async (_event, ctx) => {
    try { await bridge.start(); ctx.ui.setStatus("edgecitadel", "connected"); }
    catch { ctx.ui.setStatus("edgecitadel", "unavailable"); }
  });
  pi.on("session_shutdown", async () => bridge.close());
}
