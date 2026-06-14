/**
 * HostSession — a reusable, stateful MCPL *host* over a spawned server process.
 *
 * Spawns an MCPL server over stdio, performs the host handshake (advertising
 * MCPL support), then maintains live state (tools, channels, feature sets,
 * an event log) and answers server→host requests.
 *
 * This is the engine behind both the CLI REPL (`cli.ts`) and the web UI
 * (`web.ts`). It is transport-agnostic toward its consumer: it exposes plain
 * methods (`callTool`, `publish`, …) and emits normalized `event` records that
 * a CLI can print or a browser can render.
 *
 * Server→host requests are split into two classes:
 *   - **auto** — answered immediately with a sensible default (push/event,
 *     channels/incoming, state/update, model/info, …). The host has to ack
 *     these or the server stalls, and there is no meaningful human decision.
 *   - **interactive** — held open as `pending` requests (scope/elevate,
 *     inference/request) so a human acting as the agent can answer. A fallback
 *     default is applied after `pendingTimeoutMs` so a server never hangs
 *     forever, and an `autoApprove` toggle short-circuits them entirely.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { EventEmitter } from 'node:events';
import { randomUUID } from 'node:crypto';
import { McplConnection, ConnectionClosedError } from '@connectome/mcpl-core';
import { StateStore, type StateView, type StateCheckpoint } from './state-store.js';

// ── Public shapes ──

export interface ToolDef {
  name: string;
  description?: string;
  inputSchema?: unknown;
  /** Optional owning feature set — lets the host attribute state per tool. */
  featureSet?: string;
  _meta?: { featureSet?: string };
}

export interface ChannelDesc {
  id: string;
  label?: string;
  type?: string;
  direction?: string;
  address?: unknown;
  metadata?: unknown;
}

export type EventDir = 'in' | 'out' | 'system';

/** A normalized record of anything notable that happened on the session. */
export interface SessionEvent {
  seq: number;
  at: string;
  dir: EventDir;
  /** Coarse category for UI grouping: push | channel | request | response | tool | system | error */
  kind: string;
  /** JSON-RPC method, when applicable. */
  method?: string;
  /** One-line human summary. */
  summary: string;
  /** Structured payload (raw params/result), for the detail/inspector view. */
  data?: unknown;
  /** Set for interactive server→host requests awaiting a host answer. */
  pendingId?: string;
}

export interface PendingRequest {
  pendingId: string;
  rpcId: string | number;
  method: string;
  params: unknown;
  at: string;
  /** Resolve with the host's chosen result. */
  resolve: (result: unknown) => void;
  /** Reject with a JSON-RPC error. */
  reject: (code: number, message: string) => void;
  timer?: ReturnType<typeof setTimeout>;
}

export interface SessionSnapshot {
  serverInfo: { name?: string; version?: string } | null;
  serverCapabilities: unknown;
  tools: ToolDef[];
  channels: ChannelDesc[];
  featureSets: Record<string, unknown>;
  events: SessionEvent[];
  pending: Array<Omit<PendingRequest, 'resolve' | 'reject' | 'timer'>>;
  autoApprove: boolean;
  connected: boolean;
  command: string;
  hostState: StateView;
}

export interface HostSessionOptions {
  /** Command + args to spawn the MCPL server. */
  command: string;
  args?: string[];
  /** Extra env merged over process.env. */
  env?: NodeJS.ProcessEnv;
  /** Working directory for the child. */
  cwd?: string;
  /** Auto-approve interactive server→host requests immediately. Default false. */
  autoApprove?: boolean;
  /** Fallback timeout for unanswered interactive requests (ms). Default 60s. */
  pendingTimeoutMs?: number;
  /** Max retained events. Default 1000. */
  maxEvents?: number;
  /** Path to the JSON file backing host-managed state. null = in-memory only. */
  statePath?: string | null;
}

interface SessionEvents {
  event: [SessionEvent];
  /** State that the UI may want to re-snapshot: tools | channels | featureSets | pending */
  state: [keyof SessionSnapshot];
  exit: [number | null];
}

type TypedEmitter = {
  on<K extends keyof SessionEvents>(e: K, l: (...a: SessionEvents[K]) => void): TypedEmitter;
  emit<K extends keyof SessionEvents>(e: K, ...a: SessionEvents[K]): boolean;
} & EventEmitter;

function textOf(payload: unknown): string {
  const content = (payload as { content?: Array<{ type: string; text?: string }> })?.content ?? [];
  if (!Array.isArray(content)) return '';
  return content
    .map((b) => (b?.type === 'text' ? b.text : `[${b?.type}]`))
    .filter(Boolean)
    .join(' ');
}

export class HostSession extends (EventEmitter as new () => TypedEmitter) {
  readonly command: string;
  private child!: ChildProcess;
  private conn!: McplConnection;
  private opts: Required<Pick<HostSessionOptions, 'pendingTimeoutMs' | 'maxEvents'>>;
  private spawnOpts: { command: string; args: string[]; env?: NodeJS.ProcessEnv; cwd?: string };
  /** True while an intentional restart is tearing down the child — suppresses the exit event. */
  private restarting = false;

  private seq = 0;
  private serverInfo: { name?: string; version?: string } | null = null;
  private serverCapabilities: unknown = null;
  private tools: ToolDef[] = [];
  /** tool name → owning feature set, when the server tags tools. */
  private toolFeatureSet = new Map<string, string>();
  /** tools we've already warned about being un-attributable, to avoid log spam. */
  private ambiguityWarned = new Set<string>();
  private channels = new Map<string, ChannelDesc>();
  private featureSets: Record<string, unknown> = {};
  private events: SessionEvent[] = [];
  private pending = new Map<string, PendingRequest>();
  private autoApprove: boolean;
  private connected = false;
  private store: StateStore;

  constructor(options: HostSessionOptions) {
    super();
    this.command = [options.command, ...(options.args ?? [])].join(' ');
    this.autoApprove = options.autoApprove ?? false;
    this.opts = {
      pendingTimeoutMs: options.pendingTimeoutMs ?? 60_000,
      maxEvents: options.maxEvents ?? 1000,
    };
    this.store = new StateStore(options.statePath ?? null);
    this.spawnOpts = {
      command: options.command,
      args: options.args ?? [],
      env: options.env,
      cwd: options.cwd,
    };
    this.spawnChild();
  }

  /** Spawn the server process and wire a fresh connection. Reusable across restarts. */
  private spawnChild(): void {
    this.child = spawn(this.spawnOpts.command, this.spawnOpts.args, {
      stdio: ['pipe', 'pipe', 'inherit'],
      env: { ...process.env, ...this.spawnOpts.env },
      cwd: this.spawnOpts.cwd,
    });
    this.child.on('exit', (code) => {
      this.connected = false;
      if (this.restarting) return; // intentional teardown — handled by restart()
      this.log('system', 'system', `server exited (${code})`, undefined, { code });
      this.emit('exit', code);
    });
    this.conn = McplConnection.fromStreams(this.child.stdout!, this.child.stdin!);
  }

  // ── Lifecycle ──

  async start(): Promise<void> {
    const conn = this.conn;
    void this.pump(conn).catch((e) =>
      this.log('system', 'error', `pump error: ${(e as Error).message}`),
    );
    await this.handshake();
  }

  close(): void {
    try {
      this.conn.close();
    } catch {
      /* ignore */
    }
    this.child.kill();
  }

  /**
   * Fully restart the server: tear down the child + connection, respawn the
   * same command, re-handshake, and reset live state. The event log is kept
   * (with a marker) so the iteration history survives across restarts.
   */
  async restart(): Promise<void> {
    this.log('system', 'system', '⟳ restarting server…');
    this.restarting = true;

    // Fail any in-flight interactive requests — their connection is going away.
    for (const p of this.pending.values()) {
      if (p.timer) clearTimeout(p.timer);
      p.reject(-32000, 'server restarting');
    }
    this.pending.clear();
    this.emit('state', 'pending');

    // Tear down the old process + connection and wait for it to actually exit.
    try { this.conn.close(); } catch { /* ignore */ }
    const old = this.child;
    await new Promise<void>((resolve) => {
      if (old.exitCode !== null || old.signalCode !== null) return resolve();
      const done = () => resolve();
      old.once('exit', done);
      old.kill();
      setTimeout(() => { old.kill('SIGKILL'); resolve(); }, 3000);
    });

    // Reset live state (keep the event log).
    this.tools = [];
    this.channels.clear();
    this.featureSets = {};
    this.serverInfo = null;
    this.serverCapabilities = null;
    this.connected = false;
    this.emit('state', 'tools');
    this.emit('state', 'channels');
    this.emit('state', 'featureSets');

    // Respawn + re-handshake.
    this.restarting = false;
    this.spawnChild();
    await this.start();
    const info = this.serverInfo as { name?: string } | null;
    this.log('system', 'system', `⟳ server restarted (${info?.name ?? 'server'})`);
  }

  // ── Snapshot / accessors ──

  snapshot(): SessionSnapshot {
    return {
      serverInfo: this.serverInfo,
      serverCapabilities: this.serverCapabilities,
      tools: this.tools,
      channels: [...this.channels.values()],
      featureSets: this.featureSets,
      events: this.events,
      pending: [...this.pending.values()].map(({ resolve, reject, timer, ...rest }) => rest),
      autoApprove: this.autoApprove,
      connected: this.connected,
      command: this.command,
      hostState: this.store.view(),
    };
  }

  setAutoApprove(on: boolean): void {
    this.autoApprove = on;
    this.log('system', 'system', `autoApprove ${on ? 'on' : 'off'}`);
    this.emit('state', 'autoApprove');
  }

  // ── Host → server actions ──

  async callTool(name: string, args: unknown = {}): Promise<unknown> {
    const params: Record<string, unknown> = { name, arguments: args };

    // Inject host/server state for the feature set this tool belongs to.
    // We attribute by an explicit signal only — never guess. A tool's owning
    // set is known if the server tagged it; otherwise it's unambiguous only
    // when there's exactly one stateful set. (State can also change without a
    // tool call: the authoritative channel is `state/update`, which carries an
    // explicit featureSet — see handleServerRequest.)
    const injectFs = this.resolveStatefulSet(name);
    if (injectFs) {
      const inj = this.store.injectionFor(injectFs);
      if (inj?.state !== undefined) params.state = inj.state;
      if (inj?.checkpoint !== undefined) params.checkpoint = inj.checkpoint;
    }

    this.log('out', 'tool', `call ${name}`, 'tools/call', params);
    const res = await this.conn.sendRequest('tools/call', params);
    this.log('in', 'response', `result ${name}`, 'tools/call', res);

    // Record a checkpoint the result carried — attributed by explicit featureSet
    // first, then the tool's resolved set, narrowing by management mode. Never
    // misattribute: drop with a warning if the owning set can't be determined.
    const cp = (res as { state?: StateCheckpoint })?.state;
    if (cp && typeof cp === 'object' && 'checkpoint' in cp) {
      const mode: 'host' | 'server' = cp.data !== undefined || cp.patch !== undefined ? 'host' : 'server';
      const target = this.attributeCheckpoint(name, cp, mode);
      if (target) {
        if (!this.store.isStateful(target)) {
          this.store.registerFeatureSet(target, { hostState: mode === 'host', rollback: mode === 'server' });
        }
        this.store.recordCheckpoint(target, cp);
        this.emit('state', 'hostState');
        this.log('system', 'system', `checkpoint recorded ${target}@${cp.checkpoint}`, undefined, cp);
      } else {
        this.log('system', 'error',
          `state: tool '${name}' returned a checkpoint but no owning feature set could be determined ` +
          `(${this.store.statefulSets(mode).length} candidate ${mode}-managed sets, none declared). Dropped, not guessed. ` +
          `Fix: tag the tool with featureSet, or put featureSet on result.state, or push via state/update.`,
          undefined, cp);
      }
    }
    return res;
  }

  /** Which stateful set to inject for a tool call — explicit signal only, else null. */
  private resolveStatefulSet(toolName: string): string | null {
    const mapped = this.toolFeatureSet.get(toolName);
    if (mapped && this.store.isStateful(mapped)) return mapped;
    const sets = this.store.statefulSets();
    if (sets.length === 1) return sets[0]!; // unambiguous
    if (sets.length > 1 && !this.ambiguityWarned.has(toolName)) {
      this.ambiguityWarned.add(toolName);
      this.log('system', 'error',
        `state: can't attribute tool '${toolName}' to a feature set (${sets.length} stateful sets, ` +
        `none declared on the tool) — not injecting state. Server should tag tools with featureSet.`);
    }
    return null;
  }

  /** Which set a returned checkpoint belongs to — explicit featureSet, else resolved set, narrowed by mode. */
  private attributeCheckpoint(toolName: string, cp: StateCheckpoint, mode: 'host' | 'server'): string | null {
    if (typeof cp.featureSet === 'string') return cp.featureSet; // explicit wins (auto-registers if new)
    const mapped = this.toolFeatureSet.get(toolName);
    if (mapped && this.store.isStateful(mapped)) return mapped;
    const candidates = this.store.statefulSets(mode);
    return candidates.length === 1 ? candidates[0]! : null; // never guess among many
  }

  async refreshTools(): Promise<ToolDef[]> {
    const r = (await this.conn.sendRequest('tools/list', {})) as { tools?: ToolDef[] };
    this.tools = r.tools ?? [];
    // Build the tool→featureSet map from whatever the server tags (a top-level
    // `featureSet` or `_meta.featureSet`). Absent any tag, attribution falls back
    // to the single-stateful-set case in resolveStatefulSet/attributeCheckpoint.
    this.toolFeatureSet.clear();
    for (const t of this.tools) {
      const fs = t.featureSet ?? t._meta?.featureSet;
      if (typeof fs === 'string') this.toolFeatureSet.set(t.name, fs);
    }
    this.emit('state', 'tools');
    return this.tools;
  }

  async refreshChannels(): Promise<ChannelDesc[]> {
    try {
      const r = (await this.conn.sendRequest('channels/list', {})) as { channels?: ChannelDesc[] };
      for (const d of r.channels ?? []) this.channels.set(d.id, d);
      this.emit('state', 'channels');
    } catch {
      /* server may not support channels/list */
    }
    return [...this.channels.values()];
  }

  async openChannel(channelId: string): Promise<unknown> {
    const desc = this.channels.get(channelId);
    if (!desc) throw new Error(`unknown channel id: ${channelId}`);
    this.log('out', 'channel', `open ${channelId}`, 'channels/open', { type: desc.type, address: desc.address });
    const res = await this.conn.sendRequest('channels/open', { type: desc.type, address: desc.address });
    this.log('in', 'response', `opened ${channelId}`, 'channels/open', res);
    return res;
  }

  async publish(channelId: string, text: string, conversationId?: string): Promise<unknown> {
    const params = {
      conversationId: conversationId ?? `harness_${randomUUID()}`,
      channelId,
      content: [{ type: 'text', text }],
    };
    this.log('out', 'channel', `publish ${channelId}: ${text}`, 'channels/publish', params);
    const res = await this.conn.sendRequest('channels/publish', params);
    this.log('in', 'response', `published ${channelId}`, 'channels/publish', res);
    return res;
  }

  /** featureSets/update is a host→server notification. */
  updateFeatureSets(params: unknown): void {
    this.log('out', 'request', 'featureSets/update', 'featureSets/update', params);
    this.conn.sendNotification('featureSets/update', params);
  }

  /** Send an arbitrary request and return its result. */
  async raw(method: string, params: unknown = {}): Promise<unknown> {
    this.log('out', 'request', `raw ${method}`, method, params);
    const res = await this.conn.sendRequest(method, params);
    this.log('in', 'response', `raw result ${method}`, method, res);
    return res;
  }

  /** Send an arbitrary notification (no response). */
  notify(method: string, params: unknown = {}): void {
    this.log('out', 'request', `notify ${method}`, method, params);
    this.conn.sendNotification(method, params);
  }

  // ── Host-managed state ──

  /**
   * Roll a feature set back to a checkpoint: update the local store AND ask the
   * server to revert via a state/rollback request (Section 8.4).
   */
  async rollbackState(featureSet: string, checkpoint: string): Promise<unknown> {
    this.log('out', 'request', `state/rollback ${featureSet}@${checkpoint}`, 'state/rollback', { featureSet, checkpoint });
    let serverResult: unknown = null;
    try {
      serverResult = await this.conn.sendRequest('state/rollback', { featureSet, checkpoint });
      this.log('in', 'response', `state/rollback result`, 'state/rollback', serverResult);
    } catch (e) {
      this.log('system', 'error', `state/rollback failed: ${(e as Error).message}`);
    }
    const local = this.store.rollback(featureSet, checkpoint);
    this.emit('state', 'hostState');
    this.log('system', 'system', `rolled back ${featureSet} → ${checkpoint} (${local.success ? 'ok' : local.reason})`);
    return { server: serverResult, local };
  }

  clearState(featureSet?: string): void {
    this.store.clear(featureSet);
    this.emit('state', 'hostState');
    this.log('system', 'system', `cleared host state${featureSet ? ` for ${featureSet}` : ''}`);
  }

  // ── Answering interactive server→host requests ──

  resolvePending(pendingId: string, result: unknown): boolean {
    const p = this.pending.get(pendingId);
    if (!p) return false;
    p.resolve(result);
    return true;
  }

  rejectPending(pendingId: string, code = -32000, message = 'rejected by host'): boolean {
    const p = this.pending.get(pendingId);
    if (!p) return false;
    p.reject(code, message);
    return true;
  }

  // ── Internals ──

  private log(dir: EventDir, kind: string, summary: string, method?: string, data?: unknown, pendingId?: string): SessionEvent {
    const ev: SessionEvent = { seq: ++this.seq, at: new Date().toISOString(), dir, kind, summary, method, data, pendingId };
    this.events.push(ev);
    if (this.events.length > this.opts.maxEvents) this.events.shift();
    this.emit('event', ev);
    return ev;
  }

  private async handshake(): Promise<void> {
    const result = (await this.conn.sendRequest('initialize', {
      protocolVersion: '2024-11-05',
      capabilities: {
        experimental: { mcpl: { version: '0.4', pushEvents: true, channels: true, modelInfo: true, scopedAccess: true } },
      },
      clientInfo: { name: 'mcpl-harness-web', version: '0.1.0' },
    })) as { serverInfo?: { name?: string; version?: string }; capabilities?: unknown };
    this.conn.sendNotification('notifications/initialized', {});
    this.serverInfo = result.serverInfo ?? null;
    this.serverCapabilities = result.capabilities ?? null;
    this.connected = true;
    this.store.setServerName(result.serverInfo?.name);
    this.registerDeclaredFeatureSets(result.capabilities);
    this.log('system', 'system', `connected to ${result.serverInfo?.name ?? 'server'}`, 'initialize', result);
    await this.refreshTools();
    await this.refreshChannels();
  }

  /** Register stateful feature sets (rollback or hostState) declared in initialize capabilities. */
  private registerDeclaredFeatureSets(capabilities: unknown): void {
    const decls = (capabilities as {
      experimental?: { mcpl?: { featureSets?: Array<{ name: string; rollback?: boolean; hostState?: boolean }> } };
    })?.experimental?.mcpl?.featureSets;
    for (const fs of decls ?? []) {
      this.featureSets[fs.name] = fs;
      if (fs.rollback || fs.hostState) {
        this.store.registerFeatureSet(fs.name, { hostState: !!fs.hostState, rollback: !!fs.rollback });
      }
    }
    if (decls?.length) {
      this.emit('state', 'featureSets');
      this.emit('state', 'hostState');
    }
  }

  /** Register an interactive request and wait for the host's answer. */
  private awaitHostDecision(rpcId: string | number, method: string, params: unknown, fallback: unknown): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      const pendingId = randomUUID();
      const settle = (fn: () => void) => {
        const p = this.pending.get(pendingId);
        if (!p) return;
        if (p.timer) clearTimeout(p.timer);
        this.pending.delete(pendingId);
        this.emit('state', 'pending');
        fn();
      };
      const entry: PendingRequest = {
        pendingId,
        rpcId,
        method,
        params,
        at: new Date().toISOString(),
        resolve: (result) => settle(() => resolve(result)),
        reject: (code, message) => settle(() => reject({ code, message })),
        timer: setTimeout(() => {
          const p = this.pending.get(pendingId);
          if (p) {
            this.log('system', 'system', `auto-answered ${method} (timeout)`, method, fallback);
            p.resolve(fallback);
          }
        }, this.opts.pendingTimeoutMs),
      };
      this.pending.set(pendingId, entry);
      this.emit('state', 'pending');
      this.log('in', 'request', `⟶ host decision needed: ${method}`, method, params, pendingId);
    });
  }

  /** Pump bound to a specific connection — exits quietly when that connection closes. */
  private async pump(conn: McplConnection): Promise<void> {
    try {
      while (!conn.isClosed) {
        const msg = await conn.nextMessage();
        if (msg.type === 'request') {
          await this.handleServerRequest(msg.request);
        } else {
          this.handleServerNotification(msg.notification);
        }
      }
    } catch (e) {
      // A closed connection is expected on shutdown/restart — only surface real errors.
      if (e instanceof ConnectionClosedError || (e as Error)?.message?.includes('closed')) return;
      throw e;
    }
  }

  private async handleServerRequest(req: { id: string | number; method: string; params?: unknown }): Promise<void> {
    const { id, method, params } = req;
    try {
      switch (method) {
        case 'push/event': {
          const p = params as { origin?: { channelId?: string }; payload?: unknown };
          this.log('in', 'push', `push ${p.origin?.channelId ?? ''} ${textOf(p.payload)}`.trim(), method, params);
          this.conn.sendResponse(id, { accepted: true });
          break;
        }
        case 'channels/incoming': {
          const p = params as { messages?: Array<{ messageId: string; channelId: string; content?: unknown }> };
          for (const m of p.messages ?? []) {
            this.log('in', 'channel', `incoming ${m.channelId}: ${textOf(m)}`.trim(), method, m);
          }
          this.conn.sendResponse(id, { results: (p.messages ?? []).map((m) => ({ messageId: m.messageId, accepted: true })) });
          break;
        }
        case 'channels/register': {
          const p = params as { channels?: ChannelDesc[] };
          for (const d of p.channels ?? []) this.channels.set(d.id, d);
          this.emit('state', 'channels');
          this.log('in', 'channel', `register (+${p.channels?.length ?? 0})`, method, params);
          this.conn.sendResponse(id, {});
          break;
        }
        case 'state/update': {
          const p = params as { featureSet?: string; checkpoint?: string; parent?: string | null; data?: unknown; patch?: StateCheckpoint['patch'] };
          if (p.featureSet && p.checkpoint) {
            // Auto-register if the server pushes state for a set we didn't see declared.
            if (!this.store.isStateful(p.featureSet)) {
              this.store.registerFeatureSet(p.featureSet, { hostState: p.data !== undefined || p.patch !== undefined, rollback: false });
            }
            this.store.recordCheckpoint(p.featureSet, { checkpoint: p.checkpoint, parent: p.parent, data: p.data, patch: p.patch });
            this.emit('state', 'hostState');
          }
          this.log('in', 'request', `state/update ${p.featureSet ?? ''}@${p.checkpoint ?? ''}`, method, params);
          this.conn.sendResponse(id, { accepted: true });
          break;
        }
        case 'state/get': {
          const p = params as { featureSet?: string };
          const got = p.featureSet ? this.store.get(p.featureSet) : { checkpoint: null, data: null };
          this.log('in', 'request', `state/get ${p.featureSet ?? ''} → ${got.checkpoint ?? 'null'}`, method, { params, result: got });
          this.conn.sendResponse(id, got);
          break;
        }
        case 'model/info': {
          const info = { id: 'harness/human', vendor: 'mcpl-harness', contextWindow: 200000, capabilities: ['text'] };
          this.log('in', 'request', 'model/info', method, params);
          this.conn.sendResponse(id, info);
          break;
        }
        case 'branches/list':
          this.conn.sendResponse(id, { branches: [] });
          break;
        case 'branches/current':
          this.conn.sendResponse(id, { name: 'main', head: 0 });
          break;
        case 'branches/create':
        case 'branches/switch':
        case 'branches/delete':
          this.conn.sendResponse(id, { accepted: true });
          break;

        // ── Interactive: held for a human decision ──
        case 'scope/elevate': {
          if (this.autoApprove) {
            this.log('in', 'request', `scope/elevate auto-approved`, method, params);
            this.conn.sendResponse(id, { approved: true });
            break;
          }
          const result = await this.awaitHostDecision(id, method, params, { approved: true });
          this.conn.sendResponse(id, result);
          break;
        }
        case 'inference/request': {
          const fallback = {
            content: '(harness: no human response)',
            model: 'harness/human',
            finishReason: 'stop',
            usage: { inputTokens: 0, outputTokens: 0 },
          };
          if (this.autoApprove) {
            this.conn.sendResponse(id, fallback);
            break;
          }
          const result = await this.awaitHostDecision(id, method, params, fallback);
          this.conn.sendResponse(id, result);
          break;
        }

        default:
          this.log('in', 'request', `unhandled request ${method}`, method, params);
          this.conn.sendError(id, -32601, `harness: unhandled request ${method}`);
      }
    } catch (err) {
      const e = err as { code?: number; message?: string };
      this.conn.sendError(id, e.code ?? -32000, e.message ?? String(err));
    }
  }

  private handleServerNotification(notif: { method: string; params?: unknown }): void {
    const { method, params } = notif;
    switch (method) {
      case 'channels/changed': {
        const p = params as { added?: ChannelDesc[]; removed?: string[]; updated?: ChannelDesc[] };
        for (const d of p.added ?? []) this.channels.set(d.id, d);
        for (const d of p.updated ?? []) this.channels.set(d.id, d);
        for (const id of p.removed ?? []) this.channels.delete(id);
        this.emit('state', 'channels');
        this.log('in', 'channel', `channels/changed (+${p.added?.length ?? 0}/-${p.removed?.length ?? 0})`, method, params);
        break;
      }
      case 'featureSets/changed': {
        const p = params as { added?: Record<string, { rollback?: boolean; hostState?: boolean }>; removed?: string[] };
        for (const [k, v] of Object.entries(p.added ?? {})) {
          this.featureSets[k] = v;
          if (v?.rollback || v?.hostState) {
            this.store.registerFeatureSet(k, { hostState: !!v.hostState, rollback: !!v.rollback });
          }
        }
        for (const k of p.removed ?? []) delete this.featureSets[k];
        this.emit('state', 'featureSets');
        this.emit('state', 'hostState');
        this.log('in', 'request', `featureSets/changed`, method, params);
        break;
      }
      default:
        this.log('in', 'request', `notification ${method}`, method, params);
    }
  }
}
