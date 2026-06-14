#!/usr/bin/env node
/**
 * mcpl-harness web — a browser UI MCPL *host* for testing MCPL servers by hand.
 *
 * Spawns an MCPL server, performs the host handshake, and exposes a single-page
 * web UI where a human can act as the agent: browse tools and call them with
 * argument forms, inspect results, list/open/publish channels, watch push
 * events stream in live, and answer interactive server→host requests
 * (scope/elevate, inference/request).
 *
 * Transport to the browser:
 *   GET  /              → the single-page UI
 *   GET  /api/snapshot  → current session state (tools, channels, events, …)
 *   GET  /api/events    → Server-Sent Events stream of live session events
 *   POST /api/command   → { op, ... } host actions, returns JSON result
 *
 * Usage:
 *   mcpl-harness-web [--port 7333] [--open] -- <command> [args...]
 *   PORTAL_TOKEN=… PORTAL_PERSONA=mythos \
 *     mcpl-harness-web -- node ../portal-mcpl/dist/src/server-cli.js
 */
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { spawn } from 'node:child_process';
import { networkInterfaces } from 'node:os';
import { HostSession } from './session.js';

/** Best-guess non-internal IPv4 address, for printing a reachable URL when bound to 0.0.0.0. */
function lanAddress(): string {
  for (const ifaces of Object.values(networkInterfaces())) {
    for (const i of ifaces ?? []) {
      if (i.family === 'IPv4' && !i.internal) return i.address;
    }
  }
  return 'localhost';
}

// ── Parse argv ──
const argv = process.argv.slice(2);
const dashdash = argv.indexOf('--');
if (dashdash < 0 || dashdash === argv.length - 1) {
  console.error('usage: mcpl-harness-web [--port N] [--host H] [--open] [--state FILE|off] -- <command> [args...]');
  process.exit(1);
}
const flags = argv.slice(0, dashdash);
const portFlag = flags.indexOf('--port');
const port = portFlag >= 0 ? parseInt(flags[portFlag + 1], 10) : 7333;
const hostFlag = flags.indexOf('--host');
const host = hostFlag >= 0 ? flags[hostFlag + 1] : '127.0.0.1';
const autoOpen = flags.includes('--open');
const autoApprove = flags.includes('--auto-approve');
const stateFlag = flags.indexOf('--state');
// Host-managed state file: default `.mcpl-harness-state.json` in cwd; `--state off` disables.
const statePath =
  stateFlag >= 0
    ? flags[stateFlag + 1] === 'off' ? null : flags[stateFlag + 1]
    : join(process.cwd(), '.mcpl-harness-state.json');
const [command, ...cmdArgs] = argv.slice(dashdash + 1);

const __dirname = dirname(fileURLToPath(import.meta.url));
// Locate public/ relative to this file — works for both `dist/src/web.js`
// (built) and `src/web.ts` (tsx). Walk up until a public/index.html is found.
function findPublicDir(): string {
  let dir = __dirname;
  for (let i = 0; i < 4; i++) {
    const candidate = join(dir, 'public');
    if (existsSync(join(candidate, 'index.html'))) return candidate;
    dir = join(dir, '..');
  }
  return join(__dirname, '..', '..', 'public');
}
const publicDir = findPublicDir();

// ── Session ──
const session = new HostSession({ command, args: cmdArgs, autoApprove, statePath });

// ── SSE clients ──
const sseClients = new Set<ServerResponse>();
function broadcast(event: string, data: unknown): void {
  const frame = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(frame);
    } catch {
      sseClients.delete(res);
    }
  }
}
session.on('event', (ev) => broadcast('event', ev));
session.on('state', (what) => broadcast('state', { what, snapshot: session.snapshot() }));
session.on('exit', (code) => broadcast('exit', { code }));

// ── HTTP helpers ──
function json(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) });
  res.end(payload);
}

async function readBody(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  const raw = Buffer.concat(chunks).toString('utf-8');
  return raw ? JSON.parse(raw) : {};
}

// ── Command dispatch ──
interface Command {
  op: string;
  [k: string]: unknown;
}

async function handleCommand(cmd: Command): Promise<unknown> {
  switch (cmd.op) {
    case 'callTool':
      return session.callTool(cmd.name as string, cmd.arguments ?? {});
    case 'refreshTools':
      return { tools: await session.refreshTools() };
    case 'refreshChannels':
      return { channels: await session.refreshChannels() };
    case 'openChannel':
      return session.openChannel(cmd.channelId as string);
    case 'publish':
      return session.publish(cmd.channelId as string, cmd.text as string, cmd.conversationId as string | undefined);
    case 'featureSetsUpdate':
      session.updateFeatureSets(cmd.params ?? {});
      return { ok: true };
    case 'raw':
      return session.raw(cmd.method as string, cmd.params ?? {});
    case 'notify':
      session.notify(cmd.method as string, cmd.params ?? {});
      return { ok: true };
    case 'resolvePending':
      return { ok: session.resolvePending(cmd.pendingId as string, cmd.result ?? {}) };
    case 'rejectPending':
      return { ok: session.rejectPending(cmd.pendingId as string, cmd.code as number, cmd.message as string) };
    case 'autoApprove':
      session.setAutoApprove(Boolean(cmd.on));
      return { ok: true };
    case 'restart':
      await session.restart();
      return { ok: true, snapshot: session.snapshot() };
    case 'stateRollback':
      return session.rollbackState(cmd.featureSet as string, cmd.checkpoint as string);
    case 'stateClear':
      session.clearState(cmd.featureSet as string | undefined);
      return { ok: true };
    case 'snapshot':
      return session.snapshot();
    default:
      throw new Error(`unknown op: ${cmd.op}`);
  }
}

// ── HTTP server ──
const server = createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', `http://localhost:${port}`);
  try {
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
      const html = await readFile(join(publicDir, 'index.html'));
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      res.end(html);
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/snapshot') {
      json(res, 200, session.snapshot());
      return;
    }

    if (req.method === 'GET' && url.pathname === '/api/events') {
      res.writeHead(200, {
        'content-type': 'text/event-stream',
        'cache-control': 'no-cache',
        connection: 'keep-alive',
      });
      res.write(`event: snapshot\ndata: ${JSON.stringify(session.snapshot())}\n\n`);
      sseClients.add(res);
      const keepalive = setInterval(() => {
        try {
          res.write(': keepalive\n\n');
        } catch {
          /* ignore */
        }
      }, 25_000);
      req.on('close', () => {
        clearInterval(keepalive);
        sseClients.delete(res);
      });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/api/command') {
      const body = (await readBody(req)) as Command;
      try {
        const result = await handleCommand(body);
        json(res, 200, { ok: true, result });
      } catch (err) {
        json(res, 200, { ok: false, error: (err as Error).message ?? String(err) });
      }
      return;
    }

    res.writeHead(404, { 'content-type': 'text/plain' });
    res.end('not found');
  } catch (err) {
    json(res, 500, { ok: false, error: (err as Error).message });
  }
});

async function main(): Promise<void> {
  await session.start();
  server.listen(port, host, () => {
    const shown = host === '0.0.0.0' || host === '::' ? lanAddress() : host;
    const target = `http://${shown}:${port}`;
    console.error(`[mcpl-harness-web] host UI on ${target} (bound ${host}:${port})`);
    console.error(`[mcpl-harness-web] driving: ${session.command}`);
    console.error(`[mcpl-harness-web] host state: ${statePath ?? '(in-memory only)'}`);
    if (autoOpen) {
      const opener = process.platform === 'darwin' ? 'open' : process.platform === 'win32' ? 'start' : 'xdg-open';
      spawn(opener, [target], { stdio: 'ignore', detached: true }).unref();
    }
  });
}

session.on('exit', () => {
  // Keep the UI alive so the final state/log is inspectable; just notify.
  console.error('[mcpl-harness-web] server process exited — UI still serving last state');
});

process.on('SIGINT', () => {
  session.close();
  process.exit(0);
});

main().catch((err) => {
  console.error('[mcpl-harness-web] fatal:', err);
  process.exit(1);
});
