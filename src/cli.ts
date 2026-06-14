#!/usr/bin/env node
/**
 * mcpl-harness — a stateful CLI MCPL *host* for testing MCPL servers.
 *
 * Spawns an MCPL server over stdio, performs the host handshake (advertising
 * MCPL support), then maintains live state — tool list, registered channels,
 * and a push-event log — while accepting commands on stdin. Interactive (a
 * REPL) and scriptable (pipe commands in).
 *
 * Usage:
 *   mcpl-harness -- <command> [args...]
 *   PORTAL_TOKEN=… PORTAL_PERSONA=mythos \
 *     mcpl-harness -- node ../portal-mcpl/dist/src/server-cli.js
 *
 * Commands (stdin, one per line):
 *   help                         show commands
 *   tools                        list tools
 *   call <tool> [json-args]      tools/call
 *   channels                     list registered channels
 *   open <mcplChannelId>         channels/open (by descriptor id)
 *   publish <mcplChannelId> <text…>   channels/publish
 *   events [n]                   show last n push events (default 10)
 *   watch on|off                 live-print push events as they arrive (default on)
 *   raw <method> [json]          send an arbitrary request
 *   state                        summary
 *   quit | exit
 */
import { spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { randomUUID } from 'node:crypto';
import { McplConnection } from '@connectome/mcpl-core';

interface ToolDef {
  name: string;
  description?: string;
}
interface ChannelDesc {
  id: string;
  label?: string;
  type?: string;
  address?: unknown;
}

const argv = process.argv.slice(2);
const dashdash = argv.indexOf('--');
if (dashdash < 0 || dashdash === argv.length - 1) {
  console.error('usage: mcpl-harness -- <command> [args...]');
  process.exit(1);
}
const [cmd, ...cmdArgs] = argv.slice(dashdash + 1);

// ── State ──
const state = {
  tools: [] as ToolDef[],
  channels: new Map<string, ChannelDesc>(),
  events: [] as Array<{ at: string; channelId?: string; text: string; raw: unknown }>,
  watch: true,
};

const child = spawn(cmd, cmdArgs, { stdio: ['pipe', 'pipe', 'inherit'], env: process.env });
child.on('exit', (code) => {
  console.error(`\n[harness] server exited (${code}); bye`);
  process.exit(code ?? 0);
});

const conn = McplConnection.fromStreams(child.stdout!, child.stdin!);

function logEvent(channelId: string | undefined, text: string, raw: unknown): void {
  state.events.push({ at: new Date().toISOString(), channelId, text, raw });
  if (state.events.length > 500) state.events.shift();
  if (state.watch) process.stdout.write(`\n« push ${channelId ?? ''} ${text}\n> `);
}

function textOf(payload: unknown): string {
  const content = (payload as { content?: Array<{ type: string; text?: string }> })?.content ?? [];
  return content
    .map((b) => (b.type === 'text' ? b.text : `[${b.type}]`))
    .filter(Boolean)
    .join(' ');
}

// ── Host: handle server → host messages ──
async function pump(): Promise<void> {
  while (!conn.isClosed) {
    const msg = await conn.nextMessage();
    if (msg.type === 'request') {
      const { id, method, params } = msg.request;
      switch (method) {
        case 'push/event': {
          const p = params as { origin?: { channelId?: string }; payload?: unknown };
          logEvent(p.origin?.channelId, textOf(p.payload), params);
          conn.sendResponse(id, {});
          break;
        }
        case 'channels/incoming': {
          logEvent(undefined, `[channels/incoming] ${JSON.stringify(params)}`, params);
          conn.sendResponse(id, {});
          break;
        }
        case 'scope/elevate':
          conn.sendResponse(id, { approved: true });
          break;
        case 'state/update': {
          const p = params as { checkpoint?: string };
          conn.sendResponse(id, { checkpoint: p.checkpoint ?? '', success: true });
          break;
        }
        default:
          conn.sendError(id, -32601, `harness: unhandled request ${method}`);
      }
    } else {
      const { method, params } = msg.notification;
      if (method === 'channels/changed') {
        const p = params as { added?: ChannelDesc[]; removed?: string[] };
        for (const d of p.added ?? []) state.channels.set(d.id, d);
        for (const id of p.removed ?? []) state.channels.delete(id);
        if (state.watch) process.stdout.write(`\n« channels/changed (+${p.added?.length ?? 0}/-${p.removed?.length ?? 0})\n> `);
      }
    }
  }
}

async function handshake(): Promise<void> {
  const result = (await conn.sendRequest('initialize', {
    protocolVersion: '2024-11-05',
    capabilities: { experimental: { mcpl: { version: '0.4', pushEvents: true, channels: true } } },
    clientInfo: { name: 'mcpl-harness', version: '0.1.0' },
  })) as { serverInfo?: { name?: string }; capabilities?: unknown };
  conn.sendNotification('notifications/initialized', {});
  console.error(`[harness] connected to ${result.serverInfo?.name ?? 'server'}`);
  await refreshTools();
  await refreshChannels();
}

async function refreshTools(): Promise<void> {
  const r = (await conn.sendRequest('tools/list', {})) as { tools?: ToolDef[] };
  state.tools = r.tools ?? [];
}
async function refreshChannels(): Promise<void> {
  try {
    const r = (await conn.sendRequest('channels/list', {})) as { channels?: ChannelDesc[] };
    for (const d of r.channels ?? []) state.channels.set(d.id, d);
  } catch {
    /* server may not support channels/list */
  }
}

// ── REPL ──
function help(): void {
  console.log(
    [
      'commands:',
      '  tools                       list tools',
      '  call <tool> [json]          call a tool',
      '  channels                    list channels',
      '  open <mcplChannelId>        channels/open',
      '  publish <chanId> <text…>    channels/publish',
      '  events [n]                  last n push events',
      '  watch on|off                live event printing',
      '  raw <method> [json]         arbitrary request',
      '  state                       summary',
      '  quit|exit',
    ].join('\n'),
  );
}

async function handle(line: string): Promise<void> {
  const trimmed = line.trim();
  if (!trimmed) return;
  const sp = trimmed.indexOf(' ');
  const verb = (sp < 0 ? trimmed : trimmed.slice(0, sp)).toLowerCase();
  const rest = sp < 0 ? '' : trimmed.slice(sp + 1).trim();

  try {
    switch (verb) {
      case 'help':
        help();
        break;
      case 'tools':
        for (const t of state.tools) console.log(`  ${t.name}${t.description ? ' — ' + t.description.split('\n')[0] : ''}`);
        break;
      case 'call': {
        const s2 = rest.indexOf(' ');
        const tool = s2 < 0 ? rest : rest.slice(0, s2);
        const args = s2 < 0 ? {} : JSON.parse(rest.slice(s2 + 1));
        const res = await conn.sendRequest('tools/call', { name: tool, arguments: args });
        console.log(JSON.stringify(res, null, 2));
        break;
      }
      case 'channels':
        for (const c of state.channels.values()) console.log(`  ${c.id}  ${c.label ?? ''}`);
        break;
      case 'open': {
        const desc = state.channels.get(rest);
        if (!desc) return void console.log('unknown channel id (see `channels`)');
        const res = await conn.sendRequest('channels/open', { type: desc.type, address: desc.address });
        console.log(JSON.stringify(res, null, 2));
        break;
      }
      case 'publish': {
        const s2 = rest.indexOf(' ');
        const chan = s2 < 0 ? rest : rest.slice(0, s2);
        const text = s2 < 0 ? '' : rest.slice(s2 + 1);
        const res = await conn.sendRequest('channels/publish', {
          conversationId: `harness_${randomUUID()}`,
          channelId: chan,
          content: [{ type: 'text', text }],
        });
        console.log(JSON.stringify(res, null, 2));
        break;
      }
      case 'events': {
        const n = rest ? parseInt(rest, 10) : 10;
        for (const e of state.events.slice(-n)) console.log(`  ${e.at} [${e.channelId ?? ''}] ${e.text}`);
        break;
      }
      case 'watch':
        state.watch = rest !== 'off';
        console.log(`watch ${state.watch ? 'on' : 'off'}`);
        break;
      case 'raw': {
        const s2 = rest.indexOf(' ');
        const m = s2 < 0 ? rest : rest.slice(0, s2);
        const p = s2 < 0 ? {} : JSON.parse(rest.slice(s2 + 1));
        console.log(JSON.stringify(await conn.sendRequest(m, p), null, 2));
        break;
      }
      case 'wait': {
        const ms = parseInt(rest, 10) || 1000;
        await new Promise((r) => setTimeout(r, ms));
        await refreshChannels();
        break;
      }
      case 'state':
        console.log(`tools: ${state.tools.length}, channels: ${state.channels.size}, events: ${state.events.length}, watch: ${state.watch}`);
        break;
      case 'quit':
      case 'exit':
        conn.close();
        child.kill();
        process.exit(0);
        break;
      default:
        console.log(`unknown command: ${verb} (try \`help\`)`);
    }
  } catch (err) {
    console.error('error:', (err as Error).message);
  }
}

async function main(): Promise<void> {
  void pump().catch((e) => console.error('[harness] pump error:', (e as Error).message));
  await handshake();
  help();
  const rl = createInterface({ input: process.stdin, output: process.stdout, prompt: '> ' });
  rl.prompt();
  // Serialize command handling: with piped input, `line` events fire back-to-back,
  // so chain them rather than running concurrently (a `wait` must actually block).
  let chain: Promise<void> = Promise.resolve();
  rl.on('line', (line) => {
    chain = chain.then(() => handle(line)).then(() => void rl.prompt());
  });
  // On EOF (piped scripts close stdin immediately), drain the queued commands
  // before tearing down — otherwise close races ahead of the chain.
  rl.on('close', () => {
    chain.then(() => {
      conn.close();
      child.kill();
      process.exit(0);
    });
  });
}

main().catch((err) => {
  console.error('[harness] fatal:', err);
  process.exit(1);
});
