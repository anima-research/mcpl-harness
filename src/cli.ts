#!/usr/bin/env node
/**
 * mcpl-harness — a stateful CLI MCPL *host* for testing MCPL servers.
 *
 * Spawns an MCPL server over stdio, performs the host handshake (advertising
 * MCPL support), then maintains live state — tools, channels, a push-event log,
 * and host-managed state (Section 8) — while accepting commands on stdin.
 * Interactive (a REPL) and scriptable (pipe commands in).
 *
 * The CLI and the web UI (`web.ts`) share one engine — `HostSession` — so both
 * get the same behavior, including host-state persistence and rollback.
 *
 * Usage:
 *   mcpl-harness [--state FILE|off] [--auto-approve] -- <command> [args...]
 *   mcpl-harness -- node path/to/your-mcpl-server.js --stdio
 *   # servers that need credentials take them from the environment:
 *   SOME_TOKEN=… mcpl-harness -- node path/to/your-mcpl-server.js
 *
 * Commands (stdin, one per line):
 *   help                         show commands
 *   tools                        list tools
 *   call <tool> [json-args]      tools/call (injects host state for stateful sets)
 *   channels                     list registered channels
 *   open <mcplChannelId>         channels/open (by descriptor id)
 *   publish <mcplChannelId> <text…>   channels/publish
 *   events [n]                   show last n events (default 10)
 *   watch on|off                 live-print server→host events (default on)
 *   raw <method> [json]          send an arbitrary request
 *   state                        show host-managed state per feature set
 *   rollback <featureSet> <checkpoint>   roll a feature set back to a checkpoint
 *   stateclear [featureSet]      clear stored checkpoints (all, or one set)
 *   restart                      respawn the server process and re-handshake
 *   info                         one-line summary
 *   quit | exit
 */
import { createInterface } from 'node:readline';
import { join } from 'node:path';
import { HostSession } from './session.js';

const argv = process.argv.slice(2);
const dashdash = argv.indexOf('--');
if (dashdash < 0 || dashdash === argv.length - 1) {
  console.error('usage: mcpl-harness [--state FILE|off] [--auto-approve] -- <command> [args...]');
  process.exit(1);
}
const flags = argv.slice(0, dashdash);
const stateFlag = flags.indexOf('--state');
const statePath =
  stateFlag >= 0
    ? flags[stateFlag + 1] === 'off' ? null : flags[stateFlag + 1]
    : join(process.cwd(), '.mcpl-harness-state.json');
const autoApprove = flags.includes('--auto-approve');
const [cmd, ...cmdArgs] = argv.slice(dashdash + 1);

let watch = true;
const session = new HostSession({ command: cmd, args: cmdArgs, autoApprove, statePath });

session.on('exit', (code) => {
  console.error(`\n[harness] server exited (${code}); bye`);
  process.exit(code ?? 0);
});

// Live-print server-initiated events (and checkpoint records) while watch is on.
session.on('event', (ev) => {
  if (!watch) return;
  const interesting =
    (ev.dir === 'in' && (ev.kind === 'push' || ev.kind === 'channel')) ||
    (ev.kind === 'system' && /checkpoint recorded|host decision/.test(ev.summary));
  if (interesting) process.stdout.write(`\n« ${ev.summary}\n> `);
});

function pj(v: unknown): string {
  return JSON.stringify(v, null, 2);
}

function printState(): void {
  const v = session.snapshot().hostState;
  if (!v.featureSets.length) return void console.log('  no stateful feature sets');
  for (const fs of v.featureSets) {
    const mode = fs.hostState ? 'host-managed' : 'server-managed';
    console.log(`  ${fs.featureSet} [${mode}]  head=${fs.current ?? '—'}  (${fs.checkpoints.length} checkpoint${fs.checkpoints.length === 1 ? '' : 's'})`);
    if (fs.hostState) console.log(`    data: ${JSON.stringify(fs.data)}`);
    if (fs.checkpoints.length) console.log(`    checkpoints: ${fs.checkpoints.map((c) => c.checkpoint).join(', ')}`);
  }
  if (v.path) console.log(`  (persisted to ${v.path})`);
}

function help(): void {
  console.log(
    [
      'commands:',
      '  tools                       list tools',
      '  call <tool> [json]          call a tool (injects host state)',
      '  channels                    list channels',
      '  open <mcplChannelId>        channels/open',
      '  publish <chanId> <text…>    channels/publish',
      '  events [n]                  last n events',
      '  watch on|off                live event printing',
      '  raw <method> [json]         arbitrary request',
      '  state                       host-managed state per feature set',
      '  rollback <fs> <checkpoint>  roll a feature set back to a checkpoint',
      '  stateclear [fs]             clear stored checkpoints (all, or one set)',
      '  restart                     respawn server + re-handshake',
      '  info                        one-line summary',
      '  quit|exit',
    ].join('\n'),
  );
}

async function handle(line: string): Promise<void> {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith('#')) return;
  const sp = trimmed.indexOf(' ');
  const verb = (sp < 0 ? trimmed : trimmed.slice(0, sp)).toLowerCase();
  const rest = sp < 0 ? '' : trimmed.slice(sp + 1).trim();

  try {
    switch (verb) {
      case 'help':
        help();
        break;
      case 'tools':
        for (const t of session.snapshot().tools)
          console.log(`  ${t.name}${t.description ? ' — ' + t.description.split('\n')[0] : ''}`);
        break;
      case 'call': {
        const s2 = rest.indexOf(' ');
        const tool = s2 < 0 ? rest : rest.slice(0, s2);
        const args = s2 < 0 ? {} : JSON.parse(rest.slice(s2 + 1));
        console.log(pj(await session.callTool(tool, args)));
        break;
      }
      case 'channels':
        for (const c of session.snapshot().channels) console.log(`  ${c.id}  ${c.label ?? ''}`);
        break;
      case 'open':
        console.log(pj(await session.openChannel(rest)));
        break;
      case 'publish': {
        const s2 = rest.indexOf(' ');
        const chan = s2 < 0 ? rest : rest.slice(0, s2);
        const text = s2 < 0 ? '' : rest.slice(s2 + 1);
        console.log(pj(await session.publish(chan, text)));
        break;
      }
      case 'events': {
        const n = rest ? parseInt(rest, 10) : 10;
        for (const e of session.snapshot().events.slice(-n))
          console.log(`  ${e.at} ${e.dir === 'in' ? '←' : e.dir === 'out' ? '→' : '·'} [${e.kind}] ${e.summary}`);
        break;
      }
      case 'watch':
        watch = rest !== 'off';
        console.log(`watch ${watch ? 'on' : 'off'}`);
        break;
      case 'raw': {
        const s2 = rest.indexOf(' ');
        const m = s2 < 0 ? rest : rest.slice(0, s2);
        const p = s2 < 0 ? {} : JSON.parse(rest.slice(s2 + 1));
        console.log(pj(await session.raw(m, p)));
        break;
      }
      case 'state':
        printState();
        break;
      case 'rollback': {
        const s2 = rest.indexOf(' ');
        if (s2 < 0) return void console.log('usage: rollback <featureSet> <checkpoint>');
        const fs = rest.slice(0, s2);
        const checkpoint = rest.slice(s2 + 1).trim();
        console.log(pj(await session.rollbackState(fs, checkpoint)));
        printState();
        break;
      }
      case 'stateclear':
        session.clearState(rest || undefined);
        printState();
        break;
      case 'restart':
        await session.restart();
        break;
      case 'wait': {
        const ms = parseInt(rest, 10) || 1000;
        await new Promise((r) => setTimeout(r, ms));
        await session.refreshChannels();
        break;
      }
      case 'info': {
        const s = session.snapshot();
        console.log(
          `server: ${s.serverInfo?.name ?? '?'} | tools: ${s.tools.length} | channels: ${s.channels.length} | ` +
            `stateful: ${s.hostState.featureSets.length} | events: ${s.events.length} | watch: ${watch} | connected: ${s.connected}`,
        );
        break;
      }
      case 'quit':
      case 'exit':
        session.close();
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
  await session.start();
  const s = session.snapshot();
  console.error(`[harness] connected to ${s.serverInfo?.name ?? 'server'} — ${s.tools.length} tools, ${s.hostState.featureSets.length} stateful feature set(s)`);
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
      session.close();
      process.exit(0);
    });
  });
}

main().catch((err) => {
  console.error('[harness] fatal:', err);
  process.exit(1);
});
