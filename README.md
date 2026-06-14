# @connectome/mcpl-harness

A stateful MCPL **host** harness for testing MCPL servers without a full agent
host. Spawns any MCPL server over stdio, performs the host handshake, and lets
you drive it by hand — call tools, open/publish channels, watch push events,
and answer server→host requests — as if *you* were the agent.

Two front-ends share one engine (`src/session.ts` → `HostSession`):

- **CLI REPL** (`mcpl-harness`) — scriptable, pipe commands in or use interactively.
- **Web UI** (`mcpl-harness-web`) — a browser dashboard for manual exploration.

## Build

```bash
npm install
npm run build
```

## Web UI

```bash
# from source (tsx, no build step)
npm run web -- --open -- node path/to/your-mcpl-server.js --stdio

# or built
node dist/src/web.js --port 7333 --open -- <server-command> [args...]

# servers that need credentials take them from the environment:
SOME_TOKEN=… node dist/src/web.js -- node path/to/your-mcpl-server.js --stdio
```

Flags (before `--`): `--port N` (default 7333), `--host H` (default `127.0.0.1`;
use `0.0.0.0` to expose on the network), `--open` (open browser),
`--auto-approve` (auto-answer interactive server→host requests),
`--state FILE` (host-state file; default `.mcpl-harness-state.json` in cwd,
`--state off` to disable).

Then open `http://localhost:7333`. The UI gives you:

- **Tools** — list with descriptions; click one to get a schema-driven argument
  form (or a raw-JSON editor), call it, and inspect the result.
- **Channels** — list registered channels; open or publish to them.
- **Event log** — every message in both directions, live over SSE: push events,
  `channels/changed`, incoming messages, requests and responses. Filter by kind;
  click any row to expand its raw JSON.
- **Pending requests** — interactive server→host requests (`scope/elevate`,
  `inference/request`) surface here for you to approve/deny or answer as the
  model. Toggle **auto-approve** to answer them automatically.
- **Host State** — per stateful feature set (those declaring `rollback` or
  `hostState`), shows the reconstructed state, the checkpoint history, and the
  current head. Click a checkpoint to roll back to it.
- **Raw request** — send any JSON-RPC method/params as a request or notification.
- **⟳ restart** — kill and respawn the server process, then re-handshake, without
  losing the event log or persisted host state. For tight server iteration.

### Host-managed state (Section 8)

The harness is a real state-persisting host, not a stub:

- It registers feature sets declared with `rollback` / `hostState` from the
  `initialize` capabilities (and `featureSets/changed`).
- On `tools/call` it injects the current `state` (host-managed) or `checkpoint`
  (server-managed) into params, mirroring a production host.
- It records checkpoints from a tool result's `result.state` **and** from
  `state/update` requests, applying RFC-6902 JSON-Patch deltas to reconstruct
  full state, and answers `state/get` with the reconstructed state.
- State is persisted to a JSON file (`--state`), so it survives both the
  **⟳ restart** button and a full harness relaunch. `state/rollback` (from the
  UI) reverts both the local store and the server.

This makes host-state paths — any feature set declaring `hostState: true`
(e.g. server-side cursors persisted by the host) — testable end-to-end.

### HTTP API (for scripting the host)

The web backend is a thin HTTP/SSE layer over `HostSession`:

| Method | Path            | Purpose                                   |
|--------|-----------------|-------------------------------------------|
| GET    | `/api/snapshot` | current state (tools, channels, events…)  |
| GET    | `/api/events`   | Server-Sent Events stream of live events  |
| POST   | `/api/command`  | `{ op, … }` host actions, returns result  |

`op` values: `callTool`, `refreshTools`, `refreshChannels`, `openChannel`,
`publish`, `featureSetsUpdate`, `raw`, `notify`, `resolvePending`,
`rejectPending`, `autoApprove`, `restart`, `stateRollback`, `stateClear`,
`snapshot`.

## CLI REPL

The CLI shares the same `HostSession` engine as the web UI, so it has the same
capabilities — including host-state persistence and rollback.

```bash
npm start -- [--state FILE|off] [--auto-approve] -- node path/to/your-mcpl-server.js --stdio
# commands:
#   tools | call <tool> [json] | channels | open <id> | publish <id> <text>
#   events [n] | watch on|off | raw <method> [json]
#   state | rollback <featureSet> <checkpoint> | stateclear [featureSet]
#   restart | info | quit
```

`call` injects the current host/server state into stateful tool calls and
records any checkpoint the result returns — same as the web UI. Commands can
also be piped in for scripted tests.

## Using the engine directly

```ts
import { HostSession } from '@connectome/mcpl-harness/dist/src/session.js';

const session = new HostSession({ command: 'node', args: ['server.js', '--stdio'] });
session.on('event', (ev) => console.log(ev.summary));
await session.start();
const result = await session.callTool('my_tool', { foo: 1 });
```
