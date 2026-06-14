/**
 * StateStore — MCPL host-managed state persistence for the harness.
 *
 * A self-contained port of agent-framework's CheckpointManager
 * (src/mcpl/checkpoint-manager.ts), adapted to persist to a JSON file instead
 * of Chronicle, so host state survives the ⟳ restart button AND a full harness
 * relaunch. Implements MCPL spec Section 8 (State Management).
 *
 * Per stateful feature set it keeps a branching checkpoint tree. Two modes:
 *   - hostState: true  → the host is authoritative; it stores full state and
 *     applies JSON-Patch deltas to reconstruct it. Injected as `state` into
 *     subsequent tools/call params and returned from state/get.
 *   - hostState: false → server-managed; the host only tracks opaque checkpoint
 *     IDs, injected as `checkpoint` into tools/call params.
 *
 * Checkpoints arrive two ways, both supported:
 *   - embedded in a tool result as `result.state` (ecosystem convention), or
 *   - pushed via a `state/update` request (spec Section 8.1).
 */
import { readFileSync, writeFileSync, existsSync, mkdirSync, renameSync } from 'node:fs';
import { dirname } from 'node:path';

// ── JSON Patch (RFC 6902) ──

export interface JsonPatchOperation {
  op: 'add' | 'remove' | 'replace' | 'move' | 'copy' | 'test';
  path: string;
  value?: unknown;
  from?: string;
}

function parsePointer(pointer: string): string[] {
  if (pointer === '') return [];
  if (!pointer.startsWith('/')) throw new Error(`Invalid JSON Pointer: ${pointer}`);
  return pointer.slice(1).split('/').map((s) => s.replace(/~1/g, '/').replace(/~0/g, '~'));
}

function navigateTo(doc: unknown, segments: string[]): [Record<string, unknown> | unknown[], string] {
  let current: unknown = doc;
  for (let i = 0; i < segments.length - 1; i++) {
    const seg = segments[i]!;
    if (Array.isArray(current)) current = current[Number(seg)];
    else if (current !== null && typeof current === 'object') current = (current as Record<string, unknown>)[seg];
    else throw new Error(`Cannot navigate path segment "${seg}": not an object/array`);
  }
  return [current as Record<string, unknown> | unknown[], segments[segments.length - 1]!];
}

function valueAt(doc: unknown, pointer: string): unknown {
  const segments = parsePointer(pointer);
  if (segments.length === 0) return doc;
  const [parent, key] = navigateTo(doc, segments);
  return Array.isArray(parent) ? parent[Number(key)] : (parent as Record<string, unknown>)[key];
}

function removeAt(doc: unknown, pointer: string): void {
  const [parent, key] = navigateTo(doc, parsePointer(pointer));
  if (Array.isArray(parent)) parent.splice(Number(key), 1);
  else delete (parent as Record<string, unknown>)[key];
}

function addAt(doc: unknown, pointer: string, value: unknown): unknown {
  const segments = parsePointer(pointer);
  if (segments.length === 0) return value;
  const [parent, key] = navigateTo(doc, segments);
  if (Array.isArray(parent)) parent.splice(key === '-' ? parent.length : Number(key), 0, value);
  else (parent as Record<string, unknown>)[key] = value;
  return doc;
}

/** Apply an RFC-6902 JSON Patch to a deep clone of `doc`. Supports all ops. */
export function applyJsonPatch(doc: unknown, ops: JsonPatchOperation[]): unknown {
  let result: unknown = structuredClone(doc);
  for (const op of ops) {
    const segments = parsePointer(op.path);
    switch (op.op) {
      case 'add':
        result = addAt(result, op.path, op.value);
        break;
      case 'remove':
        removeAt(result, op.path);
        break;
      case 'replace': {
        if (segments.length === 0) { result = op.value; break; }
        const [parent, key] = navigateTo(result, segments);
        if (Array.isArray(parent)) parent[Number(key)] = op.value;
        else (parent as Record<string, unknown>)[key] = op.value;
        break;
      }
      case 'move': {
        const moved = valueAt(result, op.from!);
        removeAt(result, op.from!);
        result = addAt(result, op.path, structuredClone(moved));
        break;
      }
      case 'copy':
        result = addAt(result, op.path, structuredClone(valueAt(result, op.from!)));
        break;
      case 'test': {
        const actual = valueAt(result, op.path);
        if (JSON.stringify(actual) !== JSON.stringify(op.value))
          throw new Error(`JSON Patch test failed at "${op.path}"`);
        break;
      }
    }
  }
  return result;
}

// ── Checkpoint tree ──

export interface StateCheckpoint {
  checkpoint: string;
  parent?: string | null;
  data?: unknown;
  patch?: JsonPatchOperation[];
  /** Explicit owning feature set, when the server tags it. Removes ambiguity. */
  featureSet?: string;
}

interface CheckpointNode {
  checkpoint: string;
  parent: string | null;
  children: string[];
  data?: unknown;
  patch?: JsonPatchOperation[];
  at: string;
}

interface FeatureSetTree {
  hostState: boolean;
  rollback: boolean;
  current: string | null;
  currentState: unknown;
  nodes: Map<string, CheckpointNode>;
}

/** Serializable view for the UI / snapshot. */
export interface StateView {
  featureSets: Array<{
    featureSet: string;
    hostState: boolean;
    rollback: boolean;
    current: string | null;
    data: unknown;
    checkpoints: Array<{ checkpoint: string; parent: string | null; at: string; hasData: boolean; hasPatch: boolean }>;
  }>;
  path: string | null;
}

interface SerializedFile {
  version: 1;
  serverName?: string;
  featureSets: Record<string, {
    hostState: boolean;
    rollback: boolean;
    current: string | null;
    nodes: Record<string, { parent: string | null; children: string[]; data?: unknown; patch?: JsonPatchOperation[]; at: string }>;
  }>;
}

export class StateStore {
  private trees = new Map<string, FeatureSetTree>();
  private path: string | null;
  private serverName?: string;
  private nowIso: () => string;

  /** @param nowIso injected clock (the harness avoids `new Date()` in some contexts). */
  constructor(path: string | null, nowIso: () => string = () => new Date().toISOString()) {
    this.path = path;
    this.nowIso = nowIso;
    this.load();
  }

  setServerName(name: string | undefined): void {
    this.serverName = name;
  }

  /** Register a stateful feature set (idempotent — keeps loaded history). */
  registerFeatureSet(featureSet: string, opts: { hostState: boolean; rollback: boolean }): void {
    const existing = this.trees.get(featureSet);
    if (existing) {
      existing.hostState = opts.hostState;
      existing.rollback = opts.rollback;
      return;
    }
    this.trees.set(featureSet, {
      hostState: opts.hostState,
      rollback: opts.rollback,
      current: null,
      currentState: undefined,
      nodes: new Map(),
    });
    this.persist();
  }

  isStateful(featureSet: string): boolean {
    return this.trees.has(featureSet);
  }

  isHostManaged(featureSet: string): boolean {
    return this.trees.get(featureSet)?.hostState ?? false;
  }

  /**
   * Stateful feature sets, optionally narrowed by management mode.
   * `host` = hostState:true, `server` = server-managed (rollback only).
   */
  statefulSets(mode?: 'host' | 'server'): string[] {
    const out: string[] = [];
    for (const [fs, t] of this.trees) {
      if (mode === 'host' && !t.hostState) continue;
      if (mode === 'server' && t.hostState) continue;
      out.push(fs);
    }
    return out;
  }

  /** What to inject into a tools/call for the given (stateful) feature set. */
  injectionFor(featureSet: string): { state?: unknown; checkpoint?: string } | undefined {
    const tree = this.trees.get(featureSet);
    if (!tree) return undefined;
    if (tree.hostState) {
      return tree.currentState === undefined ? undefined : { state: tree.currentState };
    }
    return tree.current ? { checkpoint: tree.current } : undefined;
  }

  /** Record a checkpoint (from a tool result's `result.state` or a state/update). */
  recordCheckpoint(featureSet: string, cp: StateCheckpoint): void {
    const tree = this.trees.get(featureSet);
    if (!tree) return;
    const node: CheckpointNode = {
      checkpoint: cp.checkpoint,
      parent: cp.parent ?? null,
      children: [],
      data: cp.data,
      patch: cp.patch,
      at: this.nowIso(),
    };
    if (node.parent) tree.nodes.get(node.parent)?.children.push(cp.checkpoint);
    tree.nodes.set(cp.checkpoint, node);
    tree.current = cp.checkpoint;

    if (tree.hostState) {
      if (cp.data !== undefined) {
        tree.currentState = structuredClone(cp.data);
      } else if (cp.patch) {
        try {
          tree.currentState = applyJsonPatch(tree.currentState ?? {}, cp.patch);
        } catch {
          /* keep last good state on patch failure */
        }
      }
    }
    this.persist();
  }

  /** Answer a state/get: current checkpoint + reconstructed data. */
  get(featureSet: string): { checkpoint: string | null; data: unknown } {
    const tree = this.trees.get(featureSet);
    if (!tree) return { checkpoint: null, data: null };
    return { checkpoint: tree.current, data: tree.hostState ? (tree.currentState ?? null) : null };
  }

  /** Roll the current pointer back to a checkpoint, reconstructing host state. */
  rollback(featureSet: string, checkpoint: string): { success: boolean; data?: unknown; reason?: string } {
    const tree = this.trees.get(featureSet);
    if (!tree) return { success: false, reason: `unknown feature set: ${featureSet}` };
    if (!tree.nodes.has(checkpoint)) return { success: false, reason: `unknown checkpoint: ${checkpoint}` };
    tree.current = checkpoint;
    if (tree.hostState) tree.currentState = this.reconstruct(tree, checkpoint);
    this.persist();
    return { success: true, data: tree.hostState ? tree.currentState : undefined };
  }

  clear(featureSet?: string): void {
    if (featureSet) {
      const t = this.trees.get(featureSet);
      if (t) { t.nodes.clear(); t.current = null; t.currentState = undefined; }
    } else {
      for (const t of this.trees.values()) { t.nodes.clear(); t.current = null; t.currentState = undefined; }
    }
    this.persist();
  }

  view(): StateView {
    return {
      path: this.path,
      featureSets: [...this.trees.entries()].map(([featureSet, t]) => ({
        featureSet,
        hostState: t.hostState,
        rollback: t.rollback,
        current: t.current,
        data: t.hostState ? (t.currentState ?? null) : null,
        checkpoints: [...t.nodes.values()].map((n) => ({
          checkpoint: n.checkpoint,
          parent: n.parent,
          at: n.at,
          hasData: n.data !== undefined,
          hasPatch: n.patch !== undefined,
        })),
      })),
    };
  }

  // ── internals ──

  private reconstruct(tree: FeatureSetTree, checkpoint: string): unknown {
    const path: CheckpointNode[] = [];
    let cur: string | null = checkpoint;
    while (cur !== null) {
      const node = tree.nodes.get(cur);
      if (!node) break;
      path.unshift(node);
      cur = node.parent;
    }
    let state: unknown = {};
    for (const node of path) {
      if (node.data !== undefined) state = structuredClone(node.data);
      else if (node.patch) {
        try { state = applyJsonPatch(state, node.patch); } catch { /* keep last good */ }
      }
    }
    return state;
  }

  private persist(): void {
    if (!this.path) return;
    const out: SerializedFile = { version: 1, serverName: this.serverName, featureSets: {} };
    for (const [fs, t] of this.trees) {
      const nodes: SerializedFile['featureSets'][string]['nodes'] = {};
      for (const [id, n] of t.nodes) {
        nodes[id] = { parent: n.parent, children: n.children, data: n.data, patch: n.patch, at: n.at };
      }
      out.featureSets[fs] = { hostState: t.hostState, rollback: t.rollback, current: t.current, nodes };
    }
    try {
      mkdirSync(dirname(this.path), { recursive: true });
      const tmp = `${this.path}.tmp`;
      writeFileSync(tmp, JSON.stringify(out, null, 2));
      renameSync(tmp, this.path); // atomic replace
    } catch (e) {
      console.error('[StateStore] persist failed:', (e as Error).message);
    }
  }

  private load(): void {
    if (!this.path || !existsSync(this.path)) return;
    try {
      const data = JSON.parse(readFileSync(this.path, 'utf-8')) as SerializedFile;
      if (!data?.featureSets) return;
      this.serverName = data.serverName;
      for (const [fs, entry] of Object.entries(data.featureSets)) {
        const nodes = new Map<string, CheckpointNode>();
        for (const [id, n] of Object.entries(entry.nodes)) {
          nodes.set(id, { checkpoint: id, parent: n.parent, children: [...n.children], data: n.data, patch: n.patch, at: n.at });
        }
        const tree: FeatureSetTree = {
          hostState: entry.hostState,
          rollback: entry.rollback,
          current: entry.current,
          currentState: undefined,
          nodes,
        };
        if (entry.hostState && entry.current) tree.currentState = this.reconstruct(tree, entry.current);
        this.trees.set(fs, tree);
      }
    } catch (e) {
      console.error('[StateStore] load failed:', (e as Error).message);
    }
  }
}
