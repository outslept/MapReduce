import path from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID } from "node:crypto";
import { createReadStream, promises as fsp } from "node:fs";
import { createInterface } from "node:readline";
import { isPollResponse, type KV, type MapFn, type PollResponse, type ReduceFn, type PluginModule } from "./protocol.mjs";

/**
 * CLI arguments for the worker entrypoint
 *
 * @property coordUrl — Coordinator base URL (e.g., http://127.0.0.1:8787)
 * @property pluginPath — Filesystem path to the plugin module exporting map/reduce
 */
interface Args {
  readonly coordUrl: string;
  readonly pluginPath: string;
}

/**
 * Parse CLI arguments in the form:
 *  --coord=<url> --plugin=<path>
 *
 * @param argv — Raw process arguments (excluding node and script path)
 * @returns Parsed {@link Args}
 */
function parseArgs(argv: readonly string[]): Args {
  const coordUrl = (argv.find(a => a.startsWith("--coord=")) ?? "--coord=http://127.0.0.1:8787").split("=")[1]!;
  const pluginPath = (argv.find(a => a.startsWith("--plugin=")) ?? "--plugin=./plugins/wc.js").split("=")[1]!;
  return { coordUrl, pluginPath };
}

/**
 * Compute FNV-1a 32-bit hash of a string
 *
 * @param s — Input string
 * @returns Unsigned 32-bit hash
 */
function fnv1a32(s: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

/**
 * Partition key into one of nReduce buckets using {@link fnv1a32}
 *
 * @param key — Map output key
 * @param nReduce — Total number of reduce partitions
 * @returns Bucket index in [0, nReduce)
 */
function ihash(key: string, nReduce: number): number {
  return fnv1a32(key) % nReduce;
}

/**
 * POST JSON helper with strict 2xx requirement
 *
 * @param url — Target URL
 * @param body — Serializable payload
 * @returns Parsed JSON as T
 * @throws Error — If HTTP status is not ok (2xx) or parsing fails
 */
async function postJSON<T>(url: string, body: unknown): Promise<T> {
  const resp = await fetch(url, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return (await resp.json()) as T;
}

/**
 * Write a file atomically via a temporary file + rename
 *
 * @param filePath — Final file path
 * @param data — File contents
 */
async function writeAtomic(filePath: string, data: string | Buffer): Promise<void> {
  const tmp = `${filePath}.${Math.random().toString(16).slice(2)}.tmp`;
  await fsp.writeFile(tmp, data);
  await fsp.rename(tmp, filePath);
}

/**
 * Write rows as newline-delimited JSON (NDJSON)
 *
 * @param filePath — Destination path
 * @param rows — KV records
 */
async function writeJsonl(filePath: string, rows: ReadonlyArray<KV>): Promise<void> {
  const payload = rows.map(r => JSON.stringify(r)).join("\n") + (rows.length ? "\n" : "");
  await writeAtomic(filePath, payload);
}

/**
 * Read multiple NDJSON files containing KV objects
 *
 * @param paths — File paths
 * @returns Parsed KV list
 */
async function readJsonlFiles(paths: readonly string[]): Promise<KV[]> {
  const out: KV[] = [];
  for (const p of paths) {
    try {
      const rl = createInterface({ input: createReadStream(p), crlfDelay: Infinity });
      for await (const line of rl) {
        if (!line) continue;
        const j = JSON.parse(line) as unknown;
        if (typeof j === "object" && j !== null && "key" in j && "value" in j) {
          const k = (j as { key: unknown }).key;
          const v = (j as { value: unknown }).value;
          if (typeof k === "string" && typeof v === "string") out.push({ key: k, value: v });
        }
      }
    } catch { /* ignore */ }
  }
  return out;
}

/**
 * Sort KVs by key and group adjacent values
 *
 * @param kvs — Input KV list (mutated by sort)
 * @returns Array of [key, values[]] groups in ascending key order
 */
function groupByKeySorted(kvs: KV[]): ReadonlyArray<readonly [string, string[]]> {
  kvs.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  const res: Array<readonly [string, string[]]> = [];
  let curKey: string | null = null;
  let cur: string[] = [];
  for (const { key, value } of kvs) {
    if (key !== curKey) {
      if (curKey !== null) res.push([curKey, cur]);
      curKey = key; cur = [value];
    } else {
      cur.push(value);
    }
  }
  if (curKey !== null) res.push([curKey, cur]);
  return res;
}

/**
 * Execute a map task:
 * - Read input file
 * - Run plugin map
 * - Partition results and write NDJSON buckets mr-<mapId>-<reduceId>-<workerId>.jsonl
 *
 * @param mapId — Map task id
 * @param filename — Input file path
 * @param nReduce — Number of reduce buckets
 * @param workerId — Stable worker UUID for file names
 * @param mapFn — Plugin map function
 */
async function doMapTask(mapId: number, filename: string, nReduce: number, workerId: string, mapFn: MapFn): Promise<void> {
  const content = await fsp.readFile(filename, "utf8");
  const kvs = mapFn(filename, content);
  const buckets: KV[][] = Array.from({ length: nReduce }, () => []);
  for (const kv of kvs) buckets[ihash(kv.key, nReduce)]!.push(kv);
  await Promise.all(buckets.map((rows, reduceId) => writeJsonl(`mr-${mapId}-${reduceId}-${workerId}.jsonl`, rows)));
}

/**
 * Execute a reduce task:
 * - Collect all mr-*-<reduceId>-*.jsonl files
 * - Group values by key
 * - Apply plugin reduce and write mr-out-<reduceId>
 *
 * @param reduceId — Reduce partition id
 * @param reduceFn — Plugin reduce function
 */
async function doReduceTask(reduceId: number, reduceFn: ReduceFn): Promise<void> {
  const names = await fsp.readdir(process.cwd());
  const rx = new RegExp(`^mr-(\\d+)-${reduceId}-[a-f0-9-]+\\.jsonl$`);
  const files = names.filter(n => rx.test(n));
  const kvs = await readJsonlFiles(files);
  const groups = groupByKeySorted(kvs);
  let out = "";
  for (const [key, values] of groups) {
    out += `${key} ${reduceFn(key, values)}\n`;
  }
  await writeAtomic(`mr-out-${reduceId}`, out);
}

/**
 * Convert a filesystem path to a file:// URL for ESM dynamic import
 *
 * @param p — Path (absolute or relative)
 * @returns File URL
 */
function toFileUrl(p: string): string {
  const abs = path.isAbsolute(p) ? p : path.join(process.cwd(), p);
  return pathToFileURL(abs).href;
}

/**
 * Load a plugin module and validate required exports
 *
 * @param pluginPath — Filesystem path to the plugin module
 * @returns Loaded {@link PluginModule}
 * @throws Error — If module does not export map and reduce functions
 */
async function loadPlugin(pluginPath: string): Promise<PluginModule> {
  const mod = await import(toFileUrl(pluginPath));
  const map = (mod as Partial<PluginModule>).map;
  const reduce = (mod as Partial<PluginModule>).reduce;
  if (typeof map !== "function" || typeof reduce !== "function") {
    throw new Error("plugin must export map(filename, content) and reduce(key, values)");
  }
  return { map, reduce };
}

async function main(): Promise<void> {
  const { coordUrl, pluginPath } = parseArgs(process.argv.slice(2));
  const workerId = randomUUID();
  const plugin = await loadPlugin(pluginPath);

  console.log(`worker ${workerId} -> ${coordUrl}`);

  for (;;) {
    let raw: unknown;
    try {
      raw = await postJSON<unknown>(`${coordUrl}/pollTask`, { workerId });
    } catch {
      process.exit(0);
    }
    if (!isPollResponse(raw)) {
      await new Promise(r => setTimeout(r, 200));
      continue;
    }
    const task: PollResponse = raw;
    if (task.type === "sleep") { await new Promise(r => setTimeout(r, 200)); continue; }
    if (task.type === "done") { process.exit(0); }
    try {
      if (task.type === "map") {
        await doMapTask(task.mapId, task.file, task.nReduce, workerId, plugin.map);
        await postJSON(`${coordUrl}/reportTask`, { workerId, type: "map", mapId: task.mapId, success: true });
      } else {
        await doReduceTask(task.reduceId, plugin.reduce);
        await postJSON(`${coordUrl}/reportTask`, { workerId, type: "reduce", reduceId: task.reduceId, success: true });
      }
    } catch {
      if (task.type === "map") {
        await postJSON(`${coordUrl}/reportTask`, { workerId, type: "map", mapId: task.mapId, success: false });
      } else {
        await postJSON(`${coordUrl}/reportTask`, { workerId, type: "reduce", reduceId: task.reduceId, success: false });
      }
      await new Promise(r => setTimeout(r, 300));
    }
  }
}

main().catch(() => process.exit(1));