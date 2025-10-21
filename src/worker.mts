import { createReadStream, promises as fsp } from "node:fs";
import { isAbsolute, join } from "node:path";
import { isPollResponse, type KV, type MapFn, type PluginModule, type PollResponse, type ReduceFn } from "./protocol.mjs";
import { randomUUID } from "node:crypto";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { pathToFileURL } from "node:url";

/**
 * Worker CLI arguments
 *
 * @property coordUrl - Coordinator base URL (e.g., http://127.0.0.1:8787)
 * @property pluginPath - Filesystem path to a module exporting map/reduce
 */
interface Args {
  readonly coordUrl: string;
  readonly pluginPath: string;
}

const DEFAULT_COORD_URL = "http://127.0.0.1:8787";
const DEFAULT_PLUGIN_PATH = "./plugins/wc.mts";

const FLAG_VALUE_INDEX = 1;
const ARG_SLICE_INDEX = 2;

const SLEEP_IDLE_MS = 200;
const SLEEP_FAILURE_MS = 300;

const EXIT_OK = 0;
const EXIT_ERR = 1;

const EMPTY = 0;
const HEX_RADIX = 16;

const FNV_OFFSET_BASIS = 0x811C9DC5;
const FNV_PRIME = 0x01000193;

/**
 * Parse CLI arguments in the form `--coord=<url> --plugin=<path>`
 *
 * @param argv - Raw process arguments (e.g., process.argv.slice(2))
 * @returns Parsed {@link Args}
 *
 * @example
 * const args = parseArgs(["--coord=http://127.0.0.1:8787","--plugin=./plugins/wc.mts"])
 * // args.coordUrl === "http://127.0.0.1:8787"
 */
const parseArgs = (argv: readonly string[]): Args => {
  let coordUrl = DEFAULT_COORD_URL;
  let pluginPath = DEFAULT_PLUGIN_PATH;

  for (const arg of argv) {
    if (arg.startsWith("--coord=")) {
      const parts = arg.split("=");
      if (parts.length > FLAG_VALUE_INDEX) {
        coordUrl = parts[FLAG_VALUE_INDEX]!;
      }
    } else if (arg.startsWith("--plugin=")) {
      const parts = arg.split("=");
      if (parts.length > FLAG_VALUE_INDEX) {
        pluginPath = parts[FLAG_VALUE_INDEX]!;
      }
    }
  }

  return { coordUrl, pluginPath };
};

/**
 * Compute FNV‑1a 32-bit hash for a UTF‑16 string
 *
 * @param input - String to hash
 * @returns Unsigned 32-bit hash
 *
 * @example
 * const h = fnv1a32("key")
 */
const fnv1a32 = (input: string): number => {
  let hash = FNV_OFFSET_BASIS;
  for (let index = 0; index < input.length; index++) {
    hash ^= input.charCodeAt(index);
    hash = Math.imul(hash, FNV_PRIME) >>> 0;
  }
  return hash >>> 0;
};

/**
 * Map a key to a reduce bucket index
 *
 * @param key - Partition key
 * @param nReduce - Total number of reducers (must be > 0)
 * @returns Index in [0, nReduce)
 *
 * @example
 * const bucket = ihash("word", 4) // 0..3
 */
const ihash = (key: string, nReduce: number): number => fnv1a32(key) % nReduce;

/**
 * POST JSON helper with strict 2xx check
 *
 * @param url - Coordinator endpoint
 * @param body - Serializable JSON body
 * @returns Parsed JSON (unknown); call-site should narrow with type guards
 * @throws Error - If HTTP status is not ok (2xx) or JSON parsing fails
 *
 * @example
 * const reply = await postJSON("http://127.0.0.1:8787/pollTask", { workerId: "w1" })
 */
const postJSON = async (url: string, body: unknown): Promise<unknown> => {
  const response = await fetch(url, {
    body: JSON.stringify(body),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const data: unknown = await response.json();
  return data;
};

/**
 * Write a file atomically via a temporary file + rename
 *
 * @param filePath - Final file path
 * @param data - File contents
 * @returns Resolves when the file is fully written
 *
 * @example
 * await writeAtomic("mr-out-0", "payload\n")
 */
const writeAtomic = async (filePath: string, data: string | Buffer): Promise<void> => {
  const tmp = `${filePath}.${Math.random().toString(HEX_RADIX).slice(FLAG_VALUE_INDEX)}.tmp`;
  await fsp.writeFile(tmp, data);
  await fsp.rename(tmp, filePath);
};

/**
 * Write KV rows as newline-delimited JSON (NDJSON)
 *
 * @param filePath - Destination path
 * @param rows - KV records to write
 * @returns Resolves when writing completes
 *
 * @example
 * await writeJsonl("mr-0-1-worker.jsonl", [{ key: "word", value: "1" }])
 */
const writeJsonl = async (filePath: string, rows: readonly KV[]): Promise<void> => {
  let payload = rows.map((row) => JSON.stringify(row)).join("\n");
  if (rows.length > EMPTY) {
    payload += "\n";
  }
  await writeAtomic(filePath, payload);
};

/**
 * Read KV pairs from multiple NDJSON files
 *
 * @param paths - File paths to read (missing or malformed files are skipped)
 * @returns All parsed KV objects
 *
 * @example
 * const kvs = await readJsonlFiles(["mr-0-0-a.jsonl","mr-1-0-b.jsonl"])
 */
const readJsonlFiles = async (paths: readonly string[]): Promise<KV[]> => {
  const out: KV[] = [];
  for (const filePath of paths) {
    try {
      const rl = createInterface({ crlfDelay: Infinity, input: createReadStream(filePath) });
      for await (const line of rl) {
        if (line.length === EMPTY) {
          // skip empty
        } else {
          const obj = JSON.parse(line);
          if (typeof obj === "object" && obj !== null && "key" in obj && "value" in obj) {
            const keyAny = obj.key;
            const valueAny = obj.value;
            if (typeof keyAny === "string" && typeof valueAny === "string") {
              out.push({ key: keyAny, value: valueAny });
            }
          }
        }
      }
    } catch {
      // ignore
    }
  }
  return out;
};

/**
 * Sort KVs by key and group adjacent values
 *
 * @param pairs - KV records (mutated in place by sort)
 * @returns Array of [key, values[]] groups in ascending key order
 *
 * @example
 * const groups = groupByKeySorted([{key:"a",value:"1"},{key:"a",value:"1"},{key:"b",value:"1"}])
 */
const groupByKeySorted = (pairs: KV[]): readonly (readonly [string, string[]])[] => {
  pairs.sort((left, right) => left.key.localeCompare(right.key));
  const grouped: (readonly [string, string[]])[] = [];
  let currentKey: string | undefined = undefined;
  let currentValues: string[] = [];
  for (const { key, value } of pairs) {
    if (key !== currentKey) {
      if (currentKey !== undefined) {
        grouped.push([currentKey, currentValues]);
      }
      currentKey = key;
      currentValues = [value];
    } else {
      currentValues.push(value);
    }
  }
  if (currentKey !== undefined) {
    grouped.push([currentKey, currentValues]);
  }
  return grouped;
};

/**
 * Options for executing a single map task
 *
 * @property mapId - Map task id
 * @property filename - Input file path for this map
 * @property nReduce - Number of reduce partitions
 * @property workerId - Worker id used in intermediate filenames
 * @property mapFn - Plugin map function
 */
interface MapTaskOptions {
  readonly mapId: number;
  readonly filename: string;
  readonly nReduce: number;
  readonly workerId: string;
  readonly mapFn: MapFn;
}

/**
 * Execute a map task
 *
 * Reads the input file, runs the plugin map, partitions results by reduceId,
 * and writes mr-<mapId>-<reduceId>-<workerId>.jsonl files.
 *
 * @param options - {@link MapTaskOptions}
 * @returns Resolves when all bucket files are written
 *
 * @example
 * await doMapTask({ mapId: 0, filename: "data/pg-1.txt", nReduce: 4, workerId: "w1", mapFn })
 */
const doMapTask = async (options: MapTaskOptions): Promise<void> => {
  const { filename, mapFn, mapId, nReduce, workerId } = options;
  const content = await fsp.readFile(filename, "utf8");
  const kvs = mapFn(filename, content);
  const buckets: KV[][] = Array.from({ length: nReduce }, () => []);

  for (const pair of kvs) {
    const index = ihash(pair.key, nReduce);
    const bucket = buckets[index];
    if (bucket === undefined) {
      buckets[index] = [pair];
    } else {
      bucket.push(pair);
    }
  }

  await Promise.all(
    buckets.map(async (rows, reduceId) => {
      const outPath = `mr-${mapId}-${reduceId}-${workerId}.jsonl`;
      await writeJsonl(outPath, rows);
    })
  );
};

/**
 * Execute a reduce task
 *
 * Collects all intermediate files for the given reduceId, groups by key, runs
 * the plugin reduce for each group, and writes mr-out-<reduceId>.
 *
 * @param reduceId - Reduce partition id
 * @param reduceFn - Plugin reduce function
 * @returns Resolves when the final output is written
 *
 * @example
 * await doReduceTask(0, reduceFn)
 */
const doReduceTask = async (reduceId: number, reduceFn: ReduceFn): Promise<void> => {
  const names = await fsp.readdir(process.cwd());
  const rx = new RegExp(`^mr-(\\d+)-${reduceId}-[a-f0-9-]+\\.jsonl$`);
  const files = names.filter((name) => rx.test(name));
  const kvs = await readJsonlFiles(files);
  const groups = groupByKeySorted(kvs);
  let out = "";
  for (const [key, values] of groups) {
    out += `${key} ${reduceFn(key, values)}\n`;
  }
  await writeAtomic(`mr-out-${reduceId}`, out);
};

/**
 * Convert a filesystem path to a file:// URL for dynamic ESM import
 *
 * @param filePath - Absolute or relative path
 * @returns File URL suitable for import()
 *
 * @example
 * const url = toFileUrl("./plugins/wc.mts")
 */
const toFileUrl = (filePath: string): string => {
  const abs = isAbsolute(filePath) ? filePath : join(process.cwd(), filePath);
  return pathToFileURL(abs).href;
};

/**
 * Narrow an unknown to a plain record
 *
 * @param v - Value to check
 * @returns True if v is a non-null object
 * @internal
 */
const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null;

/**
 * Validate a plugin-like object (has map and reduce functions)
 *
 * @param v - Candidate object
 * @returns True if v has callable map and reduce
 * @internal
 */
const isPluginModule = (v: unknown): v is PluginModule => {
  return isRecord(v) && typeof v.map === "function" && typeof v.reduce === "function";
};

/**
 * Dynamically load a plugin module and validate its shape
 *
 * Accepts either named exports (`export const map/reduce`) or a default
 * export object `{ map, reduce }`.
 *
 * @param pluginPath - Filesystem path to the plugin module
 * @returns Loaded {@link PluginModule}
 * @throws Error - If the module does not export both map and reduce functions
 *
 * @example
 * const plugin = await loadPlugin("./plugins/wc.mts")
 */
const loadPlugin = async (pluginPath: string): Promise<PluginModule> => {
  const modNs: unknown = await import(toFileUrl(pluginPath));
  let candidate: unknown = modNs;

  if (isRecord(modNs)) {
    const maybeDefault = Reflect.get(modNs, "default");
    if (maybeDefault !== undefined) {
      candidate = maybeDefault;
    }
  }

  if (isPluginModule(candidate)) {
    return candidate;
  }

  throw new Error("plugin must export map(filename, content) and reduce(key, values)");
};

const main = async (): Promise<void> => {
  const { coordUrl, pluginPath } = parseArgs(process.argv.slice(ARG_SLICE_INDEX));
  const workerId = randomUUID();
  const plugin = await loadPlugin(pluginPath);

  console.log(`worker ${workerId} -> ${coordUrl}`);

  /**
   * Execute one scheduling iteration:
   * - Poll for a task
   * - Sleep on "sleep" or invalid payload
   * - Exit on "done"
   * - Run map/reduce with success/failure reporting
   *
   * @returns Resolves after one iteration delay or task execution
   */
  const tick = async (): Promise<void> => {
    let payload: unknown;
    try {
      payload = await postJSON(`${coordUrl}/pollTask`, { workerId });
    } catch {
      process.exit(EXIT_OK);
    }

    if (!isPollResponse(payload)) {
      await delay(SLEEP_IDLE_MS);
      return;
    }

    const task: PollResponse = payload;

    if (task.type === "sleep") {
      await delay(SLEEP_IDLE_MS);
      return;
    }

    if (task.type === "done") {
      process.exit(EXIT_OK);
    }

    try {
      if (task.type === "map") {
        await doMapTask({
          filename: task.file,
          mapFn: plugin.map,
          mapId: task.mapId,
          nReduce: task.nReduce,
          workerId,
        });
        await postJSON(`${coordUrl}/reportTask`, {
          mapId: task.mapId,
          success: true,
          type: "map",
          workerId,
        });
      } else {
        await doReduceTask(task.reduceId, plugin.reduce);
        await postJSON(`${coordUrl}/reportTask`, {
          reduceId: task.reduceId,
          success: true,
          type: "reduce",
          workerId,
        });
      }
    } catch {
      if (task.type === "map") {
        await postJSON(`${coordUrl}/reportTask`, {
          mapId: task.mapId,
          success: false,
          type: "map",
          workerId,
        });
      } else {
        await postJSON(`${coordUrl}/reportTask`, {
          reduceId: task.reduceId,
          success: false,
          type: "reduce",
          workerId,
        });
      }
      await delay(SLEEP_FAILURE_MS);
    }
  };

  for (;;) {
    await tick();
  }
};

void (async () => {
  try {
    await main();
  } catch (error) {
    console.error(error);
    process.exit(EXIT_ERR);
  }
})();
