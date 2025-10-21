/**
 * Key/value pair emitted by map and consumed by reduce
 *
 * @example
 * import type { KV } from "./protocol";
 * const kv: KV = { key: "word", value: "1" };
 */
export interface KV {
  readonly key: string;
  readonly value: string;
}

/**
 * Worker request to obtain a task
 *
 * @example
 * import type { PollRequest } from "./protocol";
 * const req: PollRequest = { workerId: "e2b0c6c5-4b1d-4a4d-9dff-9d80a0f2c0e1" };
 */
export interface PollRequest {
  readonly workerId: string;
}

/**
 * Coordinator response describing the next action for a worker
 *
 * @remarks
 * - type "sleep": no task available; worker should back off briefly and retry
 * - type "done": all work finished; worker should exit
 * - type "map": perform a map task for a single input file
 * - type "reduce": perform a reduce task for a specific partition
 *
 * @example
 * import type { PollResponse } from "./protocol";
 *
 * function handle(resp: PollResponse) {
 *   switch (resp.type) {
 *     case "sleep":
 *     case "done":
 *       break;
 *     case "map":
 *       // resp.mapId, resp.file, resp.nReduce are available here
 *       break;
 *     case "reduce":
 *       // resp.reduceId, resp.nReduce are available here
 *       break;
 *   }
 * }
 */
export type PollResponse =
  | { readonly type: "map"; readonly mapId: number; readonly file: string; readonly nReduce: number }
  | { readonly type: "reduce"; readonly reduceId: number; readonly nReduce: number }
  | { readonly type: "sleep" }
  | { readonly type: "done" };

/**
 * Worker report indicating task completion result
 *
 * @remarks
 * The union shape ties the required identifier to the task kind. Reports are
 * idempotent per task attempt; the coordinator may reissue timed-out tasks.
 *
 * @example
 * import type { ReportRequest } from "./protocol";
 *
 * const okMap: ReportRequest = { workerId: "w1", type: "map", mapId: 0, success: true };
 * const failReduce: ReportRequest = { workerId: "w1", type: "reduce", reduceId: 2, success: false };
 */
export type ReportRequest =
  | { readonly workerId: string; readonly type: "map"; readonly mapId: number; readonly success: boolean }
  | { readonly workerId: string; readonly type: "reduce"; readonly reduceId: number; readonly success: boolean };

/**
 * Map function signature for plugins
 *
 * @param filename — Logical name of the input split (often a file path)
 * @param content — Entire textual content of the split
 * @returns Readonly array of key/value pairs to be partitioned across reducers
 *
 * @remarks
 * - Should be pure and deterministic: same input yields same output
 * - Return values are consumed immediately; large arrays may impact memory
 *
 * @example
 * import type { MapFn } from "./protocol";
 *
 * const map: MapFn = (_filename, content) =>
 *   content.split(/\W+/).filter(Boolean).map(w => ({ key: w, value: "1" }));
 */
export type MapFn = (filename: string, content: string) => ReadonlyArray<KV>;

/**
 * Reduce function signature for plugins
 *
 * @param key — Group key
 * @param values — All values emitted by all mappers for this key
 * @returns Single string value representing the reduction result
 *
 * @example
 * import type { ReduceFn } from "./protocol";
 *
 * const reduce: ReduceFn = (_key, values) => String(values.reduce((s, v) => s + Number(v), 0));
 */
export type ReduceFn = (key: string, values: ReadonlyArray<string>) => string;

/**
 * Plugin module shape expected by the worker
 *
 * @remarks
 * The worker loads a module dynamically and calls {@link PluginModule.map} and
 * {@link PluginModule.reduce} during execution.
 *
 * @example
 * import type { PluginModule } from "./protocol";
 *
 * const plugin: PluginModule = { map, reduce };
 */
export interface PluginModule {
  readonly map: MapFn;
  readonly reduce: ReduceFn;
}

/**
 * Plain dictionary type for runtime inspection
 * @internal
 */
type Dict = Record<string, unknown>;

/**
 * Narrow to a non-null object record
 * @internal
 */
function isObject(x: unknown): x is Dict {
  return typeof x === "object" && x !== null;
}

/**
 * Check for a non-negative integer (JSON-safe)
 * @internal
 */
function isPositiveInt(x: unknown): x is number {
  return typeof x === "number" && Number.isInteger(x) && x >= 0;
}

/**
 * Validate and narrow an unknown value to {@link PollRequest}
 *
 * @param x — Untrusted JSON payload
 * @returns True if the payload matches {@link PollRequest}
 *
 * @example
 * import { isPollRequest } from "./protocol";
 * const ok = isPollRequest(JSON.parse('{"workerId":"w-123"}'));
 */
export function isPollRequest(x: unknown): x is PollRequest {
  return isObject(x) && typeof (x as Dict).workerId === "string";
}

/**
 * Validate and narrow an unknown value to {@link ReportRequest}
 *
 * @param x — Untrusted JSON payload
 * @returns True if the payload matches a map or reduce report shape
 *
 * @remarks
 * Requires an appropriate id field for the declared task kind.
 *
 * @example
 * import { isReportRequest } from "./protocol";
 * const ok = isReportRequest({ workerId: "w1", type: "map", mapId: 0, success: true });
 */
export function isReportRequest(x: unknown): x is ReportRequest {
  if (!isObject(x)) return false;
  const o = x as Dict;
  if (typeof o.workerId !== "string" || typeof o.type !== "string" || typeof o.success !== "boolean") return false;
  if (o.type === "map") return isPositiveInt(o.mapId);
  if (o.type === "reduce") return isPositiveInt(o.reduceId);
  return false;
}

/**
 * Validate and narrow an unknown value to {@link PollResponse}
 *
 * @param x — Untrusted JSON payload
 * @returns True if the payload matches one of the response variants
 *
 * @example
 * import { isPollResponse } from "./protocol";
 * const ok = isPollResponse({ type: "sleep" });
 */
export function isPollResponse(x: unknown): x is PollResponse {
  if (!isObject(x) || typeof (x as Dict).type !== "string") return false;
  const t = (x as Dict).type as string;
  if (t === "sleep" || t === "done") return true;
  if (t === "map") {
    return isPositiveInt((x as Dict).mapId) &&
           typeof (x as Dict).file === "string" &&
           isPositiveInt((x as Dict).nReduce);
  }
  if (t === "reduce") {
    return isPositiveInt((x as Dict).reduceId) &&
           isPositiveInt((x as Dict).nReduce);
  }
  return false;
}