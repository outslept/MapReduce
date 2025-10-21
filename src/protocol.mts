/**
 * Key/value pair emitted by map and consumed by reduce
 *
 * @example
 * import type { KV } from "./protocol";
 * const kv: KV = { key: "word", value: "1" };
 */
interface KV {
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
interface PollRequest {
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
type PollResponse =
  | {
      readonly type: "map";
      readonly mapId: number;
      readonly file: string;
      readonly nReduce: number;
    }
  | {
      readonly type: "reduce";
      readonly reduceId: number;
      readonly nReduce: number;
    }
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
type ReportRequest =
  | {
      readonly workerId: string;
      readonly type: "map";
      readonly mapId: number;
      readonly success: boolean;
    }
  | {
      readonly workerId: string;
      readonly type: "reduce";
      readonly reduceId: number;
      readonly success: boolean;
    };

/**
 * Map function signature for plugins
 *
 * @param filename - Logical name of the input split (often a file path)
 * @param content - Entire textual content of the split
 * @returns Readonly array of key/value pairs to be partitioned across reducers

 * @example
 * import type { MapFn } from "./protocol";
 *
 * const map: MapFn = (_filename, content) =>
 *   content.split(/\W+/).filter(Boolean).map(w => ({ key: w, value: "1" }));
 */
type MapFn = (filename: string, content: string) => readonly KV[];

/**
 * Reduce function signature for plugins
 *
 * @param key - Group key
 * @param values - All values emitted by all mappers for this key
 * @returns Single string value representing the reduction result
 *
 * @example
 * import type { ReduceFn } from "./protocol";
 *
 * const reduce: ReduceFn = (_key, values) => String(values.reduce((s, v) => s + Number(v), 0));
 */
type ReduceFn = (key: string, values: readonly string[]) => string;

/**
 * Plugin module shape expected by the worker
 *
 * @example
 * import type { PluginModule } from "./protocol";
 *
 * const plugin: PluginModule = { map, reduce };
 */
interface PluginModule {
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
const isObject = (val: unknown): val is Dict => {
  return typeof val === "object" && val !== null;
};

/**
 * Check for a non-negative integer (JSON-safe)
 * @internal
 */
const isPositiveInt = (val: unknown): val is number => {
  const minValue = 0;
  return typeof val === "number" && Number.isInteger(val) && val >= minValue;
};

/**
 * Validate and narrow an unknown value to {@link PollRequest}
 *
 * @param val - Untrusted JSON payload
 * @returns True if the payload matches {@link PollRequest}
 *
 * @example
 * import { isPollRequest } from "./protocol";
 * const ok = isPollRequest(JSON.parse('{"workerId":"w-123"}'));
 */
const isPollRequest = (val: unknown): val is PollRequest => {
  return isObject(val) && typeof val.workerId === "string";
};

/**
 * Validate and narrow an unknown value to {@link ReportRequest}
 *
 * @param val - Untrusted JSON payload
 * @returns True if the payload matches a map or reduce report shape
 *
 * @example
 * import { isReportRequest } from "./protocol";
 * const ok = isReportRequest({ workerId: "w1", type: "map", mapId: 0, success: true });
 */
const isReportRequest = (val: unknown): val is ReportRequest => {
  if (!isObject(val)) {
    return false;
  }
  const obj = val;
  if (
    typeof obj.workerId !== "string" ||
    typeof obj.type !== "string" ||
    typeof obj.success !== "boolean"
  ) {
    return false;
  }
  if (obj.type === "map") {
    return isPositiveInt(obj.mapId);
  }
  if (obj.type === "reduce") {
    return isPositiveInt(obj.reduceId);
  }
  return false;
};

/**
 * Validate and narrow an unknown value to {@link PollResponse}
 *
 * @param val - Untrusted JSON payload
 * @returns True if the payload matches one of the response variants
 *
 * @example
 * import { isPollResponse } from "./protocol";
 * const ok = isPollResponse({ type: "sleep" });
 */
const isPollResponse = (val: unknown): val is PollResponse => {
  if (!isObject(val) || typeof val.type !== "string") {
    return false;
  }

  const taskType = val.type;

  if (taskType === "sleep" || taskType === "done") {
    return true;
  }

  if (taskType === "map") {
    return (
      isPositiveInt(val.mapId) &&
      typeof val.file === "string" &&
      isPositiveInt(val.nReduce)
    );
  }

  if (taskType === "reduce") {
    return isPositiveInt(val.reduceId) && isPositiveInt(val.nReduce);
  }
  return false;
};

export {
  type KV,
  type PollRequest,
  type PollResponse,
  type ReportRequest,
  type MapFn,
  type ReduceFn,
  type PluginModule,
  isPollRequest,
  isReportRequest,
  isPollResponse,
};
