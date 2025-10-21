import { promises as fsp } from "node:fs";
import { glob } from "tinyglobby";
import * as http from "node:http";
import type { PollRequest, PollResponse, ReportRequest } from "./protocol.mjs";
import { isPollRequest, isReportRequest } from "./protocol.mjs";

type Phase = "map" | "reduce" | "done";

interface MapTask {
  readonly id: number;
  readonly file: string;
  state: "idle" | "in-progress" | "done";
  workerId?: string;
  startedAtMs?: number;
}

interface ReduceTask {
  readonly id: number;
  state: "idle" | "in-progress" | "done";
  workerId?: string;
  startedAtMs?: number;
}

interface Args {
  readonly port: number;
  readonly nReduce: number;
  readonly inputFiles: readonly string[];
}

const TIMEOUT_MS = 10_000;
const MIN_PORT = 1;
const MIN_REDUCERS = 1;
const FLAG_VALUE_INDEX = 1;
const ARGV_USER_INDEX = 2;
const EMPTY_LENGTH = 0;
const EXIT_ERROR = 1;

const HTTP_STATUS = {
  BAD_REQUEST: 400,
  INTERNAL_ERROR: 500,
  NOT_FOUND: 404,
  OK: 200,
} as const;

/** Return a coarse timestamp in milliseconds */
const now = (): number => Date.now();

/**
 * Check whether all tasks in a phase are complete
 *
 * @typeParam TaskType - task-like objects with a state field
 * @param items - Task collection to check
 * @returns True if every task is "done"
 */
const allDone = <TaskType extends { state: "idle" | "in-progress" | "done" }>(items: readonly TaskType[]): boolean =>
  items.every(task => task.state === "done");

/**
 * Clear transient assignee metadata for a task (worker id and start time)
 *
 * @param task - Task object to clear
 */
const clearAssignee = (task: { workerId?: string; startedAtMs?: number }): void => {
  delete task.workerId;
  delete task.startedAtMs;
};

/**
 * Parse CLI arguments and expand input file globs
 *
 * Accepts `--port=<number>`, `--nReduce=<number>`, and a list of file/glob patterns
 * (e.g., "data/pg-*.txt"). Patterns are resolved, validated as files, de-duplicated,
 * and sorted deterministically.
 *
 * @param argv - Raw process arguments starting at the first user arg (e.g., process.argv.slice(2))
 * @returns Parsed {@link Args} with resolved input files
 * @throws Error - If port or nReduce are invalid or no input files are found
 *
 * @example
 * // argv: ["--port=8787","--nReduce=4","data/pg-*.txt"]
 * const cfg = await parseArgs(argv)
 * // cfg.inputFiles => ["data/pg-0001.txt", ...]
 */
const parseArgs = async (argv: readonly string[]): Promise<Args> => {
  const portFlag = argv.find(arg => arg.startsWith("--port=")) ?? "--port=8787";
  const nReduceFlag = argv.find(arg => arg.startsWith("--nReduce=")) ?? "--nReduce=4";
  const port = Number(portFlag.split("=")[FLAG_VALUE_INDEX]);
  const nReduce = Number(nReduceFlag.split("=")[FLAG_VALUE_INDEX]);

  const rawInputs = argv.filter(arg => !arg.startsWith("--"));
  const patterns = rawInputs.map(pattern => pattern.replace(/\\/g, "/"));

  let matched: string[] = [];
  if (patterns.length > EMPTY_LENGTH) {
    const candidates = await glob(patterns);

    const validated = (await Promise.all(
      candidates.map(async (candidatePath) => {
        try {
          const stat = await fsp.stat(candidatePath);
          if (stat.isFile()) {
            return candidatePath;
          }
        } catch {
          // noop
        }
        return undefined;
      })
    )).filter((candidatePath): candidatePath is string => candidatePath !== undefined);

    const unique = new Set<string>(validated);
    matched = [...unique];
    matched.sort((first, second) => first.localeCompare(second));
  }

  if (!Number.isInteger(port) || port < MIN_PORT) {
    throw new Error("bad --port");
  }
  if (!Number.isInteger(nReduce) || nReduce < MIN_REDUCERS) {
    throw new Error("bad --nReduce");
  }
  if (matched.length === EMPTY_LENGTH) {
    throw new Error("no input files");
  }

  return { inputFiles: matched, nReduce, port };
};

/**
 * Read and parse the request body as JSON
 *
 * @param req - Incoming HTTP message
 * @returns Parsed JSON value (unknown)
 * @throws SyntaxError - If the request body is not valid JSON
 *
 * @example
 * // inside a request handler
 * const body = await readJson(req)
 */
const readJson = async (req: http.IncomingMessage): Promise<unknown> => {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    const buf = Buffer.from(chunk);
    chunks.push(buf);
  }
  const body = Buffer.concat(chunks).toString("utf8");
  if (body.length === EMPTY_LENGTH) {
    return {};
  }
  return JSON.parse(body);
};

/**
 * Create and configure the coordinator HTTP server
 *
 * Initializes task metadata from resolved input files, tracks phase transitions,
 * and exposes JSON RPC endpoints consumed by workers.
 *
 * @param args - {@link Args} including input files, reducer count, and port
 * @returns An HTTP server instance (not yet listening)
 */
const createServer = (args: Args): http.Server => {
  const mapTasks: MapTask[] = args.inputFiles.map((filePath, taskId) => ({ file: filePath, id: taskId, state: "idle" }));
  const reduceTasks: ReduceTask[] = Array.from({ length: args.nReduce }, (_unused, taskId) => ({ id: taskId, state: "idle" }));
  let phase: Phase = "map";

  /** Return in-progress tasks back to idle after TIMEOUT_MS */
  const reapTimeouts = (): void => {
    const deadline = now() - TIMEOUT_MS;

    for (const task of mapTasks) {
      if (task.state === "in-progress" && task.startedAtMs !== undefined && task.startedAtMs <= deadline) {
        task.state = "idle";
        clearAssignee(task);
      }
    }
    for (const task of reduceTasks) {
      if (task.state === "in-progress" && task.startedAtMs !== undefined && task.startedAtMs <= deadline) {
        task.state = "idle";
        clearAssignee(task);
      }
    }
  };

  /**
   * Handle a worker polling for a task
   *
   * @param req - {@link PollRequest} with the worker id
   * @returns {@link PollResponse} describing the next action
   */
  const onPoll = (req: PollRequest): PollResponse => {
    reapTimeouts();

    if (phase === "map") {
      const task = mapTasks.find(item => item.state === "idle");
      if (task) {
        task.state = "in-progress";
        task.workerId = req.workerId;
        task.startedAtMs = now();
        return { file: task.file, mapId: task.id, nReduce: args.nReduce, type: "map" };
      }
      if (!allDone(mapTasks)) {
        return { type: "sleep" };
      }
      phase = "reduce";
    }

    if (phase === "reduce") {
      const task = reduceTasks.find(item => item.state === "idle");
      if (task) {
        task.state = "in-progress";
        task.workerId = req.workerId;
        task.startedAtMs = now();
        return { nReduce: args.nReduce, reduceId: task.id, type: "reduce" };
      }
      if (!allDone(reduceTasks)) {
        return { type: "sleep" };
      }
      phase = "done";
    }

    return { type: "done" };
  };

  /**
   * Handle a worker's task completion report
   *
   * @param req - {@link ReportRequest} with task kind, id, and success flag
   */
  const onReport = (req: ReportRequest): void => {
    if (req.type === "map") {
      const task = mapTasks[req.mapId];
      if (task) {
        if (req.success) {
          task.state = "done";
        } else {
          task.state = "idle";
        }
        clearAssignee(task);
        if (allDone(mapTasks)) {
          phase = "reduce";
        }
      }
      return;
    }

    const task = reduceTasks[req.reduceId];
    if (task) {
      if (req.success) {
        task.state = "done";
      } else {
        task.state = "idle";
      }
      clearAssignee(task);
      if (allDone(reduceTasks)) {
        phase = "done";
      }
    }
  };

  // HTTP JSON server with two endpoints (/pollTask, /reportTask)
  const server = http.createServer(async (req, res) => {
    try {
      if (req.method === "POST" && req.url === "/pollTask") {
        const body = await readJson(req);
        if (!isPollRequest(body)) {
          res.writeHead(HTTP_STATUS.BAD_REQUEST).end('{"error":"bad request"}');
          return;
        }
        const reply = onPoll(body);
        res.writeHead(HTTP_STATUS.OK, { "Content-Type": "application/json" }).end(JSON.stringify(reply));
        return;
      }

      if (req.method === "POST" && req.url === "/reportTask") {
        const body = await readJson(req);
        if (!isReportRequest(body)) {
          res.writeHead(HTTP_STATUS.BAD_REQUEST).end('{"error":"bad request"}');
          return;
        }
        onReport(body);
        res.writeHead(HTTP_STATUS.OK, { "Content-Type": "application/json" }).end('{"ok":true}');
        return;
      }

      res.writeHead(HTTP_STATUS.NOT_FOUND).end('{"error":"not found"}');
    } catch {
      res.writeHead(HTTP_STATUS.INTERNAL_ERROR).end('{"error":"server error"}');
    }
  });

  return server;
};

const main = async (): Promise<void> => {
  const args = await parseArgs(process.argv.slice(ARGV_USER_INDEX));
  const server = createServer(args);
  server.listen(args.port, () => {
    console.log(`coordinator :${args.port} maps=${args.inputFiles.length} reduces=${args.nReduce}`);
  });
};

void (async () => {
  try {
    await main();
  } catch (error) {
    console.error(error);
    process.exit(EXIT_ERROR);
  }
})();
