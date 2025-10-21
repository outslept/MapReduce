import http from "node:http";
import { promises as fsp } from "node:fs";
import { glob } from "tinyglobby";
import { isPollRequest, isReportRequest, type PollRequest, type PollResponse, type ReportRequest } from "./protocol.mjs";

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

/**
 * Expand CLI args:
 *  --port=<number> --nReduce=<number> <patterns...>
 * patterns can be files or glob patterns (e.g., "data/pg-*.txt")
 */
async function parseArgs(argv: readonly string[]): Promise<Args> {
  const port = Number((argv.find(a => a.startsWith("--port=")) ?? "--port=8787").split("=")[1]);
  const nReduce = Number((argv.find(a => a.startsWith("--nReduce=")) ?? "--nReduce=4").split("=")[1]);
  const raw = argv.filter(a => !a.startsWith("--"));

  const patterns = raw.map(p => p.replace(/\\/g, "/"));

  let matched = patterns.length > 0 ? await glob(patterns) : [];
  {
    const seen = new Set<string>();
    const files: string[] = [];
    for (const m of matched) {
      if (seen.has(m)) continue;
      seen.add(m);
      try {
        const st = await fsp.stat(m);
        if (st.isFile()) files.push(m);
      } catch {
        // ignore
      }
    }
    files.sort((a, b) => a.localeCompare(b));
    matched = files;
  }

  if (!Number.isInteger(port) || port <= 0) throw new Error("bad --port");
  if (!Number.isInteger(nReduce) || nReduce <= 0) throw new Error("bad --nReduce");
  if (matched.length === 0) throw new Error("no input files");

  return { port, nReduce, inputFiles: matched };
}

function now(): number { return Date.now(); }
function allDone<T extends { state: "idle" | "in-progress" | "done" }>(xs: readonly T[]): boolean {
  return xs.every(t => t.state === "done");
}

function readJson(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let buf = "";
    req.on("data", (c: Buffer) => { buf += c.toString("utf8"); });
    req.on("end", () => { try { resolve(buf ? JSON.parse(buf) : {}); } catch (e) { reject(e); } });
    req.on("error", reject);
  });
}

function createServer(args: Args): http.Server {
  const mapTasks: MapTask[] = args.inputFiles.map((file, id) => ({ id, file, state: "idle" }));
  const reduceTasks: ReduceTask[] = Array.from({ length: args.nReduce }, (_, id) => ({ id, state: "idle" }));
  let phase: Phase = "map";

  const clearAssignee = (t: { workerId?: string; startedAtMs?: number }): void => {
    delete t.workerId;
    delete t.startedAtMs;
  };

  const reapTimeouts = (): void => {
    const cutoff = now() - TIMEOUT_MS;
    for (const t of mapTasks) {
      if (t.state === "in-progress" && t.startedAtMs !== undefined && t.startedAtMs <= cutoff) {
        t.state = "idle"; clearAssignee(t);
      }
    }
    for (const t of reduceTasks) {
      if (t.state === "in-progress" && t.startedAtMs !== undefined && t.startedAtMs <= cutoff) {
        t.state = "idle"; clearAssignee(t);
      }
    }
  };

  const onPoll = (req: PollRequest): PollResponse => {
    reapTimeouts();

    if (phase === "map") {
      const t = mapTasks.find(x => x.state === "idle");
      if (t) {
        t.state = "in-progress"; t.workerId = req.workerId; t.startedAtMs = now();
        return { type: "map", mapId: t.id, file: t.file, nReduce: args.nReduce };
      }
      if (!allDone(mapTasks)) return { type: "sleep" };
      phase = "reduce";
    }

    if (phase === "reduce") {
      const t = reduceTasks.find(x => x.state === "idle");
      if (t) {
        t.state = "in-progress"; t.workerId = req.workerId; t.startedAtMs = now();
        return { type: "reduce", reduceId: t.id, nReduce: args.nReduce };
      }
      if (!allDone(reduceTasks)) return { type: "sleep" };
      phase = "done";
    }

    return { type: "done" };
  };

  const onReport = (req: ReportRequest): void => {
    if (req.type === "map") {
      const t = mapTasks[req.mapId];
      if (t) {
        t.state = req.success ? "done" : "idle";
        clearAssignee(t);
        if (allDone(mapTasks)) phase = "reduce";
      }
      return;
    }
    const t = reduceTasks[req.reduceId];
    if (t) {
      t.state = req.success ? "done" : "idle";
      clearAssignee(t);
      if (allDone(reduceTasks)) phase = "done";
    }
  };

  const server = http.createServer(async (req, res) => {
    try {
      if (req.method === "POST" && req.url === "/pollTask") {
        const body = await readJson(req);
        if (!isPollRequest(body)) { res.writeHead(400).end('{"error":"bad request"}'); return; }
        const reply = onPoll(body);
        res.writeHead(200, { "Content-Type": "application/json" }).end(JSON.stringify(reply));
        return;
      }
      if (req.method === "POST" && req.url === "/reportTask") {
        const body = await readJson(req);
        if (!isReportRequest(body)) { res.writeHead(400).end('{"error":"bad request"}'); return; }
        onReport(body);
        res.writeHead(200, { "Content-Type": "application/json" }).end('{"ok":true}');
        return;
      }
      res.writeHead(404).end('{"error":"not found"}');
    } catch {
      res.writeHead(500).end('{"error":"server error"}');
    }
  });

  return server;
}

async function main(): Promise<void> {
  const args = await parseArgs(process.argv.slice(2));
  const server = createServer(args);
  server.listen(args.port, () => {
    console.log(`coordinator :${args.port} maps=${args.inputFiles.length} reduces=${args.nReduce}`);
  });
}

main().catch(err => {
  console.error(String(err?.message ?? err));
  process.exit(1);
});