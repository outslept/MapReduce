# TypeScript MapReduce

Minimal, strictly typed MapReduce for local exploration. One HTTP/JSON coordinator, many workers, plugin-defined map/reduce, file-based intermediates.

## Quickstart

Create a few inputs (PowerShell):

```powershell
mkdir data -ea 0 | Out-Null
Set-Content data\pg-1.txt 'all your base are belong to us'
Set-Content data\pg-2.txt 'base base base all of them'
Set-Content data\pg-3.txt 'to be or not to be base'
```

Run the coordinator (Bun):

```sh
bun run src/coordinator.mts --port=8787 --nReduce=4 "data/pg-*.txt"
# expected: "coordinator :8787 maps=3 reduces=4"
```

Start a few workers (Bun; separate terminals):

```sh
bun run src/worker.mts --coord=http://127.0.0.1:8787 --plugin=./plugins/wc.mts
```

Inspect results:

```powershell
Get-ChildItem .\mr-out-*
Get-Content .\mr-out-* | Sort-Object | Select-Object -First 20
```

## CLI
```sh
bun run src/coordinator.mts --port=8787 --nReduce=4 "data/pg-*.txt"

bun run src/worker.mts --coord=http://127.0.0.1:8787 --plugin=./plugins/wc.mts
```

### Commands and flags

Coordinator (positional args are input files/patterns):
- --port — HTTP port to listen on (default: 8787)
- --nReduce — Number of reduce partitions (default: 4)
- inputs... — File paths or glob patterns (required)

Worker:
- --coord — Coordinator base URL (default: http://127.0.0.1:8787)
- --plugin — Filesystem path to plugin module (ESM) (default: ./plugins/wc.mts)

## Architecture

```mermaid
flowchart LR
  subgraph Workers
    W1[Worker 1]:::w
    W2[Worker 2]:::w
    Wn[Worker n]:::w
  end

  C[Coordinator]:::c
  FS[(Shared FS)]:::fs

  W1 -- POST /pollTask --> C
  W2 -- POST /pollTask --> C
  Wn -- POST /pollTask --> C

  C -- map: {file,mapId,nReduce} --> W1
  C -- reduce: {reduceId,nReduce} --> W2

  W1 -- mr-map-reduce JSONL --> FS
  W2 -- reads mr-*-reduceId-*.jsonl --> FS
  W2 -- mr-out-reduceId --> FS

  W1 -- POST /reportTask --> C
  W2 -- POST /reportTask --> C

  classDef c fill:#2b90d9,color:#fff
  classDef w fill:#42b983,color:#fff
  classDef fs fill:#ddd,color:#333
```
