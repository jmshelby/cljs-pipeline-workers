# cljs-pipeline-workers

Real multi-core transducer pipelines for ClojureScript — a `core.async/pipeline`-style API
where every transducer application is guaranteed to run on a **Web Worker thread**.

> **Status:** early WIP. The worker-side core (registry + transit boundary + worker entry) is
> in place and tested; the pool, dispatch loop, and `pipeline-workers` macro are in progress.

## Why

ClojureScript's `cljs.core.async` ships `pipeline` and `pipeline-async`, but for `n > 1` they
are a *fake* — the docstring itself says *"Values of N > 1 will not result in actual
concurrency in a single-threaded runtime."* They just spawn `n` go-loops that take turns on
the one event loop, and there is no `pipeline-blocking` in CLJS at all.

`pipeline-workers` makes `n` mean something real: CPU-bound transducers run across cores via
Web Workers, with no `postMessage` plumbing exposed to the caller.

## Goal API

```clojure
(require '[pipeline-workers.core :refer [pipeline-workers]])

;; behaves like core.async/pipeline, except every xf application runs in a worker
;; and outputs are NOT guaranteed to be in input order.
(pipeline-workers 8 out-ch (comp (map heavy-fn) (filter keep?)) in-ch)
```

### How it differs from `core.async/pipeline`

- Each `xf` application runs on a **real worker thread**; `n` is real parallelism.
- **Output order is not preserved** (deliberate — removes reorder buffering / head-of-line
  blocking).
- Same per-element semantics: the transducer is applied independently to each element and may
  produce zero or more outputs per input.
- The `xf` must be **pure / closed over no runtime state** — it is compiled into the worker
  bundle, not serialized. Per-call data goes through an explicit `:params` channel instead.
- Values crossing the boundary must be **transit-serializable**; per-item work must be large
  enough to amortize the encode/copy/decode cost or the worker round-trip loses to a single
  thread.

## Design

See [`docs`](./docs) (coming) — short version: a macro captures the inline `xf`, emits a
build-guarded registration that DCE strips from the main bundle and keeps in the worker
bundle (via a `goog-define`), and rewrites the call site to dispatch by a stable id to a
reused worker pool. Works on shadow-cljs (first-class) and vanilla ClojureScript.

## Development

```bash
npm install
npx shadow-cljs compile test && node out/node-tests.js
```

## License

TBD
