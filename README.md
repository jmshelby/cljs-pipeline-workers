# cljs-pipeline-workers

Real multi-core transducer pipelines for ClojureScript — a `core.async/pipeline`-style API
where every transducer application is guaranteed to run on a **Web Worker thread**.

> **Status:** working end-to-end. Worker-side core, pool, unordered dispatch, and the macros
> are implemented and tested (18 unit tests; DCE build-correctness verified; a real-Chrome
> integration check shows ~4.5× speedup on a CPU-bound workload with identical results).

## Why

ClojureScript's `cljs.core.async` ships `pipeline` and `pipeline-async`, but for `n > 1` they
are a *fake* — the docstring itself says *"Values of N > 1 will not result in actual
concurrency in a single-threaded runtime."* They just spawn `n` go-loops that take turns on
the one event loop, and there is no `pipeline-blocking` in CLJS at all.

`pipeline-workers` makes `n` mean something real: CPU-bound transducers run across cores via
Web Workers, with no `postMessage` plumbing exposed to the caller.

## Usage

```clojure
;; 1. Define the transducer in a WORKER-SAFE namespace (no DOM at the top level).
(ns app.transforms
  (:require [pipeline-workers.core :refer [defpipeline-xf]]))

(defpipeline-xf heavy (comp (map expensive-fn) (filter keep?)))


;; 2. On the main thread, configure the pool once, then use the handle anywhere.
(ns app.core
  (:require [cljs.core.async :as a]
            [pipeline-workers.core :as pw :refer [pipeline-workers]]
            [app.transforms :as t]))

(pw/configure! {:url "/js/worker/worker.js"})        ;; size defaults to hardwareConcurrency

(let [in  (a/chan 1024)
      out (a/chan 1024)]
  (a/onto-chan! in big-seq)
  (pipeline-workers 8 out t/heavy in))               ;; every `heavy` application runs on a worker
```

`t/heavy` is a **handle** (a keyword id) bound by `defpipeline-xf`; passing it to
`pipeline-workers` works anywhere. An inline xf form — `(pipeline-workers 8 out (map f) in)` —
is also accepted, but only at the **top level**: inside a fn body a load-time registration
can't be hoisted, so you'll get a compile error telling you to use `defpipeline-xf`.

### How it differs from `core.async/pipeline`

- Each `xf` application runs on a **real worker thread**; `n` is real parallelism.
- **Output order is not preserved** (deliberate — removes reorder buffering / head-of-line
  blocking).
- Same per-element semantics: the transducer is applied independently to each element and may
  produce zero or more outputs per input.
- The `xf` must be **pure**. It is compiled into the worker bundle, not serialized, and runs in
  the worker's *own memory* — vars it references reflect worker-side state, not the main
  thread's. Per-call data goes through `:params` instead (the registered value is then a
  `(fn [params] -> transducer)` factory):
  ```clojure
  (defpipeline-xf scaled (fn [{:keys [k]}] (map #(* k %))))
  (pipeline-workers 8 out scaled in {:params {:k 10}})
  ```
- Values crossing the boundary must be **transit-serializable**; per-item work must be large
  enough to amortize the encode/copy/decode cost or the worker round-trip loses to a single
  thread. For records, register transit handlers identically on both sides via
  `pipeline-workers.transit/add-handlers!`.

### Options

`(pipeline-workers n to xf from opts)` — `opts`:
- `:close?` (default `true`) — close `to` when `from` drains.
- `:ex-handler` — called with an `ex-info` on worker/job error; return non-nil to inject a
  value onto `to`. Default logs and drops (a worker error never tears down the pipeline).
- `:params` — per-call data handed to an xf-factory in the worker.
- `:pool` — use a specific pool instead of the configured default.

## How it works (the dual-bundle trick)

A compiled CLJS closure can't cross `postMessage` (advanced compilation munges names, DCE drops
unreferenced vars, closures capture a non-serializable env). So the xf is compiled into the
**worker bundle** and referenced by a stable id:

- `defpipeline-xf` emits a registration guarded by a `goog-define`,
  `(when (identical? pipeline-workers.env/WORKER true) (register! id xf))`.
- The **worker build** sets `WORKER=true` → the registration runs on load.
- The **main build** sets `WORKER=false` → under `:advanced` the guard constant-folds and the
  registration *plus the heavy xf code it references* is dead-code-eliminated from the main
  bundle. (Verified: a worker-only marker string is absent from the main bundle, present in
  the worker bundle.)

Because `:closure-defines` are per-build, the worker is its own build — *same source, two
builds*. No codegen, no build hooks, no analyzer passes; works on shadow-cljs and vanilla cljs.

### shadow-cljs

```clojure
:builds
{:main   {:target :browser :output-dir "public/js/main"
          :modules {:main {:init-fn app.core/init}}
          :closure-defines {pipeline-workers.env/WORKER false}}
 :worker {:target :browser :output-dir "public/js/worker"
          :modules {:worker {:init-fn app.worker-init/init :web-worker true}}
          :closure-defines {pipeline-workers.env/WORKER true}}}
```

The worker entry just loads the registrations and starts the message loop:

```clojure
(ns app.worker-init
  (:require [pipeline-workers.worker :as worker]
            app.transforms))               ;; required so its registrations load in the worker
(defn init [] (worker/init))
```

### Vanilla ClojureScript

Same idea, two `cljs.build.api/build` calls (main + worker), each with its own
`:closure-defines {pipeline-workers.env/WORKER true|false}` and `:optimizations :advanced`.
The worker build's entry is `app.worker-init/init`. You lose shadow's `:web-worker` bootstrap
(emit a tiny loader yourself) and its `:shared` module (the worker bundle is larger), but the
core mechanism is identical.

## Development

```bash
npm install

# unit tests (node)
npx shadow-cljs compile test && node out/node-tests.js

# example: build both advanced bundles, then drive it in real Chrome
npx shadow-cljs release example-main example-worker
node scripts/browser-check.mjs            # asserts matching results + speedup

# or open it yourself
npx shadow-cljs watch example-main example-worker   # then serve public/ and open index.html
```

## License

MIT © 2026 Jacob Shelby. See [LICENSE](./LICENSE).
