(ns pipeline-workers.core
  "Public API.

   The transducer cannot be serialized to the worker (compiled CLJS closures don't survive
   :advanced + postMessage), so it is *compiled into the worker bundle* and referenced by a
   stable id. A macro captures the xf form and emits a build-guarded registration that the
   worker bundle keeps and the main bundle dead-code-eliminates (via the WORKER goog-define).

   Two entry points:

     (defpipeline-xf name xf-form)   ;; TOP-LEVEL. Registers the xf, binds `name` to its id.
     (pipeline-workers n to xf from [opts])

   `xf` may be a handle defined with `defpipeline-xf` (usable anywhere), or an inline xf form
   (only at top level — see the docstring). Recommended usage:

     ;; in a worker-safe namespace (no DOM at the top level):
     (defpipeline-xf heavy (comp (map expensive) (filter keep?)))

     ;; anywhere on the main thread, after (configure! {:url \"/js/worker/worker.js\"}):
     (pipeline-workers 8 out-ch heavy in-ch)

   See README for the rules: the xf must be pure (it runs in the worker's own memory — vars it
   references reflect worker-side state, not the main thread's), values crossing the boundary
   must be transit-serializable, and per-item work must dwarf serialization cost."
  ;; env + registry are required so that requiring `core` makes pipeline-workers.env/WORKER and
  ;; registry/register! analyzable at any call site that the macros expand into.
  #?(:cljs (:require [pipeline-workers.dispatch :as dispatch]
                     [pipeline-workers.pool :as pool]
                     [pipeline-workers.env]
                     [pipeline-workers.registry]))
  #?(:cljs (:require-macros [pipeline-workers.core])))

;; ---------------------------------------------------------------------------
;; Compile-time (Clojure) — the macros
;; ---------------------------------------------------------------------------
#?(:clj
   (do
     (require '[cljs.analyzer :as ana])

     (defn- current-ns []
       (str (or ana/*cljs-ns* (ns-name *ns*))))

     (defn- handle-keyword
       "Stable id for a named handle: ::current-ns/name."
       [sym]
       (keyword (current-ns) (name sym)))

     (defn- inline-keyword
       "Stable id for an inline xf form, derived from its content + ns so identical forms in
        different namespaces don't collide."
       [form]
       (keyword (current-ns) (str "xf_" (Integer/toHexString (hash [(current-ns) form])))))

     (defmacro defpipeline-xf
       "Register a transducer (or 1-arg xf-factory used with :params) for worker-side use, and
        bind `sym` to its id. MUST be used at the top level of a namespace that the worker
        bundle loads. The registration is guarded by the WORKER goog-define, so it is retained
        only in the worker bundle and DCE'd from the main bundle."
       [sym xf-form]
       (let [id (handle-keyword sym)]
         `(do
            (def ~sym ~id)
            (when (cljs.core/identical? pipeline-workers.env/WORKER true)
              (pipeline-workers.registry/register! ~id ~xf-form))
            ~id)))

     (defmacro pipeline-workers
       "Unordered, worker-parallel pipeline. See ns docstring.

        `xf` is either a handle symbol from `defpipeline-xf` (works anywhere) or an inline xf
        form (valid only at the top level — inside a fn body a registration cannot be hoisted
        to load time, so use `defpipeline-xf` and pass the handle)."
       ([n to xf from] `(pipeline-workers ~n ~to ~xf ~from {}))
       ([n to xf from opts]
        (cond
          ;; handle path — `xf` is a symbol bound to an id (works anywhere)
          (symbol? xf)
          (do
            (when (contains? &env xf)
              (throw (ex-info (str "pipeline-workers: `" xf "` is a local binding, not a "
                                   "defpipeline-xf handle. Define the xf with defpipeline-xf "
                                   "and pass its handle, or pass an inline xf form at the top level.")
                              {:xf xf})))
            `(pipeline-workers.core/pipeline-workers* ~n ~to ~xf ~from ~opts))

          ;; inline xf form — only legal at the top level (empty &env)
          :else
          (do
            (when (seq &env)
              (throw (ex-info (str "pipeline-workers: an inline xf form is only allowed at the "
                                   "top level (a registration can't be hoisted to load time from "
                                   "inside a fn). Move it to `(defpipeline-xf <name> " (pr-str xf)
                                   ")` and pass <name> here.")
                              {:xf xf :env (keys &env)})))
            (let [id (inline-keyword xf)]
              `(do
                 (when (cljs.core/identical? pipeline-workers.env/WORKER true)
                   (pipeline-workers.registry/register! ~id ~xf))
                 (pipeline-workers.core/pipeline-workers* ~n ~to ~id ~from ~opts)))))))))

;; ---------------------------------------------------------------------------
;; Runtime (ClojureScript)
;; ---------------------------------------------------------------------------
#?(:cljs
   (do
     (defonce ^:private default-pool (atom nil))

     (defn configure!
       "Create and install the default worker pool. `opts` -> pool/create:
        {:url \"/js/worker/worker.js\" :size <n>}. Call once on the main thread before using
        `pipeline-workers` without an explicit :pool."
       [opts]
       (reset! default-pool (pool/create opts)))

     (defn shutdown!
       "Terminate the default pool's workers."
       []
       (when-let [p @default-pool] (pool/shutdown! p) (reset! default-pool nil)))

     (defn pipeline-workers*
       "Runtime target of the `pipeline-workers` macro. Routes to the dispatch core using the
        :pool from opts, or the configured default pool."
       [n to xf-id from opts]
       (let [p (or (:pool opts) @default-pool)]
         (when (nil? p)
           (throw (ex-info "pipeline-workers: no pool. Call (configure! {:url ...}) first, or pass :pool in opts."
                           {})))
         (dispatch/run! p n to xf-id from opts)))))
