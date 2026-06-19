(ns pipeline-workers.registry
  "Maps a stable id -> transducer (or xf-factory). This namespace is loaded in BOTH the
   main and worker bundles, but entries are only ever populated in the worker bundle (the
   macro guards `register!` calls with the WORKER goog-define). Functions stored here are
   never serialized — they live in worker memory and are looked up by id when a job arrives.")

(defonce ^:private registry (atom {}))

(defn register!
  "Register `xf` (a transducer, or a 1-arg factory `(fn [params] -> transducer)` when the
   job supplies :params) under `id`. Returns `id`. Idempotent for a given id+value."
  [id xf]
  (swap! registry assoc id xf)
  id)

(defn lookup
  "Return the registered entry for `id`, or nil."
  [id]
  (get @registry id))

(defn registered?
  [id]
  (contains? @registry id))

(defn ids
  "All registered ids (useful for diagnostics)."
  []
  (keys @registry))
