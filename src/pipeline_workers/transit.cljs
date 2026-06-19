(ns pipeline-workers.transit
  "Serialization across the worker boundary.

   postMessage uses structured clone, which strips prototypes — CLJS persistent structures
   (maps/vectors/keywords/records) do NOT survive it. So every value crossing the boundary is
   transit-encoded to a string (which structured-clones losslessly) and decoded on the far
   side. Custom record handlers must be registered identically on BOTH sides via
   `add-handlers!` before any job runs."
  (:require [cognitect.transit :as t]))

(defonce ^:private write-handlers (atom {}))
(defonce ^:private read-handlers (atom {}))
(defonce ^:private cached (atom nil))

(defn- rebuild []
  (reset! cached
          {:w (t/writer :json {:handlers @write-handlers})
           :r (t/reader :json {:handlers @read-handlers})}))

(defn- io []
  (or @cached (rebuild)))

(defn add-handlers!
  "Merge transit handlers. `write-map` is {RecordType (t/write-handler tag-fn rep-fn)};
   `read-map` is {\"tag\" (fn [rep] -> value)}. Call with identical args on main + worker."
  [write-map read-map]
  (swap! write-handlers merge write-map)
  (swap! read-handlers merge read-map)
  (rebuild)
  nil)

(defn encode
  "CLJS value -> transit JSON string."
  [x]
  (t/write (:w (io)) x))

(defn decode
  "transit JSON string -> CLJS value."
  [s]
  (t/read (:r (io)) s))
