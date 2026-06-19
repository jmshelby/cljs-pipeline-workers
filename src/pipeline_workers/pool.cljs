(ns pipeline-workers.pool
  "Web Worker pool. Workers are long-lived and reused (they hold the registry); jobs are
   load-balanced across them. The dispatch core talks to the pool only through `IPool`, so it
   can be unit-tested against an in-process stub without real workers.

   Fault tolerance — a failure mode core.async/pipeline does not model: if a worker hard-fails
   (`error` event), its in-flight job is completed with an `:error` response and the worker is
   terminated and respawned, so the pool keeps serving."
  (:require [cljs.core.async :as async :refer [chan put! close! go <! >!]]
            [pipeline-workers.transit :as transit]))

(defprotocol IPool
  (submit [this xf-id input params]
    "Submit one input. Returns a channel that yields exactly one response map
     ({:type :result :outputs [..]} | {:type :error :error {..}}) then closes.")
  (pool-size [this])
  (shutdown! [this]))

(defn default-size
  "navigator.hardwareConcurrency, clamped to >= 1, falling back to 4 when unavailable."
  []
  (max 1 (or (some-> js/navigator .-hardwareConcurrency) 4)))

(defn- make-worker
  "Create a worker + an inbox channel fed by its message/error events."
  [url]
  (let [w     (js/Worker. url)
        inbox (chan 16)]
    (.addEventListener w "message"
                       (fn [ev] (put! inbox (transit/decode (.-data ev)))))
    (.addEventListener w "messageerror"
                       (fn [_] (put! inbox {:type :worker-error :message "messageerror (un-clonable payload)"})))
    (.addEventListener w "error"
                       (fn [e] (put! inbox {:type :worker-error :message (or (.-message e) "worker error")})))
    {:w w :inbox inbox}))

(defn- encode-job [{:keys [job-id xf-id input params]}]
  (transit/encode (cond-> {:type :job :job-id job-id :xf-id xf-id :input input}
                    (some? params) (assoc :params params))))

(defn- run-worker
  "Owns one worker slot: await readiness, then loop pulling jobs off the shared `jobs` channel,
   round-tripping each through the worker. On worker failure, fail the current job and respawn."
  [url jobs live]
  (go
    (loop [wk (make-worker url)]
      (swap! live assoc :w (:w wk))
      ;; wait for the :ready handshake (ignore anything else)
      (loop []
        (let [m (<! (:inbox wk))]
          (when (and (some? m) (not= :ready (:type m)) (not= :worker-error (:type m)))
            (recur))))
      (let [outcome
            (loop []
              (if-let [{:keys [job-id reply] :as job} (<! jobs)]
                (do
                  (.postMessage (:w wk) (encode-job job))
                  (let [resp (<! (:inbox wk))]
                    (if (= :worker-error (:type resp))
                      (do (put! reply {:type   :error
                                       :job-id job-id
                                       :error  {:message (:message resp) :data {:worker-died true}}})
                          (close! reply)
                          (try (.terminate (:w wk)) (catch :default _ nil))
                          :respawn)
                      (do (put! reply resp) (close! reply) (recur)))))
                :shutdown))]
        (when (= :respawn outcome)
          (recur (make-worker url)))))))

(defrecord WorkerPool [jobs size job-counter live-slots]
  IPool
  (submit [_ xf-id input params]
    (let [reply (chan 1)]
      (put! jobs {:job-id (swap! job-counter inc)
                  :xf-id  xf-id
                  :input  input
                  :params params
                  :reply  reply})
      reply))
  (pool-size [_] size)
  (shutdown! [_]
    (close! jobs)
    (doseq [slot live-slots]
      (when-let [w (:w @slot)]
        (try (.terminate w) (catch :default _ nil))))))

(defn create
  "Create a worker pool. `url` is the compiled worker bundle URL. `size` defaults to
   `default-size`. Workers spin up lazily-ish (constructed now, but block on `jobs`)."
  [{:keys [url size] :or {size (default-size)}}]
  (let [jobs  (chan 1024)
        slots (vec (repeatedly size #(atom {})))]
    (doseq [slot slots]
      (run-worker url jobs slot))
    (->WorkerPool jobs size (atom 0) slots)))
