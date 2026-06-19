(ns pipeline-workers.dispatch
  "The unordered, worker-parallel analogue of core.async/pipeline.

   Structurally this is an *unordered* `pipeline-async` whose async-fn routes each input to a
   worker pool. Because output order is not preserved, no per-input reorder buffer is needed:
   a single go-loop keeps <= n jobs in flight, pulls from `from` only while under capacity AND
   `to` is writable (preserving backpressure so it composes with other channels), and emits the
   0..n outputs of each completed job onto `to` as they arrive."
  (:refer-clojure :exclude [run!])
  (:require [cljs.core.async :as async :refer [chan close! go go-loop <! >! alts!]]
            [pipeline-workers.pool :as pool]))

(defn default-ex-handler
  "Logs and drops. Mirrors the spirit of core.async's ex-handler but never tears down the
   pipeline (a worker error shouldn't kill an in-flight stream). Return non-nil from a custom
   handler to inject a value onto `to` in place of the failed input."
  [e]
  (js/console.error "pipeline-workers: worker job failed:" (ex-message e) (clj->js (ex-data e)))
  nil)

(defn run!
  "Drive an unordered worker-parallel pipeline.

   p         - an IPool
   n         - max jobs in flight (real parallelism)
   to        - output channel
   xf-id     - registered transducer id to apply in the worker
   from      - input channel
   opts      - {:close? true        close `to` when `from` drains (default true)
                :ex-handler fn       called with an ex-info on worker/job error
                :params m}           per-call params handed to an xf-factory in the worker

   Returns nil; the pipeline runs on its own go-loop."
  [p n to xf-id from {:keys [close? ex-handler params]
                      :or   {close? true ex-handler default-ex-handler}}]
  (let [n    (max 1 n)
        done (chan n)]
    (letfn [(dispatch! [v]
              ;; submit, then forward the single response to the shared `done` channel
              (go (>! done (<! (pool/submit p xf-id v params)))))
            (deliver!
              ;; push a completed job's outputs to `to`, parking on `>! to` to preserve
              ;; backpressure. Returns a channel that closes when delivery is complete.
              [resp]
              (go
                (case (:type resp)
                  :result (doseq [o (:outputs resp)] (>! to o))
                  :error  (let [{:keys [message data]} (:error resp)
                                v (ex-handler (ex-info (or message "pipeline-workers worker error")
                                                       (or data {})))]
                            (when (some? v) (>! to v))))))]
      (go-loop [inflight 0 from-open? true]
        (cond
          ;; room for more work and source still open: race new input against completions
          (and from-open? (< inflight n))
          (let [[v ch] (alts! [from done])]
            (cond
              (= ch done)        (do (<! (deliver! v)) (recur (dec inflight) from-open?))
              (nil? v)           (recur inflight false)            ;; from closed
              :else              (do (dispatch! v) (recur (inc inflight) true))))

          ;; at capacity, or source drained with work still in flight: await completions only
          (pos? inflight)
          (let [resp (<! done)]
            (<! (deliver! resp))
            (recur (dec inflight) from-open?))

          ;; source drained and nothing in flight: finished
          :else
          (when close? (close! to))))
      nil)))
