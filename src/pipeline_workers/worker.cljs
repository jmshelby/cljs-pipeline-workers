(ns pipeline-workers.worker
  "Worker-side entry point. Listens for transit-encoded job envelopes, applies the registered
   transducer to the single input element (per-element, 0..n outputs — matching core.async
   pipeline semantics), and posts a transit-encoded response back.

   Message protocol (transit-encoded strings both ways):
     main -> worker : {:type :job  :job-id n :xf-id kw :input v [:params m]}
     worker -> main : {:type :result :job-id n :outputs [..]}
                      {:type :error  :job-id n :error {:message s :data m}}
                      {:type :ready}                ;; posted once on init"
  (:require [pipeline-workers.registry :as reg]
            [pipeline-workers.transit :as transit]))

(defn handle-job
  "Pure: decoded job map -> response map. No worker globals, so unit-testable under node."
  [{:keys [job-id xf-id input] :as job}]
  (let [entry (reg/lookup xf-id)]
    (if (nil? entry)
      {:type  :error
       :job-id job-id
       :error  {:message (str "pipeline-workers: no transducer registered for " (pr-str xf-id))
                :data    {:xf-id xf-id :known (vec (reg/ids))}}}
      (try
        (let [xf      (if (contains? job :params) (entry (:params job)) entry)
              outputs (into [] xf [input])]
          {:type :result :job-id job-id :outputs outputs})
        (catch :default e
          {:type   :error
           :job-id job-id
           :error  {:message (or (some-> e .-message) (str e))
                    :data    (ex-data e)}})))))

(defn- post! [resp]
  (.postMessage js/self (transit/encode resp)))

(defn- on-message [ev]
  (let [msg (transit/decode (.-data ev))]
    (when (= :job (:type msg))
      (post! (handle-job msg)))))

(defn init
  "Worker bundle :init-fn. Wires the message listener and signals readiness."
  []
  (.addEventListener js/self "message" on-message)
  (post! {:type :ready}))
