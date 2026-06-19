(ns pipeline-workers.test-support
  "Shared test helpers: an in-process IPool stub and a channel drainer."
  (:require [cljs.core.async :as a :refer [chan close! go <! >!]]
            [pipeline-workers.worker :as w]
            [pipeline-workers.pool :as pool]))

(defn stub-pool
  "An IPool that runs handle-job in-process after a tick. Tracks peak concurrency in `peak`
   (an atom) when supplied."
  ([] (stub-pool {}))
  ([{:keys [delay-ms peak]}]
   (let [inflight (atom 0)]
     (reify pool/IPool
       (submit [_ xf-id input params]
         (let [ch (chan 1)]
           (swap! inflight inc)
           (when peak (swap! peak max @inflight))
           (go
             (<! (a/timeout (or delay-ms 1)))
             (let [job  (cond-> {:type :job :job-id 0 :xf-id xf-id :input input}
                          (some? params) (assoc :params params))
                   resp (w/handle-job job)]
               (swap! inflight dec)
               (>! ch resp)
               (close! ch)))
           ch))
       (pool-size [_] 4)
       (shutdown! [_] nil)))))

(defn drain
  "Collect every value from `ch` into a vector, delivered on the returned channel."
  [ch]
  (let [out (chan)]
    (go (loop [acc []]
          (if-let [v (<! ch)]
            (recur (conj acc v))
            (do (>! out acc) (close! out)))))
    out))
