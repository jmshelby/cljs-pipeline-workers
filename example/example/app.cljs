(ns example.app
  "Main-thread demo: run the same CPU-bound workload single-threaded vs across a worker pool
   and compare wall-clock, while keeping the UI responsive."
  (:require [cljs.core.async :as a :refer [chan go <! >!]]
            [pipeline-workers.core :as pw :refer [pipeline-workers]]
            [example.transforms :as t]))

(def ^:private inputs (vec (repeatedly 64 #(+ 200000 (rand-int 100000)))))

(defn- now [] (.now js/performance))

(defn- log! [msg]
  (when-let [el (.getElementById js/document "out")]
    (set! (.-textContent el) (str (.-textContent el) msg "\n"))))

(defn- run-single! []
  (let [start (now)]
    (let [total (transduce (map t/count-primes-below) + 0 inputs)]
      (log! (str "single-thread: " (js/Math.round (- (now) start)) " ms  (sum=" total ")")))))

(defn- run-parallel! [n]
  (let [from (chan 1024) to (chan 1024) start (now)]
    (a/onto-chan! from inputs)
    (pipeline-workers n to t/count-primes from)
    (go
      (loop [sum 0]
        (if-let [v (<! to)]
          (recur (+ sum v))
          (log! (str "parallel (n=" n "): " (js/Math.round (- (now) start)) " ms  (sum=" sum ")")))))))

(defn init []
  (pw/configure! {:url "/js/worker/worker.js"})
  (when-let [btn (.getElementById js/document "run")]
    (.addEventListener btn "click"
                       (fn [_]
                         (log! "--- run ---")
                         (run-single!)
                         (run-parallel! (pw/pool-size-hint))))))
