(ns example.transforms
  "Worker-safe namespace (no DOM at the top level) holding the CPU-bound transducers.
   Loaded by BOTH bundles: in the worker bundle the registration runs; in the main bundle the
   WORKER guard is false so the registration — and `count-primes-below` with it — is DCE'd."
  (:require [pipeline-workers.core :refer [defpipeline-xf]]))

(defn- prime? [n]
  (cond
    (< n 2)   false
    (= n 2)   true
    (even? n) false
    :else     (loop [i 3]
                (cond
                  (> (* i i) n) true
                  (zero? (rem n i)) false
                  :else (recur (+ i 2))))))

(defn count-primes-below
  "CPU-bound: number of primes < n. Heavy enough to amortize the transit round-trip.
   NOTE: the demo also calls this directly on the main thread for its single-threaded
   baseline, so it legitimately appears in both bundles."
  [n]
  (count (filter prime? (range 2 n))))

(defpipeline-xf count-primes (map count-primes-below))

;; Build-correctness probe: this fn is reachable ONLY through the worker registration below
;; (the main thread never calls it — pipeline-workers passes the handle keyword, not the fn).
;; So the marker must be ABSENT from the :advanced main bundle (DCE'd) and PRESENT in the
;; worker bundle.
(defn- worker-only-marker [x]
  (js/console.log "PWX_WORKER_ONLY_7B2C")
  x)

(defpipeline-xf worker-only-xf (map worker-only-marker))
