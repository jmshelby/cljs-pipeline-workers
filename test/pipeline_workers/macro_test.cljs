(ns pipeline-workers.macro-test
  (:require [cljs.test :refer [deftest is testing async]]
            [cljs.core.async :as a :refer [chan go <!]]
            [pipeline-workers.registry :as reg]
            [pipeline-workers.macro-fixture :as fx]
            [pipeline-workers.core :as pw :refer [pipeline-workers]]
            [pipeline-workers.test-support :refer [stub-pool drain]]))

(deftest defpipeline-xf-binds-handle-and-registers
  (testing "the handle is the id keyword"
    (is (keyword? fx/triple))
    (is (= "pipeline-workers.macro-fixture" (namespace fx/triple))))
  (testing "registration ran at load (WORKER=true in the :test build)"
    (is (reg/registered? fx/triple))
    (is (reg/registered? fx/adder))))

(deftest pipeline-workers-handle-path
  (async done
    (let [from (chan) to (chan 8)]
      (a/onto-chan! from [1 2 3])
      ;; fx/triple is a handle symbol -> handle path -> dispatch via the stub pool
      (pipeline-workers 2 to fx/triple from {:pool (stub-pool {})})
      (go
        (is (= #{3 6 9} (set (<! (drain to)))) "(* 3) applied across workers, unordered")
        (done)))))

(deftest pipeline-workers-handle-path-with-params
  (async done
    (let [from (chan) to (chan 8)]
      (a/onto-chan! from [10 20 30])
      (pipeline-workers 2 to fx/adder from {:pool (stub-pool {}) :params {:n 100}})
      (go
        (is (= #{110 120 130} (set (<! (drain to)))) "xf-factory receives :params in the worker")
        (done)))))
