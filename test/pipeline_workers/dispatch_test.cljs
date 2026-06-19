(ns pipeline-workers.dispatch-test
  (:require [cljs.test :refer [deftest is testing async]]
            [cljs.core.async :as a :refer [chan close! put! go <! >! onto-chan!]]
            [pipeline-workers.registry :as reg]
            [pipeline-workers.dispatch :as d]
            [pipeline-workers.test-support :refer [stub-pool drain]]))

(deftest all-outputs-delivered-unordered
  (async done
    (reg/register! ::inc (map inc))
    (let [from (chan 8) to (chan 8)]
      (onto-chan! from (range 100))
      (d/run! (stub-pool {}) 4 to ::inc from {})
      (go
        (let [result (<! (drain to))]
          (is (= 100 (count result)) "every input produced one output")
          (is (= (set (range 1 101)) (set result)) "set of outputs is correct (order-independent)")
          (done))))))

(deftest multi-and-zero-outputs
  (async done
    (reg/register! ::dup (mapcat (fn [x] [x x])))
    (reg/register! ::evens (filter even?))
    (let [from1 (chan) to1 (chan) from2 (chan) to2 (chan)]
      (onto-chan! from1 [1 2 3])
      (onto-chan! from2 (range 10))
      (d/run! (stub-pool {}) 3 to1 ::dup from1 {})
      (d/run! (stub-pool {}) 3 to2 ::evens from2 {})
      (go
        (is (= 6 (count (<! (drain to1)))) "mapcat doubles count")
        (is (= (set [0 2 4 6 8]) (set (<! (drain to2)))) "filter keeps only evens")
        (done)))))

(deftest backpressure-caps-in-flight
  (async done
    (reg/register! ::id (map identity))
    (let [peak (atom 0)
          from (chan 64) to (chan 64)]
      (onto-chan! from (range 50))
      (d/run! (stub-pool {:delay-ms 3 :peak peak}) 3 to ::id from {})
      (go
        (<! (drain to))
        (is (<= @peak 3) (str "in-flight never exceeded n=3 (peak " @peak ")"))
        (is (pos? @peak) "some concurrency happened")
        (done)))))

(deftest close-semantics
  (async done
    (reg/register! ::id2 (map identity))
    (testing ":close? false leaves `to` open"
      (let [from (chan) to (chan 8)]
        (onto-chan! from [1 2 3])
        (d/run! (stub-pool {}) 2 to ::id2 from {:close? false})
        (go
          (<! (a/timeout 50))
          ;; collect what's there, then prove channel is still open by putting a sentinel
          (is (true? (<! (go (a/put! to ::sentinel)))) "to still open")
          (done))))))

(deftest error-routes-to-ex-handler
  (async done
    (reg/register! ::boom (map (fn [_] (throw (ex-info "kaboom" {:bad true})))))
    (let [seen (atom [])
          from (chan) to (chan 8)
          exh  (fn [e] (swap! seen conj (ex-message e)) ::recovered)]
      (onto-chan! from [1 2 3])
      (d/run! (stub-pool {}) 2 to ::boom from {:ex-handler exh})
      (go
        (let [result (<! (drain to))]
          (is (= ["kaboom" "kaboom" "kaboom"] @seen) "ex-handler called per failed input")
          (is (= [::recovered ::recovered ::recovered] result) "handler's value injected onto `to`")
          (done))))))

(deftest params-factory-through-dispatch
  (async done
    (reg/register! ::add (fn [{:keys [n]}] (map #(+ n %))))
    (let [from (chan) to (chan 8)]
      (onto-chan! from [10 20 30])
      (d/run! (stub-pool {}) 2 to ::add from {:params {:n 5}})
      (go
        (is (= #{15 25 35} (set (<! (drain to)))))
        (done)))))
