(ns pipeline-workers.transit-test
  (:require [cljs.test :refer [deftest is testing]]
            [cognitect.transit :as t]
            [pipeline-workers.transit :as transit]))

(deftest roundtrip-builtins
  (testing "structured CLJS data survives transit round-trip"
    (let [x {:a 1 :b [1 2 3] :c #{:x :y} :d "hi" :k :a-keyword :n 3.14 :nested {:m [{:p 1}]}}]
      (is (= x (transit/decode (transit/encode x)))))))

(defrecord Point [x y])

(deftest roundtrip-record-with-handlers
  (testing "records survive when handlers are registered on both sides"
    (transit/add-handlers!
     {Point (t/write-handler (constantly "point") (fn [p] [(:x p) (:y p)]))}
     {"point" (fn [[x y]] (->Point x y))})
    (let [p (->Point 3 4)
          decoded (transit/decode (transit/encode p))]
      (is (= p decoded))
      (is (instance? Point decoded)))))
