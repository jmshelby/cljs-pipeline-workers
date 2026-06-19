(ns pipeline-workers.registry-test
  (:require [cljs.test :refer [deftest is testing]]
            [pipeline-workers.registry :as reg]))

(deftest register-and-lookup
  (testing "register! stores and returns the id"
    (is (= ::inc (reg/register! ::inc (map inc)))))
  (testing "lookup / registered?"
    (is (reg/registered? ::inc))
    (is (some? (reg/lookup ::inc)))
    (is (not (reg/registered? ::nope)))
    (is (nil? (reg/lookup ::nope))))
  (testing "ids includes registered keys"
    (is (contains? (set (reg/ids)) ::inc))))
