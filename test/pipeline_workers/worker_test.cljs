(ns pipeline-workers.worker-test
  (:require [cljs.test :refer [deftest is testing]]
            [pipeline-workers.registry :as reg]
            [pipeline-workers.worker :as w]))

(deftest single-output
  (reg/register! ::inc (map inc))
  (let [resp (w/handle-job {:type :job :job-id 1 :xf-id ::inc :input 41})]
    (is (= :result (:type resp)))
    (is (= 1 (:job-id resp)))
    (is (= [42] (:outputs resp)))))

(deftest multi-output-per-input
  (testing "a transducer may emit >1 output per input"
    (reg/register! ::dup (mapcat (fn [x] [x x])))
    (is (= [7 7] (:outputs (w/handle-job {:type :job :job-id 2 :xf-id ::dup :input 7}))))))

(deftest zero-output-per-input
  (testing "a transducer may emit 0 outputs per input"
    (reg/register! ::evens (filter even?))
    (is (= [] (:outputs (w/handle-job {:type :job :job-id 3 :xf-id ::evens :input 3}))))
    (is (= [4] (:outputs (w/handle-job {:type :job :job-id 3 :xf-id ::evens :input 4}))))))

(deftest unknown-id-is-error
  (let [resp (w/handle-job {:type :job :job-id 4 :xf-id ::missing :input 1})]
    (is (= :error (:type resp)))
    (is (= 4 (:job-id resp)))))

(deftest params-factory
  (testing "when :params present, the entry is a factory (fn [params] -> xf)"
    (reg/register! ::add (fn [{:keys [n]}] (map #(+ n %))))
    (is (= [15] (:outputs (w/handle-job {:type :job :job-id 5 :xf-id ::add :input 10 :params {:n 5}}))))))

(deftest thrown-exception-is-error
  (reg/register! ::boom (map (fn [_] (throw (ex-info "boom" {:k :v})))))
  (let [resp (w/handle-job {:type :job :job-id 6 :xf-id ::boom :input 1})]
    (is (= :error (:type resp)))
    (is (= "boom" (get-in resp [:error :message])))
    (is (= {:k :v} (get-in resp [:error :data])))))
