(ns pipeline-workers.macro-fixture
  "Top-level uses of defpipeline-xf. The :test build sets WORKER=true, so the guarded
   registrations run when this namespace loads — mirroring what happens in the worker bundle."
  (:require [pipeline-workers.core :refer [defpipeline-xf]]))

(defpipeline-xf triple (map #(* 3 %)))

(defpipeline-xf adder (fn [{:keys [n]}] (map #(+ n %))))
