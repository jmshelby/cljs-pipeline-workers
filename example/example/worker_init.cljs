(ns example.worker-init
  "Worker bundle entry. Requiring example.transforms is what loads its WORKER-guarded
   registrations into the worker's registry; then we start the message loop."
  (:require [pipeline-workers.worker :as worker]
            [example.transforms]))

(defn init []
  (worker/init))
