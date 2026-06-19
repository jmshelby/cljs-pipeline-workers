(ns pipeline-workers.env)

;; Build-time flag distinguishing the worker bundle from the main bundle.
;;
;; Set per BUILD via :closure-defines {pipeline-workers.env/WORKER true} in the worker
;; build, false (the default) in the main build. The macro emits worker-only registrations
;; guarded by `(identical? WORKER true)`; under :advanced that guard is constant-folded and
;; the worker-only code (plus the heavy xf code it references) is dead-code-eliminated from
;; the main bundle entirely.
(goog-define WORKER false)

(defn worker?
  "True when running in (or compiling) the worker bundle."
  []
  (identical? WORKER true))
