(ns jepsen.etcd-demo
  (:require [jepsen.cli :as cli]
            [jepsen.tests :as tests]))

(defn etcd-test
  "Parses command-line options and returns a test value"
  [opts]
  (merge tests/noop-test ;; map merge
         opts))

(defn -main
  "Run jepsen test with command-line args"
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn etcd-test})
            args))
