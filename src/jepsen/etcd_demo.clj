(ns jepsen.etcd-demo
  (:require
   [clojure.tools.logging :refer :all]
   [clojure.string :as str]
   [jepsen
    [control :as c]
    [cli :as cli]
    [db :as db]
    [tests :as tests]]
   [jepsen.control.util :as cu]
   [jepsen.os.debian :as debian]))

(defn db
  "Etcd at given version"
  [vsn]
  (reify db/DB
    (setup! [db test node]
      (info :setting-up node))

    (teardown! [db test node]
      (info :tearing-down node))))

(defn etcd-test
  "Parses command-line options and returns a test value"
  [opts]
  (merge tests/noop-test ;; map merge 
         opts
         {:os debian/os
          :db (db "v3.1.5")}))

(defn -main
  "Run jepsen test with command-line args"
  [& args]
  (cli/run!
   (merge
    (cli/single-test-cmd {:test-fn etcd-test})
    (cli/serve-cmd))
   args))
