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

(def dir "/opt/etcd")
(def binary "etcd")
(def logfile (str dir "/etcd.log"))
(def pidfile (str dir "/etcd.pid"))

(defn node-url
  [node port]
  (str "http://" node ":" port))

(defn peer-url
  [node]
  (node-url node 2380))

(defn client-url
  [node]
  (node-url node 2379))

(defn initial-cluster
  "Construct an etcd argument giving the cluster nodes"
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str node "=" (peer-url node))))
       (str/join ",")))

(defn db
  "Etcd at given version"
  [vsn]
  (reify db/DB
    (setup! [db test node]
      (info :setting-up node)
      (c/su
       (let [url (str "https://storage.googleapis.com/etcd/" vsn
                      "/etcd-" vsn "-linux-amd64.tar.gz")]
         (cu/install-archive! url dir))  ;; takes care of caching downloads, unpacking etc.
       (cu/start-daemon! ;; takes care of daemonising etc.
        {:logfile logfile
         :pidfile pidfile
         :chdir   dir}
        binary
        :--log-output                   :stderr
        :--name                         (name node)
        :--listen-peer-urls             (peer-url   node)
        :--listen-client-urls           (client-url node)
        :--advertise-client-urls        (client-url node)
        :--initial-cluster-state        :new
        :--initial-advertise-peer-urls  (peer-url node)
        :--initial-cluster              (initial-cluster test)))
      (Thread/sleep 10000))

    (teardown! [db test node]
      (info :tearing-down node)
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir))) ;; it's the only way to be sure.

    db/LogFiles
    (log-files [db test node]
      [logfile])))

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
