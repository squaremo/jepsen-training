(ns jepsen.etcd-demo
  (:require
   [clojure.tools.logging :refer :all]
   [clojure.string :as str]
   [jepsen
    [checker :as checker]
    [control :as c]
    [client :as client]
    [generator :as gen]
    [independent :as independent]
    [cli :as cli]
    [db :as db]
    [nemesis :as nemesis]
    [tests :as tests]]
   [jepsen.checker.timeline :as timeline]
   [verschlimmbesserung.core :as v]
   [slingshot.slingshot :refer [try+]]
   [knossos.model :as model]
   [jepsen.control.util :as cu]
   [jepsen.os.debian :as debian]))

;; === ETCD DB ===

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

;; === ETCD CLIENT ===

;; Client operations
(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn parse-long "Parse a maybe string into a maybe long"
  [v]
  (when v
    (Long/parseLong v)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (client-url node) {:timeout 5000})))

  (setup! [this test])

  (invoke! [this test op] ;; -> op
    (let [[key val] (:value op)]
      (try+
       (case (:f op)
         :cas (let [[v v'] val] ;; input val is [before after]
                (if (v/cas! conn key v v' {:prev-exist? true})
                  (assoc op :type :ok)
                  (assoc op :type :fail)))
         :read (let [result (parse-long (v/get conn key {:quorum? (:quorum test)}))]
                 (assoc op :type :ok :value (independent/tuple key result)))
       :write (do (v/reset! conn key val) (assoc op :type :ok)))
    (catch [:errorCode 100] _
      (assoc op :type :fail, :error :not-found))
    (catch java.net.SocketTimeoutException _
      (assoc op :type (if (= (:f op) :read) :fail :info) :error :timeout)))))

  (teardown! [this test])

  (close! [_ test])) ;; etcd client does not need closing

;; === test runner ===

(def cli-opts [["-q" "--quorum BOOL" "Use quorum reads (or not)"]
               ["-r" "--rate Hz" "Rate at which to attempt operations"]])

(defn etcd-test
  "Parses command-line options and returns a test value"
  [opts]
  (let [quorum (boolean (:quorum opts))
        rate (or (:rate opts) 10)]
    (merge tests/noop-test ;; map merge
           opts
           ;; CLI args
           {:quorum quorum :rate rate}
           ;; Test itself
           {:name (str "etcd quorum=" quorum)
            :os debian/os
            :client (Client. nil)
            :nemesis (nemesis/partition-random-halves) ;; partition when :f :start, end when :f :stop
            :model (model/cas-register)
            :checker (checker/compose
                      {:per-key (independent/checker
                                 (checker/compose
                                  {:linear (checker/linearizable)
                                   :timeline (timeline/html)}))
                       :perf (checker/perf)})
            :generator (->> (independent/concurrent-generator
                             10 (map #(str "key" %) (range))
                             (fn [_k] ;; not used; the machinery lifts the values given to [k v]
                               (->> (gen/mix [r w cas])
                                    (gen/stagger (/ rate)) ;; do about 10 ops a second
                                    (gen/limit 100))))
                            (gen/nemesis
                             (gen/seq (cycle ;; repeat forever
                                       [(gen/sleep 5)
                                        {:type :info, :f :start}
                                        (gen/sleep 5)
                                        {:type :info, :f :stop}])))
                            (gen/time-limit (:time-limit opts)))
            :db (db "v3.1.5")})))

(defn -main
  "Run jepsen test with command-line args"
  [& args]
  (cli/run!
   (merge
    (cli/single-test-cmd {:test-fn etcd-test, :opt-spec cli-opts})
    (cli/serve-cmd))
   args))
