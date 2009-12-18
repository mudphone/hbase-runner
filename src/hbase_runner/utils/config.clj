(ns hbase-runner.utils.config
  (:import [org.apache.hadoop.hbase HBaseConfiguration])
  (:use hbase-runner.utils.file))

(def *hbase-runner-config*
     (let [hbase-runner-home (or (System/getenv "HBASE_RUNNER_HOME")
                                 (this-script-path))] 
       (ref {
             :hbase-runner-home hbase-runner-home
             :config-dir (str hbase-runner-home "/config")
             :output-dir (str hbase-runner-home "/output")
             :default-table-ns ""
             :current-table-ns nil
             :system nil
             })))

(defn fn-to-build-defn [akey]
  (list 'defn (symbol (str "hbr*" (name akey))) []
        (list akey '@*hbase-runner-config*)))

(defmacro create-config-helper-fns []
  `(do
     ~@(map fn-to-build-defn (keys @*hbase-runner-config*))))

(create-config-helper-fns)

(defn read-conn-config []
  (let [config-file (str (hbr*config-dir) "/connections.clj")]
    (try
     (load-file config-file)
     (catch java.io.FileNotFoundException e
       (println "Error loading system config.")
       (println "You may need to copy template file in same directory to:"
                config-file)
       (System/exit 1)))))

(defn hbase-config-for-system [system]
  (let [user-configs (read-conn-config)
        system-config (system user-configs)]
    (if-not system-config
      (do
        (println "Warning!!!:" system
                 "config does not exist.  Please fix config and retry.")
        (throw (Exception. "No matching system config.")))
      (let [merged-config (merge
                           (:default user-configs) system-config)
            hbase-config (HBaseConfiguration.)]
        (doto hbase-config
          (.setInt "hbase.client.retries.number"
                   (:hbase.client.retries.number merged-config))
          (.setInt "ipc.client.connect.max.retires"
                   (:ipc.client.connect.max.retries merged-config))
          (.set    "hbase.master"
                   (:hbase.master merged-config))
          (.set    "hbase.zookeeper.quorum"
                   (:hbase.zookeeper.quorum merged-config))
          )
        (if (:hbase.cluster.distribued merged-config)
          (doto hbase-config
            (.setBoolean "hbase.cluster.distributed"
                         (:hbase.cluster.distributed merged-config))
            (.set "hbase.rootdir"
                  (:hbase.rootdir merged-config)))
          )
        hbase-config))))