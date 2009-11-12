(ns mudphone.hbase-runner.hbase-repl
  (:import [java.io File])
  (:import [org.apache.hadoop.hbase HBaseConfiguration HConstants])
  (:import [org.apache.hadoop.hbase.client HBaseAdmin HTable])
  (:use mudphone.hbase-runner.config.hbase-runner)
  (:use mudphone.hbase-runner.utils.clojure)
  (:use mudphone.hbase-runner.utils.file)
  (:use mudphone.hbase-runner.utils.find)
  (:use mudphone.hbase-runner.utils.create)
  (:use mudphone.hbase-runner.utils.truncate)
  (:use clojure.contrib.pprint))

(defn set-current-table-ns [current-ns]
  (dosync
   (alter *hbase-runner-config* assoc :current-table-ns current-ns)))
(defn current-table-ns []
  (let [current-ns (hbr*current-table-ns)]
    (if-not (nil? current-ns)
      current-ns
      (hbr*default-table-ns))))

(defn- read-conn-config []
  (let [config-file (str (hbr*config-dir) "/connections.clj")]
    (try
     (load-file config-file)
     (catch java.io.FileNotFoundException e
       (println "Error loading system config.")
       (println "You may need to copy template file in same directory to:" config-file)
       (System/exit 1)))))

(defn- hbase-configuration
  ([]
     (hbase-configuration :default))
  ([system]
     (let [user-configs (read-conn-config)
           system-config (system user-configs)]
       (if-not system-config
         (do
           (println "Warning!!!:" system "config does not exist.  Please fix config and retry.")
           (throw (Exception. "No matching system config.")))
         (let [merged-config (merge (:default user-configs) (system user-configs))
               hbase-config (HBaseConfiguration.)]
           (doto hbase-config
             (.setInt "hbase.client.retries.number"    (:hbase.client.retries.number merged-config))
             (.setInt "ipc.client.connect.max.retires" (:ipc.client.connect.max.retries merged-config))
             (.set "hbase.master"                      (:hbase.master merged-config))
             (.set "hbase.zookeeper.quorum"            (:hbase.zookeeper.quorum merged-config))
             (.setBoolean "hbase.cluster.distributed"  (:hbase.cluster.distributed merged-config))
             ))))))

(declare *HBaseConfiguration*)
(defn- hbase-admin []
  (HBaseAdmin. *HBaseConfiguration*))

(defn print-current-settings []
  (println "HBase Runner Home is:" (hbr*hbase-runner-home))
  (println "System is:" (name (keyword (hbr*system))))
  (println "Current table ns is:" (current-table-ns)))

(defn start-hbase-repl
  ([]
     (start-hbase-repl :default))
  ([system-or-table-ns]
     (cond
       (keyword? system-or-table-ns) (let [system system-or-table-ns]
                                       (start-hbase-repl system (current-table-ns)))
       (string? system-or-table-ns) (let [table-ns system-or-table-ns]
                                      (start-hbase-repl :default system-or-table-ns))
       :else (do
               (println "You must provide a system name (as :keyword) or table namespace (as \"string\"), or both.")
               (println "  System default is :default")
               (println "  Table namespace default is blank"))))
  ([system table-ns]
     (set-current-table-ns table-ns)
     (def *HBaseConfiguration* (hbase-configuration system))
     (def *HBaseAdmin* (hbase-admin))
     (dosync
      (alter *hbase-runner-config* assoc :system system))
     (print-current-settings)))

(defn list-all-tables []
  (let [htable-descriptors (.listTables *HBaseAdmin*)]
    (map table-name-from htable-descriptors)))

(defn list-tables []
  (filter (partial is-in-table-ns (current-table-ns)) (list-all-tables)))

(defn find-all-tables [search-str]
  (filter-names-by search-str (list-all-tables)))

(defn find-tables [search-str]
  (filter-names-by search-str (list-tables)))

(defn table-enabled? [table-name]
  (.isTableEnabled *HBaseAdmin* table-name))

(defn table-disabled? [table-name]
  (.isTableDisabled *HBaseAdmin* (.getBytes table-name)))

(defn flush-table-or-region [table-name-or-region-name]
  (.flush *HBaseAdmin* table-name-or-region-name))
(defn flush-table [table-name]
  (flush-table-or-region table-name))

(defn major-compact [table-name-or-region-name]
  (.majorCompact *HBaseAdmin* table-name-or-region-name))

(defn drop-table [table-name]
  (if (table-enabled? table-name)
    (println "Table" table-name "is enabled.  Disable it first.")
    (do
      (println "Deleting table" table-name "...")
      (.deleteTable *HBaseAdmin* table-name)
      (flush-table HConstants/META_TABLE_NAME)
      (major-compact HConstants/META_TABLE_NAME))))

(defn disable-table [table-name]
  (println "Disabling table" table-name "...")
  (.disableTable *HBaseAdmin* table-name))

(defn enable-table [table-name]
  (println "Enabling table" table-name "...")
  (.enableTable *HBaseAdmin* table-name))

(defn create-table-from [descriptor]
  (.createTable *HBaseAdmin* descriptor))

(def-timed-fn truncate-table [table-name]
  (println "Truncating table" table-name "...")
  (let [descriptor (.getTableDescriptor (HTable. table-name))
        result {:name table-name :descriptor descriptor}]
    (try
     (disable-table table-name)
     (drop-table table-name)
     (println "Recreating table" table-name "...")
     (create-table-from descriptor)
     (assoc result :status :truncated)
     (catch Exception e
       (.printStackTrace e)
       (assoc result :status :error)))))

(defn truncate-tables [table-name-list]
  (println "Truncating" (count table-name-list) "tables ...")
  (let [result (doall (pmap truncate-table table-name-list))]
    (display-truncation-for result)
    {
     :errors (filter-errors result)
     :truncated (filter-truncated result)
     :all result
    }))

(defn dump-tables
  ([table-names]
     (dump-tables table-names "tables.clj"))
  ([table-names output-file-name]
     (let [file-path (str (hbr*output-dir) "/" output-file-name)]
       (if (.exists (File. file-path))
         (println "Output file already exists:" file-path)
         (spit-table-maps *HBaseConfiguration* table-names file-path)))))

(defn hydrate-table-maps-from [file-name]
  (let [file (str (hbr*output-dir) "/" file-name)
        table-maps (read-clojure-lines-from file)]
    (hydrate-table-maps *HBaseAdmin* table-maps)))

(defn table-exists? [table-name]
  (not (nil? (some #(= table-name %) (list-all-tables)))))

(defn rows-in [table-name]
  (let [htable (HTable. *HBaseConfiguration* table-name)
        start-end-byte-arrays (.getStartEndKeys htable)
        start-keys (map #(String. %) (.getFirst start-end-byte-arrays))
        end-keys (map #(String. %) (.getSecond start-end-byte-arrays))]
    (println "first start-key" (first start-keys))
    (println "first end-key" (first end-keys))))