(ns mudphone.hbase-runner.repl-cheat
  (:import [org.apache.hadoop.hbase HBaseConfiguration HConstants HTableDescriptor])
  (:import [org.apache.hadoop.hbase.client HBaseAdmin HTable]))

(def *default-table-ns* "koba_development")
(def *current-table-ns* (atom ""))

(defn set-current-table-ns [current-ns]
  (reset! *current-table-ns* current-ns))
(defn current-table-ns []
  (let [current-ns @*current-table-ns*]
    (if-not (empty? current-ns)
      current-ns
      *default-table-ns*)))

(declare *HBaseAdmin* *HBaseConfiguration*)

(defn hbase-configuration []
  (HBaseConfiguration.))
;;(def *HBaseConfiguration* (hbase-configuration))

(defn hbase-admin []
  (HBaseAdmin. *HBaseConfiguration*))
;;(def *HBaseAdmin* (hbase-admin))

(defn start-hbase-shell []
  (def *HBaseConfiguration* (hbase-configuration))
  (def *HBaseAdmin* (hbase-admin)))

(defn- table-name-from [htable-descriptor]
  (String. (.getName htable-descriptor)))

(defn- is-in-table-ns [table-name]
  (not (nil? (re-find (re-pattern (str "^" *current-table-ns*)) table-name))))

(defn list-all-tables []
  (let [htable-descriptors (.listTables *HBaseAdmin*)]
    (map table-name-from htable-descriptors)))

(defn list-tables []
  (let [tables (list-all-tables)
        ns-tables (filter is-in-table-ns tables)]
    (remove nil? ns-tables)))

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

(defn- create-table-from [descriptor]
  (.createTable *HBaseAdmin* descriptor))

(defn truncate-table [table-name]
  (let [descriptor (.getTableDescriptor (HTable. table-name))]
    (disable-table table-name)
    (drop-table table-name)
    (println "Recreating table" table-name "...")
    (create-table-from descriptor)))
