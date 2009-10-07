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

(defn hbase-admin []
  (HBaseAdmin. *HBaseConfiguration*))

(defn start-hbase-repl []
  (def *HBaseConfiguration* (hbase-configuration))
  (def *HBaseAdmin* (hbase-admin)))

(defn- table-name-from [htable-descriptor]
  (String. (.getName htable-descriptor)))

(defn- is-in-table-ns [table-name]
  (not (nil? (re-find (re-pattern (str "^" (current-table-ns))) table-name))))

(defn list-all-tables []
  (let [htable-descriptors (.listTables *HBaseAdmin*)]
    (map table-name-from htable-descriptors)))

(defn list-tables []
  (filter is-in-table-ns (list-all-tables)))

(defn filter-names-by [search-str names]
  (filter #(re-find (re-pattern search-str) %) names))

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

(defn- create-table-from [descriptor]
  (.createTable *HBaseAdmin* descriptor))

(defn truncate-table [table-name]
  (println "Truncating table" table-name "...")
  (try
   (let [descriptor (.getTableDescriptor (HTable. table-name))]
     (disable-table table-name)
     (drop-table table-name)
     (println "Recreating table" table-name "...")
     (create-table-from descriptor)
     {:truncated table-name})
   (catch Exception e
     {:error table-name})))

(defn truncate-tables [table-name-list]
  (println "Truncating" (count table-name-list) "tables ...")
  (pmap truncate-table table-name-list))