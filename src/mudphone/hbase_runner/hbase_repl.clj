(ns mudphone.hbase-runner.hbase-repl
  (:import [org.apache.hadoop.hbase HBaseConfiguration HConstants HTableDescriptor])
  (:import [org.apache.hadoop.hbase.client HBaseAdmin HTable])
  (:use mudphone.hbase-runner.utils.file)
  (:use mudphone.hbase-runner.utils.find)
  (:use mudphone.hbase-runner.utils.create)
  (:use clojure.contrib.pprint))

(def *default-table-ns* "koba_development")
(def *current-table-ns* (atom ""))

(defn set-current-table-ns [current-ns]
  (reset! *current-table-ns* current-ns))
(defn current-table-ns []
  (let [current-ns @*current-table-ns*]
    (if-not (nil? current-ns)
      current-ns
      *default-table-ns*)))

(declare *HBaseAdmin* *HBaseConfiguration*)

(defn hbase-configuration []
  (HBaseConfiguration.))

(defn hbase-admin []
  (HBaseAdmin. *HBaseConfiguration*))

(defn start-hbase-repl
  ([]
     (def *HBaseConfiguration* (hbase-configuration))
     (def *HBaseAdmin* (hbase-admin)))
  ([table-ns]
     (set-current-table-ns table-ns)
     (start-hbase-repl)))

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

(defn truncate-table [table-name]
  (println "Truncating table" table-name "...")
  (try
   (let [descriptor (.getTableDescriptor (HTable. table-name))]
     (disable-table table-name)
     (drop-table table-name)
     (println "Recreating table" table-name "...")
     (create-table-from *HBaseAdmin* descriptor)
     {:status :truncated :name table-name})
   (catch Exception e
     (.printStackTrace e)
     {:status :error :name table-name})))

(defn filter-truncated [results]
  (filter #(= :truncated (:status %)) results))

(defn filter-errors [results]
  (filter #(= :error (:status %)) results))

(defn display-truncation-for [result]
  (let [tables-truncated (filter-truncated result)
        tables-with-errors (filter-errors result)]
    (println "Total tables operated on:" (count result))
    (println "Tables truncated successfully:" (count tables-truncated))
    (pprint (map :name tables-truncated))
    (println "Tables with errors:" (count tables-with-errors))
    (pprint (map :name tables-with-errors))))

(defn truncate-tables [table-name-list]
  (println "Truncating" (count table-name-list) "tables ...")
  (let [result (doall (pmap truncate-table table-name-list))]
    (display-truncation-for result)
    {:errors (filter-errors result) :truncated (filter-truncated result) :all result}))

(defn dump-table [table-name]
  (let [file (str *output-dir* "/tables.clj")
        table-map (table-map-for table-name)]
    (spit file table-map)))

(defn table-exists? [table-name]
  (not (nil? (some #(= table-name %) (list-all-tables)))))