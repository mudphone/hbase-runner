(ns hbase-runner.hbase-repl
  (:import [java.io File])
  (:import [org.apache.hadoop.hbase HConstants])
  (:import [org.apache.hadoop.hbase.client HBaseAdmin HTable Scan])
  (:require [clojure.contrib [str-utils :as str-utils]])
  (:use hbase-runner.config.hbase-runner)
  (:use hbase-runner.utils.clojure)
  (:use hbase-runner.utils.file)
  (:use hbase-runner.utils.find)
  (:use hbase-runner.utils.create)
  (:use hbase-runner.utils.truncate)
  (:use clojure.contrib.pprint))

(defn set-current-table-ns [current-ns]
  (dosync
   (alter *hbase-runner-config* assoc :current-table-ns current-ns)))
(defn current-table-ns []
  (let [current-ns (hbr*current-table-ns)]
    (if-not (nil? current-ns)
      current-ns
      (hbr*default-table-ns))))

(defn hbase-configuration
  ([]
     (hbase-configuration :default))
  ([system]
     (hbase-config-for-system system)))

(declare *HBaseConfiguration*)
(defn hbase-admin []
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
      (keyword? system-or-table-ns)
      (let [system system-or-table-ns]
        (start-hbase-repl system (current-table-ns)))

      (string? system-or-table-ns)
      (let [table-ns system-or-table-ns]
        (start-hbase-repl :default system-or-table-ns))

      :else (do
              (println "You must provide a system name (as :keyword)"
                       "or table namespace (as \"string\"), or both.")
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

(declare table-exists?)
(defn disable-drop-table [table-name]
  (if (table-exists? table-name)
    (do
      (if (table-enabled? table-name)
        (disable-table table-name))
      (drop-table table-name))
    (println "Tables does not exist:" table-name "... skipping.")))

(defn enable-table [table-name]
  (println "Enabling table" table-name "...")
  (.enableTable *HBaseAdmin* table-name))

(defn create-table-from [descriptor]
  (.createTable *HBaseAdmin* descriptor))

(def-timed-fn truncate-table [table-name]
  (println "Truncating table" table-name "...")
  (let [descriptor (.getTableDescriptor *HBaseAdmin* (.getBytes table-name))
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

(defn byte-array-to-str [byte-array]
  (apply str (map char byte-array)))

(defn count-region [htable start-key end-key]
  (let [descriptor (.getTableDescriptor htable)
        first-family (first (.getFamilies descriptor))
        first-family-name (byte-array-to-str (.getNameWithColon first-family))
        scan (Scan. (.getBytes start-key) (.getBytes end-key))
        result-scanner (.getScanner htable scan)]
    (count (seq result-scanner))))

(defn count-rows [table-name]
  (let [htable (HTable. *HBaseConfiguration* table-name)
        start-end-byte-arrays (.getStartEndKeys htable)
        start-keys (map byte-array-to-str (.getFirst start-end-byte-arrays))
        end-keys (map byte-array-to-str (.getSecond start-end-byte-arrays))]
    (println "start-keys:" (str-utils/str-join "-" start-keys))
    (println "end-keys:" (str-utils/str-join "-" end-keys))
    (println "total regions:" (count start-keys))
    (reduce + (pmap #(count-region htable %1 %2) start-keys end-keys))))

(defn describe [table-name]
  (.toString (.getTableDescriptor *HBaseAdmin*)))