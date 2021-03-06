(ns hbase-runner.hbase-repl
  (:import [java.io File])
  (:import [org.apache.hadoop.hbase HConstants])
  (:import [org.apache.hadoop.hbase.client HTable])
  (:require [clojure.contrib [str-utils :as str-utils]])
  (:use [clojure.contrib.pprint :only [pp pprint]])
  (:use [hbase-runner.hbase column get put region result scan table])
  (:use hbase-runner.utils.clojure)
  (:use hbase-runner.utils.config)
  (:use hbase-runner.utils.file)
  (:use hbase-runner.utils.find)
  (:use hbase-runner.utils.create)
  (:use hbase-runner.utils.truncate))

(defn set-current-table-ns [current-ns]
  (dosync
   (alter *hbase-runner-config* assoc :current-table-ns current-ns)))
(defn current-table-ns []
  (let [current-ns (hbr*current-table-ns)]
    (if-not (nil? current-ns)
      current-ns
      (hbr*default-table-ns))))

(defn print-current-settings []
  (println "HBase Runner Home is:" (hbr*hbase-runner-home))
  (println "System is:" (name (keyword (hbr*system))))
  (println "Current table ns is:" (current-table-ns)))

(defn public-api []
  (sort (map first (ns-publics 'hbase-runner.hbase-repl))))

(defn print-api []
  (pprint (public-api)))

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
     (set-hbase-configuration system)
     (set-hbase-admin)
     (dosync
      (alter *hbase-runner-config* assoc :system system))
     (print-current-settings)))

(def start-hbase-runner start-hbase-repl)

(defn list-all-tables []
  (map table-name-from (all-htable-descriptors)))

(defn list-all-tables-pp []
  (let [tables (list-all-tables)]
    (pprint tables)
    tables))

(defn list-tables []
  (filter (partial is-in-table-ns (current-table-ns)) (list-all-tables)))

(defn list-tables-pp []
  (let [tables (list-tables)]
    (pprint tables)
    tables))

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

(defn compact [table-or-region-name]
  (.compact *HBaseAdmin* table-or-region-name))

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

(defn disabled-tables
  ([]
     (disabled-tables (list-tables)))
  ([tables]
     (remove false? (map #(and (table-disabled? %) %) tables))))

(defn disabled-tables-all-ns []
  (disabled-tables (list-all-tables)))

(defn disable-table [table-name]
  (println "Disabling table" table-name "...")
  (.disableTable *HBaseAdmin* table-name))

(defn disable-table-if-enabled [table-name]
  (if (table-enabled? table-name)
    (do
      (disable-table table-name)
      (println "Disabled:" table-name))
    (println "Already disabled:" table-name)))

(defn disable-tables
  ([]
     (disable-tables (list-tables)))
  ([tables]
     (dorun (pmap disable-table-if-enabled tables))))

(defn disable-all-tables []
  (disable-tables (list-all-tables)))

(defn table-exists? [table-name]
  (not (nil? (some #(= table-name %) (list-all-tables)))))

(defn disable-drop-table [table-name]
  (try
   (if (table-exists? table-name)
     (do
       (if (table-enabled? table-name)
         (disable-table table-name))
       (drop-table table-name))
     (println "Table does not exist:" table-name "... skipping."))
   (catch Exception e
     (println "Exception processing table:" table-name)
     (println "Exception:")
     (.printStackTrace e))))

(defn enable-table [table-name]
  (println "Enabling table" table-name "...")
  (.enableTable *HBaseAdmin* table-name))

(defn enable-table-if-disabled [table-name]
  (cond
   (not (table-exists? table-name)) (println "Cannot enable missing table:" table-name)
   (table-enabled? table-name) (println "Already enabled:" table-name)
   :else (do
           (enable-table table-name)
           (println "Enabled:" table-name))))

(defn enable-tables
  ([]
     (enable-tables (list-tables)))
  ([tables]
     (println "Enabling" (count tables) "tables...")
     (dorun (pmap enable-table-if-disabled tables))))

(defn enable-all-tables []
  (enable-tables (list-all-tables)))

(defn create-table-from [descriptor]
  (.createTable *HBaseAdmin* descriptor))

(def-timed-fn truncate-table [table-name]
  (println "Truncating table" table-name "...")
  (let [descriptor (htable-descriptor-for table-name)
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
  (let [results (doall (pmap truncate-table table-name-list))]
    (display-truncation-for results)
    (package-results results)))

(defn create-missing-results-tables [error-results]
  (let [create-missing (fn [{:keys [name descriptor] :as table-result}]
                         (if (table-exists? name)
                           (do
                             (println "Table already exists:" name)
                             table-result)
                           (do
                             (println "Creating missing table:" name)
                             (create-table-from descriptor)
                             (assoc table-result :status :truncated))))]
    (map create-missing error-results)))

(defn optionally-create-missing-results-tables [{error-results :errors
                                                 truncated-results :truncated
                                                 :as original-results}]
  (if (empty? error-results)
    (do
      (println "No missing tables.")
      original-results)
    (do
      (let [new-results (doall (create-missing-results-tables error-results))]
        (package-results (concat truncated-results new-results))
        ))))

(defn enable-disabled-results-tables [all-results]
  (let [enable-disabled (fn [{:keys [name] :as table-result}]
                          (enable-table-if-disabled name))]
    (map enable-disabled all-results)))

(defn optionally-enable-disabled-results-tables [{error-results :errors
                                                  all-results :all
                                                  :as original-results}]
  (if (empty? error-results)
    (do
      (println "No disabled tables.")
      original-results)
    (do
      (dorun (enable-disabled-results-tables all-results))
      (package-results all-results))))

(defn truncate-tables-loop
  ([tables]
     (truncate-tables-loop tables 1))
  ([tables iteration-count]
     (println "Begin iteration" iteration-count)
     (enable-tables tables)
     (cond
      (empty? tables)
      (println "Done.")

      (not-every? table-exists? tables)
      (println "All tables must exist.")

      (not-every? table-enabled? tables)
      (println "All tables must be enabled to proceed.")

      (= iteration-count 10)
      (do
        (println "Last try to truncate tables:" iteration-count)
        (truncate-tables tables))

      :else
      (do
        (let [result (-> tables
                         (truncate-tables)
                         (optionally-create-missing-results-tables)
                         (optionally-enable-disabled-results-tables))]
          (println "Done with iteration" iteration-count)
          (recur (doall (filter-errors-names result)) (inc iteration-count))))
      )))

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

(defn count-region [htable start-key end-key]
  (let [descriptor (.getTableDescriptor htable)
        first-family (first (.getFamilies descriptor))
        first-family-name (byte-array-to-str (.getNameWithColon first-family))
        scan (scan-for start-key end-key)
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

(defn count-tables [table-names]
  (->> table-names
       (pmap count-rows)
       (reduce +)))

(defn describe [table-name]
  (.toString (htable-descriptor-for table-name)))

(defn close-region [region-name]
  (let [server nil]
    (.closeRegion *HBaseAdmin* region-name server)))

(defn enable-region [region-name]
  (online region-name false))

(defn disable-region [region-name]
  (online region-name true))

(defn scan-results
  "Return results of scan (which may use a lot of memory).
   See 'scan' for print-only version of this function."
  ([table-name]
     (scan-results table-name {}))
  ([table-name options]
     (scan-table table-name (merge options {:print-only false}))))

(defn scan
  "Print results of scan, but do not return them (to avoid using memory)."
  [ & args ]
  (apply scan-table args))

(defn put-cols
  ([table-name row-id col-val-map]
     (put-cols table-name row-id col-val-map nil))
  ([table-name row-id col-val-map timestamp]
     (let [htable (hbase-table table-name)
           col-val-vec (map col-val-entry-to-vec col-val-map)]
       (.put htable (put-for-row row-id col-val-vec timestamp)))))

(defn put
  ([table-name row-id column value]
     (put table-name row-id column value nil))
  ([table-name row-id column value timestamp]
     (put-cols table-name row-id {column value} timestamp)))

(defn get-row
  "Returns row as Clojure map, by table-name, row-id, and timestamp.
   An option hash may be provided as the optional 3rd argument.
   Options: {
             :column \"f1\"
             :columns [ \"f1\" \"f2:q1\" ]
             :timestamp 1268279340489
             :timestamps [ 1268279340488 1268279340489 ]
            }
     column  - family:qualifier for single column
     columns - a vector of family:qualifier strings
     timestamp  - a single timestamp long
                  gets only values with this timestamp
     timestamps - a vector of min and max timestamp longs
                  gets all values for selected columns in this range"
  ([table-name row-id]
     )
  ([table-name row-id {:keys [column columns timestamp timestamps]
                       :or {column nil
                            columns nil
                            timestamp nil
                            timestamps []}}]
     (let [htable (hbase-table table-name)
           the-get (-> (hbase-get row-id)
                       (add-get-cols (columns-from-coll-or-str column))
                       (add-get-cols columns)
                       (add-get-ts timestamp)
                       (add-get-ts timestamps)
                       )]
       (result-column-values-to-map (.get htable the-get))
       )
     )
  )