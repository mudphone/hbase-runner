(ns hbase-runner.hbase.scan
  (:import [org.apache.hadoop.hbase.client Scan])
  (:use [clojure.contrib.str-utils :only [re-split str-join]])
  (:use hbase-runner.hbase.result)
  (:use hbase-runner.hbase.table))

(defn- columns-from-coll-or-str [columns]
  (cond
   (nil? columns) nil
   (coll? columns) columns
   (string? columns) [columns]
   :else (throw (Exception.
                 (str ":columns must be specified as a single string"
                      " column, or a collection of columns.")))))

(defn- add-family-qualifier-to [scan col-str]
  (let [col-qual (map #(.getBytes %1) (re-split #":" col-str 2))]
    (if (= (count col-qual) 2)
      (apply #(.addColumn scan %1 %2) col-qual)
      (apply #(.addFamily scan %1) col-qual))))

(defn- add-cols-to-scan [scan cols]
  (reduce add-family-qualifier-to scan cols))

(defn- empty-or-nil? [coll-or-symbol]
  (or (and (coll? coll-or-symbol) (empty? coll-or-symbol)) (nil? coll-or-symbol)))

(defn- update-if-input [scan update-fn & input]
  (or (and (not-empty (remove empty-or-nil? input)) (apply (partial update-fn scan) input))
      scan))

(defn- set-start-row [scan start-row]
  (.setStartRow scan (.getBytes start-row)))

(defn- set-stop-row [scan stop-row]
  (.setStopRow scan (.getBytes stop-row)))

(defn- set-filter [scan filter]
  (.setFilter scan filter))

(defn- set-timestamp [scan timestamp]
  (.setTimeStamp scan (long timestamp)))

(defn- set-max-versions [scan versions]
  (cond
   (= :all versions) (.setMaxVersions scan)
   :else (.setMaxVersions scan versions)))

(defn scan-gen [{:keys [scan start-row stop-row columns filter timestamp cache
                        versions]
                 :or {scan (Scan.)
                      cache true}}]
  "Create a new scan, or update a scan if passed as :scan argument.
   Will only update the scan if valid argument given."
  (let [scan
        (-> scan
            (update-if-input #(set-start-row %1 %2) start-row)
            (update-if-input #(set-stop-row %1 %2) stop-row)
            (update-if-input #(add-cols-to-scan %1 %2) columns)
            (update-if-input #(set-filter %1 %2) filter)
            (update-if-input #(set-timestamp %1 %2) timestamp)
            (update-if-input #(set-max-versions %1 %2) versions)
            )]
    (.setCacheBlocks scan cache)
    scan))

(defn scan-all-rows []
  (scan-gen {}))

(defn scan-for
  ([start-row]
     (scan-gen {:start-row start-row}))
  ([start-row stop-row]
     (scan-gen {:start-row start-row :stop-row stop-row})))

(defn scan-for-columns
  ([columns]
     (scan-gen {:columns columns}))
  ([start-row columns]
     (scan-gen {:start-row start-row :columns columns}))
  ([start-row stop-row columns]
     (scan-gen {:start-row start-row :stop-row stop-row :columns columns}))
  ([start-row stop-row columns options]
     (scan-gen (merge
                {:start-row start-row :stop-row stop-row :columns columns}
                options))))

(defn scan-table
  ([table-name]
     (scan-table table-name {}))

  ([table-name {:keys [start-row stop-row columns
                       limit filter timestamp cache print-only
                       versions]
                :or {limit -1
                     filter nil
                     start-row ""
                     stop-row nil
                     timestamp nil
                     columns nil
                     cache true
                     print-only true
                     versions 1}}]
     (let [scan-options {:filter filter
                         :timestamp timestamp
                         :cache cache
                         :versions versions}
           print-options {:print-only print-only
                          :limit limit}
           columns (columns-from-coll-or-str columns)
           scan (scan-for-columns start-row stop-row columns scan-options)
           scanner (.getScanner (hbase-table table-name) scan)]
       (results-to-map (seq scanner) columns print-options)
       )))