(ns hbase-runner.hbase.scan
  (:import [org.apache.hadoop.hbase.client Scan])
  (:use [clojure.contrib.str-utils :only [re-split str-join]]))

(defn- add-cols-to-scan [scan cols]
  (.addColumns scan (str-join " " cols)))

(defn- update-scan-if-input [scan scan-fn input]
  (or (and (not-empty input) (scan-fn input))
      scan))

(defn scan-gen [{:keys [scan start-row stop-row columns filter timestamp cache]
                 :or {scan (Scan.)
                      cache true}}]
  (let [update-scan (partial update-scan-if-input scan)
        scan (update-scan #(.setStartRow scan (.getBytes %)) start-row)
        scan (update-scan #(.setStopRow scan (.getBytes %)) stop-row)
        scan (update-scan #(add-cols-to-scan scan %) columns)
        scan (update-scan #(.setFilter scan %) filter)
        scan (update-scan #(.setTimeStamp scan %) timestamp)]
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