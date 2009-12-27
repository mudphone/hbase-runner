(ns hbase-runner.hbase.scan
  (:import [org.apache.hadoop.hbase.client Scan])
  (:use [clojure.contrib.str-utils :only [re-split str-join]]))

(defn- add-cols-to-scan [scan cols]
  (.addColumns scan (str-join " " cols)))

(defn- update-scan [scan scan-fn input]
  (or (and (not-empty input) (scan-fn))
      scan))

(defn scan-gen [{:keys [scan start-row stop-row columns]
                 :or {scan (Scan.)}}]
  (let [scan (update-scan scan
                          #(.setStartRow scan (.getBytes start-row)) start-row)
        scan (update-scan scan
                          #(.setStopRow scan (.getBytes stop-row)) stop-row)
        scan (update-scan scan
                          #(add-cols-to-scan scan columns) columns)
        ]
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
     (scan-gen {:start-row stop-row :stop-row stop-row :columns columns})))