(ns hbase-runner.scripts.check-next
  (:import (java.text DateFormat SimpleDateFormat)
           (java.util TimeZone))
  (:use [clojure.contrib
         [str-utils :as str-utils]
         [pprint :as pprint]]
        (hbase-runner hbase-repl)))

(def consumer-events-tables (find-tables "consumer_events_"))
(def STUPID-TABLE
     "furtive_production_consumer_events_93474cbf-69d9-b3b8-8b43-75fbb4d74296")

(defn first-row-after [row-id table-name]
  ((comp first keys first)
   (scan-results table-name {:start-row row-id
                             :limit 1
                             :columns ["meta:api__"]})))

(defn last-row-between [start-row-id stop-row-id table-name]
  ((comp first keys last)
   (scan-results table-name {:start-row-id start-row-id
                             :stop-row stop-row-id
                             :columns ["meta:api__"]})))

(defn timestamp-from [row-id]
  (first (str-utils/re-split #":" (or row-id ""))))

(defn set-utc [sdf]
  (.setTimeZone sdf (TimeZone/getTimeZone "UTC"))
  sdf)

(defn format-date-str-from-ts [sdf timestamp]
  (.format sdf (Long/parseLong timestamp)))

(defn pretty-date-for [timestamp]
  (if (empty? timestamp)
    "NO TIMESTAMP"
    (-> (SimpleDateFormat. "yyyy/MM/dd HH:mm:ss")
        (set-utc)
        (format-date-str-from-ts timestamp))))

(defn next-rows-for [row-id]
  (println "Given start row-id timestamp is:"
           (pretty-date-for (timestamp-from row-id)))
  (sort-by :next-row-id
           (pmap (fn [table-name]
                   (let [next-row-id (first-row-after row-id table-name)]
                     {:table table-name
                      :next-row-id next-row-id
                      :date (pretty-date-for (timestamp-from next-row-id))
                      }))
                 consumer-events-tables)))

(defn result-summary-for [found-row-id table-name]
  {:table table-name
   :found-row-id found-row-id
   :date (pretty-date-for (timestamp-from found-row-id))
   })

(defn pp-result-summary-for [found-row-id table-name]
  (let [result (result-summary-for found-row-id table-name)]
    (pprint result)
    result))

(defn result-for [start-row-id stop-row-id events-table]
  (-> (last-row-between start-row-id
                        stop-row-id
                        events-table)
      (pp-result-summary-for events-table)))

(defn last-row-between-for [start-row-id stop-row-id]
  (println "Given last possible row-id timstamp start-row:"
           (pretty-date-for (timestamp-from start-row-id))
           "and stop-row:"
           (pretty-date-for (timestamp-from stop-row-id)))
  (sort-by :found-row-id
           (pmap (partial result-fr start-row-id stop-row-id)
                 consumer-events-tables)))