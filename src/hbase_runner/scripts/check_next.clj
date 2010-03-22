(ns hbase-runner.scripts.check-next
  (:import (java.text DateFormat SimpleDateFormat)
           (java.util TimeZone))
  (:use [clojure.contrib
         [str-utils :as str-utils]]
        (hbase-runner hbase-repl)))

(def consumer-events-tables (find-tables "consumer_events_"))

(defn first-row-after [row-id table-name]
  ((comp first keys first)
   (scan-results table-name {:start-row row-id :limit 1})))

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
  (pmap (fn [table-name]
          (let [next-row-id (first-row-after row-id table-name)]
           {:table table-name
            :next-row-id next-row-id
            :date (pretty-date-for (timestamp-from next-row-id))
            }))
        consumer-events-tables))
