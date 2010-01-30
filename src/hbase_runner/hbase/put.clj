(ns hbase-runner.hbase.put
  (:import [org.apache.hadoop.hbase.client Put]))

(defn put-for-row
  ([row-id columns-and-values]
     (put-for-row row-id columns-and-values nil))
  ([row-id columns-and-values timestamp]
     (let [put (Put. (.getBytes row-id))]
       (if timestamp
         (.setTimeStamp put timestamp))
       (doseq [[family qualifier value] columns-and-values]
         (.add put family qualifier value))
       put)))