(ns hbase-runner.hbase.put
  (:import [org.apache.hadoop.hbase.client Put]))

(defn put-for-row [row-id columns-and-values]
  (let [put (Put. (.getBytes row-id))]
   (doseq [[family qualifier value] columns-and-values]
     (.add put family qualifier value))
   put))