(ns hbase-runner.hbase.get
  (:import [org.apache.hadoop.hbase.client Get]))

(defn get-row-with-cols [row-id columns]
  "row-id: as string
   columns: as collection of [family qualifier] collections"
  (let [get (Get. (.getBytes row-id))]
   (doseq [[family qualifier] columns]
     (.addColumn get family qualifier))
   get))
