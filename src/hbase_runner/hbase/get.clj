(ns hbase-runner.hbase.get
  (:import [org.apache.hadoop.hbase.client Get])
  (:use (hbase-runner.hbase column)))

(defn hbase-get
  [row-id]
  (Get. (.getBytes row-id)))

(defn add-get-col [get [family qualifier]]
  (if qualifier
    (.addColumn get family qualifier)
    (.addFamily get family))
  get)

(defn get-row-with-byte-cols
  "row-id: as string
   columns: as collection of [family qualifier] collections"
  [row-id cols-as-bytes]
  (let [get (hbase-get row-id)]
   (doseq [[family qualifier] cols-as-bytes]
     (.addColumn get family qualifier))
   get))

(defn get-for [row-id cols]
  (let [get (hbase-get row-id)]
    (dorun
     (map (comp #(add-get-col get %) col-qual-from-col-str) cols))
    get
    ))