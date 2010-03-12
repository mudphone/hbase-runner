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

(defn add-get-cols [get cols]
  (let [columns (columns-from-coll-or-str cols)]
    (doseq [family-qual-str columns]
      (add-get-col get (col-qual-from-col-str family-qual-str))))
  get)

(defn get-row-with-byte-cols
  "row-id: as string
   columns: as collection of [family qualifier] collections"
  [row-id cols-as-bytes]
  (let [get (hbase-get row-id)]
    (doseq [family-qual-as-bytes cols-as-bytes]
      (add-get-col get family-qual-as-bytes))
   get))

(defn ts-vec
  "Forces timestamp val or values into a collection.
   This makes desctructuring easier."
  [timestamps]
  (if (coll? timestamps)
    timestamps
    [timestamps]))

(defn add-get-ts
  [get timestamps]
  (let [[ts1 ts2 :as both-ts] (ts-vec timestamps)]
    (if ts1
      (if ts2
        (.setTimeRange get (long ts1) (long ts2))
        (.setTimeStamp get (long ts1)))))
  get)
