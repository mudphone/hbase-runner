(ns hbase-runner.hbase.region
  (:import [org.apache.hadoop.hbase HConstants HRegionInfo])
  (:import [org.apache.hadoop.hbase.client HTable])
  (:import [org.apache.hadoop.hbase.util Writables])
  (:use hbase-runner.hbase.get)
  (:use hbase-runner.hbase.put))

(defn meta-table []
  (HTable. HConstants/META_TABLE_NAME))

(defn online [region-name set-offline]
  (let [meta (meta-table)
        columns [[HConstants/CATALOG_FAMILY HConstants/REGIONINFO_QUALIFIER]]
        get (get-row-with-cols region-name columns)
        hri-bytes (.value (.get meta get))
        hri (doto (.getWritable Writables hri-bytes (HRegionInfo.))
              (.setOffline set-offline))
        put (put-for-row region-name [[HConstants/CATALOG_FAMILY
                                       HConstants/REGIONINFO_QUALIFIER
                                       (.getBytes Writables hri)]])]
    (.put meta put)))
