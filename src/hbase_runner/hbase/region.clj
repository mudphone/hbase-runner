(ns hbase-runner.hbase.region
  (:import [org.apache.hadoop.hbase HConstants HRegionInfo])
  (:import [org.apache.hadoop.hbase.client HTable])
  (:import [org.apache.hadoop.hbase.util Writables])
  (:use hbase-runner.hbase.get
        hbase-runner.hbase.put
        hbase-runner.utils.config))

(defn meta-table []
  ;; (HTable. HConstants/META_TABLE_NAME)
  (HTable. *HBaseConfiguration* HConstants/META_TABLE_NAME))

(defn online [region-name set-offline]
  (let [metat (meta-table)
        columns [[HConstants/CATALOG_FAMILY HConstants/REGIONINFO_QUALIFIER]]
        region-get (get-row-with-cols region-name columns)
        hri-bytes (.value (.get metat region-get))
        hri (doto (Writables/getWritable hri-bytes (HRegionInfo.))
              (.setOffline set-offline))
        put (put-for-row region-name [[HConstants/CATALOG_FAMILY
                                       HConstants/REGIONINFO_QUALIFIER
                                       (Writables/getBytes hri)]])]
    (.put metat put)))
