(ns hbase-runner.hbase.table
  (:import [org.apache.hadoop.hbase.client HTable])
  (:use hbase-runner.utils.config))

(defn hbase-table [table-name]
  (HTable. *HBaseConfiguration* table-name))

(defn all-htable-descriptors []
  (.listTables *HBaseAdmin*))

(defn htable-descriptor-for [table-name]
  (.getTableDescriptor *HBaseAdmin* (.getBytes table-name)))

(defn hcolumn-families-for [table-name]
  (.getFamilies (htable-descriptor-for table-name)))

(defn column-families-for [table-name]
  (map #(str (.getNameAsString %) ":") (hcolumn-families-for table-name)))
