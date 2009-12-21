(ns hbase-runner.hbase.table
  (:import [org.apache.hadoop.hbase.client HTable])
  (:use hbase-runner.hbase-repl))

(defn hbase-table [table-name]
  (HTable. *HBaseConfiguration* table-name))