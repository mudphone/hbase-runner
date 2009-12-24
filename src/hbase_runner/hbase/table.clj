(ns hbase-runner.hbase.table
  (:import [org.apache.hadoop.hbase.client HTable])
  (:use hbase-runner.hbase-repl))

;;(defn hbase-table [table-name]
;;  (HTable. *HBaseConfiguration* table-name))

(defn enable-table-if-disabled [table-name]
  (if (table-disabled? table-name)
    (do
      (enable-table table-name)
      (println "Enabled:" table-name))
    (println "Already enabled:" table-name)))