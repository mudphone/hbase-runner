(ns mudphone.hbase-runner.spec-helper
  (:import [org.apache.hadoop.hbase HTableDescriptor])
  (:use mudphone.hbase-runner.hbase-repl))

(defn create-table-if-does-not-exist [table-name]
  (if-not (table-exists? table-name)
    (let [table-descriptor (HTableDescriptor. table-name)]
      (.createTable *HBaseAdmin* table-descriptor))))