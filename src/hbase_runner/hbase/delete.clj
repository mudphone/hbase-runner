(ns hbase-runner.hbase.delete
  (:import (org.apache.hadoop.hbase.client Delete))
  (:use (hbase-runner.hbase table)))

(defn simple-delete-row [hbase-table-name row-id]
  (let [table (hbase-table hbase-table-name)
        delete (Delete. (.getBytes row-id))]
    (.delete table delete)))