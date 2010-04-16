(ns hbase-runner.hbase.delete
  (:import (org.apache.hadoop.hbase.client Delete))
  (:use (hbase-runner.hbase column table)))

(defn simple-delete-row [hbase-table-name row-id]
  (let [table (hbase-table hbase-table-name)
        delete (Delete. (.getBytes row-id))]
    (.delete table delete)))

(defn delete-cols-up-to*
  "Delete all versions of columns up to given timestamp.
   If no timestamp, then delete all versions of column."
  ([table-name row-id cols]
     (delete-cols-up-to* table-name row-id cols nil))
  ([table-name row-id cols timestamp]
     (let [table (hbase-table table-name)
           delete (Delete. (.getBytes row-id))]
       (doseq [col cols]
         (let [[family qualifier] (family-qualifier-from-column col)]
           (if timestamp
             (.deleteColumns delete (.getBytes family) (.getBytes qualifier) timestamp)
             (.deleteColumns delete (.getBytes family) (.getBytes qualifier)))))
       (.delete table delete)
       )))

(defn delete-cols-at*
  "Delete column at exact version of given timestamp.
   If no timestamp given, then deletes the latest version."
  ([table-name row-id cols]
     (delete-cols-at* table-name row-id cols nil))
  ([table-name row-id cols timestamp]
     (let [table (hbase-table table-name)
           delete (Delete. (.getBytes row-id))]
       (doseq [col cols]
         (let [[family qualifier] (family-qualifier-from-column col)]
           (if timestamp
             (.deleteColumn delete (.getBytes family) (.getBytes qualifier) timestamp)
             (.deleteColumn delete (.getBytes family) (.getBytes qualifier)))))
       (.delete table delete)
       )))