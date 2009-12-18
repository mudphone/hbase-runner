(ns hbase-runner.hbase.get
  (:import [org.apache.hadoop.hbase.client Get])
  (:require [clojure.contrib [str-utils :as str-utils]])

(defn get-for-row-id [row-id]
  (Get. (.getBytes row-id)))

(defn get-row-with-cols [row-id columns]
  "row-id: as string
   columns: as collection of family:qualifier strings"
  (let [get (get-for-row-id row-id)]
   (doseq [full-name columns]
     (let [[family qualifier] (str-utils/re-split full-name)]
       (.addColumn get familiy qualifier)))))
