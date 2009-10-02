(ns mudphone.hbase-runner.repl-cheat
  (:import [org.apache.hadoop.hbase HBaseConfiguration])
  (:import [org.apache.hadoop.hbase.client HBaseAdmin]))

(defn hbase-configuration []
  (HBaseConfiguration.))
(def *HBaseConfiguration* (hbase-configuration))

(defn hbase-admin []
  (HBaseAdmin. *HBaseConfiguration*))
(def *HBaseAdmin* (hbase-admin))

(defn list-tables []
  (let [htable-descriptors (.listTables *HBaseAdmin*)]
    (map #(String. (.getName %)) htable-descriptors)))
