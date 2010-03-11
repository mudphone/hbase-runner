(ns hbase-runner.hbase.column
  (:use [clojure.contrib.str-utils :only [re-split]]))

(defn col-qual-from-col-str [col-str]
  (map #(.getBytes %1) (re-split #":" col-str 2)))
