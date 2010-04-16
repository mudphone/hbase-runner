(ns hbase-runner.hbase.column
  (:require [clojure.contrib [str-utils :as str-utils]])
  (:use [clojure.contrib.str-utils :only [re-split]]))

(defn col-qual-from-col-str [col-str]
  (map #(.getBytes %1) (re-split #":" col-str 2)))

(defn columns-from-coll-or-str [columns]
  (cond
   (nil? columns) nil
   (coll? columns) columns
   (string? columns) [columns]
   :else (throw (Exception.
                 (str ":columns must be specified as a single string"
                      " column, or a collection of columns.")))))

(defn family-qualifier-from-column
  "Split column name into column family and qualifier: [family qualifier]"
  [column-name]
  (str-utils/re-split #":" column-name 2)
  )

(defn col-val-entry-to-vec [col-val-entry]
  (let [column (first col-val-entry)
        value (second col-val-entry)
        [family qualifier] (family-qualifier-from-column column)]
    [(.getBytes family) (.getBytes (or qualifier "")) (.getBytes value)]
    ))