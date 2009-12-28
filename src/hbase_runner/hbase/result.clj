(ns hbase-runner.hbase.result
  (:use hbase-runner.utils.clojure))

(defn- col-value [col result]
  (byte-array-to-str (.getValue result (.getBytes col))))

(defn- assoc-result-col-value-to-map [result map col]
  (assoc map col (col-value col result)))

(defn- result-column-values-to-map [result columns]
  (reduce (partial assoc-result-col-value-to-map result) {} columns))

(defn- row-id-for [result]
  (byte-array-to-str (.getRow result)))

(defn results-to-map [results columns]
  (reduce #(assoc %1
             (row-id-for %2) (result-column-values-to-map %2 columns))
          {} results)
  )
