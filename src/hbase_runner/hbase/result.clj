(ns hbase-runner.hbase.result
  (:use [clojure.contrib.pprint :only [pprint]])
  (:use hbase-runner.utils.clojure))

(defn- col-value [col result]
  (byte-array-to-str (.getValue result (.getBytes col))))

(defn- assoc-result-col-value-to-map [result map col]
  (assoc map col (col-value col result)))

(defn- result-column-values-to-map [result columns]
  (reduce (partial assoc-result-col-value-to-map result) {} columns))

(defn- row-id-for [result]
  (byte-array-to-str (.getRow result)))

(defn- pprint-result [result columns ]
  (pprint {(row-id-for result)
           (result-column-values-to-map result columns)}))
(defn- limit-results [limit results]
  (or (and (> limit 0) (take limit results))
      results))

(defn results-to-map [results columns {:keys [limit print-only]}]
  (let [limited-results #(limit-results limit results)]
    (if print-only
      (doseq [result (limited-results)]
        (pprint-result result columns))
      (reduce #(assoc %1
                 (row-id-for %2) (result-column-values-to-map %2 columns))
              {} (limited-results))))
  )
