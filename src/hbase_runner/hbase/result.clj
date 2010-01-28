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

(defn- limit-results [limit results]
  (or (and limit (> limit 0) (take limit results))
      results))

(defn- stringify-version-entry [v-entry]
  {(.getKey v-entry) (String. (.getValue v-entry))})

(defn- stringify-column-entry [c-entry]
  {(String. (.getKey c-entry)) (apply merge (map stringify-version-entry (.getValue c-entry)))})

(defn- stringify-qualifier-entry [q-entry]
  {(String. (.getKey q-entry)) (apply merge (map stringify-column-entry (.getValue q-entry)))})

(defn stringify-nav-map [nmap]
  (apply merge (map stringify-qualifier-entry nmap)))

(defn all-result-cols-to-map [result]
  (stringify-nav-map (.getMap result)))

(defn print-scan [results]
  (doseq [result results]
    (pprint {(row-id-for result)
             (all-result-cols-to-map result)})))

(defn results-to-map [results columns {:keys [limit print-only]}]
  (let [limited-results (limit-results limit results)]
    (if print-only
      (print-scan limited-results)
      (reduce #(assoc %1
                 (row-id-for %2) (all-result-cols-to-map %2))
              {} (reverse limited-results))))
  )
