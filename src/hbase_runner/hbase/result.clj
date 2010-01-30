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

;; (defn- stringify-version-entry [v-entry]
;;   {(.getKey v-entry) (String. (.getValue v-entry))})

;; (defn- stringify-column-entry [c-entry]
;;   {(String. (.getKey c-entry)) (apply merge (map stringify-version-entry (.getValue c-entry)))})

;; (defn- stringify-qualifier-entry [q-entry]
;;   {(String. (.getKey q-entry)) (apply merge (map stringify-column-entry (.getValue q-entry)))})

;; (defn stringify-nav-map [nmap]
;;   (apply merge (map stringify-qualifier-entry nmap)))

;; (defn all-result-cols-to-map [result]
;;   (stringify-nav-map (.getMap result)))

(defn row-id-from-kv [kv]
  (String. (.getRow kv)))

(defn family-from-kv [kv]
  (String. (.getFamily kv)))

(defn qualifier-from-kv [kv]
  (String. (.getQualifier kv)))

(defn timestamp-from-kv [kv]
  (.getTimestamp kv))

(defn value-from-kv [kv]
  (String. (.getValue kv)))

(defn merge-kv-value [row-map kv]
  (let [map-keys [(family-from-kv kv) (qualifier-from-kv kv) (timestamp-from-kv kv)]
        aggregate-value #(conj (or %1 []) %2)]
    ;; (println "row-map:" row-map)
    ;; (println "value is:" (timestamp-from-kv kv) (value-from-kv kv))
    (update-in row-map map-keys aggregate-value (value-from-kv kv))))

(defn- collect-from-kv [map-bucket kv]
  (update-in map-bucket [(row-id-from-kv kv)] merge-kv-value kv))

(defn result-to-map [result]
  (let [kv-array (.sorted result)]
    (reduce collect-from-kv {} kv-array)))

(defn print-scan [results]
  (doseq [result results]
    (pprint {(row-id-for result)
             (result-to-map result)})))

(defn results-to-map [results {:keys [limit print-only]}]
  (let [limited-results (limit-results limit results)]
    (if print-only
      (print-scan limited-results)
      (map result-to-map limited-results)))
  )

;; (defn results-to-map [results {:keys [limit print-only]}]
;;   (let [limited-results (limit-results limit results)]
;;     (if print-only
;;       (print-scan limited-results)
;;       (reduce #(assoc %1
;;                  (row-id-for %2) (all-result-cols-to-map %2))
;;               {} (reverse limited-results))))
;;   )
