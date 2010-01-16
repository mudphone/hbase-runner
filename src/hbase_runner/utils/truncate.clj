(ns hbase-runner.utils.truncate
  (:use clojure.contrib.pprint))

(defn filter-truncated [results]
  (filter #(= :truncated (:status %)) results))

(defn filter-errors [results]
  (filter #(= :error (:status %)) results))

(defn display-truncation-for [result]
  (let [tables-truncated (filter-truncated result)
        tables-with-errors (filter-errors result)]
    (println "Total tables operated on:" (count result))
    (println "Tables truncated successfully:" (count tables-truncated))
    (pprint (map :name tables-truncated))
    (println "Tables with errors:" (count tables-with-errors))
    (pprint (map :name tables-with-errors))))

(defn filter-truncated-names [packaged-results]
  (map :name (:truncated packaged-results)))

(defn filter-errors-names [packaged-results]
  (map :name (:errors packaged-results)))

(defn package-results [results]
  {
   :errors (filter-errors results)
   :truncated (filter-truncated results)
   :all results
   })