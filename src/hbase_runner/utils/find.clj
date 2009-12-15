(ns hbase-runner.utils.find)

(defn is-in-table-ns [table-ns table-name]
  (not (nil? (re-find (re-pattern (str "^" table-ns)) table-name))))

(defn table-name-from [htable-descriptor]
  (.getNameAsString htable-descriptor))

(defn filter-names-by [search-str names]
  (filter #(re-find (re-pattern search-str) %) names))