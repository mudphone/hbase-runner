(ns mudphone.hbase-runner.spec-helper
  (:import [org.apache.hadoop.hbase HTableDescriptor])
  (:use mudphone.hbase-runner.hbase-repl))

(defn create-table-if-does-not-exist [table-name]
  (if-not (table-exists? table-name)
    (let [table-descriptor (HTableDescriptor. table-name)]
      (.createTable *HBaseAdmin* table-descriptor)
      (println "Created table" table-name))))

(defn create-tables-if-do-not-exist [table-names]
  (doseq [table-name table-names]
    (create-table-if-does-not-exist table-name)))

(defn drop-table-if-exists [table-name]
  (if (table-exists? table-name)
    (do
      (if (table-enabled? table-name)
        (disable-table table-name))
      (drop-table table-name)
      (println "Dropped table" table-name))
    (println "No" table-name "table found, skipping.")))

(defn drop-tables-if-exist [table-names]
  (doseq [table-name table-names]
    (drop-table-if-exists table-name)))

(defn ns-table-name [canonical-table-name]
  (str (current-table-ns) "_" (name (keyword canonical-table-name))))

(defmacro with-test-tables [test-tables & body]
  `(try
    (create-tables-if-do-not-exist ~test-tables)
    (do ~@body)
    (finally
     (drop-tables-if-exist ~test-tables))))