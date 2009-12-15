(ns hbase-runner.hbase-repl-spec
  (:require [clojure.contrib.java-utils :as java-utils])
  (:use [clojure.test :only [run-tests deftest is]])
  (:use hbase-runner.config.hbase-runner)
  (:use hbase-runner.spec-helper)
  (:use hbase-runner.hbase-repl))

(deftest current-table-ns-test
  (is (= "hbr_spec" (current-table-ns))))

(deftest table-exists?-test
  (let [table-name (ns-table-name :t1)]
    (with-test-tables [table-name]
      (create-table-if-does-not-exist table-name)
      (is (table-exists? table-name))
      (is (not (table-exists? "bogus_table_name_nanoo"))))))

(deftest enable-disable-table-test
  (let [table-name (ns-table-name :t1)]
    (with-test-tables [table-name]
      (create-table-if-does-not-exist table-name)
      (is (table-enabled? table-name))
    
      (disable-table table-name)
      (is (table-disabled? table-name))
    
      (enable-table table-name)
      (is (table-enabled? table-name)))))

(deftest list-tables-and-list-all-tables-test
  (let [in-ns-table-name (ns-table-name :t1)
        other-table-name "other_test_table"
        test-tables [in-ns-table-name other-table-name]]
    (with-test-tables test-tables
     (let [ns-tables (list-tables)]
       (is (some #{in-ns-table-name} ns-tables))
       (is (not (some #{other-table-name} ns-tables))))

     (let [all-tables (list-all-tables)]
       (is (some #{in-ns-table-name} all-tables))
       (is (some #{other-table-name} all-tables))))))

(deftest dump-tables-test
  (let [test-table (ns-table-name :t1)
        test-file-name "test-tables.clj"
        test-file-path (str (hbr*output-dir) "/" test-file-name)]
    (with-test-tables [test-table]
      (with-cleared-file test-file-path
        (dump-tables [test-table] test-file-name)
        (is (= "hbr_spec_t1" (:name (first (hydrate-table-maps-from test-file-name))))))
)))