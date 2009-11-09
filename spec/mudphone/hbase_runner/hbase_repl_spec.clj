(ns mudphone.hbase-runner-spec
  (:require [clojure.contrib.java-utils :as j-utils])
  (:use [clojure.test :only [run-tests deftest is]])
  (:use mudphone.hbase-runner.config.hbase-runner)
  (:use mudphone.hbase-runner.spec-helper)
  (:use mudphone.hbase-runner.hbase-repl))

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

(deftest dump-table-test
  (let [test-table (ns-table-name :t1)]
    (with-test-tables [test-table]
      (try
       (dump-table test-table "test-tables.clj")
       (is "hbr_spec_t1" (:name (hydrate-table-map-from "test-tables.clj")))
       (finally
        (j-utils/delete-file (str (hbr*output-dir) "/test-tables.clj"))))
      )))