(ns mudphone.hbase-runner-spec
  (:use [clojure.test :only [run-tests deftest is]])
  (:use mudphone.hbase-runner.spec-helper)
  (:use mudphone.hbase-runner.hbase-repl))

(deftest table-exists?-test
  (let [table-name "hbr_spec_t1"]
   (create-table-if-does-not-exist table-name)
   (is (table-exists? table-name))
   (is (not (table-exists? "bogus_table_name_nanoo")))))

(deftest enable-disable-table-test
  (let [table-name "hbr_spec_t1"]
    (create-table-if-does-not-exist table-name)
    (is (table-enabled? table-name))
    (disable-table table-name)
    (is (table-disabled? table-name))
    (enable-table table-name)
    (is (table-enabled? table-name))))