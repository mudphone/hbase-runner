(ns hbase-runner.all-hbase-repl-spec
  (:use [clojure.test :only [run-tests deftest is]])
  (:use hbase-runner.hbase-repl)
  (:use hbase-runner.spec-helper)
  (:use hbase-runner.hbase-repl-spec))

(start-hbase-repl :test "hbr_spec")

(run-tests)
