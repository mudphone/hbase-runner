(ns hbase-runner.spec-suite
  (:use [clojure.test :only [run-all-tests run-tests deftest is]])
  (:use hbase-runner.hbase-repl)
  (:use hbase-runner.spec-helper)
  (:use hbase-runner.hbase-repl-spec))

(start-hbase-repl :test "hbr_spec")

(run-all-tests #".*-spec")
