(ns hbase-runner.hbase-repl-spec
  (:use hbase-runner.hbase-repl))

(start-hbase-repl :test "hbr_spec")

(load-file "src/hbase_runner/spec_helper.clj")
(load-file "test/hbase_runner/hbase_repl_spec.clj")

(run-tests)
