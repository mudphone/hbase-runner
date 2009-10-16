(ns mudphone.hbase-runner-spec
  (:use mudphone.hbase-runner.hbase-repl))

(start-hbase-repl "hbr_spec")

(load-file "src/mudphone/hbase_runner/spec_helper.clj")
(load-file "spec/mudphone/hbase_runner/hbase_repl_spec.clj")

(run-tests)
