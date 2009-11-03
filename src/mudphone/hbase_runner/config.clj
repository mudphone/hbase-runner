(ns mudphone.hbase-runner.config
  (:use mudphone.hbase-runner.utils.file))

(def *hbase-runner-config*
     (let [hbase-runner-home (or (.get (System/getenv) "HBASE_RUNNER_HOME")
                                 (this-script-path))] 
       (ref {
             :hbase-runner-home hbase-runner-home
             :config-dir (str hbase-runner-home "/config")
             :output-dir (str hbase-runner-home "/output")
             :default-table-ns "koba_development"
             :current-table-ns nil
             :system nil
             })))

(defn fn-to-build-defn [akey]
  (list 'defn (symbol (str "hbr*" (name akey))) []
        (list akey '@*hbase-runner-config*)))
 
(defmacro create-config-helper-fns []
  `(do
     ~@(map fn-to-build-defn (keys @*hbase-runner-config*))))

(create-config-helper-fns)
