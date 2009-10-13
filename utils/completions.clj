(use 'mudphone.hbase-runner.hbase-repl)
(def completions
  (reduce concat (map (fn [p] (keys (ns-publics (find-ns p))))
                      '(clojure.core clojure.set clojure.xml clojure.zip mudphone.hbase-runner.hbase-repl))))

(with-open [f (java.io.BufferedWriter. (java.io.FileWriter. (str (System/getenv "HOME") "/.clj_completions")))]
  (.write f (apply str (interleave completions (repeat "\n")))))

(println "Created ~/.clj_completions file.")