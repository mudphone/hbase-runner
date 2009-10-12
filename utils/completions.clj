(use 'mudphone.hbase-runner.repl-cheat)
(def completions
     (reduce concat (map (fn [p] (keys (ns-publics (find-ns p))))
                         '(clojure.core clojure.set clojure.xml clojure.zip mudphone.hbase-runner.repl-cheat))))

(with-open [f (java.io.BufferedWriter. (java.io.FileWriter. (str (System/getenv "HOME") "/.clj_completions")))]
  (.write f (apply str (interleave completions (repeat "\n")))))

(println "Created ~/.clj_completions file.")