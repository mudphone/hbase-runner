(ns mudphone.hbase-runner.utils.file
  (:import (java.io BufferedReader BufferedWriter File FileInputStream FileWriter InputStreamReader)
           (org.apache.commons.io FileUtils))
  (:use clojure.contrib.pprint))

(defn spit [f content] 
  (let [file (File. f)]
    (if (not (.exists file))
      (FileUtils/touch file))
    (with-open [#^FileWriter fw (FileWriter. f true)]
      (with-open [#^BufferedWriter bw (BufferedWriter. fw)]
        (.write bw
                (with-out-str (pprint content)))))))

(defn lines-of-file [file-name]
 (line-seq
  (BufferedReader.
   (InputStreamReader.
    (FileInputStream. file-name)))))
