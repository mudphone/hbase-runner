(ns mudphone.hbase-runner.utils.file
  (:import (java.io BufferedReader BufferedWriter File FileInputStream FileWriter InputStreamReader)
           (org.apache.commons.io FileUtils))
  (:use mudphone.hbase-runner.utils.clojure)
  (:use clojure.contrib.pprint)
  (:require [clojure.contrib.str-utils2 :as su2]))

(defn spit [f content] 
  (let [file (File. f)]
    (if (not (.exists file))
      (FileUtils/touch file))
    (with-open [#^FileWriter fw (FileWriter. f true)]
      (with-open [#^BufferedWriter bw (BufferedWriter. fw)]
        (let [pretty-content (with-out-str (pprint content))
              single-line (su2/replace pretty-content "\n" "")]
          (.write bw single-line))))))

(defn lines-of-file [file-name]
 (line-seq
  (BufferedReader.
   (InputStreamReader.
    (FileInputStream. file-name)))))

(defn read-clojure-lines-from [file]
  (map read-clojure-str (lines-of-file file)))

(defn this-script-path []
  (let [sf (File. *file*)]
    (.substring (.getPath sf) 0 (.indexOf (.getPath sf) (.getName sf)))))
