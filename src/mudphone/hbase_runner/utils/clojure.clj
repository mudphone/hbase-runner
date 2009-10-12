(ns mudphone.hbase-runner.utils.clojure
  (:import (java.io PushbackReader StringReader)))

(defn read-clojure-str [object-str]
  (read (PushbackReader. (StringReader. object-str))))
