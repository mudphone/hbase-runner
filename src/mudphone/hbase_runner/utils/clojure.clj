(ns mudphone.hbase-runner.utils.clojure
  (:import (java.io PushbackReader StringReader)))

(defn read-clojure-str [object-str]
  (read (PushbackReader. (StringReader. object-str))))

(defmacro def-timed-fn [fname args & body]
  `(defn ~fname ~args
     (let [start-time# (System/nanoTime)]
       (do ~@body)
       (println "Call to" ~fname "with" ~args "took" (- (System/nanoTime) start-time#) "nanoseconds"))))