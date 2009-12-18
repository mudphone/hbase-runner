(ns hbase-runner.utils.clojure
  (:import (java.io PushbackReader StringReader)))

(defn read-clojure-str [object-str]
  (read (PushbackReader. (StringReader. object-str))))

(defmacro def-timed-fn [fname args & body]
  `(defn ~fname ~args
     (let [start-time# (System/nanoTime)
           result# (do ~@body)
           end-time# (- (System/nanoTime) start-time#)]
       (println "Call to" (var ~fname) "with" ~args "took" end-time# "nanoseconds.")
       result#)))

(defn byte-array-to-str [byte-array]
  (apply str (map char byte-array)))