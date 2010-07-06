(ns hbase-runner.utils.start
  (:use hbase-runner.utils.config))

(defn set-current-table-ns [current-ns]
  (dosync
   (alter *hbase-runner-config* assoc :current-table-ns current-ns)))

(defn current-table-ns []
  (let [current-ns (hbr*current-table-ns)]
    (if-not (nil? current-ns)
      current-ns
      (hbr*default-table-ns))))

(defn print-current-settings []
  (println "HBase Runner Home is:" (hbr*hbase-runner-home))
  (println "System is:" (name (keyword (hbr*system))))
  (println "Current table ns is:" (current-table-ns)))

(defn choose-system-and-ns
  ([]
     [:system-default])
  ([arg1]
     (if (keyword? arg1)
       [:system]
       [:ns]))
  ([arg1 arg2]
     (if (keyword? arg1)
       [:system :ns]
       [:ns :system])))

(defmulti start-hbase-repl choose-system-and-ns)

(defmethod start-hbase-repl [:system :ns] [system ns]
  (println "Attempting to connect to system:" system)
  (set-current-table-ns ns)
  (set-hbase-configuration system)
  (set-hbase-admin)
  (dosync
   (alter *hbase-runner-config* assoc :system system))
  (print-current-settings))

(defmethod start-hbase-repl [:ns :system] [ns system]
  (start-hbase-repl system ns))

(defmethod start-hbase-repl [:system] [system]
  (start-hbase-repl system (current-table-ns)))

(defmethod start-hbase-repl [:ns] [ns]
  (start-hbase-repl :default ns))

(defmethod start-hbase-repl [:system-default] []
  (start-hbase-repl :default (current-table-ns)))
