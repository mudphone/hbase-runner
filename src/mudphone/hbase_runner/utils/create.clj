(ns mudphone.hbase-runner.utils.create
  (:import (java.io FileWriter BufferedWriter File)
           (org.apache.commons.io FileUtils))
  (:import [org.apache.hadoop.hbase.client HTable]))

(defn create-table-from [hbase-admin descriptor]
  (.createTable hbase-admin descriptor))

(defn spit [f content] 
  (let [file (File. f)]
    (if (not (.exists file))
      (FileUtils/touch file))
    (with-open [#^FileWriter fw (FileWriter. f true)]
      (with-open [#^BufferedWriter bw (BufferedWriter. fw)]
        (.write bw (str content "\n"))))))

(defn dump-tables [table-names]
  (let [file "/Users/koba/tables.txt"
        spit-descriptor-hash (fn [table-name]
                               (spit file (str (.getTableDescriptor (HTable. table-name)))))]
      (map spit-descriptor-hash table-names)))