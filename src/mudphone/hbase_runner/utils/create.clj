(ns mudphone.hbase-runner.utils.create
  (:import [org.apache.hadoop.hbase.client HTable])
  (:use mudphone.hbase-runner.utils.clojure)
  (:use mudphone.hbase-runner.utils.file))

(defn- compression-name-for [hcolumn-descriptor]
  (.getName (.getCompression hcolumn-descriptor)))

(defn- family-map-from-hcolumn-descriptor [hcolumn-descriptor]
  {:name (.getNameAsString hcolumn-descriptor)
   :compression (compression-name-for hcolumn-descriptor)
   :versions (.getMaxVersions hcolumn-descriptor)
   :ttl (.getTimeToLive hcolumn-descriptor)
   :blocksize (.getBlocksize hcolumn-descriptor)
   :in-memory? (.isInMemory hcolumn-descriptor)
   :block-cache-enabled? (.isBlockCacheEnabled hcolumn-descriptor)
   :bloom-filter? (.isBloomfilter hcolumn-descriptor)})

(defn table-map-for [htable]
  (let [table-descriptor (.getTableDescriptor htable)
        hcolumn-descriptors (.getFamilies table-descriptor)
        table-map {:name (.getNameAsString table-descriptor)}
        family-maps (map #(family-map-from-hcolumn-descriptor %) hcolumn-descriptors )]
    (assoc table-map :families (into-array family-maps))))

(defn dump-table-to-ruby [output-dir htable]
  (let [file (str output-dir "/tables.rb")]
    (spit file (str (.getTableDescriptor htable)))))
                                                                            
(defn dump-tables-to-ruby [output-dir table-names]
  (map #(dump-table-to-ruby output-dir %) table-names))
