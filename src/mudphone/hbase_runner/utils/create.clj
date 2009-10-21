(ns mudphone.hbase-runner.utils.create
  (:import [org.apache.hadoop.hbase.client HTable])
  (:use mudphone.hbase-runner.utils.clojure)
  (:use mudphone.hbase-runner.utils.file))

(def *output-dir* "/Users/koba/work/clojure/hbase-runner/output")

(defn- table-descriptor-for [table-name]
  (.getTableDescriptor (HTable. table-name)))

(defn- compression-name-for [hcolumn-descriptor]
  (.getName (.getCompression hcolumn-descriptor)))

(defn- family-map-from-hcolumn-descriptor [hcolumn-descriptor]
  {:name (.getNameAsString hcolumn-descriptor)
   :compression (compression-name-for hcolumn-descriptor)
   :versions (.getMaxVersions hcolumn-descriptor)
   :ttl (.getTimeToLive hcolumn-descriptor)
   :blocksize (.getBlocksize hcolumn-descriptor)
   :in-memory (.isInMemory hcolumn-descriptor)
   :block-cache-enabled (.isBlockCacheEnabled hcolumn-descriptor)})

(defn table-map-for [table-name]
  (let [table-descriptor (table-descriptor-for table-name)
        hcolumn-descriptors (.getFamilies table-descriptor)
        table-map {:name (.getNameAsString table-descriptor)}
        family-maps (map #(family-map-from-hcolumn-descriptor %) hcolumn-descriptors )]
    (assoc table-map :families (into-array family-maps))))

(defn dump-table-to-ruby [table-name]
  (let [file (str *output-dir* "/tables.rb")]
    (spit file (str (table-descriptor-for table-name)))))
                                                                            
(defn dump-tables-to-ruby [table-names]
  (map dump-table-to-ruby table-names))

(defn hydrate-table-maps-from [file]
  (map read-clojure-str (lines-of-file file)))
