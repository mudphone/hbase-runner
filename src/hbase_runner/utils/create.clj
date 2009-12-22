(ns hbase-runner.utils.create
  (:import [org.apache.hadoop.hbase HColumnDescriptor HTableDescriptor])
  (:import [org.apache.hadoop.hbase.client HTable])
  (:use hbase-runner.utils.clojure)
  (:use hbase-runner.utils.file))

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

(defn spit-table-maps [the-hbase-config table-names file-path]
  (println "Dumping files to:" file-path)
  (doseq [table-name table-names]
    (println "  ...dumping:" table-name)
    (let [table-map (table-map-for (HTable. the-hbase-config table-name))]
      (spit file-path table-map))))

(defn column-descriptor-from [family-map]
  (println "using family-map:" family-map)
  (HColumnDescriptor. (.getBytes (:name family-map))
                      (:versions family-map)
                      (:compression family-map)
                      (:in-memory? family-map)
                      (:block-cache-enabled? family-map)
                      (:blocksize family-map)
                      (:ttl family-map)
                      (:bloom-filter? family-map)))

(defn hydrate-table-map [the-hbase-admin table-map]
  (let [column-family-maps (:families table-map)
        column-descriptors (map column-descriptor-from column-family-maps)
        table-descriptor (HTableDescriptor. (:name table-map))
        add-family (fn [family]
                     (.addFamily table-descriptor family))]
    (dorun (map add-family column-descriptors))
    (.createTable the-hbase-admin table-descriptor)))

(defn hydrate-table-maps [the-hbase-admin table-maps]
  (doseq [table-map table-maps]
    (hydrate-table-map the-hbase-admin table-map)))
