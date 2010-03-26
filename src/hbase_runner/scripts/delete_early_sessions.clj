(ns hbase-runner.scripts.delete-early-sessions
  (:import (org.apache.hadoop.hbase.client Delete))
  (:use [clojure.contrib
         [str-utils :as str-utils]]
        (hbase-runner hbase-repl)
        (hbase-runner.hbase table)))

(def SESSION-STREAM-TABLE "koba_development_session_event_stream")
(def SESSION-SCAN-BATCH-SIZE 1000)
(def CUTOFF-TS "1256840968518") ;; Cutoff: Thu Oct 29 18:29:28 UTC 2009

;; SHOULD BE ADDED TO MAIN API
(defn simple-delete-row [hbase-table-name row-id]
  (let [table (hbase-table hbase-table-name)
        delete (Delete. (.getBytes row-id))]
    (.delete table delete)))
;; END SHOULD BE ADDED TO MAIN API

(defn timestamp-from [row-id]
  (first (str-utils/re-split #":" row-id)))

(defn session-id-of [session-map]
  (first (keys session-map)))

(defn event-ids-of [session-map]
  (apply concat (vals (get-in (first (vals session-map)) ["consumer_event" "id"]))))

(defn last-event-id-of [session-map]
  (last (sort (event-ids-of session-map))))

(defn delete-session-if-below-cutoff [session-map]
  (let [last-event-id (last-event-id-of session-map)]
    (if (< (Long/parseLong (timestamp-from last-event-id))
           (Long/parseLong CUTOFF-TS))
      (let [session-id (session-id-of session-map)]
        (println "deleting session:" session-id " last-event-id:" last-event-id)
        (simple-delete-row SESSION-STREAM-TABLE session-id)
        ))))

(defn delete-if-early-starting-at [start-session-id]
  (let [session-maps (scan-results SESSION-STREAM-TABLE {:start-row start-session-id
                                                         :limit SESSION-SCAN-BATCH-SIZE
                                                         :versions :all
                                                         :columns ["consumer_event:id"]})]
    (if (seq session-maps)
      (do
        (dorun
         (pmap delete-session-if-below-cutoff session-maps))
        (let [last-session-id (session-id-of (last session-maps))]
          (if-not (= last-session-id start-session-id)
            (do
              (println "starting another batch of" SESSION-SCAN-BATCH-SIZE "sessions.  last session-id:" last-session-id)
              (recur last-session-id)))
          ))
      )
    ))
