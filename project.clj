(defproject hbase-runner "0.1.0-SNAPSHOT"
  :description "An HBase shell replacement, with Clojure LISP REPL flavor."
  :url "http://github.com/mudphone/hbase-runner"
  :repositories [["clojure-releases" "http://build.clojure.org/releases/"]]
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0"]
                 [clj-stacktrace "0.1.0-SNAPSHOT"]
                 [jline "0.9.94" :exclusions [junit/junit]]
                 [org.clojars.mudphone/hbase "0.20.3"]
                 [org.clojars.mudphone/hadoop "0.20.1-hdfs127-core"]
                 [org.clojars.mudphone/zookeeper "3.2.2"]
                 [commons-io "1.4"]
                 [commons-logging "1.0.4"]
                 [log4j "1.2.15" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]
                 ]
  :dev-dependencies [[lein-clojars "0.5.0-SNAPSHOT"]
                     [swank-clojure "1.1.0-SNAPSHOT"]
                     ;; [leiningen/lein-swank "1.0.0-SNAPSHOT"]
                     ]
)
