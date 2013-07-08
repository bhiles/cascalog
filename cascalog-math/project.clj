(def cascalog-version "2.0.0-SNAPSHOT")

(defproject cascalog/cascalog-math cascalog-version
  :description "Math modules for Cascalog."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :dependencies [[org.clojure/algo.generic "0.1.1"]]
  :profiles {:1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :provided {:dependencies [[cascalog/cascalog-core ~cascalog-version]]}
             :dev {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]
                                  [cascalog/midje-cascalog ~cascalog-version]]
             :ci-dev {:dependencies [[org.apache.hadoop/hadoop-core "1.1.2"]
                                     [cascalog/midje-cascalog ~cascalog-version]]}
                   :plugins [[lein-midje "3.0.1"]]}})
