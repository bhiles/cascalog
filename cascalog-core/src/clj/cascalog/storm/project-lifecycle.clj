(ns cascalog.storm.project-lifecycle
  (:use [clojure.pprint]
        [clojure.java.io])
  (:require [cascalog.api :as api]
            [cascalog.logic.ops :as ops]
            [clojure.data.json :as json]
            [cascalog.logic.vars :as v]
            [clj-time.format :as time]
            [cascalog.logic.platform :as plat]
            [cascalog.storm.platform :as splat]
            [storm.trident.testing :as tri]
            [jackknife.core :as u]
            [jackknife.seq :as s]
            [marceline.storm.trident :as m]
            [marceline.storm.builtin :as mops]
            [backtype.storm.clojure :refer (to-spec normalize-fns)])
  (:import [cascalog.ops IdentityBuffer]
           [cascalog.storm.platform StormPlatform DRPCStateTap]
           [storm.trident TridentTopology TridentState Stream]
           [backtype.storm LocalDRPC LocalCluster]
           [marceline.storm.trident.clojure ClojureFunction]))


(plat/set-context! (StormPlatform.))




;; (def data-source (api/hfs-textline "resources/github"
;; :source-pattern "/data/2014-07-*"))


(defmacro doseq-indexed [index-sym [item-sym coll] & body]
  `(let [idx-atom# (atom 0)]
     (doseq [~item-sym ~coll]
       (let [~index-sym (deref idx-atom#)]
         ~@body
         (swap! idx-atom# inc)))))

(defn feed-spout-file!
  [spout file]
  (with-open [rdr (reader file)]
    (let [chunk-size 100
          report-size 10]
      (doseq-indexed idx [line-chunk (partition-all chunk-size (line-seq rdr))]
                     (tri/feed spout (map (fn [x] [x]) line-chunk))
                     (if (and (= (mod idx report-size) 0) (> idx 0))
                       (do (prn "Feed up to line: " (* chunk-size idx))
                           (Thread/sleep 1000)
                           ))))))



(defn build-topology [spout]
  (let [trident-topology (TridentTopology.)
        word-counts (-> (m/new-stream trident-topology "word-counts" spout)
                        (m/each ["line"] (splat/mk-map-tridentfn (fn [x] (str x "!!!"))) ["ex-words"])
                        (m/each ["ex-words"] (splat/mk-map-tridentfn (fn [x] (str x "???")))  ["ex-q-words"])
                        (m/debug))]
    trident-topology))

(def my-spout (tri/feeder-spout ["line"]))

(def json-stream-orig
  (splat/with-topology (api/<- [?json]
                         (my-spout ?line)
                         ((api/mapfn [t] (json/read-str t)) ?line :> ?json))))

(def json-query (plat/compile-query json-stream-orig))
(def json-stream (:stream json-query))

(def repo-existence
  (api/<- [?repo_name ?count]
          (json-stream ?json2)
          ((api/filterfn [json] (and (= (get json "type") "CreateEvent")
                                     (= (-> ( get json "payload") (get "ref_type"))
                                        "repository")))
           ?json2)
          ((api/mapfn [json]
                       (let [repo (get json "repository")
                             repo_name (str (get repo "owner") "/" (get repo "name"))]
                         repo_name))
           ?json2 :> ?repo_name)
          (ops/count ?repo_name :> ?count)))

(def repo-existence-query (plat/compile-query repo-existence))

(def stars
  (api/<- [?repo_name ?created_at ?login]
          (json-stream ?json2)

          ((api/mapfn [json]
                       (let [login (-> (get json "actor_attributes")
                                       (get "login"))
                             created_at (get json "created_at")
                             repo (get json "repository")
                             repo_name (str (get repo "owner") "/" (get repo "name"))]
                         [repo_name created_at login]
                         ))
           ?json2 :> ?repo_name ?created_at ?login)
          ))

(api/defmapfn iso->day
  [iso-date]
  (time/unparse (time/formatter "yyyy-MM-dd")
                (time/parse (time/formatters :date-time-parser)
                            iso-date)))

(def stars-query (plat/compile-query stars))
(def stars-stream (:stream stars-query))

(api/defaggregatefn merge-day-vals
  ([] {})
  ([state tuple]
     (let [[day] tuple
           m {day 1}]
       (merge-with + state m)))
  ([state] [state]))

(def stars-over-time
  (api/<- [?repo_name2 ?merged_day_count]
          (stars-stream ?repo_name2 ?star_created_at2 _)
          (iso->day ?star_created_at2 :> ?day)
          (merge-day-vals ?day :> ?merged_day_count)))

(def stars-time-query (plat/compile-query stars-over-time))
(def stars-time-state (:stream stars-time-query))

(def stars-totals
  (api/<- [?repo_name2 ?star_count]
          (stars-stream ?repo_name2 _ _)
          (ops/count :> ?star_count)))

(def stars-totals-query (plat/compile-query stars-totals))
(def stars-totals-state (:stream stars-totals-query))

(def repo-existence-state (:stream repo-existence-query))

(def drpc-repo-existence-tap (DRPCStateTap. repo-existence-state
                                            (:topology json-query)))

(def drpc-stars-time-tap (DRPCStateTap. stars-time-state
                                            (:topology json-query)))


(def drpc-existing-repo-total-count-2
  (api/<- [?repo_name3 ?merged_day_count ]
          (drpc-stars-time-tap ?repo_name3 ?day2 ?day_star_count2)        
          (merge-day-vals ?day2 ?day_star_count2 :> ?merged_day_count)))


(comment
  ;; after this function, we need to mapcat the output into it's
  ;; output values
  (api/defaggregatefn first-10
           ([] [])
           ([state tuple]
              (let [max-size 10
                    new-state (reverse (sort (cons (first tuple) state)))]
                (if (> (count new-state) max-size)
                  (take max-size new-state)
                  new-state)))
           ([state] [state])))


(def drpc-existing-repo-total-count
  (api/<- [?repo_name3 ?total_star_count ?day_counts]
          (drpc-repo-existence-tap ?repo_name3 _)
          (stars-totals-state ?repo_name3 :> ?total_star_count)
          (stars-time-state ?repo_name3 :> ?day_counts)
          ((mops/first-n 10 "?total_star_count" true) :< ?total_star_count)))

(def total-count-query (plat/compile-query drpc-existing-repo-total-count))
(def drpc-total-count (:drpc total-count-query))
;; (tri/exec-drpc (first drpc-total-count) (second drpc-total-count) "")

(defn run! []
   (def cluster (LocalCluster.))
  ;;(.submitTopology cluster "wordcounter" {} (.build (build-topology my-spout)))
   (.submitTopology cluster "wordcounter" {} (.build (:topology json-query)))
   ;;(tri/feed my-spout [["hi"]])
   (feed-spout-file! my-spout "/Users/bennetthiles/src/github-data-challenge/resources/github/test.json")
   
)
