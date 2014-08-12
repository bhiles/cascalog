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
            [marceline.storm.trident :as m]
            [backtype.storm.clojure :refer (to-spec normalize-fns)])
  (:import [cascalog.ops IdentityBuffer]
           [cascalog.storm.platform StormPlatform]
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

(def my-spout (tri/feeder-spout ["line"]))

(defn build-topology [spout]
  (let [trident-topology (TridentTopology.)
        word-counts (-> (m/new-stream trident-topology "word-counts" spout)
                        (m/each ["line"] (splat/mk-map-tridentfn (fn [x] (str x "!!!"))) ["ex-words"])
                        (m/each ["ex-words"] (splat/mk-map-tridentfn (fn [x] (str x "???")))  ["ex-q-words"])
                        (m/debug))]
    trident-topology))

;; (def cluster (LocalCluster.))
;; (def local-drpc (LocalDRPC.))

(defn test-topo-2 []
  (api/<- [?w3]
          (my-spout ?w)
          ((fn [x] (str x "???")) ?w :> ?w2)
          ((fn [x] (str x "!!!")) ?w2 :> ?w3)))

(defn test-topo []
  (api/<- [?w2]
          (my-spout ?l)
          ((api/mapcatfn [x] (clojure.string/split x #" ")) ?l :> ?w)
          ((api/mapfn [x] (str x "!!!")) ?w :> ?w2)
          ((api/mapfn [x] (str x "???")) ?w2 :> ?w3)
          ((api/filterfn [x] (= "hi" x)) ?w)
          (ops/count ?w :> ?count)
          ))

 (def query (plat/compile-query (test-topo)))

;; test to see that the name is the same:
;; (take 2 (repeatedly #(splat/mk-map-tridentfn "abc" + "a" "b")))

(defn run! []
   (def cluster (LocalCluster.))
  ;;(.submitTopology cluster "wordcounter" {} (.build (build-topology my-spout)))
   (.submitTopology cluster "wordcounter" {} (.build (:topology query)))
   (tri/feed my-spout [["hi"]])
)

(defn create-repos []
  (api/<- [?json]
          (my-spout ?line)
          ((api/mapfn [t]
                      (prn "text is " t)
                      (json/read-str t)) ?line :> ?json)
          ((api/filterfn [json]
                         (prn "json is " json)
                         (and (= (get json "type") "CreateEvent")
                                     (= (-> ( get json "payload") (get "ref_type")) "repository")
                                     )) ?json)          
          ))

(defn create-repos-2 []
  (api/<- [?repo_name ?created_at ?login !repo_lang]
          (my-spout ?line)
          ((api/mapfn [t]
                      (prn "text is " t)
                      (json/read-str t)) ?line :> ?json)
          ((api/filterfn [json]
                         (prn "json is " json )
                         (and (= (get json "type") "CreateEvent")
                                     (= (-> ( get json "payload") (get "ref_type")) "repository")
                                     )) ?json)          
          ((api/mapfn [json]
                      (prn "json type is " (type json))
                       (let [login (-> (get json "actor_attributes")
                                       (get "login"))
                             created_at (get json "created_at")
                             repo (get json "repository")
                             repo_name (str (get repo "owner") "/" (get repo "name"))
                             repo_lang (get repo "language")]
                         [login created_at repo_name repo_lang]
                         ))
           ?json :> ?login ?created_at ?repo_name !repo_lang)
          ))


