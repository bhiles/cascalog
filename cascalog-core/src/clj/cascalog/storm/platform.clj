(ns cascalog.storm.platform
  (:require [cascalog.logic.predicate :as pred]
            [cascalog.logic.platform :refer (compile-query  IPlatform)]
            [cascalog.logic.parse :as parse]
            [jackknife.core :as u]
            [jackknife.seq :as s]
            [cascalog.logic.def :as d]
            [storm.trident.testing :as tri]
            [marceline.storm.trident :as m])
  (:import [cascalog.logic.parse TailStruct Projection Application
            FilterApplication Unique Join Grouping Rename]
           [cascalog.logic.predicate Generator RawSubquery]
           [cascalog.logic.def ParallelAggregator ParallelBuffer]
           [storm.trident.testing FixedBatchSpout MemoryMapState$Factory]
           [storm.trident TridentTopology TridentState Stream]
           [storm.trident.spout ITridentSpout]
           [cascalog.logic.predicate Operation]
           [storm.trident.operation.builtin MapGet]
           [backtype.storm LocalDRPC]))

;; TODO:

;;    how to re-use a stream so that it doesn't need to be re-created?
;;      - 2 options:
;;             1. realize it while parsing <-  A: No good spot
;;             2. build it into the tap <- A: can't figure out 
;;    - what is a predmacro? mentioned in parse.clj
;;    investigate how to use a tap to feed the topology
;;    splitting streams into multiple outputs
;;    move tridentstate application into the join cascalog type
;;    setup with exec-drpc
;;    wrap functions in taps
;;      - how to pass in topology at the end? pass in sink before
;;        before compiling query
;;      - for time topology should be implicit


;; Later TODO:

;;   enable inline parameters like (= ?pronoun "the")
;;      trouble with parsing "the" since it doesn't appear in the stream

;; TODO: this hasn't yet been tested
(defmacro mk-aggregator
  [op]
  (let [fn-name (symbol (u/uuid))]
    `(do
       (m/defaggregator ~fn-name
         ([batch-id# coll#] (m/emit-fn coll# (~op)))
         ([state# tuple# coll#] (m/emit-fn coll# (~op state# tuple#)))
         ([state# coll#] (m/emit-fn coll# (~op state#))))
       ~fn-name)))

(defmacro mk-combineraggregator
  [init-var combine-var present-var]
  (let [fn-name (symbol (u/uuid))]
    `(do
       (m/defcombineraggregator ~fn-name
         ([] nil)
         ([tuple#] (~init-var))
         ([t1# t2#] (~combine-var t1# t2#)))
       ~fn-name)))

(defmacro mk-mapcat-tridentfn
  [op]
  (let [fn-name (symbol (u/uuid))]
    `(do (m/deftridentfn ~fn-name
           [tuple# coll#]
           (when-let [args# (m/first tuple#)]
             (let [results# (~op args#)]
               (doseq [result# results#]
                 (m/emit-fn coll# result#)))))
         ~fn-name)))

(defmacro mk-map-tridentfn
  [op]
  (let [fn-name (symbol (u/uuid))]
    `(do (m/deftridentfn ~fn-name
           [tuple# coll#]
           (when-let [args# (m/first tuple#)]
             (let [result# (~op args#)]
               (m/emit-fn coll# result#))))
         ~fn-name)))

(defmacro mk-filterfn
  [op]
  (let [fn-name (symbol (u/uuid))]
    `(do (m/deffilter ~fn-name
           [tuple#]
           (when-let [args# (m/first tuple#)]
             (~op args#)))
         ~fn-name)))

(defmulti op-storm
  (fn [op]
    (type op)))

(defmethod op-storm ::d/map
  [op]
  (mk-map-tridentfn op))

(defmethod op-storm ::d/mapcat
  [op]
  (mk-mapcat-tridentfn op))

(defn filter-op-storm
  [op]
  (mk-filterfn op))

(defmulti agg-op-storm
  (fn [op]
    (type op)))

(defmethod agg-op-storm ::d/aggregate
  [op]
  (mk-aggregator op))

(defmethod agg-op-storm ParallelAggregator
  [op]
  (let [ {:keys [init-var combine-var present-var]} op]
       (mk-combineraggregator init-var combine-var present-var)))

;; Extending to-predicate functions to allow for additional types of
;; operations
(defmethod pred/to-predicate TridentState
  [op input output]
  (Operation. op input output))

(m/deftridentfn identity-args
  [tuple coll]
  (when-let [v (m/vals tuple)]
    (apply m/emit-fn coll v)))

(defn rename-fields [stream input output]
  (-> stream (m/each input identity-args output)
      (m/project output)))

(defprotocol IRunner
  (to-generator [item]))

(extend-protocol IRunner

  Projection
  (to-generator [{:keys [source fields]}]
    source)

  Generator
  (to-generator [{:keys [gen]}]
    gen)

  Application
  (to-generator [{:keys [source operation]}]
    (let [{:keys [drpc topology stream]} source
          {:keys [op input output]} operation]
      (if (instance? TridentState op)
        (do
          (let [updated-stream (m/state-query stream op input (MapGet.) output)]
            {:drpc drpc :topology topology :stream updated-stream}))
        (let [revised-op (op-storm op)]
          (let [updated-stream (m/each stream input revised-op output)]
            {:drpc drpc :topology topology :stream updated-stream})))))

  FilterApplication
  (to-generator [{:keys [source filter]}]
    (prn "source is " source )
    (prn "filter is " filter)
    (let [{:keys [drpc topology stream]} source
          {:keys [op input]} filter
          revised-op (filter-op-storm op)]
      {:drpc drpc :topology topology :stream (m/each stream input revised-op)}))

  Grouping
  (to-generator [{:keys [source aggregators grouping-fields options]}]
    (let [{:keys [drpc topology stream]} source
          {:keys [op input output]} (first aggregators)]
      ;; TODO: need to handle multiple aggregators use chained agg
      (let [revised-op (agg-op-storm op)
            revised-grouping-fields (if (empty? grouping-fields) input grouping-fields)
            updated-stream (-> stream
                               (m/group-by revised-grouping-fields)
                               (m/persistent-aggregate (MemoryMapState$Factory.)
                                                       input
                                                       revised-op
                                                       output)
                               (m/debug))]
        {:drpc drpc :topology topology :stream updated-stream})))

  TailStruct
  (to-generator [{:keys [node available-fields]}]
    (prn "inside Tailstruct")
    (let [{:keys [drpc topology stream]} node]
      (if (instance? TridentState stream)
        node
        {:drpc drpc :topology topology :stream (m/project stream available-fields)}))))

(defprotocol IGenerator
  (generator [x output]))

(extend-protocol IGenerator
  
  ;; storm generators
  
  ITridentSpout
  (generator [gen output]
    (let [topology (TridentTopology.)
          stream (-> (m/new-stream topology (u/uuid) gen)
                     (rename-fields (.getOutputFields gen) output))]
      {:drpc nil :topology topology :stream stream}))

  Stream
  (generator [stream output]
    (let [updated-stream (-> stream
                             (rename-fields (.getOutputFields stream) output))]
      {:drpc nil :topology nil :stream updated-stream}))
  
  TridentTopology
  (generator [topology output]
    (let [local-drpc (LocalDRPC.)
          drpc-name (u/uuid)
          stream (-> (m/drpc-stream topology drpc-name local-drpc)
                     (rename-fields ["args"] output))]
      {:drpc [local-drpc drpc-name] :topology topology :stream stream}))
  
  ;; These generators act differently than the ones above
  TailStruct
  (generator [sq output]
    (prn "generator TailStruct " sq output)
    (compile-query sq))

  RawSubquery
  (generator [sq output]
    (prn "generator RawSubquery " sq output)
    (generator (parse/build-rule sq) output)))


(defn mk-fixed-batch-spout [field-names]
  (FixedBatchSpout.
   ;; Name the tuples that the spout will emit.
   (apply m/fields field-names)
   3
   (into-array (map m/values '("lord ogdoad"
                               "master of level eight shadow world"
                               "the willing vessel offers forth its pure essence")))))

(defrecord StormPlatform []
  IPlatform
  (generator? [_ x]
    (satisfies? IGenerator x))

  (generator [_ gen output options]
    (prn "before generator")
    (generator gen output))

  (to-generator [_ x]
    (to-generator x)))
