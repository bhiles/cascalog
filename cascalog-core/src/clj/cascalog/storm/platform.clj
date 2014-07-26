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
           [storm.trident TridentTopology TridentState]
           [cascalog.logic.predicate Operation]
           [storm.trident.operation.builtin MapGet]
           [backtype.storm LocalDRPC]))

(defmacro mk-paragg-combineraggregator
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

(defmulti op-storm
  (fn [op]
    (type op)))

(defmethod op-storm ::d/map
  [op]
  (mk-map-tridentfn op))

(defmethod op-storm ::d/mapcat
  [op]
  (mk-mapcat-tridentfn op))

(defmulti agg-op-storm
  (fn [op]
    (type op)))

(defmethod agg-op-storm ParallelAggregator
  [op]
  (let [ {:keys [init-var combine-var present-var]} op]
       (mk-paragg-combineraggregator init-var combine-var present-var)))

;; Extending to-predicate functions to allow for additional types of
;; operations
(defmethod pred/to-predicate TridentState
  [op input output]
  (Operation. op input output))

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
    (let [{:keys [op input]} filter]
      (m/each source input op)))

  Grouping
  (to-generator [{:keys [source aggregators grouping-fields options]}]
    (let [{:keys [drpc topology stream]} source
          {:keys [op input output]} (first aggregators)]
      (let [revised-op (agg-op-storm op)
            ;;TODO: currently grouping by input, but sometimes there
            ;;is a grouping field           
            updated-stream (-> (m/project stream input)
                               (m/group-by input)
                               (m/persistent-aggregate (MemoryMapState$Factory.)
                                                       input
                                                       revised-op
                                                       output))]
        {:drpc drpc :topology topology :stream updated-stream})))

  TailStruct
  (to-generator [{:keys [node available-fields]}]
    node))


(m/deftridentfn
  identity-args
  [tuple coll]
  (when-let [args (m/first tuple)]
    (m/emit-fn coll args)))

(defprotocol IGenerator
  (generator [x output]))

(extend-protocol IGenerator
  
  ;; storm generators
  
  FixedBatchSpout
  (generator [gen output]
    (let [topology (TridentTopology.)
          stream (-> (m/new-stream topology (u/uuid) gen)
                     (m/each (.getOutputFields gen) identity-args output)
                     (m/project output))]
      {:drpc nil :topology topology :stream stream}))

  TridentTopology
  (generator [topology output]
    (let [local-drpc (LocalDRPC.)
          drpc-name (u/uuid)
          stream (-> (m/drpc-stream topology drpc-name local-drpc)
                     (m/each ["args"] identity-args output)
                     (m/project output))]
      {:drpc [local-drpc drpc-name] :topology topology :stream stream}))
  
  ;; These generators act differently than the ones above
  TailStruct
  (generator [sq output]
    (compile-query sq))

  RawSubquery
  (generator [sq output]
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
    (generator gen output))

  (to-generator [_ x]
    (to-generator x)))
