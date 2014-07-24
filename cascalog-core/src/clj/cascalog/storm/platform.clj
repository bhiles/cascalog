(ns cascalog.storm.platform
  (:require [cascalog.logic.predicate]
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
           [storm.trident.testing MemoryMapState$Factory]
           [storm.trident TridentTopology]))

;;(defn to-tuple)

;;(defn to-tuples)

;;(defn to-tuples-filter-nullable)

;;(defn select-fields)

;;(defn select-fields-w-default)

;;(defn extract-values)

;;(defn inner-join)

;;(defn left-join)

;;(defn left-excluding-join)

;;(defn outer-join)

;;(defn join)

;;(defn smallest-arity [fun])

;(defn tuple-sort)

(defmulti op-storm
  (fn [op]
    (type op)))

(defmethod op-storm ::d/map
  [op]  
  (m/deftridentfn split-args
    [tuple coll]
    (when-let [args (m/first tuple)]
      (let [result (apply op args)]
        (m/emit-fn coll result)))))

(defmacro mk-tridentfn
  [op]
  (let [fn-name (symbol (str op "___"))]
    `(do (m/deftridentfn ~fn-name
           [tuple# coll#]
           (when-let [args# (m/first tuple#)]
             (let [results# (apply ~op args#)]
               (doseq [result# results#]
                 (m/emit-fn coll# result#)))))
         ~fn-name)))

(defmethod op-storm ::d/mapcat
  [op]
  (mk-tridentfn op))

(defmulti agg-op-storm
  (fn [op]
    (type op)))

(defmethod agg-op-storm ::d/aggregate
  [op]
  (m/defcombineraggregator count-words
    ([] (op))
    ([tuple] (op tuple))
    ([t1 t2] (op t1 t2))))

(defprotocol IRunner
  (to-generator [item]))

(extend-protocol IRunner

  Projection
  (to-generator [{:keys [source fields]}]
    source)

  Generator
  (to-generator [{:keys [gen]}]
    (let [topology (TridentTopology.)
          stream (m/new-stream topology (u/uuid) gen)]
      {:topology topology :stream stream}))

  Application
  (to-generator [{:keys [source operation]}]
    (prn "source is " source)
    (let [{:keys [topology stream]} source
          {:keys [op input output]} operation
          revised-op (op-storm op)]         
      (prn "op is " op " with type " (type op))
      (prn "rop is " revised-op " with type " (type revised-op))
      (let [updated-stream (m/each stream input revised-op output)]
        {:topology topology :stream updated-stream})))

  FilterApplication
  (to-generator [{:keys [source filter]}]
    (let [{:keys [op input]} filter]
      (m/each source input op)))

  Grouping
  (to-generator [{:keys [source aggregators grouping-fields options]}]
    (let [{:keys [topology stream]} source
          {:keys [op input output]} (first aggregators)
          revised-op (agg-op-storm op)
          updated-stream (-> (m/group-by stream grouping-fields)
                             (m/persistent-aggregate (MemoryMapState$Factory.)
                                                     input
                                                     revised-op
                                                     output))]
      {:topology topology :stream updated-stream}))

  TailStruct
  (to-generator [{:keys [node available-fields]}]
    node))

(defprotocol IGenerator
  (generator [x]))

(extend-protocol IGenerator
  
  ;; A bunch of generators that finally return  a seq
  clojure.lang.IPersistentVector
  (generator [v]
    (generator (or (seq v) ())))
  
  clojure.lang.ISeq
  (generator [v] v)

  java.util.ArrayList
  (generator [coll]
    (generator (into [] coll)))  
  
  ;; These generators act differently than the ones above
  TailStruct
  (generator [sq]
    (compile-query sq))

  RawSubquery
  (generator [sq]
    (generator (parse/build-rule sq))))

(defrecord StormPlatform []
  IPlatform
  (generator? [_ x]
    (satisfies? IGenerator x))

  (generator [_ gen output options]
    (tri/feeder-spout output))

  (to-generator [_ x]
    (to-generator x)))
