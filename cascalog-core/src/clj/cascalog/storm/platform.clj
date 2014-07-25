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
           [storm.trident.testing FixedBatchSpout MemoryMapState$Factory]
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

(defmacro mk-combine
  [init-var combine-var present-var]
  (let [fn-name (symbol (str init-var "combagg" "___"))]
    (prn "init var is " init-var)
    `(do
       (m/defcombineraggregator ~fn-name
         ([]
            (prn "combagg first arg")
            nil)
         ([tuple#]
            (prn "combagg second arg and init-var " ~init-var " and tuple " tuple#)
            (~init-var))
         ([t1# t2#]
            (prn "combagg third agg is " ~combine-var " and t1 " t1# " and t2 " t2# " and rs " (~combine-var t1# t2#)  )
            (~combine-var t1# t2#))
         )
       ~fn-name)))

(defmacro mk-tridentfn
  [op]
  (let [fn-name (symbol (str op "___"))]
    `(do (m/deftridentfn ~fn-name
           [tuple# coll#]
           (prn "tuple is " tuple#)
           (when-let [args# (m/first tuple#)]
             (prn "args are " args#)
             (let [results# (~op (apply str args#))]
               (prn "results are " results#)
               (prn "coll is " coll#)
               (m/emit-fn coll# "abc"))))
         ~fn-name)))

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

(defmethod op-storm ::d/mapcat
  [op]
  (mk-tridentfn op))

(defmulti agg-op-storm
  (fn [op]
    (type op)))

(defmethod agg-op-storm ParallelAggregator
  [op]
  (let [ {:keys [init-var combine-var present-var]} op]
       (mk-combine init-var combine-var present-var)))    

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
          {:keys [op input output]} (first aggregators)]
      (prn "op is " op)
      (prn "grouping fields  are " grouping-fields)
      (prn "Groupgin input fields are " input)
      (prn "output fields are " output)
      (let [revised-op (agg-op-storm op)
            updated-stream (-> (m/group-by stream input)
                               (m/persistent-aggregate (MemoryMapState$Factory.)
                                                       input
                                                       revised-op
                                                       output))]
        {:topology topology :stream updated-stream})))

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
    (prn "generator output fields are " output)
    (doto (mk-fixed-batch-spout output)
      (.setCycle true)))

  (to-generator [_ x]
    (to-generator x)))
