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
    (prn "inside to-runner generator " gen)
    gen)

  Application
  (to-generator [{:keys [source operation]}]
    (prn "source is " source)
    (let [{:keys [drpc topology stream]} source
          {:keys [op input output]} operation]
      (if (instance? TridentState op)
        (do
          (prn "inside Trident state application")
          (let [updated-stream (m/state-query stream op input (MapGet.) output)]
            {:drpc drpc :topology topology :stream updated-stream}))
        (let [revised-op (op-storm op)]
          (prn "op is " op " with type " (type op))
          (prn "rop is " revised-op " with type " (type revised-op))
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
  (generator [x]))

(extend-protocol IGenerator
  
  ;; storm generators
  
  FixedBatchSpout
  (generator [gen]
    (let [topology (TridentTopology.)
          stream (m/new-stream topology (u/uuid) gen)]
      (prn "inside IGen and gen is " gen)
      {:drpc nil :topology topology :stream stream}))

  TridentTopology
  (generator [topology]
    (let [local-drpc (LocalDRPC.)
          ;; TODO: need to remove the hardcoded drpc title
          stream (-> (m/drpc-stream topology "words" local-drpc)
                     ;; TODO: allow output fields to be passed into
                     ;; here (for output fields of identity args
                     (m/each ["args"] identity-args ["?args"]))]
      {:drpc local-drpc :topology topology :stream stream}))
  
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
    (generator gen))

  (to-generator [_ x]
    (to-generator x)))
