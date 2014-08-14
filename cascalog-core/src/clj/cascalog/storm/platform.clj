(ns cascalog.storm.platform
  (:require [cascalog.logic.predicate :as pred]
            [cascalog.logic.platform :refer (compile-query  IPlatform)]
            [cascalog.logic.platform :as plat]
            [cascalog.logic.parse :as parse]
            [cascalog.logic.vars :as v]
            [jackknife.core :as u]
            [jackknife.seq :as s]
            [cascalog.logic.def :as d]
            [storm.trident.testing :as tri]
            [marceline.storm.trident :as m]
            [backtype.storm.testing :as st]
            [backtype.storm.util :as util]
            )
  (:import [cascalog.logic.parse TailStruct Projection Application
            FilterApplication Unique Join Grouping Rename]
           [cascalog.logic.predicate Generator RawSubquery]
           [cascalog.logic.def ParallelAggregator ParallelBuffer]
           [storm.trident.testing FixedBatchSpout MemoryMapState$Factory Split]
           [storm.trident.operation.builtin Count FirstN]
           [storm.trident TridentTopology TridentState Stream]
           [storm.trident.spout ITridentSpout IBatchSpout]
           [cascalog.logic.predicate Operation FilterOperation]
           [storm.trident.operation.builtin MapGet TupleCollectionGet]
           [backtype.storm LocalDRPC LocalCluster]))

;; TODO:

;;    improve in-memory to wait for completion of feed
;;       see if feeder-spout with wait will allow premature feed
;;       see if I can set a wait until topology completion?
;;           
;;    potentially pull arrays out and feed them in directly

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

;; TODO: this hasn't yet been tested and it uses the old single
;; implementation style
(defmacro mk-aggregator-2
  [op]
  (let [fn-name (symbol (u/uuid))]
    `(do
       (m/defaggregator ~fn-name
         ([batch-id# coll#] (m/emit-fn coll# (~op)))
         ([state# tuple# coll#] (m/emit-fn coll# (~op state# tuple#)))
         ([state# coll#] (m/emit-fn coll# (~op state#))))
       ~fn-name)))

(defn mk-aggregator
  [op]
  (let [fn-name (gensym "aggregator")]
    (intern *ns* fn-name (fn []
                           (fn [conf context]
                             (reify storm.trident.operation.Aggregator
                               (init [_ batch-id coll] (apply m/emit-fn coll (s/collectify (op))))
                               (aggregate [_ state tuple coll]
                                 (apply m/emit-fn coll (s/collectify (op state tuple))))
                               (complete [_ state coll] (apply m/emit-fn coll (s/collectify (op state))))))))
    (let [my-var (ns-resolve *ns* fn-name)]
      (m/clojure-aggregator* my-var []))))

(defn mk-reduceraggregator
  [op]
  (let [fn-name (gensym "reduceraggregator")]
    (intern *ns* fn-name (fn []
                           (reify storm.trident.operation.ReducerAggregator
                             (init [_] (op))
                             (reduce [_ state tuple]
                               (when-let [v (into [] (m/vals tuple))]
                                 (op state v))))))
    (let [my-var (ns-resolve *ns* fn-name)]
      (m/clojure-reducer-aggregator* my-var []))))

(defn mk-combineraggregator
  [init-var combine-var present-var]
  (let [fn-name (gensym "combineraggregator")]
    (intern *ns* fn-name (fn []
                           (reify storm.trident.operation.CombinerAggregator
                             (zero [_] nil)
                             (init [_ tuple]
                               (apply init-var tuple))
                             (combine [_ t1 t2] (combine-var t1 t2)))))
    (let [my-var (ns-resolve *ns* fn-name)]
      (m/clojure-combiner-aggregator* my-var []))))

(defn mk-mapcat-tridentfn
  [op]
  (let [fn-name (gensym "mapcattridentfn")]
    (intern *ns* fn-name (fn []
                           (fn [conf context]
                             (reify storm.trident.operation.Function
                               (execute [_ tuple coll]
                                 (when-let [args (into [] (m/vals tuple))]
                                   (let [results (apply op args)]
                                     (doseq [result results]
                                       (apply m/emit-fn coll (s/collectify result))))))))))
    (let [my-var (ns-resolve *ns* fn-name)]
      (m/clojure-tridentfn* my-var []))))

(defn mk-map-tridentfn
  [op]
  (let [fn-name (gensym "maptridentfn")]
    (intern *ns* fn-name (fn []
                           (fn [conf context]
                             (reify storm.trident.operation.Function
                               (execute [_ tuple coll]
                                 (when-let [args (into [] (m/vals tuple))]
                                   (let [result (apply op args)]
                                     (apply m/emit-fn coll (s/collectify result)))))))))
    (let [my-var (ns-resolve *ns* fn-name)]
      (m/clojure-tridentfn* my-var []))))

(defn select-fields-w-default
  [fields tuple]
  (map
   (fn [field]
     (if (v/cascalog-var? field)
       (m/get tuple field)
       field))
   fields))

(defn mk-filterfn
  [op input]
  (let [fn-name (gensym "filterfn")]
    (intern *ns* fn-name (fn []
                           (fn [conf context]
                             (reify storm.trident.operation.Filter
                               (isKeep [_ tuple]
                                 (if (m/first tuple)
                                   (let [args (select-fields-w-default input tuple)]
                                     (apply op args))
                                   false))))))
    (let [my-var (ns-resolve *ns* fn-name)]
      (m/clojure-filter* my-var []))))

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
  [op input]
  (mk-filterfn op input))

(defmethod pred/to-predicate FirstN
  [op input output]
  (FilterOperation. op input))

(defmulti agg-op-storm
  (fn [op]
    (type op)))

(defmethod agg-op-storm ::d/aggregate
  [op]
  (mk-reduceraggregator op))

(defmethod agg-op-storm ParallelAggregator
  [op]
  (let [ {:keys [init-var combine-var present-var]} op]
    (mk-combineraggregator init-var combine-var present-var)))

;; Extending to-predicate functions to allow for additional types of
;; operations
(defmethod pred/to-predicate TridentState
  [op input output]
  (Operation. op input output))

(m/deffilter filter-first-null
  [tuple]
  (not (nil? (m/first tuple))))

(m/deffilter filter-any-null
  [tuple]
  (not-any? nil? (into [] (m/vals tuple))))

(m/deftridentfn identity-args
  [tuple coll]
  (when-let [v (m/vals tuple)]
    (apply m/emit-fn coll v)))

(m/deftridentfn flatten-vals
  [tuple coll]
  (when-let [vs (into [] (m/first tuple))]
    (doseq [v vs]
      (apply m/emit-fn coll v))))

(m/defcombineraggregator last-value-update
  ([] nil)
  ([tuple] tuple)
  ([t1 t2] t2))

(m/defcombineraggregator all-values-update
  ([] nil)
  ([tuple] [tuple])
  ([t1 t2] (concat t1 t2)))

(defn rename-fields [stream input output]
  (-> stream (m/each input identity-args output)
      (m/project output)))

(defn remove-val [vals v]
  (remove #(some #{%} (s/collectify vals)) v))

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
            (merge source {:stream updated-stream})))
        (do (let [revised-op (op-storm op)]
              (let [updated-stream (-> (m/each stream input revised-op output))]
                (merge source {:stream updated-stream})))))))

  FilterApplication
  (to-generator [{:keys [source filter]}]
    (let [{:keys [drpc topology stream]} source
          {:keys [op input]} filter
          revised-op (filter-op-storm op input)
          stream-input (clojure.core/filter v/cascalog-var? input)]
      (if (instance? FirstN op)
        (merge source {:stream (.applyAssembly stream op)})
        (merge source {:stream (-> (m/each stream stream-input revised-op)
                                   (m/debug)
                                   )}))))

  Join
  (to-generator [{:keys [sources join-fields type-seq options]}]
    (let [[l-source r-source & rest-sources] sources
          l-topology (:topology l-source)
          r-topology (:topology r-source)
          l-stream (:stream l-source)
          r-stream (:stream r-source)
          l-feeders (:feeders l-source)
          r-feeders (:feeders r-source)
          [l-type-seq r-type-seq & rest-type-seqs] type-seq
          [l-fields l-type] l-type-seq
          [r-fields r-type] r-type-seq]
      (let [
            ;; make one of the streams a state
            rand-field [(gensym "random")]
            l-state-fields (remove-val join-fields l-fields)
            l-state-stream (-> l-stream
                               (m/group-by join-fields)
                               (m/persistent-aggregate (MemoryMapState$Factory.)
                                                       l-state-fields
                                                       all-values-update
                                                       rand-field))

            ;; fetch values from l-stream using r-stream values
            r-query-stream (-> (m/state-query r-stream
                                              l-state-stream
                                              join-fields
                                              (MapGet.)
                                              rand-field)
                               (m/each rand-field flatten-vals l-state-fields)
                               (m/each l-state-fields filter-any-null))]
        
        (merge l-source
               {:stream r-query-stream
                :feeders (concat l-feeders r-feeders)}))))
  
  Grouping
  (to-generator [{:keys [source aggregators grouping-fields options]}]
    (let [{:keys [drpc topology stream]} source
          {:keys [op input output]} (first aggregators)]
      (if (instance? TridentState op)
        (do
          (let [updated-stream (m/state-query stream op input (MapGet.) output)]
            (merge source {:stream updated-stream})))        
        ;; TODO: I don't think you can apply partitionAggregate to a DRPC call
        (if (and (= (type op) ::d/aggregate)
                 (not (empty? drpc)))
          (do
            (let [revised-op (agg-op-storm op)
                  revised-grouping-fields (if (empty? grouping-fields)
                                            input grouping-fields)
                updated-stream (-> stream
                                   (m/group-by revised-grouping-fields)
                                   (m/aggregate input revised-op output))]
              (merge source {:stream updated-stream})))
          (if (> (count aggregators) 1)
            (do
              ;; do a chained seq when there are multiple aggregators
              (loop [loop-aggregators aggregators
                     loop-chained-agg (-> stream
                                          (m/group-by grouping-fields)
                                          (.chainedAgg))]
                (let [[agg & rest-agg] loop-aggregators
                      {:keys [op input output]} agg]
                  (let [revised-op (agg-op-storm op)
                        revised-chained-agg (.partitionAggregate
                                             loop-chained-agg
                                             (apply m/fields input)
                                             revised-op
                                             (apply m/fields output))]
                    (if (empty? rest-agg)
                      (merge source {:stream (-> (.chainEnd revised-chained-agg)
                                                 (m/debug))})
                      (recur rest-agg revised-chained-agg))))))
            (do
              (let [revised-op (agg-op-storm op)
                    revised-grouping-fields (if (empty? grouping-fields)
                                              input grouping-fields)
                    updated-stream (-> stream
                                       (m/group-by revised-grouping-fields)
                                       (m/aggregate input
                                                    revised-op
                                                    output))]
                (merge source {:stream updated-stream}))))))))

  TailStruct
  (to-generator [{:keys [node available-fields]}]
    (let [{:keys [drpc topology stream]} node]
      (if (instance? TridentState stream)
        node
        (merge node {:stream (-> (m/project stream available-fields))})))))

(defprotocol IGenerator
  (generator [x output]))

(defrecord VectorFeederSpout [spout vals])
(defrecord DRPCStateTap [state topology])

;; HACK: allows the same topology to be used for multiple generators
(def ^:dynamic *TOPOLOGY* nil)

(defmacro with-topology [& body]
  `(binding [*TOPOLOGY* (TridentTopology.)]
     ~@body))

(extend-protocol IGenerator
  
  ;; storm generators

  clojure.lang.IPersistentVector
  (generator [v output]
    (let [spout (tri/feeder-spout (map #(str % "_spout") output))]
      (.setWaitToEmit spout true)
      (generator
       (VectorFeederSpout. spout v)
       output)))

  VectorFeederSpout
  (generator [gen output]
    (let [spout (:spout gen)
          spout-vals (:vals gen)
          stream (-> (m/new-stream *TOPOLOGY* (u/uuid) spout)
                     (rename-fields (.getOutputFields spout) output))]
      {:topology *TOPOLOGY* :stream stream :feeders [gen]}))
  
  ITridentSpout
  (generator [gen output]
    (let [stream (-> (m/new-stream *TOPOLOGY* (u/uuid) gen)
                     (rename-fields (.getOutputFields gen) output))]
      {:topology *TOPOLOGY* :stream stream}))

  IBatchSpout
  (generator [gen output]
    (let [stream (-> (m/new-stream *TOPOLOGY* (u/uuid) gen)
                     (rename-fields (.getOutputFields gen) output))]
      {:topology *TOPOLOGY* :stream stream}))
  
  Stream
  (generator [stream output]
    (let [updated-stream (-> stream
                             (rename-fields (.getOutputFields stream) output))]
      {:stream updated-stream}))
  
  TridentTopology
  (generator [topology output]
    (let [local-drpc (LocalDRPC.)
          drpc-name (u/uuid)
          stream (-> (m/drpc-stream topology drpc-name local-drpc)
                     (rename-fields ["args"] output))]
      {:drpc [local-drpc drpc-name] :topology topology :stream stream}))

  DRPCStateTap
  (generator [tap output]
    (let [{:keys [state topology]} tap
          local-drpc (LocalDRPC.)
          drpc-name (u/uuid)
          stream (-> (m/drpc-stream topology drpc-name local-drpc)
                     (m/broadcast)
                     (m/state-query state
                                    ["args"]
                                    (TupleCollectionGet.)
                                    output))]
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

(defn run! [tstruct]
  (let [query (plat/compile-query tstruct)
        {:keys [stream drpc topology feeders]} query
        cluster (LocalCluster.)]
    (m/debug stream)
    (.submitTopology cluster (u/uuid) {} (.build topology))
    cluster))

(defn ??- [tstruct]
  (let [results (atom nil)]
    (st/with-local-cluster [cluster]
      (tri/with-drpc [drpc]
        (util/letlocals
         (bind query (plat/compile-query tstruct))
         (bind stream (:stream query))
         (bind topo (:topology query))
         (bind feeders (:feeders query))
         (bind output-fields (:available-fields tstruct))
         (bind drpc-name (u/uuid))
         (if (instance? TridentState stream)
           (-> (m/drpc-stream topo drpc-name drpc)
               (m/broadcast)
               (m/state-query stream
                              ["args"]
                              (TupleCollectionGet.)
                              output-fields)
               (m/project output-fields))
           (do
             (let [rand-field [(gensym "random")]
                   stream-output-fields (.getOutputFields stream)
                   updated-stream (-> stream
                                      (m/group-by stream-output-fields)
                                      (m/persistent-aggregate (MemoryMapState$Factory.)
                                                              stream-output-fields
                                                              all-values-update
                                                              rand-field))]
                 (-> (m/drpc-stream topo drpc-name drpc)
                     (m/broadcast)
                     (m/state-query updated-stream
                                    ["args"]
                                    (TupleCollectionGet.)
                                    (concat stream-output-fields rand-field))
                     (m/project rand-field)
                     (m/each rand-field flatten-vals stream-output-fields)
                     (m/project output-fields)))))
         (tri/with-topology [cluster topo]
           (doseq [{:keys [spout vals]} feeders]
             (tri/feed spout vals))
           (reset! results (tri/exec-drpc drpc drpc-name ""))))))
    @results))

(defmacro ??<- [& sq]
  `(let [tstruct# (with-topology (parse/<- ~@sq))]
     (??- tstruct#)))
