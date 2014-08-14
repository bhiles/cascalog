(ns cascalog.api-storm-test
  (:use clojure.test
        [midje sweet cascalog]
        cascalog.logic.testing
        cascalog.api)
  (:require [cascalog.logic.ops :as c]
            [cascalog.logic.def :as d]
            [cascalog.logic.platform :as plat])
  (:import [cascalog.test KeepEven OneBuffer CountAgg SumAgg]
           [cascalog.ops IdentityBuffer]
           [cascading.operation.text DateParser]
           [cascalog.storm.platform StormPlatform]))

(use-fixtures :once
  (fn  [f]
    (plat/set-context! (StormPlatform.))
    (f)))

(defmapfn mk-one
  "Returns 1 for any input."
  [& tuple] 1)

(deftest test-no-input
  (let [nums [[1] [2] [3]]]
    (test?<- [[3 1] [2 1] [1 1]] ;; re-arranged the order
             [?n ?n2]
             (nums ?n)
             (mk-one :> ?n2))
    (test?<- [[3 1] [2 1] [1 1]
              [2 2] [2 3] [1 3]
              [3 2] [3 3] [1 2]]
             [?n ?n3]
             (nums ?n)
             (mk-one :> ?n2)
             (nums ?n3))))

(deftest test-empty-vector-input
  (let [empty-vector []]
    (test?<- []
             [?a]
             (empty-vector ?a))))


(deftest test-simple-query
  (let [age [["n" 24] ["n" 23] ["i" 31] ["c" 30] ["j" 21] ["q" nil]]]
    ;; removed a test with distinct in it
    (test?<- [["j"] ["n"] ["n"]]
             [?p]
             (age ?p ?a)
             (< ?a 25))))

(deftest test-larger-tuples
  (let [stats [["n" 6 190 nil] ["n" 6 195 nil]
               ["i" 5 180 31] ["g" 5 150 60]]
        friends [["n" "i" 6] ["n" "g" 20]
                 ["g" "i" nil]]]
    (test?<- [["g" 60]]
             [?p ?a]
             (stats ?p _ _ ?a)
             (friends ?p _ _))
    (test?<- []
             [?p ?a]
             (stats ?p 1000 _ ?a))
    ;; removed distinct test
    ))

(defmapcatfn split [^String words]
  (seq (.split words "\\s+")))

(deftest test-countall
  (let [sentence [["hello this is a"]
                  ["say hello hello to the man"]
                  ["this is the cool beans man"]]]
    (test?<- [["hello" 3] ["cool" 1] ["beans" 1]
              ["the" 2] ["is" 2] ["say" 1] ["a" 1]
              ["this" 2] ["to" 1] ["man" 2]]
             [?w ?c]
             (sentence ?s)
             (split ?s :> ?w)
             (c/count ?c))))

