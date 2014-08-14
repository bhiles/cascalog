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
