(ns blaze.coll.core
  (:import
    [clojure.lang IReduceInit])
  (:refer-clojure :exclude [eduction empty? first]))


(defn first
  "Like `clojure.core/first` but for reducible collections."
  [coll]
  (reduce (fn [_ x] (reduced x)) nil coll))


(defn empty?
  "Like `clojure.core/empty?` but for reducible collections."
  [coll]
  (nil? (first coll)))


(defn eduction
  "Like `clojure.core/eduction` but faster."
  [xform coll]
  (reify IReduceInit
    (reduce [_ f init]
      (transduce xform (completing f) init coll))))
