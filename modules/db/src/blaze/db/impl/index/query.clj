(ns blaze.db.impl.index.query
  (:require
    [blaze.coll.core :as coll])
  (:import
    [java.nio ByteBuffer]))


(set! *warn-on-reflection* true)


(def by-id-grouper
  "Transducer which groups `[id hash-prefix]` tuples by `id` and concatenates
  all hash-prefixes within each group, outputting `[id hash-prefixes]` tuples."
  (comp
    (partition-by coll/first)
    (map
      (fn group-hash-prefixes [[[id hash-prefix] & more]]
        [(.array ^ByteBuffer id) (cons hash-prefix (map second more))]))))
