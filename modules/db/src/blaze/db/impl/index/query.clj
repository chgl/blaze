(ns blaze.db.impl.index.query
  (:require [blaze.db.impl.codec :as codec])
  (:import
    [java.nio ByteBuffer]))


(set! *warn-on-reflection* true)


(def by-id-grouper
  "Transducer which groups `[id hash-prefix]` tuples by `id` and concatenates
  all hash-prefixes within each group, outputting `[id hash-prefixes]` tuples."
  (comp
    (partition-by (fn [[_ id]] (ByteBuffer/wrap id)))
    (map
      (fn group-hash-prefixes [[[_ id hash-prefix] & more]]
        [id (cons hash-prefix (map #(nth % 2) more))]))))
