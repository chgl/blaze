(ns blaze.db.impl.index.compartment-query
  (:require
    [blaze.db.impl.codec :as codec]
    [blaze.db.impl.index.query :as query])
  (:import
    [java.nio ByteBuffer]))


(set! *warn-on-reflection* true)


(defn- key->id-hash-prefix [k]
  [(ByteBuffer/wrap (codec/compartment-search-param-value-key->id k))
   (codec/compartment-search-param-value-key->hash-prefix k)])


(def key-by-id-grouper
  "Transducer which takes CSVR index keys and outputs `[id hash-prefixes]`
  tuples."
  (comp
    (map key->id-hash-prefix)
    query/by-id-grouper))
