(ns blaze.db.impl.bytes
  (:import
    [com.google.common.primitives Bytes]
    [java.util Arrays])
  (:refer-clojure :exclude [= < <= concat empty]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn = [^bytes a ^bytes b]
  (Arrays/equals a b))


(defn starts-with?
  {:arglists '([bs prefix] [bs prefix prefix-length])}
  ([^bytes bs ^bytes prefix]
   (and (clojure.core/<= (alength prefix) (alength bs))
        (starts-with? bs prefix (alength prefix))))
  ([^bytes bs ^bytes prefix ^long prefix-length]
   (Arrays/equals bs 0 prefix-length prefix 0 prefix-length)))


(defn < [^bytes a ^bytes b]
  (clojure.core/< (Arrays/compareUnsigned a b) 0))


(defn <= [^bytes a ^bytes b]
  (clojure.core/<= (Arrays/compareUnsigned a b) 0))


(def empty (byte-array 0))


(defn concat [byte-arrays]
  (if (seq byte-arrays)
    (Bytes/concat (into-array byte-arrays))
    empty))


(defn copy [^bytes bs]
  (Arrays/copyOf bs (alength bs)))
