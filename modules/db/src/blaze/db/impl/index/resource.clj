(ns blaze.db.impl.index.resource
  (:require
    [blaze.db.impl.bytes :as bytes]
    [blaze.db.impl.codec :as codec]
    [blaze.db.kv :as kv])
  (:import
    [clojure.lang IMeta IPersistentMap]
    [com.github.benmanes.caffeine.cache LoadingCache]
    [java.util Arrays])
  (:refer-clojure :exclude [hash]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


;; Used as cache key. Implements equals on top of the byte array of a hash.
(deftype Hash [hash]
  Object
  (equals [this other]
    (if (identical? this other)
      true
      (if (or (nil? other) (not= Hash (class other)))
        false
        (bytes/= hash (.hash ^Hash other)))))
  (hashCode [_]
    (Arrays/hashCode ^bytes hash)))


(defn tx [kv-store t]
  (when-let [v (kv/get kv-store :tx-success-index (codec/t-key t))]
    (codec/decode-tx v t)))


(deftype ResourceMeta [kv-store type state t ^:volatile-mutable tx]
  IPersistentMap
  (valAt [this key]
    (.valAt this key nil))

  (valAt [_ key not-found]
    (case key
      :type type
      :blaze.db/t t
      :blaze.db/num-changes (codec/state->num-changes state)
      :blaze.db/op (codec/state->op state)
      :blaze.db/tx
      (if tx
        tx
        (do (set! tx (blaze.db.impl.index.resource/tx kv-store t))
            tx))
      not-found))

  (count [_] 5))


(defn- enhance-content ^IPersistentMap [content t]
  (update content :meta assoc :versionId (str t)))


(defn- mk-meta [kv-store type state t]
  (ResourceMeta. kv-store (keyword "fhir" type) state t nil))


(deftype ResourceContentMeta
  [^LoadingCache cache hash t ^:volatile-mutable ^IPersistentMap meta]

  IPersistentMap
  (valAt [_ key]
    (case key
      :versionId t
      (if meta
        (.valAt meta key)
        (do (set! meta (.valAt ^IPersistentMap (.get cache hash) :meta {}))
            (.valAt meta key))))))


(deftype Resource
  [kv-store ^LoadingCache cache type id hash ^long state ^long t
   content-meta
   ^:volatile-mutable ^IPersistentMap content
   ^:volatile-mutable meta]

  IPersistentMap
  (containsKey [_ key]
    (case key
      :id true
      :resourceType true
      :meta true
      (if content
        (.containsKey content key)
        (do (set! content (.get cache hash))
            (.containsKey content key)))))

  (seq [_]
    (if content
      (.seq (enhance-content content t))
      (do (set! content (.get cache hash))
          (.seq (enhance-content content t)))))

  (valAt [_ key]
    (case key
      :id id
      :resourceType type
      :meta content-meta
      (if content
        (.valAt content key)
        (do (set! content (.get cache hash))
            (.valAt content key)))))

  (valAt [_ key not-found]
    (case key
      :id id
      :resourceType type
      :meta content-meta
      (if content
        (.valAt content key not-found)
        (do (set! content (.get cache hash))
            (.valAt content key not-found)))))

  (count [_]
    (if content
      (count (enhance-content content t))
      (do (set! content (.get cache hash))
          (count (enhance-content content t)))))

  (equiv [this other]
    (.equals this other))

  IMeta
  (meta [_]
    (if meta
      meta
      (do (set! meta (mk-meta kv-store type state t))
          meta)))

  Object
  (equals [this other]
    (if (identical? this other)
      true
      (if (or (nil? other) (not= Resource (class other)))
        false
        (and (= hash (.hash ^Resource other))
             (= t (.t ^Resource other))))))

  (hashCode [_]
    (-> (unchecked-multiply-int 31 (.hashCode hash))
        (unchecked-add-int t))))


(defn hash [^Resource resource]
  (.hash ^Hash (.hash resource)))


(defn mk-resource
  [{:blaze.db/keys [kv-store resource-cache]} type id hash state t]
  (Resource. kv-store resource-cache type id (Hash. hash) state t
             (ResourceContentMeta. resource-cache hash (str t) nil)
             nil nil))
