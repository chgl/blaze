(ns blaze.db.impl.index
  (:require
    [blaze.coll.core :as coll]
    [blaze.db.impl.bytes :as bytes]
    [blaze.db.impl.codec :as codec]
    [blaze.db.impl.index.resource :as resource :refer [mk-resource]]
    [blaze.db.impl.iterators :as i]
    [blaze.db.impl.search-param :as search-param]
    [blaze.db.impl.util :as util]
    [blaze.db.kv :as kv]
    [blaze.fhir.util :as fhir-util]
    [taoensso.nippy :as nippy]
    [blaze.db.impl.index.query :as query])
  (:import
    [blaze.db.impl.index.resource Hash Resource ResourceContentMeta]
    [clojure.lang IReduceInit]
    [java.io Closeable]
    [java.util HashMap Map]
    [blaze.db.impl.codec ResourceAsOfKV]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def ^:private ^Map type-reg
  (let [map (HashMap.)]
    (doseq [{:keys [type]} (fhir-util/resources)]
      (.put map (codec/tid type) type))
    map))


(defn- tid->type [tid]
  (.get type-reg tid))


(defn tx [kv-store t]
  (resource/tx kv-store t))


(defn load-resource-content [kv-store ^Hash hash]
  (some-> (kv/get kv-store :resource-index (.hash hash)) (nippy/fast-thaw)))


(defn- resource*** [context k v]
  (mk-resource
    context
    (tid->type (codec/resource-as-of-key->tid k))
    (codec/id (codec/resource-as-of-key->id k))
    (codec/resource-as-of-value->hash v)
    (codec/resource-as-of-value->state v)
    (codec/resource-as-of-key->t k)))


(defn- resource**
  [context resource-as-of-iter target]
  (when-let [k (kv/seek! resource-as-of-iter target)]
    ;; we have to check that we are still on target, because otherwise we would
    ;; find the next resource
    (when (codec/bytes-eq-without-t target k)
      (resource*** context k (kv/value resource-as-of-iter)))))


(defn resource* [context raoi tid id t]
  (resource** context raoi (codec/resource-as-of-key tid id t)))


(defn- hash-state-t** [k v]
  [(codec/resource-as-of-value->hash v)
   (codec/resource-as-of-value->state v)
   (codec/resource-as-of-key->t k)])


(defn- hash-state-t* [resource-as-of-iter target]
  (when-let [k (kv/seek! resource-as-of-iter target)]
    ;; we have to check that we are still on target, because otherwise we would
    ;; find the next resource
    (when (codec/bytes-eq-without-t target k)
      (hash-state-t** k (kv/value resource-as-of-iter)))))


(defn hash-state-t
  "Returns a triple of `hash`, `state` and `t` of the resource with `tid` and
  `id` at or before `t`."
  [resource-as-of-iter tid id t]
  (hash-state-t* resource-as-of-iter (codec/resource-as-of-key tid id t)))


(defn resource-as-of-iter ^Closeable [snapshot]
  (kv/new-iterator snapshot :resource-as-of-index))


(defn resource [{:blaze.db/keys [kv-store] :as context} tid id t]
  (with-open [snapshot (kv/new-snapshot kv-store)
              raoi (resource-as-of-iter snapshot)]
    (resource* context raoi tid id t)))


(defn- resource-t**
  [resource-as-of-iter target]
  (when-let [k (kv/seek! resource-as-of-iter target)]
    ;; we have to check that we are still on target, because otherwise we would
    ;; find the next resource
    (when (codec/bytes-eq-without-t target k)
      (codec/resource-as-of-key->t k))))


(defn resource-t* [resource-as-of-iter tid id t]
  (resource-t** resource-as-of-iter (codec/resource-as-of-key tid id t)))


(defn resource-state*
  [resource-as-of-iter target]
  (when-let [k (kv/seek! resource-as-of-iter target)]
    ;; we have to check that we are still on target, because otherwise we would
    ;; find the next resource
    (when (codec/bytes-eq-without-t target k)
      (codec/resource-as-of-value->state (kv/value resource-as-of-iter)))))


(defn- t-by-instant*
  [t-by-instant-iter instant]
  (when (kv/seek! t-by-instant-iter (codec/tx-by-instant-key instant))
    (codec/decode-t (kv/value t-by-instant-iter))))


(defn- t-by-instant-iter ^Closeable [snapshot]
  (kv/new-iterator snapshot :t-by-instant-index))


(defn t-by-instant
  [store instant]
  (with-open [snapshot (kv/new-snapshot store)
              i (t-by-instant-iter snapshot)]
    (t-by-instant* i instant)))


(defn deleted? [^Resource resource]
  (codec/deleted? (.state resource)))


(defn- resource****
  [kv-store resource-cache ^ResourceAsOfKV kv]
  (Resource.
    kv-store
    resource-cache
    (tid->type (.tid kv))
    (.id kv)
    (Hash. (.hash kv))
    (.state kv)
    (.t kv)
    (ResourceContentMeta. resource-cache hash (.t kv) nil)
    nil
    nil))



;; ---- Type-Level Functions ------------------------------------------------

(defn- type-list-start-key [tid start-id t]
  (if start-id
    (codec/resource-as-of-key tid start-id t)
    (codec/resource-as-of-key tid)))


(defn type-list
  "Returns a reducible collection of all resources of type with `tid` ordered
  by resource id.

  The list starts at the optional `start-id`."
  ^IReduceInit
  [{:blaze.db/keys [kv-store resource-cache]} raoi tid start-id t]
  (coll/eduction
    (comp
      (take-while #(= ^int tid (.tid ^ResourceAsOfKV %)))
      (filter #(<= (.t ^ResourceAsOfKV %) ^long t))
      (coll/first-by #(.id ^ResourceAsOfKV %))
      (map #(resource**** kv-store resource-cache %))
      (remove deleted?))
    (i/kvs raoi (codec/resource-as-of-kv-decoder) (type-list-start-key tid start-id t))))


(defn- non-deleted-resource [context raoi tid id t]
  (when-let [resource (resource* context raoi tid id t)]
    (when-not (deleted? resource)
      resource)))


(defn- resource-mapper [context raoi tid t]
  (mapcat
    (fn [[id hash-prefixes]]
      (when-let [resource (non-deleted-resource context raoi tid id t)]
        [[resource hash-prefixes]]))))


(def ^:private matches-hash-prefixes-filter
  (mapcat
    (fn [[resource hash-prefixes]]
      (when (some (partial bytes/starts-with? (resource/hash resource)) hash-prefixes)
        [resource]))))


(defn- other-clauses-filter [snapshot tid clauses]
  (if (seq clauses)
    (filter
      (fn [resource]
        (let [id (codec/id-bytes (:id resource))
              hash (resource/hash resource)]
          (loop [[[search-param values] & clauses] clauses]
            (if search-param
              (when (search-param/matches? search-param snapshot tid id hash values)
                (recur clauses))
              resource)))))
    identity))


(defn type-query [context snapshot svri raoi tid clauses t]
  (let [[[search-param values] & other-clauses] clauses]
    (coll/eduction
      (util/comp
        query/by-id-grouper
        (resource-mapper context raoi tid t)
        matches-hash-prefixes-filter
        (other-clauses-filter snapshot tid other-clauses))
      (search-param/keys search-param snapshot svri tid values))))



;; ---- System-Level Functions ------------------------------------------------

(defn system-list [context start-tid start-id t])



;; ---- Compartment-Level Functions -------------------------------------------

(defn- compartment-list-start-key [{:keys [c-hash res-id]} tid start-id]
  (if start-id
    (codec/compartment-resource-type-key c-hash res-id tid start-id)
    (codec/compartment-resource-type-key c-hash res-id tid)))


(defn- compartment-list-cmp-key [{:keys [c-hash res-id]} tid]
  (codec/compartment-resource-type-key c-hash res-id tid))


(defn compartment-list
  "Returns a reducible collection of all resources of type with `tid` linked to
  `compartment` and ordered by resource id.

  The list starts at `start-id`.

  The implementation uses the :resource-type-index to obtain an iterator over
  all resources of the type with `tid` ever known (independent from `t`). It
  then looks up the newest version of each resource in the :resource-as-of-index
  not newer then `t`."
  ^IReduceInit
  [context cri raoi compartment tid start-id t]
  (let [start-key (compartment-list-start-key compartment tid start-id)
        cmp-key (compartment-list-cmp-key compartment tid)]
    (coll/eduction
      (comp
        (take-while (fn [[prefix]] (bytes/= prefix cmp-key)))
        (map (fn [[_ id]] (resource* context raoi tid id t)))
        (remove nil?)
        (remove deleted?))
      (i/keys cri codec/decode-compartment-resource-type-key start-key))))


(defn compartment-query
  "Iterates over the CSV index "
  [context snapshot csvri raoi compartment tid clauses t]
  (let [[[search-param values] & other-clauses] clauses]
    (coll/eduction
      (util/comp
        query/by-id-grouper
        (resource-mapper context raoi tid t)
        matches-hash-prefixes-filter
        (other-clauses-filter snapshot tid other-clauses))
      (search-param/compartment-keys search-param csvri compartment tid values))))



;; ---- Instance-Level History Functions --------------------------------------

(defn instance-history
  "Returns a reducible collection of all versions between `start-t` (inclusive)
  and `end-t` (exclusive) of the resource with `tid` and `id`.

  Versions are resources itself."
  [{:blaze.db/keys [kv-store resource-cache]} raoi tid id start-t end-t]
  (let [start-key (codec/resource-as-of-key tid id start-t)
        id (codec/id id)]
    (coll/eduction
      (comp
        (take-while
          (fn [^ResourceAsOfKV kv]
            (and (= (.tid kv) tid) (= (.id kv) id) (< ^long end-t (.t kv)))))
        (map #(resource**** kv-store resource-cache %)))
      (i/kvs raoi (codec/resource-as-of-kv-decoder) start-key))))


(defn- num-of-instance-changes* ^long [iter tid id t]
  (-> (some-> (resource-state* iter (codec/resource-as-of-key tid id t))
              codec/state->num-changes)
      (or 0)))


(defn num-of-instance-changes
  "Returns the number of changes between `start-t` (inclusive) and `end-t`
  (inclusive) of the resource with `tid` and `id`."
  {:arglists '([context tid id start-t end-t])}
  [{:blaze.db/keys [kv-store]} tid id start-t end-t]
  (with-open [snapshot (kv/new-snapshot kv-store)
              iter (resource-as-of-iter snapshot)]
    (- (num-of-instance-changes* iter tid id start-t)
       (num-of-instance-changes* iter tid id end-t))))


(defn- type-as-of-key-prefix= [^long tid ^long since-t k]
  (and (= tid (codec/type-as-of-key->tid k))
       (< since-t ^long (codec/type-as-of-key->t k))))


(defn type-as-of-key [tid start-t start-id]
  (if start-id
    (codec/type-as-of-key tid start-t start-id)
    (codec/type-as-of-key tid start-t)))


(defn- type-history-entry [context resource-as-of-iter tid k]
  (resource*
    context
    resource-as-of-iter
    tid
    (codec/type-as-of-key->id k)
    (codec/type-as-of-key->t k)))


(defn type-history
  "Returns a reducible collection of all versions between `start-t` (inclusive),
  `start-id` (optional, inclusive) and `end-t` (inclusive) of resources with
  `tid`.

  Versions are resources itself."
  {:arglists '([context tid start-t start-id end-t])}
  [{:blaze.db/keys [kv-store] :as context} tid start-t start-id end-t]
  (let [key-still-valid? (partial type-as-of-key-prefix= tid end-t)]
    (reify IReduceInit
      (reduce [_ rf init]
        (with-open [snapshot (kv/new-snapshot kv-store)
                    i (kv/new-iterator snapshot :type-as-of-index)
                    ri (resource-as-of-iter snapshot)]
          (loop [ret init
                 k (kv/seek! i (type-as-of-key tid start-t start-id))]
            (if (some-> k key-still-valid?)
              (let [ret (rf ret (type-history-entry context ri tid k))]
                (if (reduced? ret)
                  @ret
                  (recur ret (kv/next! i))))
              ret)))))))


(defn- system-as-of-key-prefix= [^long since-t k]
  (< since-t ^long (codec/system-as-of-key->t k)))


(defn- system-as-of-key [start-t start-tid start-id]
  (cond
    start-id (codec/system-as-of-key start-t start-tid start-id)
    start-tid (codec/system-as-of-key start-t start-tid)
    :else (codec/system-as-of-key start-t)))


(defn- system-history-entry [context resource-as-of-iter k]
  (resource*
    context
    resource-as-of-iter
    (codec/system-as-of-key->tid k)
    (codec/system-as-of-key->id k)
    (codec/system-as-of-key->t k)))


(defn system-history
  "Returns a reducible collection of all versions between `start-t` (inclusive),
  `start-tid` (optional, inclusive), `start-id` (optional, inclusive) and
  `end-t` (inclusive) of all resources.

  Versions are resources itself."
  {:arglists '([context start-t start-tid start-id end-t])}
  [{:blaze.db/keys [kv-store] :as context} start-t start-tid start-id end-t]
  (let [key-still-valid? (partial system-as-of-key-prefix= end-t)]
    (reify IReduceInit
      (reduce [_ rf init]
        (with-open [snapshot (kv/new-snapshot kv-store)
                    i (kv/new-iterator snapshot :system-as-of-index)
                    ri (resource-as-of-iter snapshot)]
          (loop [ret init
                 k (kv/seek! i (system-as-of-key start-t start-tid start-id))]
            (if (some-> k key-still-valid?)
              (let [ret (rf ret (system-history-entry context ri k))]
                (if (reduced? ret)
                  @ret
                  (recur ret (kv/next! i))))
              ret)))))))
