(ns blaze.db.impl.index
  (:require
    [blaze.coll.core :as coll]
    [blaze.db.impl.bytes :as bytes]
    [blaze.db.impl.codec :as codec]
    [blaze.db.impl.index.compartment-query :as cq]
    [blaze.db.impl.index.type-query :as tq]
    [blaze.db.impl.index.resource :as resource :refer [mk-resource]]
    [blaze.db.impl.search-param :as search-param]
    [blaze.db.kv :as kv]
    [blaze.fhir.util :as fhir-util]
    [taoensso.nippy :as nippy])
  (:import
    [blaze.db.impl.index.resource Hash Resource]
    [clojure.lang IReduceInit]
    [java.io Closeable]
    [java.util Arrays HashMap Map]))


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


(defn- num-of-instance-changes* ^long [i tid id t]
  (-> (some-> (resource-state* i (codec/resource-as-of-key tid id t))
              codec/state->num-changes)
      (or 0)))


(defn num-of-instance-changes
  [{:blaze.db/keys [kv-store]} tid id start-t since-t]
  (with-open [snapshot (kv/new-snapshot kv-store)
              i (resource-as-of-iter snapshot)]
    (- (num-of-instance-changes* i tid id start-t)
       (num-of-instance-changes* i tid id since-t))))


(defn type-stats [i tid t]
  (when-let [k (kv/seek! i (codec/type-stats-key tid t))]
    (when (= tid (codec/type-stats-key->tid k))
      (kv/value i))))


(defn- type-stats-iter ^Closeable [snapshot]
  (kv/new-iterator snapshot :type-stats-index))


(defn- num-of-type-changes* ^long [i tid t]
  (or (some-> (type-stats i tid t) codec/type-stats-value->num-changes) 0))


(defn num-of-type-changes
  [{:blaze.db/keys [kv-store]} tid start-t since-t]
  (with-open [snapshot (kv/new-snapshot kv-store)
              i (type-stats-iter snapshot)]
    (- (num-of-type-changes* i tid start-t)
       (num-of-type-changes* i tid since-t))))


(defn- system-stats-iter ^Closeable [snapshot]
  (kv/new-iterator snapshot :system-stats-index))


(defn system-stats [i t]
  (when (kv/seek! i (codec/system-stats-key t))
    (kv/value i)))


(defn- num-of-system-changes* ^long [i t]
  (or (some-> (system-stats i t) codec/system-stats-value->num-changes) 0))


(defn num-of-system-changes
  [{:blaze.db/keys [kv-store]} start-t since-t]
  (with-open [snapshot (kv/new-snapshot kv-store)
              i (system-stats-iter snapshot)]
    (- (num-of-system-changes* i start-t)
       (num-of-system-changes* i since-t))))


(defn deleted? [^Resource resource]
  (codec/deleted? (.state resource)))


(defn- non-deleted-resource-resource [context raoi tid id t]
  (when-let [resource (resource* context raoi tid id t)]
    (when-not (deleted? resource)
      resource)))


(defn- type-list-move-to-t [iter start-k ^long t]
  (loop [k start-k]
    (when (and k (codec/bytes-eq-without-t k start-k))
      (if (< t ^long (codec/resource-as-of-key->t k))
        (recur (kv/next! iter))
        k))))


(defn- type-list-seek [iter tid start-id t]
  (if start-id
    (kv/seek! iter (codec/resource-as-of-key tid start-id t))
    (type-list-move-to-t iter (kv/seek! iter (codec/resource-as-of-key tid)) t)))


(defn- resource-as-of-key-id= [^bytes k ^bytes start-k]
  (Arrays/equals k codec/tid-size (- (alength k) codec/t-size)
                 start-k codec/tid-size (- (alength start-k) codec/t-size)))


(defn- type-list-next [iter ^bytes start-k ^long t]
  (loop [k (kv/next! iter)]
    (when (some-> k (bytes/starts-with? start-k codec/tid-size))
      (if (resource-as-of-key-id= k start-k)
        (recur (kv/next! iter))
        (type-list-move-to-t iter k t)))))


(defn type-list
  "Returns a reducible collection of `HashStateT` records of all resources of
  type with `tid` ordered by resource id.

  The list starts at `start-id`."
  [{:blaze.db/keys [kv-store] :as context} tid start-id t]
  (let [key-prefix (codec/resource-as-of-key tid)]
    (reify
      IReduceInit
      (reduce [_ rf init]
        (with-open [snapshot (kv/new-snapshot kv-store)
                    iter (resource-as-of-iter snapshot)]
          (loop [ret init
                 k (type-list-seek iter tid start-id t)]
            (if (some-> k (bytes/starts-with? key-prefix codec/tid-size))
              (let [resource (resource*** context k (kv/value iter))]
                (if-not (deleted? resource)
                  (let [ret (rf ret resource)]
                    (if (reduced? ret)
                      @ret
                      (recur ret (type-list-next iter k t))))
                  (recur ret (type-list-next iter k t))))
              ret)))))))


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
  [{:blaze.db/keys [kv-store] :as context} compartment tid start-id t]
  (let [start-key (compartment-list-start-key compartment tid start-id)
        cmp-key (compartment-list-cmp-key compartment tid)]
    (reify
      IReduceInit
      (reduce [_ rf init]
        (with-open [snapshot (kv/new-snapshot kv-store)
                    iter (kv/new-iterator snapshot :compartment-resource-type-index)
                    raoi (resource-as-of-iter snapshot)]
          (loop [ret init
                 k (kv/seek! iter start-key)]
            (if (some-> k (bytes/starts-with? cmp-key))
              (if-let [val (non-deleted-resource-resource
                             context raoi tid (codec/compartment-resource-type-key->id k) t)]
                (let [ret (rf ret val)]
                  (if (reduced? ret)
                    @ret
                    (recur ret (kv/next! iter))))
                (recur ret (kv/next! iter)))
              ret)))))))


(defn- resource-as-of-key-prefix= [^long tid ^bytes id ^long since-t k]
  (and (= tid (codec/resource-as-of-key->tid k))
       (bytes/= id (codec/resource-as-of-key->id k))
       (< since-t ^long (codec/resource-as-of-key->t k))))


(defn instance-history
  "Returns a reducible collection of `HashStateT` records of instance history
  entries.

  The history starts at `t`."
  [{:blaze.db/keys [kv-store] :as context} tid id start-t since-t]
  (let [key-still-valid? (partial resource-as-of-key-prefix= tid id since-t)]
    (reify IReduceInit
      (reduce [_ rf init]
        (with-open [snapshot (kv/new-snapshot kv-store)
                    i (resource-as-of-iter snapshot)]
          (loop [ret init
                 k (kv/seek! i (codec/resource-as-of-key tid id start-t))]
            (if (some-> k key-still-valid?)
              (let [ret (rf ret (resource*** context k (kv/value i)))]
                (if (reduced? ret)
                  @ret
                  (recur ret (kv/next! i))))
              ret)))))))


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
  "Returns a reducible collection of `HashStateT` records of type history
  entries.

  The history starts at `t`."
  [{:blaze.db/keys [kv-store] :as context} tid start-t start-id since-t]
  (let [key-still-valid? (partial type-as-of-key-prefix= tid since-t)]
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
  "Returns a reducible collection of `HashStateT` records of system history
  entries.

  The history starts at `t`."
  [{:blaze.db/keys [kv-store] :as context} start-t start-tid start-id since-t]
  (let [key-still-valid? (partial system-as-of-key-prefix= since-t)]
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


(defn comp'
  ([] identity)
  ([f] f)
  ([f g]
   (fn
     ([] (f (g)))
     ([x] (f (g x)))
     ([x y] (f (g x y)))
     ([x y z] (f (g x y z)))
     ([x y z & args] (f (apply g x y z args)))))
  ([f g h]
   (fn
     ([] (f (g (h))))
     ([x] (f (g (h x))))
     ([x y] (f (g (h x y))))
     ([x y z] (f (g (h x y z))))
     ([x y z & args] (f (g (apply h x y z args))))))
  ([f g h i]
   (fn
     ([] (f (g (h (i)))))
     ([x] (f (g (h (i x)))))
     ([x y] (f (g (h (i x y)))))
     ([x y z] (f (g (h (i x y z)))))
     ([x y z & args] (f (g (h (apply i x y z args))))))))


(defn type-query [context snapshot svri raoi tid clauses t]
  (let [[[search-param values] & other-clauses] clauses]
    (coll/eduction
      (comp'
        tq/key-by-id-grouper
        (resource-mapper context raoi tid t)
        matches-hash-prefixes-filter
        (other-clauses-filter snapshot tid other-clauses))
      (search-param/keys search-param snapshot svri tid values))))


(defn compartment-query
  "Iterates over the CSV index "
  [context snapshot csvri raoi compartment tid clauses t]
  (let [[[search-param values] & other-clauses] clauses]
    (coll/eduction
      (comp'
        cq/key-by-id-grouper
        (resource-mapper context raoi tid t)
        matches-hash-prefixes-filter
        (other-clauses-filter snapshot tid other-clauses))
      (search-param/compartment-keys search-param csvri compartment tid values))))


(defn- type-total* [i tid t]
  (or (some-> (type-stats i tid t) codec/type-stats-value->total) 0))


(defn type-total [{:blaze.db/keys [kv-store]} tid t]
  (with-open [snapshot (kv/new-snapshot kv-store)
              i (type-stats-iter snapshot)]
    (type-total* i tid t)))
