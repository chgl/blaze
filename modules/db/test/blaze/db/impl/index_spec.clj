(ns blaze.db.impl.index-spec
  (:require
    [blaze.db.impl.codec-spec]
    [blaze.db.impl.index :as index]
    [blaze.db.impl.search-param-spec]
    [clojure.spec.alpha :as s])
  (:import
    [blaze.db.impl.index.resource Hash]
    [com.github.benmanes.caffeine.cache LoadingCache]))


(s/def :blaze.db/hash
  #(instance? Hash %))


(s/def :blaze.db/resource-cache
  #(instance? LoadingCache %))


(s/def :blaze.db.index/context
  (s/keys :req [:blaze.db/kv-store :blaze.db/resource-cache]))


(s/fdef index/tx
  :args (s/cat :kv-store :blaze.db/kv-store :t :blaze.db/t)
  :ret (s/nilable :blaze.db/tx))


(s/fdef index/load-resource-content
  :args (s/cat :kv-store :blaze.db/kv-store :hash :blaze.db/hash))


(s/fdef index/hash-state-t
  :args (s/cat :resource-as-of-iter :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :id :blaze.db/id-bytes
               :t :blaze.db/t)
  :ret (s/nilable (s/tuple :blaze.resource/hash :blaze.db/state :blaze.db/t)))


(s/fdef index/resource
  :args (s/cat :context :blaze.db.index/context
               :tid :blaze.db/tid
               :id :blaze.db/id-bytes
               :t :blaze.db/t)
  :ret (s/nilable :blaze/resource))


(s/fdef index/num-of-instance-changes
  :args (s/cat :context :blaze.db.index/context
               :tid :blaze.db/tid
               :id :blaze.db/id-bytes
               :start-t :blaze.db/t
               :end-t :blaze.db/t)
  :ret nat-int?)


(s/fdef index/type-list
  :args (s/cat :context :blaze.db.index/context
               :raoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :start-id (s/nilable bytes?)
               :t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))


(s/fdef index/compartment-list
  :args (s/cat :context :blaze.db.index/context
               :cri :blaze.db/kv-iterator
               :raoi :blaze.db/kv-iterator
               :compartment :blaze.db/compartment
               :tid :blaze.db/tid
               :start-id (s/nilable bytes?)
               :t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))


(s/fdef index/instance-history
  :args (s/cat :context :blaze.db.index/context
               :raoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :id :blaze.db/id-bytes
               :start-t :blaze.db/t
               :end-t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))


(s/fdef index/type-history
  :args (s/cat :context :blaze.db.index/context
               :tid :blaze.db/tid
               :start-t :blaze.db/t
               :start-id (s/nilable bytes?)
               :end-t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))


(s/fdef index/system-history
  :args (s/cat :context :blaze.db.index/context
               :start-t :blaze.db/t
               :start-tid (s/nilable :blaze.db/tid)
               :start-id (s/nilable bytes?)
               :end-t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))


(s/def :blaze.db.index.query/clause
  (s/tuple :blaze.db/search-param (s/coll-of some?)))


;; it's a bit faster to have the clauses as seq instead of an vector
(s/def :blaze.db.index.query/clauses
  (s/coll-of :blaze.db.index.query/clause :kind seq? :min-count 1))


(s/fdef index/type-query
  :args (s/cat :context :blaze.db.index/context
               :snapshot :blaze.db/kv-snapshot
               :svri :blaze.db/kv-iterator
               :raoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :clauses :blaze.db.index.query/clauses
               :t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))


(s/fdef index/compartment-query
  :args (s/cat :context :blaze.db.index/context
               :snapshot :blaze.db/kv-snapshot
               :csvri :blaze.db/kv-iterator
               :raoi :blaze.db/kv-iterator
               :compartment :blaze.db/compartment
               :tid :blaze.db/tid
               :clauses :blaze.db.index.query/clauses
               :t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))


(s/fdef index/type-total
  :args (s/cat :context :blaze.db.index/context
               :tid :blaze.db/tid
               :t :blaze.db/t)
  :ret nat-int?)
