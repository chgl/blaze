(ns blaze.db.impl.index.resource-as-of-spec
  (:require
    [blaze.kv.spec]
    [blaze.db.impl.codec-spec]
    [blaze.db.impl.index.resource-as-of :as resource-as-of]
    [blaze.db.impl.index.resource-spec]
    [blaze.db.impl.index.spec]
    [blaze.db.impl.iterators-spec]
    [blaze.fhir.spec]
    [clojure.spec.alpha :as s]))


(s/fdef resource-as-of/type-list
  :args (s/cat :context :blaze.db.index/context
               :raoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :start-id (s/nilable bytes?)
               :t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))


(s/fdef resource-as-of/instance-history
  :args (s/cat :context :blaze.db.index/context
               :raoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :id :blaze.resource/id
               :start-t :blaze.db/t
               :end-t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))


(s/fdef resource-as-of/hash-state-t
  :args (s/cat :raoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :id :blaze.db/id-bytes
               :t :blaze.db/t)
  :ret (s/nilable (s/tuple :blaze.resource/hash :blaze.db/state :blaze.db/t)))


(s/fdef resource-as-of/resource
  :args (s/cat :context :blaze.db.index/context
               :raoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :id :blaze.db/id-bytes
               :t :blaze.db/t)
  :ret (s/nilable :blaze/resource))


(s/fdef resource-as-of/num-of-instance-changes
  :args (s/cat :raoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :id :blaze.db/id-bytes
               :start-t :blaze.db/t
               :end-t :blaze.db/t)
  :ret nat-int?)
