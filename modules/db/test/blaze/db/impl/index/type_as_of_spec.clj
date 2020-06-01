(ns blaze.db.impl.index.type-as-of-spec
  (:require
    [blaze.db.kv-spec]
    [blaze.db.impl.codec-spec]
    [blaze.db.impl.index.resource-spec]
    [blaze.db.impl.index.spec]
    [blaze.db.impl.index.type-as-of :as type-as-of]
    [blaze.db.impl.iterators-spec]
    [blaze.fhir.spec]
    [clojure.spec.alpha :as s]))


(s/fdef type-as-of/type-history
  :args (s/cat :context :blaze.db.index/context
               :taoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :start-t :blaze.db/t
               :start-id (s/nilable bytes?)
               :end-t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))
