(ns blaze.db.impl.index.type-list-spec
  (:require
    [blaze.db.impl.index.spec]
    [blaze.db.impl.index.type-list :as type-list]
    [clojure.spec.alpha :as s]))


(s/fdef type-list/type-list
  :args (s/cat :context :blaze.db.index/context
               :raoi :blaze.db/kv-iterator
               :tid :blaze.db/tid
               :start-id (s/nilable bytes?)
               :t :blaze.db/t)
  :ret (s/coll-of :blaze/resource))
