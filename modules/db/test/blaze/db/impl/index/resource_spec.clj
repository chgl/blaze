(ns blaze.db.impl.index.resource-spec
  (:require
    [blaze.db.impl.codec-spec]
    [blaze.db.impl.index.resource :as resource]
    [blaze.db.kv-spec]
    [blaze.db.spec]
    [clojure.spec.alpha :as s]))


(s/fdef resource/mk-resource
  :args (s/cat :kv-store :blaze.db/kv-store
               :resource-cache :blaze.db/resource-cache
               :tid :blaze.db/tid
               :id string?
               :hash bytes?
               :state :blaze.db/state
               :t :blaze.db/t))
