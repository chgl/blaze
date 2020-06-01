(ns blaze.db.impl.iterators-spec
  (:require
    [blaze.db.kv-spec]
    [blaze.db.impl.iterators :as i]
    [clojure.spec.alpha :as s]))


(s/fdef i/kvs
  :args (s/cat :iter :blaze.db/kv-iterator :decode fn? :start-key bytes?))
