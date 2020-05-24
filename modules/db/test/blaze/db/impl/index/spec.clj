(ns blaze.db.impl.index.spec
  (:require
    [blaze.db.api-spec]
    [clojure.spec.alpha :as s])
  (:import
    [blaze.db.impl.index.resource Hash]))


(s/def :blaze.db/hash
  #(instance? Hash %))


(s/def :blaze.db.index/context
  (s/keys :req [:blaze.db/kv-store :blaze.db/resource-cache]))
