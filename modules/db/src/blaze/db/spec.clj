(ns blaze.db.spec
  (:require
    [blaze.db.impl.protocols :as p]
    [blaze.db.indexer :as indexer]
    [clojure.spec.alpha :as s])
  (:import
    [com.github.benmanes.caffeine.cache LoadingCache]))


(s/def :blaze.db/node
  #(satisfies? p/Node %))


(s/def :blaze.db/resource-cache
  #(instance? LoadingCache %))


(s/def :blaze.db.indexer/resource
  #(satisfies? indexer/Resource %))


(s/def :blaze.db/t
  nat-int?)
