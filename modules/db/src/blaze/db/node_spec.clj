(ns blaze.db.node-spec
  (:require
    [blaze.db.indexer-spec]
    [blaze.kv.spec]
    [blaze.db.node :as node]
    [blaze.db.search-param-registry-spec]
    [blaze.db.spec]
    [blaze.db.tx-log-spec]
    [clojure.spec.alpha :as s]))


(s/fdef node/init-node
  :args (s/cat :tx-log :blaze.db/tx-log
               :tx-indexer :blaze.db.indexer/tx
               :kv-store :blaze.db/kv-store
               :resource-cache :blaze.db/resource-cache
               :search-param-registry :blaze.db/search-param-registry))
