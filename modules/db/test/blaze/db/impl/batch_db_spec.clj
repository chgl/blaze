(ns blaze.db.impl.batch-db-spec
  (:require
    [blaze.db.impl.batch-db :as batch-db]
    [blaze.db.impl.index.spec]
    [blaze.db.impl.index.resource-as-of-spec]
    [blaze.db.impl.index.system-as-of-spec]
    [blaze.db.impl.index.type-as-of-spec]
    [clojure.spec.alpha :as s]))


(s/fdef batch-db/new-batch-db
  :args (s/cat :context :blaze.db.index/context
               :node :blaze.db/node
               :t :blaze.db/t))
