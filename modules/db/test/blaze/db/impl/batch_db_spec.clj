(ns blaze.db.impl.batch-db-spec
  (:require
    [blaze.db.impl.batch-db :as batch-db]
    [blaze.db.impl.index.spec]
    [clojure.spec.alpha :as s]))


(s/fdef batch-db/new-batch-db
  :args (s/cat :context :blaze.db.index/context
               :node :blaze.db/node
               :t :blaze.db/t))
