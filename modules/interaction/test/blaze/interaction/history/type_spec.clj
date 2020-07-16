(ns blaze.interaction.history.type-spec
  (:require
    [blaze.handler.fhir.util-spec]
    [blaze.interaction.history.type :as type]
    [blaze.interaction.history.util-spec]
    [blaze.middleware.fhir.metrics-spec]
    [clojure.spec.alpha :as s]
    [ring.core.spec]))


(s/fdef type/handler
  :args (s/cat :node :blaze.db/node)
  :ret :ring/handler)
