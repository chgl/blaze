(ns blaze.interaction.search.params-spec
  (:require
    [blaze.handler.fhir.util-spec]
    [blaze.interaction.search.params :as params]
    [clojure.spec.alpha :as s]))


(s/def ::params
  map?)


(s/fdef params/decode
  :args (s/cat :query-params :ring.request/query-params)
  :ret ::params)
