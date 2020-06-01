(ns blaze.fhir.spec-spec
  (:require
    [blaze.fhir.spec :as fhir-spec]
    [clojure.alpha.spec :as s2]
    [clojure.spec.alpha :as s]))


(s/fdef fhir-spec/type-exists?
  :args (s/cat :type string?))


(s/fdef fhir-spec/child-specs
  :args (s/cat :spec keyword?))


(s/fdef fhir-spec/choices
  :args (s/cat :spec (s/cat :op #(= `s2/or %) :choices (s/* (s/cat :key keyword? :spec some?))))
  :ret (s/coll-of (s/tuple keyword? some?)))
