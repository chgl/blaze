(ns blaze.db.search-param-registry-test
  (:require
    [blaze.db.impl.codec :as codec]
    [blaze.db.search-param-registry :as sr]
    [blaze.db.search-param-registry-spec]
    [clojure.spec.test.alpha :as st]
    [clojure.test :as test :refer [deftest testing]]
    [juxt.iota :refer [given]])
  (:refer-clojure :exclude [get]))


(defn fixture [f]
  (st/instrument)
  (f)
  (st/unstrument))


(test/use-fixtures :each fixture)


(def search-param-registry (sr/init-mem-search-param-registry))


(deftest get
  (testing "_id"
    (given (sr/get search-param-registry "_id")
      :type := "token"
      :url := "http://hl7.org/fhir/SearchParameter/Resource-id")))


(deftest linked-compartments
  (given
    (sr/linked-compartments
      search-param-registry
      {:resourceType "Condition"
       :id "id-0"
       :subject
       {:reference "Patient/id-1"}})
    [0 :c-hash] := (codec/c-hash "Patient")
    [0 :res-id codec/hex] := (codec/hex (codec/id-bytes "id-1"))
    1 := nil))
