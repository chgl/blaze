(ns blaze.db.search-param-registry-test
  (:require
    [blaze.anomaly :refer [when-ok]]
    [blaze.db.search-param-registry :as sr]
    [blaze.db.search-param-registry-spec]
    [blaze.fhir-path :as fhir-path]
    [clojure.spec.test.alpha :as st]
    [clojure.test :as test :refer [deftest is testing]]
    [juxt.iota :refer [given]]
    [taoensso.timbre :as log])
  (:refer-clojure :exclude [get]))


(defn fixture [f]
  (st/instrument)
  (log/with-merged-config {:level :info} (f))
  (st/unstrument))


(test/use-fixtures :each fixture)


(defmethod sr/search-param "token"
  [{:keys [url type expression]}]
  (when expression
    (when-ok [expression (fhir-path/compile expression)]
      {:type type
       :url url
       :expression expression})))


(defmethod sr/search-param "reference"
  [{:keys [url type expression]}]
  (when expression
    (when-ok [expression (fhir-path/compile expression)]
      {:type type
       :url url
       :expression expression})))


(def search-param-registry
  (log/with-merged-config {:level :info} (sr/init-search-param-registry)))


(deftest get
  (testing "_id"
    (given (sr/get search-param-registry "_id")
      :type := "token"
      :url := "http://hl7.org/fhir/SearchParameter/Resource-id")))


(deftest linked-compartments
  (is (= [["Patient" "id-1"]]
         (sr/linked-compartments
           search-param-registry
           {:resourceType "Condition"
            :id "id-0"
            :subject
            {:reference "Patient/id-1"}}))))
