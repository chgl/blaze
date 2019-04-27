(ns life-fhir-store.integration-test
  (:require
    [cheshire.core :as json]
    [clojure.core.cache :as cache]
    [clojure.spec.alpha :as s]
    [clojure.spec.test.alpha :as st]
    [clojure.test :refer :all]
    [datomic.api :as d]
    [datomic-tools.schema :as dts]
    [juxt.iota :refer [given]]
    [life-fhir-store.cql-translator :as cql]
    [life-fhir-store.datomic.schema :as schema]
    [life-fhir-store.datomic.transaction :as tx]
    [life-fhir-store.elm.compiler :as compiler]
    [life-fhir-store.elm.date-time :as date-time]
    [life-fhir-store.elm.deps-infer :refer [infer-library-deps]]
    [life-fhir-store.elm.equiv-relationships :refer [find-equiv-rels-library]]
    [life-fhir-store.elm.normalizer :refer [normalize-library]]
    [life-fhir-store.elm.spec]
    [life-fhir-store.elm.type-infer :refer [infer-library-types]]
    [life-fhir-store.elm.evaluator :as evaluator]
    [life-fhir-store.structure-definition :refer [read-structure-definitions]])
  (:import
    [java.time OffsetDateTime Year]))


(st/instrument)


(def structure-definitions (read-structure-definitions "fhir/r4/structure-definitions"))


(defn- insert-tx-data
  [db resource]
  (tx/resource-update db (dissoc resource "meta" "text")))


(defn- connect []
  (d/delete-database "datomic:mem://integration-test")
  (d/create-database "datomic:mem://integration-test")
  (let [conn (d/connect "datomic:mem://integration-test")]
    @(d/transact conn (dts/schema))
    @(d/transact conn (schema/structure-definition-schemas (vals structure-definitions)))
    conn))


(defn- db-with [data]
  (let [conn (connect)
        tx-data (into [] (mapcat #(insert-tx-data (d/db conn) %)) data)]
    (:db-after @(d/transact conn tx-data))))


(defn- evaluate [db query]
  @(evaluator/evaluate db (OffsetDateTime/now)
                       (compiler/compile-library db (cql/translate query) {})))


(defn read-data [query-name]
  (-> (slurp (str "integration-test/" query-name "/data.json"))
      (json/parse-string)))

(defn read-query [query-name]
  (slurp (str "integration-test/" query-name "/query.cql")))

(deftest query-test
  (are [query-name num]
    (= num (get-in (evaluate (db-with (read-data query-name))
                             (read-query query-name))
                   ["NumberOfPatients" :result]))

    "query-3" 1
    "query-5" 3
    "query-6" 1
    "query-7" 2
    "readme-example" 3))

(deftest arithmetic-test
  (given (evaluate (db-with []) (read-query "arithmetic"))
    ["OnePlusOne" :result] := 2
    ["OnePointOnePlusOnePointOne" :result] := 2.2M
    ["Year2019PlusOneYear" :result] := (Year/of 2020)
    ["OneYearPlusOneYear" :result] := (date-time/period 2 0 0)
    ["OneYearPlusOneMonth" :result] := (date-time/period 1 1 0)
    ["OneSecondPlusOneSecond" :result] := (date-time/period 0 0 2000)))

(comment
  (s/valid? :elm/library (cql/translate (read-query "query-3")))
  (clojure.repl/pst)
  )
