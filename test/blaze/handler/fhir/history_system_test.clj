(ns blaze.handler.fhir.history-system-test
  "Specifications relevant for the FHIR history interaction:

  https://www.hl7.org/fhir/http.html#history
  https://www.hl7.org/fhir/operationoutcome.html
  https://www.hl7.org/fhir/http.html#ops"
  (:require
    [blaze.datomic.test-util :as datomic-test-util]
    [blaze.handler.fhir.history.test-util :as history-test-util]
    [blaze.handler.fhir.test-util :as fhir-test-util]
    [blaze.handler.fhir.history-system :refer [handler]]
    [clojure.spec.alpha :as s]
    [clojure.spec.test.alpha :as st]
    [clojure.test :refer :all]
    [datomic-spec.test :as dst]
    [reitit.core :as reitit]
    [taoensso.timbre :as log]))


(defn fixture [f]
  (st/instrument)
  (dst/instrument)
  (st/instrument
    [`handler]
    {:spec
     {`handler
      (s/fspec
        :args (s/cat :conn #{::conn}))}})
  (log/with-merged-config {:level :error} (f))
  (st/unstrument))


(use-fixtures :each fixture)


(deftest handler-test-0
  (testing "Returns empty History"
    (fhir-test-util/stub-t ::query-params nil?)
    (datomic-test-util/stub-db ::conn ::db)
    (history-test-util/stub-page-t ::query-params nil?)
    (history-test-util/stub-since-t ::db ::query-params nil?)
    (history-test-util/stub-tx-db ::db nil? nil? ::db)
    (datomic-test-util/stub-system-transaction-history ::db [])
    (fhir-test-util/stub-page-size ::query-params 50)
    (history-test-util/stub-page-eid ::query-params nil?)
    (datomic-test-util/stub-as-of-t ::db nil?)
    (datomic-test-util/stub-basis-t ::db 125346)
    (datomic-test-util/stub-system-version ::db 0)

    (let [{:keys [status body]}
          @((handler ::conn)
            {::reitit/router ::router
             ::reitit/match ::match
             :query-params ::query-params})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "history" (:type body)))

      (is (= 0 (:total body)))

      (is (empty? (:entry body))))))


(deftest handler-test-1
  (testing "Returns History with one Patient"
    (let [tx {:db/id ::tx-eid}]
      (fhir-test-util/stub-t ::query-params nil?)
      (datomic-test-util/stub-db ::conn ::db)
      (history-test-util/stub-page-t ::query-params nil?)
      (history-test-util/stub-since-t ::db ::query-params nil?)
      (history-test-util/stub-tx-db ::db nil? nil? ::db)
      (datomic-test-util/stub-system-transaction-history ::db [tx])
      (fhir-test-util/stub-page-size ::query-params 50)
      (history-test-util/stub-page-eid ::query-params nil?)
      (datomic-test-util/stub-entity-db #{tx} ::db)
      (datomic-test-util/stub-datoms
        ::db :eavt (s/cat :e #{::tx-eid} :a #{:tx/resources} :v nil?)
        (constantly [{:v ::patient-eid}]))
      (datomic-test-util/stub-as-of-t ::db nil?)
      (datomic-test-util/stub-basis-t ::db 141653)
      (datomic-test-util/stub-system-version ::db 1)
      (history-test-util/stub-nav-link
        ::match ::query-params 141653 tx #{::patient-eid}
        (constantly ::self-link-url))
      (history-test-util/stub-build-entry
        ::router ::db #{tx} #{::patient-eid} (constantly ::entry)))

    (let [{:keys [status body]}
          @((handler ::conn)
            {::reitit/router ::router
             ::reitit/match ::match
             :query-params ::query-params})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "history" (:type body)))

      (is (= 1 (:total body)))

      (is (= 1 (count (:entry body))))

      (is (= "self" (-> body :link first :relation)))

      (is (= ::self-link-url (-> body :link first :url)))

      (is (= ::entry (-> body :entry first))))))


(deftest handler-test-2
  (testing "Returns History with two Patients in one Transaction"
    (let [tx {:db/id ::tx-eid}]
      (fhir-test-util/stub-t ::query-params nil?)
      (datomic-test-util/stub-db ::conn ::db)
      (history-test-util/stub-page-t ::query-params nil?)
      (history-test-util/stub-since-t ::db ::query-params nil?)
      (history-test-util/stub-tx-db ::db nil? nil? ::db)
      (datomic-test-util/stub-system-transaction-history ::db [tx])
      (fhir-test-util/stub-page-size ::query-params 50)
      (history-test-util/stub-page-eid ::query-params nil?)
      (datomic-test-util/stub-entity-db #{tx} ::db)
      (datomic-test-util/stub-datoms
        ::db :eavt (s/cat :e #{::tx-eid} :a #{:tx/resources} :v nil?)
        (constantly [{:v ::patient-1-eid} {:v ::patient-2-eid}]))
      (datomic-test-util/stub-as-of-t ::db nil?)
      (datomic-test-util/stub-basis-t ::db 141702)
      (datomic-test-util/stub-system-version ::db 2)
      (history-test-util/stub-nav-link
        ::match ::query-params 141702 tx #{::patient-1-eid}
        (constantly ::self-link-url))
      (history-test-util/stub-build-entry
        ::router ::db #{tx} #{::patient-1-eid ::patient-2-eid}
        (fn [_ _ _ resource-eid]
          (case resource-eid
            ::patient-1-eid ::entry-1
            ::patient-2-eid ::entry-2))))

    (let [{:keys [status body]}
          @((handler ::conn)
            {::reitit/router ::router
             ::reitit/match ::match
             :query-params ::query-params})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "history" (:type body)))

      (is (= 2 (:total body)))

      (is (= 2 (count (:entry body))))

      (is (= "self" (-> body :link first :relation)))

      (is (= ::self-link-url (-> body :link first :url)))

      (is (= [::entry-1 ::entry-2] (:entry body))))))


(deftest handler-test-3
  (testing "Returns History with two Patients in two Transactions"
    (let [tx-1 {:db/id ::tx-1-eid}
          tx-2 {:db/id ::tx-2-eid}]
      (fhir-test-util/stub-t ::query-params nil?)
      (datomic-test-util/stub-db ::conn ::db)
      (history-test-util/stub-page-t ::query-params nil?)
      (history-test-util/stub-since-t ::db ::query-params nil?)
      (history-test-util/stub-tx-db ::db nil? nil? ::db)
      (datomic-test-util/stub-system-transaction-history ::db [tx-1 tx-2])
      (fhir-test-util/stub-page-size ::query-params 50)
      (history-test-util/stub-page-eid ::query-params nil?)
      (datomic-test-util/stub-entity-db #{tx-1 tx-2} ::db)
      (datomic-test-util/stub-datoms
        ::db :eavt (s/cat :e #{::tx-1-eid ::tx-2-eid} :a #{:tx/resources} :v nil?)
        (fn [_ _ tx-eid _ _]
          (case tx-eid
            ::tx-1-eid [{:v ::patient-1-eid}]
            ::tx-2-eid [{:v ::patient-2-eid}])))
      (datomic-test-util/stub-as-of-t ::db nil?)
      (datomic-test-util/stub-basis-t ::db 141708)
      (datomic-test-util/stub-system-version ::db 2)
      (history-test-util/stub-nav-link
        ::match ::query-params 141708 tx-1 #{::patient-1-eid}
        (constantly ::self-link-url))
      (history-test-util/stub-build-entry
        ::router ::db #{tx-1 tx-2} #{::patient-1-eid ::patient-2-eid}
        (fn [_ _ _ resource-eid]
          (case resource-eid
            ::patient-1-eid ::entry-1
            ::patient-2-eid ::entry-2))))

    (let [{:keys [status body]}
          @((handler ::conn)
            {::reitit/router ::router
             ::reitit/match ::match
             :query-params ::query-params})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "history" (:type body)))

      (is (= 2 (:total body)))

      (is (= 2 (count (:entry body))))

      (is (= "self" (-> body :link first :relation)))

      (is (= ::self-link-url (-> body :link first :url)))

      (is (= [::entry-1 ::entry-2] (:entry body))))))


(deftest handler-test-4
  (testing "Returns History with next Link"
    (let [tx {:db/id ::tx-eid}]
      (fhir-test-util/stub-t ::query-params nil?)
      (datomic-test-util/stub-db ::conn ::db)
      (history-test-util/stub-page-t ::query-params nil?)
      (history-test-util/stub-since-t ::db ::query-params nil?)
      (history-test-util/stub-tx-db ::db nil? nil? ::db)
      (datomic-test-util/stub-system-transaction-history ::db [tx])
      (fhir-test-util/stub-page-size ::query-params 1)
      (history-test-util/stub-page-eid ::query-params nil?)
      (datomic-test-util/stub-entity-db #{tx} ::db)
      (datomic-test-util/stub-datoms
        ::db :eavt (s/cat :e #{::tx-eid} :a #{:tx/resources} :v nil?)
        (constantly [{:v ::patient-1-eid} {:v ::patient-2-eid}]))
      (datomic-test-util/stub-as-of-t ::db nil?)
      (datomic-test-util/stub-basis-t ::db 141715)
      (datomic-test-util/stub-system-version ::db 1)
      (history-test-util/stub-nav-link
        ::match ::query-params 141715 tx
        #{::patient-1-eid ::patient-2-eid}
        (fn [_ _ _ _ resource-eid]
          (case resource-eid
            ::patient-1-eid ::self-link-url
            ::patient-2-eid ::next-link-url)))
      (history-test-util/stub-build-entry
        ::router ::db #{tx} #{::patient-1-eid} (constantly ::entry))

      (let [{:keys [status body]}
            @((handler ::conn)
              {::reitit/router ::router
               ::reitit/match ::match
               :query-params ::query-params})]

        (is (= 200 status))

        (is (= "Bundle" (:resourceType body)))

        (is (= "history" (:type body)))

        (is (= 1 (:total body)))

        (is (= 1 (count (:entry body))))

        (is (= "self" (-> body :link first :relation)))

        (is (= ::self-link-url (-> body :link first :url)))

        (is (= "next" (-> body :link second :relation)))

        (is (= ::next-link-url (-> body :link second :url)))

        (is (= ::entry (-> body :entry first)))))))
