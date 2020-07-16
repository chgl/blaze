(ns blaze.interaction.read-test
  "Specifications relevant for the FHIR read interaction:

  https://www.hl7.org/fhir/http.html#read
  https://www.hl7.org/fhir/operationoutcome.html
  https://www.hl7.org/fhir/http.html#ops"
  (:require
    [blaze.db.api-stub :refer [mem-node-with]]
    [blaze.interaction.read :refer [handler]]
    [blaze.interaction.read-spec]
    [clojure.spec.test.alpha :as st]
    [clojure.test :as test :refer [deftest is testing]]
    [juxt.iota :refer [given]]
    [reitit.core :as reitit]
    [taoensso.timbre :as log]))


(defn fixture [f]
  (st/instrument)
  (log/with-merged-config {:level :error} (f))
  (st/unstrument))


(test/use-fixtures :each fixture)


(defn- handler-with [txs]
  (handler (mem-node-with txs)))


(def ^:private match
  {:data {:fhir.resource/type "Patient"}})


(deftest handler-test
  (testing "Returns Not Found on Non-Existing Resource"
    (let [{:keys [status body]}
          @((handler-with [])
            {:path-params {:id "0"}
             ::reitit/match match})]

      (is (= 404 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "not-found")))


  (testing "Returns Not Found on Invalid Version ID"
    (let [{:keys [status body]}
          @((handler-with [])
            {:path-params {:id "0" :vid "a"}
             ::reitit/match match})]

      (is (= 404 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "not-found")))


  (testing "Returns Gone on Deleted Resource"
    (let [{:keys [status body headers]}
          @((handler-with
              [[[:put {:resourceType "Patient" :id "0"}]]
               [[:delete "Patient" "0"]]])
            {:path-params {:id "0"}
             ::reitit/match match})]

      (is (= 410 status))

      (testing "Transaction time in Last-Modified header"
        (is (= "Thu, 1 Jan 1970 00:00:00 GMT" (get headers "Last-Modified"))))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "deleted")))


  (testing "Returns Existing Resource"
    (let [{:keys [status headers body]}
          @((handler-with [[[:put {:resourceType "Patient" :id "0"}]]])
            {:path-params {:id "0"}
             ::reitit/match match})]

      (is (= 200 status))

      (testing "Transaction time in Last-Modified header"
        (is (= "Thu, 1 Jan 1970 00:00:00 GMT" (get headers "Last-Modified"))))

      (testing "Version in ETag header"
        ;; 1 is the T of the transaction of the resource update
        (is (= "W/\"1\"" (get headers "ETag"))))

      (given body
        [:meta :versionId] := "1")))


  (testing "Returns Existing Resource on versioned read"
    (let [{:keys [status headers body]}
          @((handler-with [[[:put {:resourceType "Patient" :id "0"}]]])
            {:path-params {:id "0" :vid "1"}
             ::reitit/match match})]

      (is (= 200 status))

      (testing "Transaction time in Last-Modified header"
        (is (= "Thu, 1 Jan 1970 00:00:00 GMT" (get headers "Last-Modified"))))

      (testing "Version in ETag header"
        ;; 1 is the T of the transaction of the resource update
        (is (= "W/\"1\"" (get headers "ETag"))))

      (given body
        [:meta :versionId] := "1"))))
