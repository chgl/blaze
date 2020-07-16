(ns blaze.interaction.transaction-test
  "Specifications relevant for the FHIR batch/transaction interaction:

  https://www.hl7.org/fhir/http.html#transaction
  https://www.hl7.org/fhir/operationoutcome.html
  https://www.hl7.org/fhir/http.html#ops"
  (:require
    [blaze.db.api-stub :refer [mem-node-with]]
    [blaze.executors :as ex]
    [blaze.interaction.transaction :refer [handler]]
    [blaze.interaction.transaction-spec]
    [blaze.uuid :refer [random-uuid]]
    [clojure.spec.alpha :as s]
    [clojure.spec.test.alpha :as st]
    [clojure.test :as test :refer [deftest is testing]]
    [juxt.iota :refer [given]]
    [manifold.deferred :as md]
    [reitit.core :as reitit]
    [ring.util.response :as ring]
    [taoensso.timbre :as log]))


(defn fixture [f]
  (st/instrument)
  (log/with-merged-config {:level :error} (f))
  (st/unstrument))


(test/use-fixtures :each fixture)


(def ^:private router
  (reitit/router
    [["/Patient/{id}" {:name :Patient/instance}]
     ["/Patient/{id}/_history/{vid}" {:name :Patient/versioned-instance}]]
    {:syntax :bracket}))


(def ^:private operation-outcome
  "http://terminology.hl7.org/CodeSystem/operation-outcome")


(defonce executor (ex/single-thread-executor))


(defn- handler-with [txs]
  (handler (mem-node-with txs) executor))


(deftest handler-test
  (testing "Returns Error on missing request"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "value"
        [:issue 0 :expression 0] := "Bundle.entry[0]"
        [:issue 0 :diagnostics] := "Missing request.")))

  (testing "Returns Error on missing request url"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:request {}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "value"
        [:issue 0 :expression 0] := "Bundle.entry[0].request"
        [:issue 0 :diagnostics] := "Missing url.")))

  (testing "Returns Error on missing request method"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:request
                {:url "Patient/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "value"
        [:issue 0 :expression 0] := "Bundle.entry[0].request"
        [:issue 0 :diagnostics] := "Missing method.")))

  (testing "Returns Error on unknown method"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:request
                {:method "FOO"
                 :url "Patient/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "value"
        [:issue 0 :expression 0] := "Bundle.entry[0].request.method"
        [:issue 0 :diagnostics] := "Unknown method `FOO`.")))

  (testing "Returns Error on unsupported method"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:request
                {:method "PATCH"
                 :url "Patient/0"}}]}})]

      (is (= 422 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "not-supported"
        [:issue 0 :expression 0] := "Bundle.entry[0].request.method"
        [:issue 0 :diagnostics] := "Unsupported method `PATCH`.")))

  (testing "Returns Error on missing type"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:request
                {:method "PUT"
                 :url ""}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "value"
        [:issue 0 :expression 0] := "Bundle.entry[0].request.url"
        [:issue 0 :diagnostics] := "Can't parse type from `entry.request.url` ``.")))

  (testing "Returns Error on unknown type"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:request
                {:method "PUT"
                 :url "Foo/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "value"
        [:issue 0 :expression 0] := "Bundle.entry[0].request.url"
        [:issue 0 :diagnostics] := "Unknown type `Foo` in bundle entry URL `Foo/0`.")))

  (testing "Returns Error on invalid JSON type for resource"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:resource []
                :request
                {:method "PUT"
                 :url "Patient/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "structure"
        [:issue 0 :expression 0] := "Bundle.entry[0].resource"
        [:issue 0 :diagnostics] := "Expected resource of entry 0 to be a JSON Object.")))


  (testing "Returns Error on type mismatch of a update"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:resource
                {:resourceType "Observation"}
                :request
                {:method "PUT"
                 :url "Patient/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "invariant"
        [:issue 0 :details :coding 0 :system] := operation-outcome
        [:issue 0 :details :coding 0 :code] := "MSG_RESOURCE_TYPE_MISMATCH"
        [:issue 0 :expression 0] := "Bundle.entry[0].request.url"
        [:issue 0 :expression 1] := "Bundle.entry[0].resource.resourceType")))


  (testing "Returns Error on missing ID of a update"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:resource
                {:resourceType "Patient"}
                :request
                {:method "PUT"
                 :url "Patient/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "required"
        [:issue 0 :details :coding 0 :system] := operation-outcome
        [:issue 0 :details :coding 0 :code] := "MSG_RESOURCE_ID_MISSING"
        [:issue 0 :expression 0] := "Bundle.entry[0].resource.id")))


  (testing "Returns Error on invalid ID of a update"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:resource
                {:resourceType "Patient"
                 :id "A_B"}
                :request
                {:method "PUT"
                 :url "Patient/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "value"
        [:issue 0 :details :coding 0 :system] := operation-outcome
        [:issue 0 :details :coding 0 :code] := "MSG_ID_INVALID"
        [:issue 0 :expression 0] := "Bundle.entry[0].resource.id")))


  (testing "Returns Error on ID mismatch of a update"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:resource
                {:resourceType "Patient"
                 :id "1"}
                :request
                {:method "PUT"
                 :url "Patient/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "invariant"
        [:issue 0 :details :coding 0 :system] := operation-outcome
        [:issue 0 :details :coding 0 :code] := "MSG_RESOURCE_ID_MISMATCH"
        [:issue 0 :expression 0] := "Bundle.entry[0].request.url"
        [:issue 0 :expression 1] := "Bundle.entry[0].resource.id")))

  (testing "Returns Error on Optimistic Locking Failure of a update"
    (let [{:keys [status body]}
          @((handler-with [[[:create {:resourceType "Patient" :id "0"}]]
                           [[:put {:resourceType "Patient" :id "0"}]]])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:resource
                {:resourceType "Patient"
                 :id "0"}
                :request
                {:method "PUT"
                 :url "Patient/0"
                 :ifMatch "W/\"1\""}}]}})]

      (is (= 412 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "conflict"
        [:issue 0 :diagnostics] := "Precondition `W/\"1\"` failed on `Patient/0`.")))


  (testing "Returns Error on invalid resource"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:resource
                {:resourceType "Patient"
                 :id "0"
                 :gender {}}
                :request
                {:method "PUT"
                 :url "Patient/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "invariant"
        [:issue 0 :expression 0] := "Bundle.entry[0].resource"
        [:issue 0 :diagnostics] := "Resource invalid.")))


  (testing "Returns Error on duplicate resources"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:resource
                {:resourceType "Patient"
                 :id "0"}
                :request
                {:method "PUT"
                 :url "Patient/0"}}
               {:resource
                {:resourceType "Patient"
                 :id "0"}
                :request
                {:method "PUT"
                 :url "Patient/0"}}]}})]

      (is (= 400 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "invariant"
        [:issue 0 :diagnostics] := "Duplicate resource `Patient/0`.")))


  (testing "Returns Error violated referential integrity"
    (let [{:keys [status body]}
          @((handler-with [])
            {:body
             {:resourceType "Bundle"
              :type "transaction"
              :entry
              [{:resource
                {:resourceType "Observation" :id "0"
                 :subject {:reference "Patient/0"}}
                :request
                {:method "POST"
                 :url "Observation"}}]}})]

      (is (= 409 status))

      (given body
        :resourceType := "OperationOutcome"
        [:issue 0 :severity] := "error"
        [:issue 0 :code] := "conflict"
        [:issue 0 :diagnostics] := "Referential integrity violated. Resource `Patient/0` doesn't exist.")))


  (testing "On newly created resource of a update in transaction"
    (let [resource
          {:resourceType "Patient"
           :id "0"}
          entries
          [{:resource
            resource
            :request
            {:method "PUT"
             :url "Patient/0"}}]

          {:keys [status body]}
          @((handler-with [])
            {::reitit/router router
             ::reitit/match {:data {:blaze/context-path ""}}
             :body
             {:resourceType "Bundle"
              :type "transaction"
              :entry entries}})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "transaction-response" (:type body)))

      (is (= "201" (-> body :entry first :response :status)))

      (is (= "/Patient/0/_history/1" (-> body :entry first :response :location)))

      (is (= "W/\"1\"" (-> body :entry first :response :etag)))

      (is (= "1970-01-01T00:00:00Z"
             (-> body :entry first :response :lastModified)))))


  (testing "On updated resource in transaction"
    (let [entries
          [{:resource
            {:resourceType "Patient"
             :id "0"
             :gender "male"}
            :request
            {:method "PUT"
             :url "Patient/0"}}]]

      (testing "with no Prefer header"
        (let [{:keys [status body]}
              @((handler-with
                  [[[:put {:resourceType "Patient" :id "0" :gender "female"}]]])
                {:body
                 {:resourceType "Bundle"
                  :type "transaction"
                  :entry entries}})]

          (is (= 200 status))

          (is (= "Bundle" (:resourceType body)))

          (is (= "transaction-response" (:type body)))

          (is (= "200" (-> body :entry first :response :status)))

          (is (= "W/\"2\"" (-> body :entry first :response :etag)))

          (is (= "1970-01-01T00:00:00Z"
                 (-> body :entry first :response :lastModified)))

          (testing "there is no resource embedded in the entry"
            (is (nil? (-> body :entry first :resource))))))))


  (testing "On created resource in transaction"
    (let [resource
          {:resourceType "Patient"
           :id "0"}
          entries
          [{:resource
            resource
            :request
            {:method "POST"
             :url "Patient"}}]]

      (testing "with no Prefer header"
        (with-redefs
          [random-uuid (constantly #uuid "b11daf6d-4c7b-4f81-980e-8c599bb6bf2d")]
          (let [{:keys [status body]}
                @((handler-with [])
                  {::reitit/router router
                   ::reitit/match {:data {:blaze/context-path ""}}
                   :body
                   {:resourceType "Bundle"
                    :type "transaction"
                    :entry entries}})]

            (is (= 200 status))

            (is (= "Bundle" (:resourceType body)))

            (is (= "transaction-response" (:type body)))

            (is (= "201" (-> body :entry first :response :status)))

            (is (= "/Patient/b11daf6d-4c7b-4f81-980e-8c599bb6bf2d/_history/1" (-> body :entry first :response :location)))

            (is (= "W/\"1\"" (-> body :entry first :response :etag)))

            (is (= "1970-01-01T00:00:00Z"
                   (-> body :entry first :response :lastModified)))

            (testing "there is no resource embedded in the entry"
              (is (nil? (-> body :entry first :resource))))))))))


(defn- stub-match-by-path [router path match]
  (st/instrument
    [`reitit/match-by-path]
    {:spec
     {`reitit/match-by-path
      (s/fspec
        :args (s/cat :router #{router} :path #{path})
        :ret #{match})}
     :stub
     #{`reitit/match-by-path}}))


(deftest handler-batch-create-test
  (testing "Successful"
    (let [handler
          (fn [{:keys [body]}]
            (is (= {:resourceType "Patient"} body))
            (md/success-deferred
              (-> (ring/created "location" ::response-body)
                  (ring/header "Last-Modified" "Mon, 24 Jun 2019 09:54:26 GMT")
                  (ring/header "ETag" "etag"))))]
      (stub-match-by-path
        ::router "/Patient" {:result {:post {:handler handler}}}))

    (let [{:keys [status body]}
          @((handler-with [])
            {::reitit/router ::router
             ::reitit/match {:data {:blaze/context-path ""}}
             :body
             {:resourceType "Bundle"
              :type "batch"
              :entry
              [{:resource
                {:resourceType "Patient"}
                :request
                {:method "POST"
                 :url "Patient"}}]}})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "batch-response" (:type body)))

      (is (= "201" (-> body :entry first :response :status)))

      (is (= "location" (-> body :entry first :response :location)))

      (is (= "etag" (-> body :entry first :response :etag)))

      (is (= "2019-06-24T09:54:26Z" (-> body :entry first :response :lastModified)))

      (is (= ::response-body (-> body :entry first :resource)))))

  (testing "Failing"
    (let [handler
          (fn [_]
            (md/success-deferred
              (ring/bad-request ::operation-outcome)))]
      (stub-match-by-path
        ::router "/Patient" {:result {:post {:handler handler}}}))

    (let [{:keys [status body]}
          @((handler-with [])
            {::reitit/router ::router
             ::reitit/match {:data {:blaze/context-path ""}}
             :body
             {:resourceType "Bundle"
              :type "batch"
              :entry
              [{:resource
                {:resourceType "Patient"}
                :request
                {:method "POST"
                 :url "Patient"}}]}})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "batch-response" (:type body)))

      (is (= "400" (-> body :entry first :response :status)))

      (is (= ::operation-outcome (-> body :entry first :response :outcome))))))


(deftest handler-batch-read-test
  (testing "Successful"
    (let [handler
          (fn [_]
            (md/success-deferred
              (-> (ring/response ::response-body)
                  (ring/header "Last-Modified" "Mon, 24 Jun 2019 09:54:26 GMT")
                  (ring/header "ETag" "etag"))))]
      (stub-match-by-path
        ::router "/Patient/0" {:result {:get {:handler handler}}}))

    (let [{:keys [status body]}
          @((handler-with [])
            {::reitit/router ::router
             ::reitit/match {:data {:blaze/context-path ""}}
             :body
             {:resourceType "Bundle"
              :type "batch"
              :entry
              [{:request
                {:method "GET"
                 :url "Patient/0"}}]}})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "batch-response" (:type body)))

      (is (= "200" (-> body :entry first :response :status)))

      (is (= "etag" (-> body :entry first :response :etag)))

      (is (= "2019-06-24T09:54:26Z" (-> body :entry first :response :lastModified)))

      (is (= ::response-body (-> body :entry first :resource)))))

  (testing "Failing"
    (let [handler
          (fn [_]
            (md/success-deferred
              (ring/bad-request ::operation-outcome)))]
      (stub-match-by-path
        ::router "/Patient/0" {:result {:get {:handler handler}}}))

    (let [{:keys [status body]}
          @((handler-with [])
            {::reitit/router ::router
             ::reitit/match {:data {:blaze/context-path ""}}
             :body
             {:resourceType "Bundle"
              :type "batch"
              :entry
              [{:request
                {:method "GET"
                 :url "Patient/0"}}]}})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "batch-response" (:type body)))

      (is (= "400" (-> body :entry first :response :status)))

      (is (= ::operation-outcome (-> body :entry first :response :outcome))))))


(deftest handler-batch-search-type-test
  (testing "Successful"
    (let [handler
          (fn [_]
            (md/success-deferred
              (ring/response ::response-body)))]
      (stub-match-by-path
        ::router "/Patient" {:result {:get {:handler handler}}}))

    (let [{:keys [status body]}
          @((handler-with [])
            {::reitit/router ::router
             ::reitit/match {:data {:blaze/context-path ""}}
             :body
             {:resourceType "Bundle"
              :type "batch"
              :entry
              [{:request
                {:method "GET"
                 :url "Patient"}}]}})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "batch-response" (:type body)))

      (is (= "200" (-> body :entry first :response :status)))

      (is (= ::response-body (-> body :entry first :resource))))))


(deftest handler-batch-update-test
  (testing "Successful"
    (let [handler
          (fn [{:keys [body]}]
            (is (= {:resourceType "Patient"} body))
            (md/success-deferred
              (-> (ring/response ::response-body)
                  (ring/header "Last-Modified" "Mon, 24 Jun 2019 09:54:26 GMT")
                  (ring/header "ETag" "etag"))))]
      (stub-match-by-path
        ::router "/Patient/0" {:result {:put {:handler handler}}}))

    (let [{:keys [status body]}
          @((handler-with [])
            {::reitit/router ::router
             ::reitit/match {:data {:blaze/context-path ""}}
             :body
             {:resourceType "Bundle"
              :type "batch"
              :entry
              [{:resource
                {:resourceType "Patient"}
                :request
                {:method "PUT"
                 :url "Patient/0"}}]}})]

      (is (= 200 status))

      (is (= "Bundle" (:resourceType body)))

      (is (= "batch-response" (:type body)))

      (is (= "200" (-> body :entry first :response :status)))

      (is (= "etag" (-> body :entry first :response :etag)))

      (is (= "2019-06-24T09:54:26Z" (-> body :entry first :response :lastModified)))

      (is (= ::response-body (-> body :entry first :resource))))))
