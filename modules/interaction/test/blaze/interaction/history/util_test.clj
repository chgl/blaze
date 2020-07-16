(ns blaze.interaction.history.util-test
  (:require
    [blaze.interaction.history.util :as history-util]
    [clojure.spec.test.alpha :as st]
    [clojure.test :as test :refer [are deftest is testing]]
    [juxt.iota :refer [given]]
    [reitit.core :as reitit])
  (:import
    [java.time Instant]))


(defn fixture [f]
  (st/instrument)
  (f)
  (st/unstrument))


(test/use-fixtures :each fixture)


(deftest since
  (testing "no query param"
    (is (nil? (history-util/since {}))))

  (testing "invalid query param"
    (are [t] (nil? (history-util/since {"_since" t}))
      "<invalid>"
      "-1"
      ""))

  (testing "valid query param"
    (are [v t] (= t (history-util/since {"_since" v}))
      "2015-02-07T13:28:17+02:00" (Instant/ofEpochSecond 1423308497)
      ["<invalid>" "2015-02-07T13:28:17+02:00"] (Instant/ofEpochSecond 1423308497)
      ["2015-02-07T13:28:17+02:00" "2015-02-07T13:28:17Z"] (Instant/ofEpochSecond 1423308497))))


(deftest page-t
  (testing "no query param"
    (is (nil? (history-util/page-t {}))))

  (testing "invalid query param"
    (are [t] (nil? (history-util/page-t {"__page-t" t}))
      "<invalid>"
      "-1"
      ""))

  (testing "valid query param"
    (are [v t] (= t (history-util/page-t {"__page-t" v}))
      "1" 1
      ["<invalid>" "2"] 2
      ["3" "4"] 3)))


(deftest page-type
  (testing "no query param"
    (is (nil? (history-util/page-type {}))))

  (testing "invalid query param"
    (are [type] (nil? (history-util/page-type {"__page-type" type}))
      "<invalid>"
      ""))

  (testing "valid query param"
    (are [v type] (= type (history-util/page-type {"__page-type" v}))
      "A" "A"
      ["<invalid>" "A"] "A"
      ["A" "B"] "A")))


(def ^:private router
  (reitit/router
    [["/Patient" {:name :Patient/type}]
     ["/Patient/{id}" {:name :Patient/instance}]]
    {:syntax :bracket}))


(deftest build-entry-test
  (testing "Initial version with server assigned id"
    (given
      (history-util/build-entry
        router
        (with-meta
          {:resourceType "Patient"
           :id "0"
           :meta {:versionId "1"}}
          {:blaze.db/op :create
           :blaze.db/num-changes 1
           :blaze.db/tx {:blaze.db.tx/instant Instant/EPOCH}}))
      :fullUrl := "/Patient/0"
      [:request :method] := "POST"
      [:request :url] := "/Patient"
      [:resource :resourceType] := "Patient"
      [:resource :id] := "0"
      [:response :status] := "201"
      [:response :lastModified] := "1970-01-01T00:00:00Z"
      [:response :etag] := "W/\"1\""))


  (testing "Initial version with client assigned id"
    (given
      (history-util/build-entry
        router
        (with-meta
          {:resourceType "Patient"
           :id "0"
           :meta {:versionId "1"}}
          {:blaze.db/op :put
           :blaze.db/num-changes 1
           :blaze.db/tx {:blaze.db.tx/instant Instant/EPOCH}}))
      :fullUrl := "/Patient/0"
      [:request :method] := "PUT"
      [:request :url] := "/Patient/0"
      [:resource :resourceType] := "Patient"
      [:resource :id] := "0"
      [:response :status] := "201"
      [:response :lastModified] := "1970-01-01T00:00:00Z"
      [:response :etag] := "W/\"1\""))


  (testing "Non-initial version"
    (given
      (history-util/build-entry
        router
        (with-meta
          {:resourceType "Patient"
           :id "0"
           :meta {:versionId "2"}}
          {:blaze.db/op :put
           :blaze.db/num-changes 2
           :blaze.db/tx {:blaze.db.tx/instant Instant/EPOCH}}))
      :fullUrl := "/Patient/0"
      [:request :method] := "PUT"
      [:request :url] := "/Patient/0"
      [:resource :resourceType] := "Patient"
      [:resource :id] := "0"
      [:response :status] := "200"
      [:response :lastModified] := "1970-01-01T00:00:00Z"
      [:response :etag] := "W/\"2\""))


  (testing "Deleted version"
    (given
      (history-util/build-entry
        router
        (with-meta
          {:resourceType "Patient"
           :id "0"
           :meta {:versionId "2"}}
          {:blaze.db/op :delete
           :blaze.db/num-changes 2
           :blaze.db/tx {:blaze.db.tx/instant Instant/EPOCH}}))
      :fullUrl := "/Patient/0"
      [:request :method] := "DELETE"
      [:request :url] := "/Patient/0"
      [:response :status] := "204"
      [:response :lastModified] := "1970-01-01T00:00:00Z"
      [:response :etag] := "W/\"2\"")))
