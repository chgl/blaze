(ns blaze.db.impl.index.resource-test
  (:require
    [blaze.db.impl.codec :as codec]
    [blaze.db.impl.codec-stub :as codec-stub]
    [blaze.db.impl.index :as index]
    [blaze.db.impl.index.resource :as resource]
    [blaze.db.impl.index.resource-spec]
    [blaze.db.impl.index-spec]
    [blaze.db.kv :as kv]
    [blaze.db.kv.mem :refer [init-mem-kv-store]]
    [blaze.db.kv-stub :as kv-stub]
    [cheshire.core :as json]
    [clojure.spec.test.alpha :as st]
    [clojure.test :as test :refer [deftest is testing]]
    [juxt.iota :refer [given]]
    [taoensso.nippy :as nippy])
  (:import
    [com.github.benmanes.caffeine.cache LoadingCache]
    [java.time Instant]))


(defn fixture [f]
  (st/instrument)
  (f)
  (st/unstrument))


(test/use-fixtures :each fixture)


(defn resource-cache [kv-store]
  (reify LoadingCache
    (get [_ hash]
      (index/load-resource-content kv-store hash))))


(defn new-context []
  (let [kv-store
        (init-mem-kv-store
          {:search-param-value-index nil
           :resource-value-index nil
           :compartment-search-param-value-index nil
           :compartment-resource-value-index nil
           :resource-index nil
           :resource-as-of-index nil
           :tx-success-index nil
           :t-by-instant-index nil})]
    {:blaze.db/kv-store kv-store
     :blaze.db/resource-cache (resource-cache kv-store)}))


(defn- mk-resource [{:blaze.db/keys [kv-store resource-cache]} type id state t]
  (resource/mk-resource kv-store resource-cache (codec/tid type) id
                        (codec/hash {:resourceType type :id id}) state t))


(deftest resource
  (testing "hash is part of equals"
    (is (not (.equals (mk-resource (new-context) "Patient" "0" 0 0)
                      (mk-resource (new-context) "Patient" "1" 0 0)))))

  (testing "state is not part of equals"
    (is (.equals (mk-resource (new-context) "Patient" "0" 0 0)
                 (mk-resource (new-context) "Patient" "0" 1 0))))

  (testing "t is part of equals"
    (is (not (.equals (mk-resource (new-context) "Patient" "0" 0 0)
                      (mk-resource (new-context) "Patient" "0" 0 1)))))

  (testing "resources can be serialized to JSON"
    (let [resource-cache
          (reify LoadingCache
            (get [_ _]
              {:id "0"
               :resourceType "Patient"}))
          context (assoc (new-context) :blaze.db/resource-cache resource-cache)
          resource (mk-resource context "Patient" "0" 0 0)]
      (is (= "{\"id\":\"0\",\"resourceType\":\"Patient\",\"meta\":{\"versionId\":\"0\"}}"
             (json/generate-string resource)))))

  (testing "resources has the right meta data"
    (let [{:blaze.db/keys [kv-store] :as context} (new-context)
          type "Patient"
          id "0"
          resource (mk-resource context "Patient" "0" (codec/state 1 :put) 0)]
      (kv/put kv-store (conj
                         (codec/tx-success-entries 0 (Instant/ofEpochSecond 194004))
                         [:resource-index
                          (codec/hash {:resourceType type :id id})
                          (nippy/fast-freeze {:resourceType type :id id})]))
      (given (meta resource)
        :type := :fhir/Patient
        :blaze.db/num-changes := 1
        :blaze.db/op := :put
        :blaze.db/t := 0
        [:blaze.db/tx :blaze.db/t] := 0
        [:blaze.db/tx :blaze.db.tx/instant] := (Instant/ofEpochSecond 194004)
        count := 5))))


(deftest tx
  (st/unstrument `resource/tx)
  (codec-stub/t-key ::t ::t-key)

  (testing "existing transaction"
    (kv-stub/get ::kv-store :tx-success-index ::t-key #{::tx-bytes})
    (codec-stub/decode-tx ::tx-bytes ::t ::tx)

    (is (= ::tx (resource/tx ::kv-store ::t))))

  (testing "missing transaction"
    (kv-stub/get ::kv-store :tx-success-index ::t-key nil?)

    (is (nil? (resource/tx ::kv-store ::t)))))
