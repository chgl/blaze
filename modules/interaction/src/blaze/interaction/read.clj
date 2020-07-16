(ns blaze.interaction.read
  "FHIR read interaction.

  https://www.hl7.org/fhir/http.html#read"
  (:require
    [blaze.db.api :as d]
    [blaze.handler.util :as handler-util]
    [blaze.middleware.fhir.metrics :refer [wrap-observe-request-duration]]
    [cognitect.anomalies :as anom]
    [integrant.core :as ig]
    [manifold.deferred :as md]
    [reitit.core :as reitit]
    [ring.util.response :as ring]
    [taoensso.timbre :as log])
  (:import
    [java.time ZonedDateTime ZoneId]
    [java.time.format DateTimeFormatter]))


(set! *warn-on-reflection* true)


(def ^:private gmt (ZoneId/of "GMT"))


(defn- last-modified [{{:blaze.db.tx/keys [instant]} :blaze.db/tx}]
  (->> (ZonedDateTime/ofInstant instant gmt)
       (.format DateTimeFormatter/RFC_1123_DATE_TIME)))


(defn- etag [resource]
  (str "W/\"" (-> resource :meta :versionId) "\""))


(defn- db [node vid]
  (cond
    (and vid (re-matches #"\d+" vid))
    (let [vid (Long/parseLong vid)]
      (-> (d/sync node vid) (md/chain #(d/as-of % vid))))

    vid
    (md/error-deferred
      {::anom/category ::anom/not-found
       :fhir/issue "not-found"})

    :else
    (d/db node)))


(defn- handler-intern [node]
  (fn [{{{:fhir.resource/keys [type]} :data} ::reitit/match
        {:keys [id vid]} :path-params}]
    (-> (db node vid)
        (md/chain'
          (fn [db]
            (if-let [resource (d/resource db type id)]
              (if (d/deleted? resource)
                (-> (handler-util/operation-outcome
                      {:fhir/issue "deleted"})
                    (ring/response)
                    (ring/status 410)
                    (ring/header "Last-Modified" (last-modified (meta resource)))
                    (ring/header "ETag" (etag resource)))
                (-> (ring/response resource)
                    (ring/header "Last-Modified" (last-modified (meta resource)))
                    (ring/header "ETag" (etag resource))))
              (handler-util/error-response
                {::anom/category ::anom/not-found
                 :fhir/issue "not-found"}))))
        (md/catch' handler-util/error-response))))


(defn wrap-interaction-name [handler]
  (fn [{{:keys [vid]} :path-params :as request}]
    (-> (handler request)
        (md/chain'
          (fn [response]
            (assoc response :fhir/interaction-name (if vid "vread" "read")))))))


(defn handler [node]
  (-> (handler-intern node)
      (wrap-interaction-name)
      (wrap-observe-request-duration)))


(defmethod ig/init-key :blaze.interaction/read
  [_ {:keys [node]}]
  (log/info "Init FHIR read interaction handler")
  (handler node))
