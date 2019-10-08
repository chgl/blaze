(defproject blaze "0.7.0-feature.47.9"
  :description "A FHIR Store with internal, fast CQL Evaluation Engine"
  :url "https://github.com/life-research/blaze"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :min-lein-version "2.0.0"
  :pedantic? :abort

  :dependencies
  [[aleph "0.4.7-alpha1"
    :exclusions
    [io.netty/netty-codec
     io.netty/netty-resolver
     io.netty/netty-handler
     io.netty/netty-transport
     io.netty/netty-transport-native-epoll]]
   [camel-snake-kebab "0.4.0"]
   [cheshire "5.9.0"]
   [com.cognitect/anomalies "0.1.12"]
   [com.h2database/h2 "1.4.199"]
   [com.taoensso/timbre "4.10.0"]
   [commons-codec "1.13"]
   [info.cqframework/cql-to-elm "1.4.6"
    :exclusions
    [com.google.code.javaparser/javaparser
     org.eclipse.persistence/eclipselink
     info.cqframework/qdm
     junit
     xpp3
     xpp3/xpp3_xpath]]
   [integrant "0.7.0"]
   [io.netty/netty-codec-http "4.1.39.Final"]
   [io.netty/netty-handler-proxy "4.1.39.Final"]
   [io.netty/netty-resolver-dns "4.1.39.Final"]
   [io.netty/netty-transport-native-epoll "4.1.39.Final"
    :classifier "linux-x86_64"]
   [io.prometheus/simpleclient_hotspot "0.6.0"]
   [javax.measure/unit-api "1.0"]
   [metosin/reitit-ring "0.3.9"]
   [org.apache.httpcomponents/httpcore "4.4.12"]
   [org.clojars.akiel/datomic-spec "0.5.2"]
   [org.clojars.akiel/datomic-tools "0.4"]
   [org.clojars.akiel/env-tools "0.3.0"]
   [org.clojars.akiel/spec-coerce "0.4.0"]
   [org.clojure/clojure "1.10.1"]
   [org.clojure/core.cache "0.8.2"]
   [org.clojure/tools.reader "1.3.2"]
   [org.eclipse.persistence/org.eclipse.persistence.moxy "2.7.4"
    :scope "runtime"]
   [phrase "0.3-alpha3"]
   [prom-metrics "0.5-alpha2"]
   [ring/ring-core "1.7.1"
    :exclusions [clj-time commons-fileupload
                 commons-io crypto-equality crypto-random]]
   [systems.uom/systems-ucum "0.9"]
   [systems.uom/systems-quantity "1.0"]

   ;; Needed to work with Java 11. Doesn't hurt Java 8.
   [javax.xml.bind/jaxb-api "2.4.0-b180830.0359"]]

  :profiles
  {:dev
   {:source-paths ["dev"]
    :dependencies
    [[criterium "0.4.5"]
     [org.clojars.akiel/iota "0.1"]
     [org.clojure/data.xml "0.0.8"]
     [org.clojure/test.check "0.10.0"]
     [org.clojure/tools.namespace "0.3.1"]]}

   :uberjar
   {:aot [blaze.core]
    :main ^:skip-aot blaze.core}

   :provided
   {:dependencies
    [[com.datomic/datomic-free "0.9.5697"
      :exclusions [io.netty/netty-all]]]}

   :datomic-free
   {:dependencies
    [[com.datomic/datomic-free "0.9.5697"
      :exclusions [io.netty/netty-all]]]}

   :datomic-pro
   {:repositories
    [["my.datomic.com"
      {:url "https://my.datomic.com/repo"}]]
    :dependencies
    [[com.datastax.cassandra/cassandra-driver-core "3.7.2"
      :exclusions [io.netty/netty-handler
                   org.slf4j/slf4j-api]]
     [com.datomic/datomic-pro "0.9.5966"
      :exclusions [io.netty/netty-all]]
     [com.google.guava/guava "19.0"]]
    :uberjar-name "blaze-pro-%s-standalone.jar"}}

  :aliases
  {"build" ["with-profile" "+datomic-free" "uberjar"]
   "build-pro" ["with-profile" "-provided,+datomic-pro" "uberjar"]
   "deps-pro" ["with-profile" "-provided,+datomic-pro" "deps"]})
