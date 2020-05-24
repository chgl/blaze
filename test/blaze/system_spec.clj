(ns blaze.system-spec
  (:require
    [blaze.system :as system]
    [clojure.spec.alpha :as s]))


(s/fdef system/init!
  :args (s/cat :env any?))
