(ns blaze.dev
  (:require
    [blaze.db.api :as d]
    [blaze.db.api-spec]
    [blaze.kv.rocksdb :as rocksdb]
    [blaze.kv.rocksdb-spec]
    [blaze.spec]
    [blaze.system :as system]
    [blaze.system-spec]
    [clojure.repl :refer [pst]]
    [clojure.spec.test.alpha :as st]
    [clojure.tools.namespace.repl :refer [refresh]]
    [criterium.core :refer [bench quick-bench]])
  (:import
    [com.github.benmanes.caffeine.cache Cache]))


;; Spec Instrumentation
(st/instrument)


(defonce system nil)


(defn init []
  (alter-var-root #'system (constantly (system/init! (System/getenv))))
  nil)


(defn reset []
  (some-> system system/shutdown!)
  (refresh :after `init))


;; Init Development
(comment
  (init)
  (pst)
  )


;; Reset after making changes
(comment
  (reset)
  (st/unstrument)
  )

;; Specimen
;; 21% patient resource seek
;; 16% compartment seek
;; 12% compartment next
;; 10% observation resource seek
;; 18% O.value.coding contains Code 'LA15920-4' from loinc
;; -----
;; 77 %

;; Patient
;; 16% compartment seek
;;  4% compartment next
;; 14% observation resource seek
;; 11% (O.value as Quantity) > 30 'kg/m2'
;; -----
;; 43%

;; seek 43%
;; key   6%
;; next  3%
;; value 2%
;; --------
;;      54%

(comment
  (def node (:blaze.db/node system))
  (def db (d/db node))

  (.invalidateAll ^Cache (:blaze.db/resource-cache system))

  (rocksdb/compact-range
    (get system [:blaze.kv/rocksdb :blaze.db/kv-store])
    :resource-as-of-index
    true
    1)

  (rocksdb/compact-range
    (get system [:blaze.kv/rocksdb :blaze.db/kv-store])
    :search-param-value-index
    true
    1)

  ;; 27 % read key
  ;; 28 % read value
  ;; 16 % next
  ;;  2 % valid?
  ;;  8 % decode key and value
  ;; 17 % create resource

  (+ 27 28 16 2 8 17)

  ;;  2 % remove higher t's
  ;;  5 % tid->type
  ;;  4 % codec/id
  ;;  5 % mk-resource


  ;; 180 ms
  (quick-bench (count (into [] (d/list-resources db "Specimen"))))
  (bench (count (into [] (d/list-resources db "Specimen"))))
  (time (dotimes [_ 10] (count (into [] (d/list-resources db "Specimen")))))
  (time (dotimes [_ 10] (count (into [] (d/list-resources db "Patient")))))
  (time (dotimes [_ 100] (count (into [] (d/list-resources db "Specimen")))))
  (time (dotimes [_ 100000] (count (into [] (take 10) (d/list-resources db "Patient")))))

  ;; 3 versions history
  ;; ----------------
  ;; 37 % read key
  ;; 15 % read value
  ;; 24 % next
  ;;  3 % valid?
  ;;  2 % create id
  ;;  5 % id equals
  ;;  3 % conj + delete?
  ;;  3 % create resource

  (+ 37 15 24 3 2 5 3)

  (double (/ 5000 27))

  )
