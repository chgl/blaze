(ns blaze.db.impl.batch-db
  "Batch Database Implementation

  A batch database keeps key-value store iterators open in order to avoid the
  cost associated with open and closing them."
  (:require
    [blaze.db.impl.codec :as codec]
    [blaze.db.impl.index :as index]
    [blaze.db.impl.index.system-stats :as system-stats]
    [blaze.db.impl.index.type-stats :as type-stats]
    [blaze.db.impl.protocols :as p]
    [blaze.db.kv :as kv])
  (:import
    [java.io Closeable Writer]))


(set! *warn-on-reflection* true)


(defrecord BatchDb [context node snapshot raoi svri cri csvri t]
  p/Db

  ;; ---- Instance-Level Functions --------------------------------------------

  (-resource [_ type id]
    (index/resource* context raoi (codec/tid type) (codec/id-bytes id) t))



  ;; ---- Type-Level Functions ------------------------------------------------

  (-list-resources [_ type start-id]
    (index/type-list context raoi (codec/tid type) (some-> start-id codec/id-bytes) t))

  (-type-total [_ type]
    (with-open [iter (type-stats/new-iterator snapshot)]
      (:total (type-stats/get! iter (codec/tid type) t) 0)))



  ;; ---- System-Level Functions ----------------------------------------------

  (-system-list [_ start-type start-id]
    (index/system-list context (some-> start-type codec/tid) (some-> start-id codec/id-bytes) t))

  (-system-total [_]
    (with-open [iter (system-stats/new-iterator snapshot)]
      (:total (system-stats/get! iter t) 0)))



  ;; ---- Compartment-Level Functions -----------------------------------------

  (-list-compartment-resources [_ code id type start-id]
    (let [compartment {:c-hash (codec/c-hash code) :res-id (codec/id-bytes id)}]
      (index/compartment-list context cri raoi compartment (codec/tid type)
                              (some-> start-id codec/id-bytes) t)))



  ;; ---- Common Query Functions ----------------------------------------------

  (-execute-query [_ query]
    (p/-execute query context snapshot raoi svri csvri t))

  (-execute-query [_ query arg1]
    (p/-execute query context snapshot raoi svri csvri t arg1))



  ;; ---- Instance-Level History Functions ------------------------------------

  (-instance-history [_ type id start-t since]
    (let [start-t (if (some-> start-t (<= t)) start-t t)
          end-t (or (some->> since (index/t-by-instant context)) 0)]
      (index/instance-history context raoi (codec/tid type) (codec/id-bytes id)
                              start-t end-t)))

  (-total-num-of-instance-changes [_ type id since]
    (let [end-t (or (some->> since (index/t-by-instant context)) 0)]
      (index/num-of-instance-changes context (codec/tid type)
                                     (codec/id-bytes id) t end-t)))



  ;; ---- Type-Level History Functions ----------------------------------------

  (-type-history [_ type start-t start-id since]
    (let [start-t (if (some-> start-t (<= t)) start-t t)
          end-t (or (some->> since (index/t-by-instant context)) 0)]
      (index/type-history context (codec/tid type) start-t
                          (some-> start-id codec/id-bytes) end-t)))

  (-total-num-of-type-changes [_ type since]
    (let [tid (codec/tid type)
          end-t (some->> since (index/t-by-instant context))]
      (with-open [snapshot (kv/new-snapshot (:blaze.db/kv-store context))
                  iter (type-stats/new-iterator snapshot)]
        (- (:num-changes (type-stats/get! iter tid t) 0)
           (:num-changes (some->> end-t (type-stats/get! iter tid)) 0)))))



  ;; ---- System-Level History Functions --------------------------------------

  (-system-history [_ start-t start-type start-id since]
    (assert (or (nil? start-id) start-type) "missing start-type on present start-id")
    (let [start-t (if (some-> start-t (<= t)) start-t t)
          end-t (or (some->> since (index/t-by-instant context)) 0)]
      (index/system-history context start-t (some-> start-type codec/tid)
                            (some-> start-id codec/id-bytes) end-t)))

  (-total-num-of-system-changes [_ since]
    (let [end-t (some->> since (index/t-by-instant context))]
      (with-open [snapshot (kv/new-snapshot (:blaze.db/kv-store context))
                  iter (system-stats/new-iterator snapshot)]
        (- (:num-changes (system-stats/get! iter t) 0)
           (:num-changes (some->> end-t (system-stats/get! iter)) 0)))))

  p/QueryCompiler
  (-compile-type-query [_ type clauses]
    (p/-compile-type-query node type clauses))

  (-compile-compartment-query [_ code type clauses]
    (p/-compile-compartment-query node code type clauses))

  Closeable
  (close [_]
    (.close ^Closeable raoi)
    (.close ^Closeable svri)
    (.close ^Closeable cri)
    (.close ^Closeable csvri)
    (.close ^Closeable snapshot)))


(defmethod print-method BatchDb [^BatchDb db ^Writer w]
  (.write w (format "BatchDb[t=%d]" (.t db))))


(defrecord TypeQuery [tid clauses]
  p/Query
  (-execute [_ context snapshot raoi svri _ t]
    (index/type-query context snapshot svri raoi tid clauses t)))


(defrecord CompartmentQuery [c-hash tid clauses]
  p/Query
  (-execute [_ context snapshot raoi _ cspvi t arg1]
    (let [compartment {:c-hash c-hash :res-id (codec/id-bytes arg1)}]
      (index/compartment-query context snapshot cspvi raoi compartment
                               tid clauses t))))


(defn new-batch-db
  ^Closeable
  [{:blaze.db/keys [kv-store] :as context} node t]
  (let [snapshot (kv/new-snapshot kv-store)]
    (->BatchDb
      context node snapshot
      (index/resource-as-of-iter snapshot)
      (kv/new-iterator snapshot :search-param-value-index)
      (kv/new-iterator snapshot :compartment-resource-type-index)
      (kv/new-iterator snapshot :compartment-search-param-value-index)
      t)))
