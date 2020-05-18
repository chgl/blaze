(ns blaze.db.impl.iterators
  (:require
    [blaze.db.kv :as kv])
  (:import
    [clojure.lang IReduceInit])
  (:refer-clojure :exclude [keys]))


(defn keys
  "Returns a reducible collection of keys of `iter`.

  Doesn't close the iterator."
  ([iter]
   (reify IReduceInit
     (reduce [_ rf init]
       (loop [ret init
              k (kv/seek-to-first! iter)]
         (if k
           (let [ret (rf ret k)]
             (if (reduced? ret)
               @ret
               (recur ret (kv/next! iter))))
           ret)))))
  ([iter start-key]
   (reify IReduceInit
     (reduce [_ rf init]
       (loop [ret init
              k (kv/seek! iter start-key)]
         (if k
           (let [ret (rf ret k)]
             (if (reduced? ret)
               @ret
               (recur ret (kv/next! iter))))
           ret))))))


(defn keys-prev [iter start-key]
 (reify IReduceInit
   (reduce [_ rf init]
     (loop [ret init
            k (kv/seek-for-prev! iter start-key)]
       (if k
         (let [ret (rf ret k)]
           (if (reduced? ret)
             @ret
             (recur ret (kv/prev! iter))))
         ret)))))
