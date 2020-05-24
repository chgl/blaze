(ns blaze.db.impl.iterators
  (:require
    [blaze.db.kv :as kv])
  (:import
    [clojure.lang IReduceInit]
    [java.nio ByteBuffer BufferOverflowException])
  (:refer-clojure :exclude [keys]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def ^:const ^:private ^long buffer-size 1024)


(defn- read-key! [iter ^ByteBuffer bb]
  (when (< buffer-size ^long (kv/-key iter (.clear bb)))
    (throw (BufferOverflowException.))))


(defn- reduce-keys! [iter decode dir rf init]
  (let [kb (ByteBuffer/allocateDirect buffer-size)]
    (loop [ret init]
      (if (kv/valid? iter)
        (do
          (read-key! iter kb)
          (let [ret (rf ret (decode kb))]
            (if (reduced? ret)
              @ret
              (do (dir iter) (recur ret)))))
        ret))))


(defn keys
  "Returns a reducible collection of keys of `iter`.

  Doesn't close the iterator."
  ([iter decode]
   (reify IReduceInit
     (reduce [_ rf init]
       (kv/-seek-to-first' iter)
       (reduce-keys! iter decode kv/-next' rf init))))
  ([iter decode start-key]
   (reify IReduceInit
     (reduce [_ rf init]
       (kv/-seek' iter start-key)
       (reduce-keys! iter decode kv/-next' rf init)))))


(defn keys-prev [iter decode start-key]
  (reify IReduceInit
    (reduce [_ rf init]
      (kv/-seek-for-prev' iter start-key)
      (reduce-keys! iter decode kv/-prev' rf init))))


(defn- read-value! [iter ^ByteBuffer bb]
  (when (< buffer-size ^long (kv/-value iter (.clear bb)))
    (throw (BufferOverflowException.))))


(defn- reduce-kvs! [iter decode dir rf init]
  (let [kb (ByteBuffer/allocateDirect buffer-size)
        vb (ByteBuffer/allocateDirect buffer-size)]
    (loop [ret init]
      (if (kv/valid? iter)
        (do
          (read-key! iter kb)
          (read-value! iter vb)
          (let [ret (rf ret (decode kb vb))]
            (if (reduced? ret)
              @ret
              (do (dir iter) (recur ret)))))
        ret))))


(defn kvs
  "Returns a reducible collection of keys and values of `iter`.

  Doesn't close the iterator."
  ([iter decode start-key]
   (reify IReduceInit
     (reduce [_ rf init]
       (kv/-seek' iter start-key)
       (reduce-kvs! iter decode kv/-next' rf init)))))


(defn iter
  "Returns a reducible collection of `iter` itself.

  Doesn't close the iterator."
  ([iter start-key]
   (reify IReduceInit
     (reduce [_ rf init]
       (kv/-seek' iter start-key)
       (loop [ret init]
         (if (kv/valid? iter)
           (let [ret (rf ret iter)]
             (if (reduced? ret)
               @ret
               (do (kv/-next' iter) (recur ret))))
           ret))))))


(defn key-reader [buf-size]
  (let [buf (ByteBuffer/allocateDirect buf-size)]
    (fn [iter]
      (kv/-key iter (.clear buf))
      buf)))
