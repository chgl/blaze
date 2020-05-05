(ns blaze.db.kv
  "Protocols for key-value store backend implementations."
  (:import
    [java.io Closeable]
    [java.nio ByteBuffer])
  (:refer-clojure :exclude [get key next]))


(set! *warn-on-reflection* true)


(defprotocol KvIterator
  "A mutable iterator over a KvSnapshot."

  (-seek [iter target])

  (-seek-buffer! [iter target])

  (-seek-for-prev [iter target])

  (seek-to-first [iter])

  (seek-to-last [iter])

  (next [iter]
    "Moves this iterator to the next entry and returns the key of the entry if
    there is one.

    Must not be called if a previous operation returned nil.")

  (prev [iter]
    "Moves this iterator to the previous entry and returns the key of the entry
    if there is one.

    Must not be called if a previous operation returned nil.")

  (-valid? [iter])

  (-key [iter] [iter buf])

  (-value [iter] [iter buf]))


(defn seek
  "Positions this iterator at the first entry whose key is at or past `target`
  and returns the key of the entry if there is one.

  The `target` is a byte array describing a key or a key prefix to seek for.

  Must not be called if a previous operation returned nil."
  [iter target]
  (-seek iter target))


(defn seek-buffer!
  [iter target]
  (-seek-buffer! iter target))


(defn seek-for-prev
  "Positions this iterator at the first entry whose key is at or before `target`
  and returns the key of the entry if there is one.

  The `target` is a byte array describing a key or a key prefix to seek for.

  Must not be called if a previous operation returned nil."
  [iter target]
  (-seek-for-prev iter target))


(defn valid?
  [iter]
  (-valid? iter))


(defn key
  ([iter]
   (-key iter))
  ([iter buf]
   (-key iter buf)))


(defn value
  "Returns the value of the current entry of this iterator.

  Must not be called if a previous operation returned nil."
  ([iter]
   (-value iter))
  ([iter buf]
   (-value iter buf)))


(defprotocol KvBufferIterator
  (-seek-buffer [iter])

  (-fill-key-buffer! [iter])

  (-key-buffer [iter])

  (-fill-value-buffer! [iter])

  (-value-buffer [iter]))


(defn ^ByteBuffer seek-buffer
  [iter]
  (-seek-buffer iter))


(defn fill-key-buffer!
  [iter]
  (-fill-key-buffer! iter))


(defn ^ByteBuffer key-buffer
  [iter]
  (-key-buffer iter))


(defn fill-value-buffer!
  [iter]
  (-fill-value-buffer! iter))


(defn ^ByteBuffer value-buffer
  [iter]
  (-value-buffer iter))


(deftype KvBufferIteratorImpl
  [^Closeable iter ^ByteBuffer seek-buffer ^ByteBuffer key-buffer
   ^ByteBuffer value-buffer]
  KvIterator
  (-seek [_ target]
    (seek iter target))

  (-seek-buffer! [_ target]
    (seek-buffer! iter target))

  (next [_]
    (next iter))

  (-valid? [_]
    (valid? iter))

  (-key [_]
    (key iter))

  KvBufferIterator
  (-seek-buffer [_]
    seek-buffer)

  (-fill-key-buffer! [_]
    (.clear key-buffer)
    (key iter key-buffer))

  (-key-buffer [_]
    key-buffer)

  (-fill-value-buffer! [_]
    (.clear value-buffer)
    (value iter value-buffer))

  (-value-buffer [_]
    value-buffer)

  Closeable
  (close [_]
    (.close iter)))


(defn new-value-buffer-iterator
  "Creates a new iterator based on conventional `iterator` with a shared, direct
  byte buffer of `buffer-size` for its values.

  "
  [iterator key-buf-cap val-buf-cap]
  (KvBufferIteratorImpl.
    iterator
    (ByteBuffer/allocateDirect key-buf-cap)
    (ByteBuffer/allocateDirect key-buf-cap)
    (ByteBuffer/allocateDirect val-buf-cap)))


(defprotocol KvSnapshot
  "A snapshot of the contents of a KvStore."

  (new-iterator
    ^java.io.Closeable [snapshot]
    ^java.io.Closeable [snapshot column-family])

  (snapshot-get [snapshot key] [snapshot column-family key]
    "Returns the value if there is any."))


(defprotocol KvStore
  "A key-value store."

  (new-snapshot ^java.io.Closeable [store])

  (get [store key] [store column-family key]
    "Returns the value if there is any.")

  (-put [store entries] [store key value])

  (delete [store keys]
    "Deletes keys.")

  (write [store entries]
    "Entries are either triples of operator, key and value or quadruples of
    operator, column-family, key and value.

    Operators are :put, :merge and :delete.

    Writes are atomic. Blocks."))


(defn put
  "Entries are either tuples of key and value or triples of column-family, key
  and value. Puts are atomic. Blocks."
  ([store entries]
   (-put store entries))
  ([store key value]
   (-put store key value)))
