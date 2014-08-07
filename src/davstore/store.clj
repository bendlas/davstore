(ns davstore.store
  (:import javax.xml.bind.DatatypeConverter
           (java.io ByteArrayOutputStream OutputStreamWriter)
           (java.security MessageDigest))
  (:require [davstore.schema :refer [schema]]
            [davstore.blob :refer [make-store store-file sha-file]]
            [clojure.tools.logging :as log]
            [datomic.api :as d :refer [q tempid transact transact-async create-database connect]]
            [webnf.datomic.query :refer [reify-entity]]
            [clojure.repl :refer :all]
            [clojure.pprint :refer :all]))

(def test-uri "datomic:mem://davstore-test")
(def test-blobstore (make-store "/tmp/davstore-test"))

(set! *warn-on-reflection* true)

(def root-id :davstore.container/root)

(declare conn path-entry insert-testdata)

(def zero-sha1 (apply str (repeat 40 \0)))

(defn get-entry [db path]
  (path-entry db root-id path))

(defn xor-bytes [^bytes a1 ^bytes a2]
  (amap a1 i _ (unchecked-byte (bit-xor (aget a1 i)
                                        (aget a2 i)))))

(defn sha1-str [bytes]
  (.toLowerCase
   (DatatypeConverter/printHexBinary bytes)))

(def child-marker (.getBytes "childNode:" "UTF-8"))

(defn child-hash [hash]
  (.digest (doto (MessageDigest/getInstance "SHA-1")
             (.update ^bytes child-marker)
             (.update ^bytes hash))))

(defn calc-sha1 [entry]
  (let [bao (ByteArrayOutputStream. 2048)
        kvs (->> entry seq
                 (remove (comp #{:db/id :davstore.entry/sha1 :davstore.container/children}
                               first))
                 (sort-by first))
        _ (with-open [w (OutputStreamWriter. bao "UTF-8")]
            (binding [*out* w
                      *print-dup* false]
              (doseq [[k v] kvs]
                (assert (#{String Long clojure.lang.Keyword} (class v))
                        (str "Unexpected " (pr-str v)))
                (println k v))))
        hb (.toByteArray bao)
        ehash (.digest (MessageDigest/getInstance "SHA-1")
                       hb)
        _ (log/debug "Hashing entity" (:db/id entry)
                     "to Hash:" (sha1-str ehash) \newline
                     (String. hb "UTF-8"))
        res (sha1-str
             (reduce (fn [hash child]
                       (let [chash (DatatypeConverter/parseHexBinary
                                    (or (:davstore.entry/sha1 child)
                                        (throw (ex-info "Child without sha1"
                                                        {:entry entry :child child}))))]
                         (xor-bytes hash (child-hash chash))))
                     ehash (:davstore.container/children entry)))]
    (log/debug "Final hash for entity" (:db/id entry) ":" res)
    res))

(defn touch-tx [db path mime-type current-entry-sha1 blob-sha1]
  (let [name (last path)
        entry {:davstore.entry/name name
               :davstore.entry/type :davstore.entry.type/file
               :davstore.file.content/sha1 blob-sha1
               :davstore.file.content/mime-type mime-type}]
    [[:davstore.fn/cu-tx root-id (butlast path) name
      :davstore.entry.type/file current-entry-sha1 (calc-sha1 entry) entry]]))

(defn mkdir-tx [db path]
  (let [name (last path)
        entry {:davstore.entry/name name
               :davstore.entry/type :davstore.entry.type/container}]
    [[:davstore.fn/cu-tx root-id (butlast path) name
      nil nil (calc-sha1 entry) entry]]))

(defn touch! [conn db path mime-type current-entry-sha1 blob-sha1]
  (if (zero? (count path))
    {:error :method-not-allowed
     :message "Can't PUT to root"}
    (let [tx (touch-tx db path mime-type current-entry-sha1 blob-sha1)
          res @(transact conn tx)]
      (log/debug "File touch success" res)
      (if (get-entry db path)
        {:success :updated}
        {:success :created}))))

(defn mkdir! [conn db path]
  (if (zero? (count path))
    {:error :method-not-allowed
     :message "Can't PUT to root"}
    (let [tx (mkdir-tx db path)
          res @(transact conn tx)]
      (log/debug "Mkdir success" res)
      {:success :created})))

(defn rm! [conn db path current-entry-sha1 recursive]
  (if (zero? (count path))
    {:error :method-not-allowed
     :message "Can't DELETE to root"}
    (let [tx [[:davstore.fn/rm-tx root-id path current-entry-sha1 recursive]]
          res @(transact conn tx)]
      (log/debug "rm success" res)
      {:success :deleted})))

(defn- ls-seq [{:keys [davstore.entry/name
                       davstore.container/children]
                :as e}
               dir depth]
  (let [path (if (= "" name) ;; omit root dir
               dir
               (conj dir name))]
    (cons (assoc (reify-entity e) :davstore.ls/path path)
          (when (pos? depth)
            (mapcat #(ls-seq % path (dec depth)) children)))))

(defn ls [db path depth]
  (when-let [e (get-entry db path)]
    (ls-seq e [] depth)))

(defn cat [db store path]
  (when-let [sha1 (:davstore.file.content/sha1 (get-entry db path))]
    (println (slurp (sha-file store sha1)))))

;; init

(defn open-store! [db-uri]
  (let [created (create-database db-uri)
        conn (connect db-uri)
        entity {:db/doc "File root singleton"
                :db/id (tempid :db.part/davstore.entries)
                :db/ident root-id
                :davstore.entry/name ""
                :davstore.entry/type :davstore.entry.type/container}]
    (when created
      @(transact conn schema)
      @(transact conn [(assoc entity :davstore.entry/sha1 (calc-sha1 entity))]))
    (let [db (d/db conn)]
      (def path-entry (:db/fn (d/entity db :davstore.fn/path-entry)))
      (def conn conn))))

(defn- store-str [blob-store ^String s]
  (store-file blob-store (java.io.ByteArrayInputStream.
                          (.getBytes s "UTF-8"))))

;; dev

(defn insert-testdata [conn blob-store]
  (let [store-str (partial store-str blob-store)
        db (d/db conn)]
    (touch! conn db ["a"] "text/plain; charset=utf-8" nil (store-str "a's new content"))
    (touch! conn db ["b"] "text/plain; charset=utf-8" nil (store-str "b's content"))
    (mkdir! conn db ["d"])
    (touch! conn db ["d" "c"] "text/plain; charset=utf-8" nil (store-str "d/c's content"))))

(defmulti print-entry (fn [entry depth] (:davstore.entry/type entry)))
(defmethod print-entry :davstore.entry.type/container
  [{:keys [davstore.entry/name davstore.entry/sha1 davstore.container/children]} depth]
  (apply concat
         (repeat depth "  ")
         [name "/" " - EH: " sha1 "\n"]
         (map #(print-entry % (inc depth)) children)))

(defmethod print-entry :davstore.entry.type/file
  [{:keys [davstore.entry/name davstore.entry/sha1]
    content :davstore.file.content/sha1} depth]
  (concat
   (repeat depth "  ")
   [name " - EH: " sha1 " - CH: " content "\n"]))

(defn pr-tree [db]
  (doseq [s (print-entry (d/entity db root-id) 0)]
    (print s)))
