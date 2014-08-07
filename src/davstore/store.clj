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

(defmacro spy [& exprs]
  (let [args (butlast exprs)
        expr (last exprs)]
    `(do 
       ~@(for [arg args]
           `(let [res# ~arg]
              (pprint '~arg)
              (println "=>")
              (pprint res#)))
       (let [res# ~expr]
         (pprint '~expr)
         (println "=>")
         (pprint res#)
         res#))))

(def zero-sha1 (apply str (repeat 40 \0)))

(defn store-db [{:keys [db conn]}]
  (if db
    db
    (do (log/debug "Store not opened, getting current snapshot")
        (d/db conn))))

(defn get-entry [{:keys [root path-entry] :as store} path]
  (path-entry (store-db store) root path))

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
            (binding [*out* w]
              (doseq [[k v] kvs]
                (assert (#{String Long clojure.lang.Keyword java.util.UUID} (class v))
                        (str "Unexpected " (pr-str v)))
                (println (str k) (str v)))))
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

(defmacro deffileop [name verb [store-sym path-sym & args] & body]
  `(defn ~name [store# path# ~@args]
     (if (zero? (count path#))
       {:error :method-not-allowed
        :message (str "Can't " ~verb " to root")}
       (let [~store-sym (assoc store# :db (store-db store#))
             ~path-sym path#]
         ~@body))))

(deffileop touch! "PUT" [{:keys [conn db root] :as store} path mime-type current-entry-sha1 blob-sha1]
  (let [name (last path)
        entry {:davstore.entry/name name
               :davstore.entry/type :davstore.entry.type/file
               :davstore.file.content/sha1 blob-sha1
               :davstore.file.content/mime-type mime-type}
        res @(transact conn [[:davstore.fn/cu-tx root (butlast path) name :davstore.entry.type/file 
                              current-entry-sha1 (calc-sha1 entry) entry]])]
    (log/debug "File touch success" res)
    (if (get-entry store path)
      {:success :updated}
      {:success :created})))


(deffileop mkdir! "MKCOLL" [{:keys [conn db root]} path]
  (let [name (last path)
        entry {:davstore.entry/name name
               :davstore.entry/type :davstore.entry.type/container}
        res @(transact conn [[:davstore.fn/cu-tx root (butlast path) name
                              nil nil (calc-sha1 entry) entry]])]
    (log/debug "Mkdir success" res)
    {:success :created}))

(deffileop rm! "DELETE" [{:keys [conn db root]} path current-entry-sha1 recursive]
  (let [res @(transact conn [[:davstore.fn/rm-tx root path current-entry-sha1 recursive]])]
    (log/debug "rm success" res)
    {:success :deleted}))

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

(defn ls [store path depth]
  (when-let [e (get-entry store path)]
    (ls-seq e [] depth)))

(defn cat [store path]
  (when-let [sha1 (:davstore.file.content/sha1 (get-entry store path))]
    (println (slurp (sha-file (:blob-store store) sha1)))))

;; init

(def root-id :davstore.container/root)

(declare crate-store!)

(defn open-root! [{:keys [conn] :as store} uuid create-if-missing]
  (let [db (d/db conn)
        entity {:db/doc (str "File root {" uuid "}")
                :db/id (tempid :db.part/davstore.entries)
                :davstore.root/id uuid
                :davstore.entry/name (str \{ uuid \})
                :davstore.entry/type :davstore.entry.type/container}
        root (or (d/entity db [:davstore.root/id uuid])
                 (when-let
                     [db (and create-if-missing
                              (:db-after @(transact conn [(assoc entity :davstore.entry/sha1 (calc-sha1 entity))])))]
                   (d/entity db [:davstore.root/id uuid]))
                 (throw (ex-info (str "No store {" uuid "}")
                                 {:conn conn :uuid uuid})))]
    (assoc store
      :store-id uuid
      :root (:db/id root))))

(defn init-store! 
  ([db-uri blob-store] (init-store! db-uri blob-store (d/squuid) true))
  ([db-uri blob-store main-root-uuid create-if-missing]
     (let [created (create-database db-uri)
           conn (connect db-uri)
           _ (when created
               @(transact conn schema))
           db (d/db conn)
           res (assoc (open-root! {:conn conn} main-root-uuid create-if-missing)
                 :path-entry (:db/fn (d/entity db :davstore.fn/path-entry))
                 :blob-store blob-store)
           db (if created
                (:db-after @(transact conn [[:db/add [:davstore.root/id main-root-uuid]
                                             :db/ident root-id]]))
                db)]
       (assoc res :main-root (:db/id (d/entity db [:davstore.root/id main-root-uuid]))))))

;; dev

(set! *warn-on-reflection* true)

(def test-uri "datomic:mem://davstore-test")
(defonce test-blobstore (make-store "/tmp/davstore-test"))
(defonce test-store (init-store! test-uri test-blobstore))

(defn- store-str [{:keys [blob-store]} ^String s]
  (store-file blob-store (java.io.ByteArrayInputStream.
                          (.getBytes s "UTF-8"))))


(defn insert-testdata [{:keys [conn] :as store}]
  (let [store-str (partial store-str store)
        db (d/db conn)]
    (touch! test-store ["a"] "text/plain; charset=utf-8" nil (store-str "a's new content"))
    (touch! test-store ["b"] "text/plain; charset=utf-8" nil (store-str "b's content"))
    (mkdir! test-store ["d"])
    (touch! test-store ["d" "c"] "text/plain; charset=utf-8" nil (store-str "d/c's content"))))

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

(defn pr-tree [store]
  (doseq [s (print-entry (d/entity (store-db store) (:root store)) 0)]
    (print s)))
