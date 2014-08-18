(ns davstore.store
  (:import javax.xml.bind.DatatypeConverter
           (java.io ByteArrayOutputStream OutputStreamWriter
                    InputStream OutputStream)
           (java.security MessageDigest)
           java.util.UUID)
  (:require [davstore.schema :refer [schema]]
            [davstore.blob :refer [make-store store-file get-file BlobStore]]
            [clojure.tools.logging :as log]
            [webnf.datomic.query :refer [reify-entity]]
            [clojure.repl :refer :all]
            [clojure.pprint :refer :all]
            [clojure.core.typed :as typ :refer
             [ann ann-form cf defalias
              Bool List HMap HVec Map Set Vec Value IFn Option Keyword Seqable Future 
              U I Rec Any All]]))

(require '[datomic.api :as d :refer [q tempid transact transact-async create-database connect]])

;; # Type declaractions

;; ## External Types

(ann clojure.core/sort-by
     (All [a b] ;[[a -> b] (java.util.Comparator b) (Seqable a) -> (Seqable a)]
          [[a -> b] (Seqable a) -> (List a)]))

(defalias Logger Object)
(defalias LoggerFactory Object)
(ann clojure.tools.logging.impl/get-logger [LoggerFactory clojure.lang.Namespace -> Logger])
(ann clojure.tools.logging.impl/enabled? [Logger Keyword -> Bool])
(ann clojure.tools.logging/*logger-factory* LoggerFactory)
(ann clojure.tools.logging/log* [Logger Keyword (Option Throwable) String -> nil])

(defalias TxResult (HMap :mandatory {:db-after datomic.db.Db}))
(defalias TxItem (U (Vec Any)
                    (Map Keyword Any)))
(defalias DbId (U datomic.db.DbId Long Keyword (HVec [Keyword Any])))
(defalias Entity (HMap :mandatory {:db/id DbId}))
(ann datomic.api/transact [datomic.Connection (Seqable TxItem) -> (Future TxResult)])
(ann datomic.api/db [datomic.Connection -> datomic.db.Db])
(ann datomic.api/entity [datomic.db.Db DbId -> Entity])
(ann datomic.api/tempid (IFn [Keyword -> datomic.db.DbId]
                             [Keyword Long -> datomic.db.DbId]))
(ann datomic.api/squuid [-> UUID])
(ann datomic.api/create-database [String -> Bool])
(ann datomic.api/connect [String -> datomic.Connection])
(ann davstore.schema/schema (List TxItem))

(ann webnf.datomic.query/reify-entity [(Map Keyword Any) -> (Map Keyword Any)])

;; ## Application types

(defalias Sha1B (Array byte))
(defalias Sha1 String)

(defalias Entry
  (Rec [entry]
       (I (Map Keyword Any)
          (U (HMap :mandatory {:davstore.entry/name String
                               :davstore.entry/type (Value :davstore.entry.type/container)}
                   :optional {:davstiore.container/children (Set entry)
                              :davstore.entry/sha1 Sha1})
             (HMap :mandatory {:davstore.entry/name String
                               :davstore.entry/type (Value :davstore.entry.type/file)
                               :davstore.file.content/sha1 Sha1}
                   :optional {:davstore.file.content/mime-type String
                              :davstore.entry/sha1 Sha1})))))

(defalias Dir
  (I Entry (HMap :mandatory
                 {:davstore.entry/type (Value :davstore.entry.type/container)})))
(defalias File
  (I Entry (HMap :mandatory
                 {:davstore.entry/type (Value :davstore.entry.type/file)})))

(defalias Path (List String))

(defalias Store "A datomic-backed file store"
  (HMap :mandatory {:conn datomic.Connection
                    :store-id UUID
                    :root UUID
                    :path-entry [datomic.db.Db DbId Path -> Entry]}
        :optional {:db datomic.db.Db}))

;; # Code

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

;; ## SHA-1 Stuff

(ann zero-sha1 Sha1)
(def zero-sha1 (apply str (repeat 40 \0)))

(ann xor-bytes [Sha1B Sha1B -> Sha1B])
;; FIXME CTYP-167
(defn ^:no-check xor-bytes [^bytes a1 ^bytes a2]
  (amap a1 i _ (unchecked-byte (bit-xor (aget a1 i)
                                        (aget a2 i)))))


;; ## File Store

(ann store-db [Store -> datomic.db.Db])
(defn store-db [{:keys [db conn]}]
  (if db
    db
    (do (log/debug "Store not opened, getting current snapshot")
        (d/db conn))))

(ann get-entry [Store Path -> Entry])
(defn get-entry [{:keys [root path-entry] :as store} path]
  (path-entry (store-db store) [:davstore.root/id root] path))

;; ### File Ops

(defmacro deffileop [name verb [store-sym path-sym & args] & body]
  `(defn ~name [store# path# ~@args]
     (if (empty? path#)
       {:error :method-not-allowed
        :message (str "Can't " ~verb " to root")}
       (let [~store-sym (assoc store# :db (store-db store#))
             ~path-sym path#]
         ~@body))))

(defalias OpResult (Map Keyword Any))

(ann touch! [Store Path String (Option Sha1) Sha1 -> OpResult])
(deffileop touch! "PUT" [{:keys [conn root path-entry] :as store} path mime-type current-entry-sha1 blob-sha1]
  (let [db (store-db store)
        {:keys [davstore.entry/sha1 davstore.entry/type] :as have} (path-entry db [:davstore.root/id root] path)
        _ (when (not= current-entry-sha1 sha1)
            (throw (ex-info "SHA-1 mismatch"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/sha1
                             :cas/expected current-entry-sha1
                             :cas/current sha1})))
        _ (when (and have (not= :davstore.entry.type/file type))
            (throw (ex-info "Target type mismatch"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/type
                             :cas/expected :davstore.entry.type/file
                             :cas/current type})))
        res @(transact conn [[:davstore.fn/cu-tx [:davstore.root/id root] path current-entry-sha1
                              {:davstore.entry/name (last path)
                               :davstore.entry/type :davstore.entry.type/file
                               :davstore.file.content/sha1 blob-sha1
                               :davstore.file.content/mime-type mime-type}]])]
    (log/debug "File touch success" res)
    (if have
      {:success :updated}
      {:success :created})))

(ann mkdir! [Store Path -> OpResult])
(deffileop mkdir! "MKCOLL" [{:keys [conn root]} path]
  (let [res @(transact conn [[:davstore.fn/cu-tx [:davstore.root/id root] path nil
                              {:davstore.entry/name (last path)
                               :davstore.entry/type :davstore.entry.type/container}]])]
    (log/debug "Mkdir success" res)
    {:success :created}))

(ann rm! [Store Path Sha1 Bool -> OpResult])
(deffileop ^:no-check rm! "DELETE" [{:keys [conn root path-entry] :as store} path current-entry-sha1 recursive]
  (let [{:keys [davstore.entry/sha1 davstore.container/children] :as have} (path-entry (store-db store) [:davstore.root/id root] path)
        _ (when (not= current-entry-sha1 sha1)
            (throw (ex-info "SHA-1 mismatch"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/sha1
                             :cas/expected current-entry-sha1
                             :cas/current sha1})))
        _ (when-not (and (not recursive) (seq children))
            (throw (ex-info "Directory not empty"
                            {:error :dir-not-empty
                             :path path})))
        res @(transact conn [[:davstore.fn/rm-tx [:davstore.root/id root] path current-entry-sha1]])]
    (log/debug "rm success" res)
    {:success :deleted}))

(ann cp! [Store Path Path Bool -> OpResult])
(deffileop cp! "COPY" [{:keys [conn path-entry root] :as store}
                       from-path to-path recursive overwrite]
  (let [db (store-db store)
        {:keys [davstore.entry/type] :as have} (path-entry db [:davstore.root/id root] from-path)
        target (path-entry db [:davstore.root/id root] to-path)
        _ (when-not have (throw (ex-info "Not found" {:error :not-found :path from-path})))
        _ (when-not (or recursive (= :davstore.entry.type/file type))
            (throw (ex-info "Copy source is directory"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/type
                             :cas/expected :davstore.entry.type/file
                             :cas/current type})))
        _ (when-not (or overwrite (not target))
            (throw (ex-info "Copy target exists"
                            {:error :target-exists :path to-path})))
        res @(transact conn [[:davstore.fn/cp-tx [:davstore.root/id root]
                              from-path (:davstore.entry/sha1 have)
                              to-path (:davstore.entry/sha1 target)
                              false]])]
    (log/debug "cp success" res)
    {:success :copied}))

(ann mv! [Store Path Path Bool -> OpResult])
(deffileop mv! "MOVE" [{:keys [conn path-entry root] :as store}
                       from-path to-path recursive overwrite]
  (let [db (store-db store)
        {:keys [davstore.entry/type] :as have} (path-entry db [:davstore.root/id root] from-path)
        target (path-entry db [:davstore.root/id root] to-path)
        _ (when-not have (throw (ex-info "Not found" {:error :not-found :path from-path})))
        _ (when-not (or recursive (= :davstore.entry.type/file type))
            (throw (ex-info "Move source is directory"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/type
                             :cas/expected :davstore.entry.type/file
                             :cas/current type})))
        _ (when-not (or overwrite (not target))
            (throw (ex-info "Move target exists"
                            {:error :target-exists :path to-path})))
        res @(transact conn [[:davstore.fn/cp-tx [:davstore.root/id root]
                              from-path (:davstore.entry/sha1 have)
                              to-path (:davstore.entry/sha1 target)
                              true]])]
    (log/debug "mv success" res)
    {:success :copied}))

;; FIXME Port to Idris
(defn- ^:no-check ls-seq
  [{:keys [davstore.container/children] :as e} dir depth]
  (cons (assoc (reify-entity e) :davstore.ls/path dir)
        (when (pos? depth)
          (mapcat #(ls-seq % (conj dir (:davstore.entry/name %)) (dec depth)) children))))

(ann ls [Store Path Long -> (List Entry)])
(defn ^:no-check ls [store path depth]
  (when-let [e (get-entry store path)]
    (ls-seq e [] depth)))

;; ### File Store Init

(ann root-id Keyword)
(def root-id :davstore.container/root)

;(declare crate-store! Store)

(ann open-root! (IFn [(HMap :mandatory {:conn datomic.Connection})
                      UUID (Option (HMap))
                      -> (HMap :mandatory {:conn datomic.Connection :root UUID})]
                     [Store UUID (Option (HMap)) -> Store]))
(defn open-root! [{:keys [conn] :as store} uuid create-if-missing]
  (let [db (d/db conn)
        root' (d/entity db [:davstore.root/id uuid])
        root (cond
              root' root'
              create-if-missing
              (let [root-id (tempid :db.part/user)
                    rdir-id (tempid :db.part/davstore.entries)
                    root-dir {:db/id rdir-id
                              :davstore.entry/name (str \{ uuid \})
                              :davstore.entry/type :davstore.entry.type/container}
                    tx [(assoc create-if-missing
                          :db/doc (str "File root {" uuid "}")
                          :db/id root-id
                          :davstore.root/id uuid
                          :davstore.root/dir rdir-id)
                        (assoc root-dir
                          :davstore.entry/sha1 (davstore.schema/entry-sha1 root-dir nil))]
                    {:keys [db-after]} @(transact conn tx)]
                (d/entity db-after [:davstore.root/id uuid]))
              :else (throw (ex-info (str "No store {" uuid "}")
                                    {:conn conn :uuid uuid})))]
    (assoc store :root uuid)))

(ann db-fn (All [f] [datomic.db.Db DbId -> f]))
(defn ^:no-check db-fn [db id]
  (let [res (:db/fn (d/entity db id))]
    (assert res)
    res))

(ann init-store! (IFn [String BlobStore -> Store]
                      [String BlobStore UUID Bool -> Store]))
(defn init-store! 
  ([db-uri blob-store] (init-store! db-uri blob-store (d/squuid) true))
  ([db-uri blob-store main-root-uuid create-if-missing]
     (let [created (create-database db-uri)
           conn (connect db-uri)
           _ (when created
               @(transact conn schema))
           db (d/db conn)]
       (assoc (open-root! {:conn conn} main-root-uuid (when create-if-missing
                                                        (if created
                                                          {:db/ident root-id}
                                                          {})))
         :store-id main-root-uuid
         :path-entry (db-fn db :davstore.fn/path-entry)
         :blob-store blob-store))))

;; # Testing and maintenance

(typ/tc-ignore
 ;; maintenance

 (defn verify [{:keys [davstore.container/children davstore.entry/name
                       davstore.entry/sha1] :as entry}]
   (let [subresults (map #(cons name %)
                         (mapcat verify children))
         actual-sha1 (davstore.schema/entry-sha1 entry nil)]
     (cond-> subresults (not= sha1 actual-sha1)
             (conj [name {:error :sha1-mismatch
                          :entry entry
                          :stored-sha1 sha1
                          :actual-sha1 actual-sha1}]))))
 (defn verify-store [store]
   (verify (get-entry store [])))

 ;; dev

 (set! *warn-on-reflection* true)

 (defn cat [store path]
   (when-let [sha1 (:davstore.file.content/sha1 (get-entry store path))]
     (println (slurp (get-file (:blob-store store) sha1)))))

 (defn- store-str [{:keys [blob-store]} ^String s]
   (store-file blob-store (java.io.ByteArrayInputStream.
                           (.getBytes s "UTF-8"))))

 (defn store-tp [store path content]
   (touch! store path "text/plain; charset=utf-8" nil (store-str store content)))

 (defn write! [store path content]
   (touch! store path "text/plain" nil (store-str store content)))

 (defn insert-testdata [{:keys [conn] :as store}]
   (store-tp store ["a"] "a's new content")
   (store-tp store ["b"] "b's content")
   (mkdir! store ["d"])
   (store-tp store ["d" "c"] "d/c's content"))

 (declare test-store)

 (defn init-test! []
   (def test-uri "datomic:mem://davstore-test")
   (when (bound? #'test-store)
     (d/delete-database test-uri))
   (def test-blobstore (make-store "/tmp/davstore-test"))
   (def test-store (init-store! test-uri test-blobstore))
   (insert-testdata test-store))

 (defmulti print-entry (fn [entry depth] (:davstore.entry/type entry)))
 (defmethod print-entry :davstore.entry.type/container
   [{:keys [db/id davstore.entry/name davstore.entry/sha1 davstore.container/children]} depth]
   (apply concat
          (repeat depth "  ")
          [name "/ #" id " - EH: " sha1 "\n"]
          (map #(print-entry % (inc depth)) children)))

 (defmethod print-entry :davstore.entry.type/file
   [{:keys [db/id davstore.entry/name davstore.entry/sha1]
     content :davstore.file.content/sha1} depth]
   (concat
    (repeat depth "  ")
    [name " #" id " - EH: " sha1 " - CH: " content "\n"]))

 (defn pr-tree [store]
   (doseq [s (print-entry (get-entry store []) 0)]
     (print s))))
