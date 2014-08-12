(ns davstore.schema
  (:import datomic.db.Db java.util.UUID)
  (:require
   [webnf.datomic :refer [field enum function defn-db]]
   [clojure.core.typed :as typ :refer
    [ann ann-form cf defalias non-nil-return typed-deps
     Bool List HMap HVec Map Set Vec Value IFn Option Keyword Seqable Future Coll
     U I Rec Any All]]))

(require '[datomic.api :as d :refer [tempid]])

(ann webnf.datomic/extract-fn (All [f] [Db Keyword -> f]))

(non-nil-return java.security.MessageDigest/getInstance :all)
(non-nil-return java.security.MessageDigest/digest :all)
(non-nil-return javax.xml.bind.DatatypeConverter/printHexBinary :all)
(non-nil-return javax.xml.bind.DatatypeConverter/parseHexBinary :all)
(non-nil-return java.lang.String/toLowerCase :all)

(ann clojure.repl/pst [Throwable -> nil])

(defalias Logger Object)
(defalias LoggerFactory Object)
(ann clojure.tools.logging.impl/get-logger [LoggerFactory clojure.lang.Namespace -> Logger])
(ann clojure.tools.logging.impl/enabled? [Logger Keyword -> Bool])
(ann clojure.tools.logging/*logger-factory* LoggerFactory)
(ann clojure.tools.logging/log* [Logger Keyword (Option Throwable) String -> nil])

(defalias DbId (U datomic.db.DbId Long Keyword (HVec [Keyword Any])))

(defalias Sha1B (Array byte))
(defalias Sha1 String)

(defalias Entity
  (Rec [entity]
       (I (Map Keyword Any)
          (HMap :optional
                {:db/id DbId
                 :davstore.entry/name String
                 :davstore.entry/type (U (Value :davstore.entry.type/container)
                                         (Value :davstore.entry.type/file))
                 :davstore.container/children (Set entity)
                 :davstore.container/_children (Set entity)
                 :davstore.entry/sha1 Sha1
                 :davstore.root/dir entity}))))

(defalias TxItem (U (HVec [Keyword DbId Any *])
                    Entity))
(defalias Tx (List TxItem))
(ann datomic.api/db [datomic.Connection -> datomic.db.Db])
(ann datomic.api/entity [datomic.db.Db DbId -> Entity])
(ann datomic.api/tempid (IFn [Keyword -> datomic.db.DbId]
                             [Keyword Long -> datomic.db.DbId]))
(ann datomic.api/squuid [-> UUID])

(defalias Path (List String))

(defn-db davstore.fn/child-sha1 :- [Sha1B -> Sha1B]
  {:imports [java.security.MessageDigest]}
  [sha1]
  (if sha1
    (.digest (doto (MessageDigest/getInstance "SHA-1")
               (.update (.getBytes "childNode:" "UTF-8"))
               (.update ^bytes sha1)))
    (make-array Byte/TYPE 20)))

(defn-db davstore.fn/parse-sha1 :- [Sha1 -> Sha1B]
  {:imports [javax.xml.bind.DatatypeConverter]}
  [sha1]
  (when sha1
    (DatatypeConverter/parseHexBinary sha1)))

(defn-db davstore.fn/sha1-str :- [Sha1B -> Sha1]
  {:imports [javax.xml.bind.DatatypeConverter]}
  [sha1]
  (.toLowerCase
   (DatatypeConverter/printHexBinary sha1)))

(defn-db ^:no-check davstore.fn/xor-bytes :- [Sha1B Sha1B -> Sha1B]
  [b1 b2]
  (amap ^bytes b1 i _
        (unchecked-byte (bit-xor (aget ^bytes b1 i)
                                 (aget ^bytes b2 i)))))

(defn-db davstore.fn/update-child-sha1 :- [Db DbId Sha1 Sha1 -> Tx]
  "Update entry + parent hash for updated child"
  {:requires [[datomic.api :as d]]
   :imports [javax.xml.bind.DatatypeConverter java.security.MessageDigest]
   :db-requires [[child-sha1] [xor-bytes] [sha1-str]]}
  [db id from to]
  (let [entity (d/entity db id)
        fromcb (child-sha1 from)
        tocb (if (coll? to)
               (reduce (fn [res sha1]
                         (xor-bytes res (child-sha1 sha1)))
                       (child-sha1 (first to)) (next to))
               (child-sha1 to))
        updb (xor-bytes fromcb tocb)

        cur (:davstore.entry/sha1 entity)
        _ (assert entity "Entity must exist")
        _ (assert cur (str "Entity " (pr-str entity) " must have a sha"))
        curb (DatatypeConverter/parseHexBinary cur)
        nxt (sha1-str (xor-bytes curb updb))]
    []
    (cons [:db/add id :davstore.entry/sha1 nxt]
          (map #(vector :davstore.fn/update-child-sha1 (:db/id %) cur nxt)
               (:davstore.container/_children entity)))))

(defn-db davstore.fn/path-entries :- [Db DbId Path -> (Vec Entry)]
  "Resolve path entries from root"
  {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]}
  [db root path]
  (loop [{id :db/id :as entry} (:davstore.root/dir (d/entity db root))
         [fname & names] (seq path)
         res [entry]]
    (assert root)
    (if fname
      (when-let [name-entries (seq (d/q '[:find ?id :in $ ?root ?name :where
                                          [?root :davstore.container/children ?id]
                                          [?id :davstore.entry/name ?name]]
                                        db root fname))]
        (assert (= 1 (count name-entries)))
        (let [e (d/entity db (ffirst name-entries))]
          (recur e names (conj res e))))
      res)))

(defn-db davstore.fn/update-child-sha1 :- [Db DbId (Vec Entry) Sha1 (U Sha1 (Coll Sha1)) -> Tx]
  "Update entry + parent hash for updated child"
  {:requires [[datomic.api :as d]]
   :imports [javax.xml.bind.DatatypeConverter java.security.MessageDigest]
   :db-requires [[child-sha1] [xor-bytes] [sha1-str]]}
  [db root entry-path from to]
  (loop [[entry & path] (reverse entry-path)
         fromb (parse-sha1 from)
         tob   (if (coll? to)
                 (reduce #(xor-bytes %1 (parse-sha1 %2)))
                 (parse-sha1 to))
         res []]
    (let []))
  (let [entity (d/entity db root)
        fromcb (child-sha1 from)
        tocb (if (coll? to)
               (reduce (fn [res sha1]
                         (xor-bytes res (child-sha1 sha1)))
                       (child-sha1 (first to)) (next to))
               (child-sha1 to))
        updb (xor-bytes fromcb tocb)

        cur (:davstore.entry/sha1 entity)
        _ (assert entity "Entity must exist")
        _ (assert cur (str "Entity " (pr-str entity) " must have a sha"))
        curb (DatatypeConverter/parseHexBinary cur)
        nxt (sha1-str (xor-bytes curb updb))]
    []
    (cons [:db/add id :davstore.entry/sha1 nxt]
          (map #(vector :davstore.fn/update-child-sha1 (:db/id %) cur nxt)
               (:davstore.container/_children entity)))))

(defn-db davstore.fn/path-entry :- [Db DbId Path -> Entry]
  "Get entry at path"
  {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]}
  [db root path]
  (log/info "Before" :root root :path path)
  (loop [root (:db/id (:davstore.root/dir (d/entity db root)))
         [fname & names] (seq path)]
    (log/info :root root :fname fname :names name)
    (assert root)
    (if fname
      (when-let [id (ffirst (d/q '[:find ?id :in $ ?root ?name :where
                                   [?root :davstore.container/children ?id]
                                   [?id :davstore.entry/name ?name]]
                                 db root fname))]
        (recur id names))
      (d/entity db root))))

(defn-db davstore.fn/cu-tx :- [Db DbId Path String String (Option Sha1) Sha1 (Map Keyword Any) -> Tx]
  "Create/Update entry, comparing hashes"
  {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]
   :db-requires [[path-entry]]}
  [db root dir-path name match-type match-sha1 new-sha1 entry]
  (let [{parent-id :db/id :as parent} (path-entry db root dir-path)
        _ (when (empty? name)
            (throw (ex-info "Cannot create unnamed file"
                            {:error :missing/name
                             :missing/path dir-path})))
        _ (when-not parent
            (throw (ex-info "Directory missing"
                            {:error :missing/directory
                             :missing/path dir-path})))
        {:keys [:davstore.entry/sha1
                :davstore.entry/type
                :db/id]
         :as current-entry}
        (d/entity db
                  (ffirst (d/q '[:find ?id :in $ ?root ?name :where
                                 [?root :davstore.container/children ?id]
                                 [?id :davstore.entry/name ?name]]
                               db parent-id name)))
        {existing :db/id} (d/entity db [:davstore.entry/sha1 new-sha1])]
    (when (not= sha1 match-sha1)
      (throw (ex-info "SHA-1 mismatch"
                      {:error :cas/mismatch
                       :cas/attribute :davstore.entry/sha1
                       :cas/expected match-sha1
                       :cas/current sha1})))
    (when (and current-entry (not= match-type type))
      (throw (ex-info "Target type mismatch"
                      {:error :cas/mismatch
                       :cas/attribute :davstore.entry/type
                       :cas/expected match-type
                       :cas/current type})))
    (log/info root dir-path name)
    (cons (assoc entry
            :db/id (log/spy (or existing (d/tempid :db.part/davstore.entries)))
            :davstore.entry/sha1 new-sha1
            :davstore.container/_children parent-id)
          (if current-entry
            [[:db/retract parent-id :davstore.container/children id]
             [:davstore.fn/update-child-sha1 parent-id match-sha1
              [new-sha1 sha1]]]
            [[:davstore.fn/update-child-sha1 parent-id match-sha1 new-sha1]]))))

(defn-db davstore.fn/rm-tx :- [Db DbId Path Sha1 Bool -> Tx]
  "Remove entry"
  {:requires [[datomic.api :as d]]
   :db-requires [[path-entry]]}
  [db root path match-sha1 recursive]
  (if-let [{:keys [:davstore.entry/sha1
                   :davstore.container/children
                   :db/id] :as entry}
           (path-entry db root path)]
    (let [parent (path-entry db root (butlast path))]
      (cond (not= sha1 match-sha1)
            (throw (ex-info "SHA-1 mismatch"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/sha1
                             :cas/expected match-sha1
                             :cas/current sha1}))
            (and (not recursive)
                 (seq children))
            (throw (ex-info "Directory not empty"
                            {:error :dir-not-empty
                             :path path}))
            :else [[:db/retract parent :davstore.container/children id]
                   [:davstore.fn/update-child-sha1 parent match-sha1 nil]]))
    (throw (ex-info "Entry missing"
                    {:error :missing/entry
                     :missing/path path}))))

(def schema
  (-> [{:db/id (tempid :db.part/db)
        :db/ident :db.part/davstore.entries
        :db.install/_partition :db.part/db}

       (field "Identity of a file root"
              :davstore.root/id :uuid :unique :identity)
       (field "Directory of root"
              :davstore.root/dir :ref)

       (field "Entry type"
              :davstore.entry/type :ref)
       (field "File name of an entity"
              :davstore.entry/name :string :index)
       (field "Entry sha-1"
              :davstore.entry/sha1 :string :unique :identity)

       (field "Directory entries"
              :davstore.container/children :ref :many :index)

       (field "File content sha-1"
              :davstore.file.content/sha1 :string :index)
       (field "File content type"
              :davstore.file.content/mime-type :string)

       (enum "Directory type" :davstore.entry.type/container)
       (enum "File type" :davstore.entry.type/file)]
      (into (map (comp :dbfn/entity meta)
                 [#'update-child-sha1 #'path-entry #'cu-tx #'rm-tx
                  #'child-sha1 #'xor-bytes #'sha1-str]))))
