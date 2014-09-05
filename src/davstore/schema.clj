(ns davstore.schema
  (:import java.util.UUID)
  (:require
   [clojure.core.typed :as typ :refer
    [ann ann-form cf defalias non-nil-return typed-deps
     Bool List HMap HVec Map Set Vec Value IFn Option Keyword Seqable Future Coll
     U I Rec Any All]]))

(require '[datomic.api :as d :refer [tempid]])
(require '[webnf.datomic :refer [field enum function defn-db]])
(require '[webnf.datomic.version :as ver])
(import datomic.db.Db)

(ann webnf.datomic/extract-fn (All [f] [Db Keyword -> f]))
(ann clojure.core/sort-by
     (All [a b] ;[[a -> b] (java.util.Comparator b) (Seqable a) -> (Seqable a)]
          [[a -> b] (Seqable a) -> (List a)]))

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

(defalias TxItem (U (HVec [Keyword Any *])
                    Entity))
(defalias Tx (Seqable TxItem))
(ann datomic.api/db [datomic.Connection -> datomic.db.Db])
(ann datomic.api/entity [datomic.db.Db DbId -> Entity])
(ann datomic.api/tempid (IFn [Keyword -> datomic.db.DbId]
                             [Keyword Long -> datomic.db.DbId]))
(ann datomic.api/squuid [-> UUID])

(defalias PathElem String)
(defalias Path (List PathElem))

(defmacro alias-ns [alias ns-sym & ans]
  `(do (create-ns '~ns-sym)
       (alias '~alias '~ns-sym)
       ~(when (seq ans)
          `(alias-ns ~@ans))))

(alias-ns
 de  davstore.entry
 det davstore.entry.type
 des davstore.entry.snapshot
 dr  davstore.root
 dd  davstore.dir
 dfc davstore.file.content)

(defn-db davstore.fn/assert-val
  {:requires [[clojure.tools.logging :as log]]}
  [db id attr val]
  (when (if (nil? val)
          (contains? (d/entity db id) attr)
          (not (seq (d/q (conj '[:find ?id :in $ ?id ?val :where]
                               ['?id attr '?val])
                         db id val))))
    (throw (ex-info "CAS error" {:error :cas/mismatch
                                 :in :davstore.fn/assert-val
                                 :cas/attribute attr
                                 :cas/expected val
                                 :cas/current (get (d/entity db id) attr)}))))

(defn-db davstore.fn/assert-available
  [db parent name]
  (when-let [[[id]] (seq (d/q '[:find ?id :in $ ?parent ?name :where
                                [?parent ::dd/children ?id]
                                [?id ::de/name ?name]]
                              db parent name))]
    (ex-info "Entry exists"
             {:error :cas/mismatch
              :parent parent
              :conflict-entry id
              :cas/attribute ::de/name
              :cas/expected nil
              :cas/current name})))


(def schema-ident :davstore/schema)
(def schema-version "1.0")

(ann schema Tx)
(def ^:no-check schema
  (-> [{:db/id (tempid :db.part/db)
        :db/ident :db.part/davstore.entries
        :db.install/_partition :db.part/db}

       (field "File name of an entity"
              :davstore.entry/name :string :index)
       (field "Entry type"
              :davstore.entry/type :ref)
       (enum "Directory type" :davstore.entry.type/dir)
       (enum "File type" :davstore.entry.type/file)

       (field "Entry sha-1"
              :davstore.entry.snapshot/sha-1 :string)
       (field "Entry last modified"
              :davstore.entry.snapshot/last-modified :instant :index)


       (field "Global identity of a file root"
              :davstore.root/id :uuid :unique :identity)
       (field "Working directory of root"
              :davstore.root/dir :ref)
       (field "Current and former entries of root"
              :davstore.root/all-snapshot-dirs :ref :many)


       (field "Directory entries"
              :davstore.dir/children :ref :many :component)

       (field "File content sha-1"
              :davstore.file.content/sha-1 :string :index)
       (field "File content type"
              :davstore.file.content/mime-type :string)]
      (into (for [[var-sym the-var] (ns-interns *ns*)
                  :let [entity (:dbfn/entity (meta the-var))]
                  :when entity]
              entity))
      (into (ver/version-tx schema-ident schema-version nil))))

(defn ensure-schema! [conn]
  (ver/ensure-schema! conn schema-ident schema-version schema))
