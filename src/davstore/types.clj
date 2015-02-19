(ns davstore.types
  (:import java.util.UUID datomic.db.Db java.io.File)
  (:require
   [clojure.core.typed :as typ :refer
    [ann defalias non-nil-return typed-deps
     Bool List HMap HVec Map Set Vec Value IFn Option Keyword Seqable Future Coll
     U I Rec Any All]]))

(ann webnf.datomic/extract-fn (All [f] [Db Keyword -> f]))
(ann webnf.datomic.query/reify-entity [(Map Keyword Any) -> (Map Keyword Any)])


(non-nil-return java.security.MessageDigest/getInstance :all)
(non-nil-return java.security.MessageDigest/digest :all)
(non-nil-return javax.xml.bind.DatatypeConverter/printHexBinary :all)
(non-nil-return javax.xml.bind.DatatypeConverter/parseHexBinary :all)
(non-nil-return java.lang.String/toLowerCase :all)
(non-nil-return java.io.File/getAbsoluteFile :all)
(non-nil-return java.io.File/createTempFile :all)
(non-nil-return java.security.MessageDigest/getInstance :all)
(non-nil-return java.security.MessageDigest/digest :all)
(non-nil-return javax.xml.bind.DatatypeConverter/printHexBinary :all)
(non-nil-return javax.xml.bind.DatatypeConverter/parseHexBinary :all)

(ann clojure.java.io/file [(U String File) (U String File) * -> File])
(ann clojure.java.io/copy [(U File
                              java.io.InputStream
                              java.io.Reader
                              String)
                           (U File
                              java.io.OutputStream
                              java.io.Writer)
                           -> nil])
(ann clojure.java.io/output-stream [(U String File) -> java.io.OutputStream])
(ann clojure.java.io/input-stream [(U String File) -> java.io.InputStream])

(defalias Logger Object)
(defalias LoggerFactory Object)
(ann clojure.tools.logging.impl/get-logger [LoggerFactory clojure.lang.Namespace -> Logger])
(ann clojure.tools.logging.impl/enabled? [Logger Keyword -> Bool])
(ann clojure.tools.logging/*logger-factory* LoggerFactory)
(ann clojure.tools.logging/log* [Logger Keyword (Option Throwable) String -> nil])

;; Datomic annotations
(defalias TxResult Object)
(defalias Entity (Map Keyword Object))
(ann datomic.api/transact [datomic.Connection (Seqable TxItem) -> (Future TxResult)])
(ann datomic.api/db [datomic.Connection -> Db])
(ann datomic.api/entity [Db DbId -> Entity])
(ann datomic.api/tempid (IFn [Keyword -> datomic.db.DbId]
                             [Keyword Long -> datomic.db.DbId]))
(ann datomic.api/squuid [-> UUID])
(ann datomic.api/create-database [String -> Bool])
(ann datomic.api/connect [String -> datomic.Connection])

(defalias DbId (U datomic.db.DbId Long Keyword (HVec [Keyword Any])))

(defalias Sha1B (Array byte))
(defalias Sha1 String)

(defalias Entry
  (Rec [entity]
       (I Entity
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

(defalias PathElem String)
(defalias Path (List PathElem))

;; Davstore annotations

(defalias DirEntry
  (I Entry (HMap :mandatory
                 {:davstore.entry/type (Value :davstore.entry.type/container)})))
(defalias FileEntry
  (I Entry (HMap :mandatory
                 {:davstore.entry/type (Value :davstore.entry.type/file)})))

(defalias Path (List String))

(defalias Store "A datomic-backed file store"
  (HMap :mandatory {:conn datomic.Connection
                    :store-id UUID
                    :root UUID
                    :path-entry [Db DbId Path -> Entry]}
        :optional {:db Db}))

(ann davstore.schema/schema Tx)

(defalias OpResult (Map Keyword Any))
(ann davstore.store/zero-sha1 Sha1)
(ann davstore.store/xor-bytes [Sha1B Sha1B -> Sha1B])
(ann davstore.store/open-db [Store -> Store])
(ann davstore.store/store-db [Store -> Db])
(ann davstore.store/path-entries [Db DbId Path -> (Option (Vec Entity))])
(ann davstore.store/path-entry [Db DbId Path -> (Option Entity)])
(ann davstore.store/get-entry [Store Path -> Entry])
(ann davstore.store/touch! [Store Path String (Option Sha1) Sha1 -> OpResult])
(ann davstore.store/mkdir! [Store Path -> OpResult])
(ann davstore.store/rm! [Store Path Sha1 Bool -> OpResult])
(ann davstore.store/cp! [Store Path Path Bool -> OpResult])
(ann davstore.store/mv! [Store Path Path Bool -> OpResult])
(ann davstore.store/blob-file [Store Entry -> File])
(ann davstore.store/ls [Store Path Long -> (List Entry)])
(ann davstore.store/root-id Keyword)
(ann davstore.store/open-root! (IFn [(HMap :mandatory {:conn datomic.Connection})
                                     UUID (Option (HMap))
                                     -> (HMap :mandatory {:conn datomic.Connection :root UUID})]
                                    [Store UUID (Option (HMap)) -> Store]))
(ann davstore.store/db-fn (All [f] [Db DbId -> f]))
(ann davstore.store/init-store! (IFn [String BlobStore -> Store]
                                     [String BlobStore UUID Bool -> Store]))


(defalias BlobStore
  (HMap :mandatory {:root-dir File}
        :complete? true))

(defalias TempFile
  (HMap :mandatory {:sha-1 String
                    :tempfile File}
        :complete? true))

(defalias Sha1 String)

(ann davstore.blob/make-store [(U String File) -> BlobStore])
(ann davstore.blob/store-temp [BlobStore java.io.InputStream -> TempFile])
(ann davstore.blob/sha-file [BlobStore Sha1 -> File])
(ann davstore.blob/get-file [BlobStore Sha1 -> (Option File)])
(ann davstore.blob/open-file [BlobStore Sha1 -> (Option java.io.InputStream)])
(ann davstore.blob/merge-temp [BlobStore TempFile -> Boolean])
(ann davstore.blob/store-file [BlobStore java.io.InputStream -> Sha1])
(ann davstore.blob/stream-temp [BlobStore [java.io.OutputStream -> nil] -> TempFile])
(ann davstore.blob/stream-file [BlobStore [java.io.OutputStream -> nil] -> Sha1])
(ann davstore.blob/write-file [BlobStore [java.io.PrintWriter -> nil] -> Sha1])
