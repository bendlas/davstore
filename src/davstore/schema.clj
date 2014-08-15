(ns davstore.schema
  (:import java.util.UUID)
  (:require
   [clojure.core.typed :as typ :refer
    [ann ann-form cf defalias non-nil-return typed-deps
     Bool List HMap HVec Map Set Vec Value IFn Option Keyword Seqable Future Coll
     U I Rec Any All]]))

(require '[datomic.api :as d :refer [tempid]])
(require '[webnf.datomic :refer [field enum function defn-db]])
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

(defalias Path (List String))

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

(defn-db ^:no-check davstore.fn/entry-sha1 :- [Entity -> Sha1]
  {:imports [(java.io ByteArrayOutputStream OutputStreamWriter) java.security.MessageDigest
             javax.xml.bind.DatatypeConverter]
   :requires [[clojure.tools.logging :as log]]}
  [entry]
                                        ; can't use db-requires here, because not a db fn
  (let [parse-sha1 (fn [sha1] (when sha1 (DatatypeConverter/parseHexBinary sha1)))
        sha1-str (fn [sha1] (.toLowerCase (DatatypeConverter/printHexBinary sha1)))
        bao (ByteArrayOutputStream. 2048)
        ch (:davstore.container/children entry)
        child-shas (when (seq ch)
                     (->> ch
                          (map :davstore.entry/sha1)
                          sort vec))
        _ (assert (every? boolean child-shas) "Child without sha1")
        kvs' (remove (comp #{:db/id :davstore.entry/sha1 :davstore.container/children}
                           first)
                     entry)
        kvs (sort-by first (if child-shas 
                             (cons [:davstore.container/children child-shas] kvs')
                             kvs'))
        _ (with-open [w (OutputStreamWriter. bao "UTF-8")]
            (binding [*out* w]
              (doseq [[k v] kvs]
                (println (str k) (str v)))))
        hb (.toByteArray bao)
        res (sha1-str (.digest (MessageDigest/getInstance "SHA-1")
                               hb))
        _ (log/debug "Hashing entity" (:db/id entry)
                     "to Hash:" res \newline
                     (String. hb "UTF-8"))]
    res))


(defn-db ^:no-check davstore.fn/path-entries :- [Db DbId Path -> (Option (Vec Entity))]
  "Resolve path entries from root"
  {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]}
  [db root path]
  (log/debug "path-entries" root path)
  (loop [{id :db/id :as entry} (:davstore.root/dir (d/entity db root))
         [fname & names] (seq path)
         res [entry]]
    (log/debug "  => " (into {:db/id (:db/id entry)} entry) " => " fname)
    (assert entry)
    (if fname
      (if-let [name-entries (seq (d/q '[:find ?id :in $ ?root ?name :where
                                        [?root :davstore.container/children ?id]
                                        [?id :davstore.entry/name ?name]]
                                      db id fname))]
        (do (assert (= 1 (count name-entries)))
            (let [e (d/entity db (ffirst name-entries))]
              (recur e names (conj res e))))
        (do (log/debug "No entry at path" root path)
            nil))
      (do (log/debug "  > path-entries => " res)
          res))))

(defn-db ^:no-check davstore.fn/path-entry :- [Db DbId Path -> (Option Entity)]
  "Get entry at path"
  {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]
   :db-requires [[path-entries]]}
  [db root path]
  (last (path-entries db root path)))

(defn-db ^:no-check davstore.fn/update-parent-hashes :- [Db (List Entity) Sha1 Sha1 -> Tx]
  {:requires [[clojure.tools.logging :as log]]
   :db-requires [[entry-sha1]]}
  [db
   [{:keys [db/id davstore.entry/sha1 davstore.container/children] :as parent} & rev-parents]
   rm-child-sha1 add-child-sha1]
  (when parent
    (let [cur-children (if rm-child-sha1
                         (let [cc (remove #(= rm-child-sha1 (:davstore.entry/sha1 %))
                                          children)]
                           (log/debug "Child hash" (:davstore.entry/name parent) rm-child-sha1
                                      "\n Children" (map :davstore.entry/sha1 children)
                                      "\n CC" cc
                                      \newline (count cc) (count children) )
                           (assert (= (count cc) (dec (count children))) "Child not found")
                           cc)
                         children)
          new-sha1 (entry-sha1 (merge {} parent {:davstore.container/children
                                                 (if add-child-sha1
                                                   (conj cur-children {:davstore.entry/sha1 add-child-sha1})
                                                   cur-children)}))]
      [[:db/add id :davstore.entry/sha1 new-sha1]
       [:davstore.fn/update-parent-hashes rev-parents sha1 new-sha1]])))

(defn-db ^:no-check davstore.fn/cu-tx :- [Db DbId Path String String (Option Sha1) Sha1 (Map Keyword Any) -> Tx]
  "Create/Update entry, comparing hashes"
  {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]
   :db-requires [[path-entries] [entry-sha1]]}
  [db root path match-sha1 entry]
  (let [dir-path (butlast path)
        file-name (last path)
        _ (when (empty? file-name)
            (throw (ex-info "Cannot create unnamed file"
                            {:error :missing/name
                             :missing/path path})))
        dir-entries (log/spy (path-entries db root dir-path))
        _ (when-not dir-entries
            (throw (ex-info "Directory missing"
                            {:error :missing/directory
                             :missing/path dir-path})))
        {parent-id :db/id :as parent} (last dir-entries)
        {:keys [:davstore.entry/sha1
                :davstore.entry/type
                :db/id]
         :as current-entry}
        (d/entity db
                  (ffirst (d/q '[:find ?id :in $ ?parent ?name :where
                                 [?parent :davstore.container/children ?id]
                                 [?id :davstore.entry/name ?name]]
                               db parent-id file-name)))
        _ (when (not= sha1 match-sha1)
            (throw (ex-info "SHA-1 mismatch"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/sha1
                             :cas/expected match-sha1
                             :cas/current sha1})))
        new-sha1 (entry-sha1 (merge {} current-entry entry))]
    [(assoc entry
       :db/id (or id (d/tempid :db.part/davstore.entries))
       :davstore.entry/sha1 new-sha1
       :davstore.container/_children parent-id)
     [:davstore.fn/update-parent-hashes (reverse dir-entries) sha1 new-sha1]]))

(defn-db ^:no-check davstore.fn/rm-tx :- [Db DbId Path Sha1 -> Tx]
  "Remove entry"
  {:requires [[datomic.api :as d]]
   :db-requires [[path-entries]]}
  [db root path match-sha1]
  (if-let [entries (path-entries db root path)]
    (let [{:keys [:davstore.entry/sha1] :as entry} (last entries)]
      (if (= sha1 match-sha1)
        (cons [:davstore.fn/update-parent-hashes (reverse (butlast entries)) sha1 nil]
              (map #(vector :db.fn/retractEntity (:db/id %))
                   (tree-seq (comp seq :davstore.container/children) :davstore.container/children entry)))
        (throw (ex-info "SHA-1 mismatch"
                        {:error :cas/mismatch
                         :cas/attribute :davstore.entry/sha1
                         :cas/expected match-sha1
                         :cas/current sha1}))))
    (throw (ex-info "Entry missing"
                    {:error :missing/entry
                     :missing/path path}))))

(ann schema Tx)
(def ^:no-check schema
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
              :davstore.entry/sha1 :string)

       (field "Directory entries"
              :davstore.container/children :ref :many)

       (field "File content sha-1"
              :davstore.file.content/sha1 :string :index)
       (field "File content type"
              :davstore.file.content/mime-type :string)

       (enum "Directory type" :davstore.entry.type/container)
       (enum "File type" :davstore.entry.type/file)]
      (into (for [[var-sym the-var] (ns-interns *ns*)
                  :let [entity (:dbfn/entity (meta the-var))]
                  :when entity]
              entity))))
