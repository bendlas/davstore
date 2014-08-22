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

(defalias PathElem String)
(defalias Path (List PathElem))

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
  [{:as entry :keys [davstore.container/children davstore.entry/type]} child-shas]
                                        ; can't use db-requires here, because not a db fn
  (let [parse-sha1 (fn [sha1] (when sha1 (DatatypeConverter/parseHexBinary sha1)))
        sha1-str (fn [sha1] (.toLowerCase (DatatypeConverter/printHexBinary sha1)))
        bao (ByteArrayOutputStream. 2048)
        kvs' (remove (comp #{:db/id :davstore.entry/sha1 :davstore.container/children}
                           first)
                     entry)
        kvs (sort-by first (if (= :davstore.entry.type/container type)
                             (cons [:davstore.container/children
                                    (vec (sort (or child-shas
                                                   (when (seq children)
                                                     (->> children
                                                          (map :davstore.entry/sha1))))))]
                                   kvs')
                             kvs'))
        _ (with-open [w (OutputStreamWriter. bao "UTF-8")]
            (binding [*out* w]
              (doseq [[k v] kvs]
                (println (str k) (pr-str v)))))
        hb (.toByteArray bao)
        res (sha1-str (.digest (MessageDigest/getInstance "SHA-1")
                               hb))
        _ (log/debug "Hashing entity" (:db/id entry)
                     "to Hash:" res (str \newline
                                         (String. hb "UTF-8")))]
    res))


(defn-db ^:no-check davstore.fn/path-entries :- [Db DbId Path -> (Option (Vec Entity))]
  "Resolve path entries from root"
  {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]}
  [db root path]
  (loop [{id :db/id :as entry} (:davstore.root/dir (d/entity db root))
         [fname & names] (seq path)
         res [entry]]
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
      res)))

(defn-db ^:no-check davstore.fn/path-entry :- [Db DbId Path -> (Option Entity)]
  "Get entry at path"
  {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]
   :db-requires [[path-entries]]}
  [db root path]
  (last (path-entries db root path)))


(defalias TreeUpdates
  (Rec [tu]
       (HMap :optional {:updates (Map Entity tu)
                        :additions (Seqable Sha1)
                        :removals (Set Sha1)})))

(defn-db ^:no-check davstore.fn/update-parent-hashes :- [Db DbId TreeUpdates -> Tx]
  {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]
   :db-requires [[entry-sha1]]}
  [db root updates]
  (log/debug (str "Updates: \n" (with-out-str (clojure.pprint/pprint updates))))
  (let [tx-data (fn tx-data [tx
                             {:as dir :keys [db/id davstore.container/children davstore.entry/sha1]}
                             {:keys [updates removals additions]
                              :or {removals #{}} :as upd}]
                  (log/debug (str "The updates\n" (with-out-str (clojure.pprint/pprint upd))) \newline :additions additions :removals removals :subdates updates)
                  (let [[tx' rms adds] (reduce-kv (fn [[tx rms adds] ch update]
                                                    (let [[tx' sha1'] (tx-data tx ch update) ]
                                                      (log/debug "@" id "@CHLD" (:db/id ch) "ADD" (conj adds sha1') "RMS" (conj rms (:davstore.entry/sha1 ch)))
                                                      [(into tx tx')
                                                       (conj rms (:davstore.entry/sha1 ch))
                                                       (conj adds sha1')]))
                                                  [tx removals additions] updates)
                        child-shas (vec (concat (remove rms (map :davstore.entry/sha1 children)) adds))
                        _ (log/debug "@" id :adds adds :rms rms)
                        _ (assert (= (count child-shas)
                                     (-> (count children)
                                         (- (count rms))
                                         (+ (count adds)))))
                        sha1* (entry-sha1 dir child-shas)]
                    [(cons [:db/add id :davstore.entry/sha1 sha1*] tx')
                     sha1*]))
        [tx sha1] (tx-data [] (:davstore.root/dir (d/entity db root)) (first (vals updates)))]
    (log/debug (str "New root sha1 " sha1 " => \n" (with-out-str (clojure.pprint/pprint tx))))
    tx))

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
        dir-entries (path-entries db root dir-path)
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
        new-sha1 (entry-sha1 (merge {} current-entry entry) nil)]
    [(assoc entry
       :db/id (or id (d/tempid :db.part/davstore.entries))
       :davstore.entry/sha1 new-sha1
       :davstore.container/_children parent-id)
     [:davstore.fn/update-parent-hashes root (update-in {} (interpose :updates dir-entries)
                                                        (if match-sha1
                                                          #(assoc %
                                                             :additions #{new-sha1}
                                                             :removals #{sha1})
                                                          #(assoc %
                                                             :additions #{new-sha1})))]]))

(defn-db ^:no-check davstore.fn/rm-tx :- [Db DbId Path Sha1 -> Tx]
  "Remove entry"
  {:requires [[datomic.api :as d]]
   :db-requires [[path-entries]]}
  [db root path match-sha1]
  (if-let [entries (path-entries db root path)]
    (let [{:keys [davstore.entry/sha1 db/id] :as entry} (last entries)]
      (if (= sha1 match-sha1)
        [[:davstore.fn/update-parent-hashes root (update-in {} (interpose :updates (butlast entries))
                                                            assoc :removals #{sha1})]
         [:db.fn/retractEntity id]]
        (throw (ex-info "SHA-1 mismatch"
                        {:error :cas/mismatch
                         :cas/attribute :davstore.entry/sha1
                         :cas/expected match-sha1
                         :cas/current sha1}))))
    (throw (ex-info "Entry missing"
                    {:error :missing/entry
                     :missing/path path}))))

(defn-db ^:no-check davstore.fn/cp-tx :- [Db DbId Path Sha1 Path Sha1 -> Tx]
  "Copy or Move Entry"
  {:requires [[datomic.api :as d]]
   :db-requires [[path-entries] [entry-sha1]]}
  [db root from-path match-from-sha1 to-path match-to-sha1 move]
  (let [from-entries (path-entries db root from-path)
        source (last from-entries)
        from-dir-entries (pop from-entries)
        from-dir (last from-dir-entries)
        _ (when-not (= match-from-sha1 (:davstore.entry/sha1 source))
            (throw (ex-info "SHA-1 mismatch for source"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/sha1
                             :cas/expected match-from-sha1
                             :cas/current (:davstore.entry/sha1 source)})))
        to-dir-entries (path-entries db root (butlast to-path))
        to-dir (last to-dir-entries)
        _ (when-not (= :davstore.entry.type/container
                       (:davstore.entry/type to-dir))
            (throw (ex-info "Target dir doesn't exist"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/type
                             :cas/expected :davstore.entry.type/container
                             :cas/current (:davstore.entry/type to-dir)})))
        tname (last to-path)
        target (some #(and (= tname (:davstore.entry/name %)) %)
                     (:davstore.container/children to-dir))
        _ (when-not (= match-to-sha1 (:davstore.entry/sha1 target))
            (throw (ex-info "SHA-1 mismatch for destination"
                            {:error :cas/mismatch
                             :cas/attribute :davstore.entry/sha1
                             :cas/expected match-to-sha1
                             :cas/current (:davstore.entry/sha1 target)})))
        to-sha1 (entry-sha1 (merge {} source {:davstore.entry/name tname}) nil)]
    (concat (when match-to-sha1 [[:db.fn/retractEntity (:db/id target)]])
            [[:davstore.fn/update-parent-hashes root
              (-> {}
                  (assoc-in (interpose :updates to-dir-entries) (cond-> {:additions #{to-sha1}}
                                                                        match-to-sha1 (assoc :removals #{match-to-sha1})))
                  (cond-> move (update-in (concat (interpose :updates from-dir-entries) [:removals])
                                          (fnil conj #{}) match-from-sha1)))]]
            (if move
              (concat [[:db/add (:db/id source) :davstore.entry/name tname]
                       [:db/add (:db/id source) :davstore.entry/sha1 to-sha1]
                       [:db/add (:db/id to-dir) :davstore.container/children (:db/id source)]]
                      (when-not (= to-dir from-dir)
                        [[:db/retract (:db/id from-dir) :davstore.container/children (:db/id source)]]))
              ((fn tf [pid entry]
                 (let [tid (d/tempid :db.part/davstore.entries)]
                   (list* (dissoc (into {:db/id tid} entry) :davstore.container/children)
                          [:db/add pid :davstore.container/children tid]
                          (mapcat #(tf tid %) (:davstore.container/children entry)))))
               (:db/id to-dir) (merge {} source {:davstore.entry/name tname
                                                 :davstore.entry/sha1 to-sha1}))))))

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
              :davstore.container/children :ref :many :component)

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
