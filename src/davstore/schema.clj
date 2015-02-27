(ns davstore.schema
  (:import java.util.UUID)
  (:require
   [datomic.api :as d :refer [tempid]]
   [webnf.datomic :refer [field enum function defn-db]]
   [webnf.datomic.version :as ver]
   [webnf.base :refer [pprint]]))

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
(def schema-version "2.0")

(def schema
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

       (field "Creation Date" :davstore.entry/created :instant)
       (field "Last Modified Date" :davstore.entry/last-modified :instant)


       (field "Global identity of a file root"
              :davstore.root/id :uuid :unique :identity)
       (field "Working directory of root"
              :davstore.root/dir :ref)
       (field "Current and former entries of root"
              :davstore.root/all-snapshot-dirs :ref :many)


       (field "Directory entries"
              :davstore.dir/children :ref :many :component)
       (field "Index file of directory"
              :davstore.dir/index-file :string)

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

(comment
  [1.1 -> 2.0]
  (letfn [(changed-entries [db datoms]
            (let [txi (d/entid db :db/txInstant)
                  det (d/entid db ::de/type)
                  inst (d/q [:find '?i '. :where ['_ txi '?i]] datoms)]
              (assert inst)
              (for [[e a _ _ added] datoms
                    :when (= det a)]
                [e (when added inst)])))

          (find-cm-instants [conn]
            (let [db (d/db conn)]
              (reduce (fn [cms {:keys [data]}]
                        (reduce (fn [cms [eid txi]]
                                  (if txi
                                    (assoc cms eid
                                           (if-let [erec (get cms eid)]
                                             (assoc erec :last-mod txi)
                                             {:created txi :last-mod txi}))
                                    (dissoc cms eid)))
                                cms (changed-entries db data)))
                      {} (d/tx-range (d/log conn) nil nil))))

          (cm-instants-tx [cm-instants]
            (for [[e {:keys [created last-mod]}] cm-instants]
              {:db/id e
               ::de/created created
               ::de/last-modified last-mod}))
          (update-11-20 [conn]
            (let [{db :db-after}
                  @(d/transact conn (-> [(field "Creation Date" :davstore.entry/created :instant)
                                         (field "Last Modidied Date" :davstore.entry/last-modified :instant)]
                                        (into (ver/version-tx schema-ident schema-version nil))))]
              @(d/sync-schema conn (d/basis-t db))
              @(d/transact conn (cm-instants-tx (find-cm-instants conn)))))]
    (update-11-20 davstore.store/conn)))
