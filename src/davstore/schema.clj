(ns davstore.schema
  (:require
   [datomic.api :as d :refer [tempid]]
   [webnf.datomic :refer [field enum function]]))

(def schema
  [{:db/id (tempid :db.part/db)
    :db/ident :db.part/davstore.entries
    :db.install/_partition :db.part/db}

   (field "Entry type"
          :davstore.entry/type :ref)
   (field "File name of an entity"
          :davstore.entry/name :string)
   (field "Entry sha-1"
          :davstore.entry/sha1 :string :unique-identity)

   (field "Directory entries"
          :davstore.container/children :ref :many :index)

   (field "File content sha-1"
          :davstore.file.content/sha1 :string :index)
   (field "File content type"
          :davstore.file.content/mime-type :string)

   (enum "Directory type" :davstore.entry.type/container)
   (enum "File type" :davstore.entry.type/file)

   (function davstore.fn/update-child-sha1 "Update entry + parent hash for updated child"
             {:requires [[datomic.api :as d] [clojure.tools.logging :as log]]
              :imports [javax.xml.bind.DatatypeConverter java.security.MessageDigest]}
             [db id from to]
             (let [child-sha1 #(if %
                                 (.digest (doto (MessageDigest/getInstance "SHA-1")
                                            (.update (.getBytes "childNode:" "UTF-8"))
                                            (.update (DatatypeConverter/parseHexBinary %))))
                                 (make-array Byte/TYPE 20))
                   xor-bytes #(amap ^bytes %1 i _
                                    (unchecked-byte (bit-xor (aget ^bytes %1 i)
                                                             (aget ^bytes %2 i))))
                   entity (d/entity db id)
                   fromcb (child-sha1 from)
                   tocb (child-sha1 to)
                   updb (xor-bytes fromcb tocb)

                   cur (:davstore.entry/sha1 entity)
                   _ (assert entity "Entity must exist")
                   _ (assert cur (str "Entity " (pr-str entity) " must have a sha"))
                   curb (DatatypeConverter/parseHexBinary cur)
                   nxt (.toLowerCase (DatatypeConverter/printHexBinary (xor-bytes curb updb)))]
               (log/spy
                (cons [:db/add id :davstore.entry/sha1 nxt]
                      (map #(vector :davstore.fn/update-child-sha1 (:db/id %) cur nxt)
                           (:davstore.container/_children entity))))))

   (function davstore.fn/path-entry "Get entry at path"
             {:requires [[datomic.api :as d]]}
             [db root path]
             (loop [root root
                    [fname & names] (seq path)]
               (if fname
                 (when-let [id (ffirst (d/q '[:find ?id :in $ ?root ?name :where
                                              [?root :davstore.container/children ?id]
                                              [?id :davstore.entry/name ?name]]
                                            db root fname))]
                   (recur id names))
                 (d/entity db root))))
   
   (function davstore.fn/cu-tx "Create/Update entry, comparing hashes"
             {:requires [[datomic.api :as d]]}
             [db root dir-path name match-type match-sha1 new-sha1 entry]
             (let [path-entry (:db/fn (d/entity db :davstore.fn/path-entry))
                   {parent-id :db/id :as parent} (path-entry db root dir-path)
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
                   (path-entry db parent-id [name])
                   _ (when (not= sha1 match-sha1)
                       (throw (ex-info "SHA-1 mismatch"
                                       {:error :cas/mismatch
                                        :cas/attribute :davstore.entry/sha1
                                        :cas/expected match-sha1
                                        :cas/current sha1})))
                   eid (cond
                        (not current-entry) (d/tempid :db.part/davstore.entries)
                        (= match-type type) id
                        :else (throw (ex-info "Target type mismatch"
                                              {:error :cas/mismatch
                                               :cas/attribute :davstore.entry/type
                                               :cas/expected match-type
                                               :cas/current type})))]
               [(assoc entry
                  :db/id eid
                  :davstore.entry/sha1 new-sha1
                  :davstore.container/_children parent-id)
                [:davstore.fn/update-child-sha1 parent-id match-sha1 new-sha1]]))

   (function davstore.fn/rm-entry "Recursively remove entities"
             [db entry]
             (cons [:db.fn/retractEntity (:db/id entry)]
                   (map #(vector :davstore.fn/rm-entry %)
                        (:davstore.container/children entry))))

   (function davstore.fn/rm-tx "Remove entry"
             {:requires [[datomic.api :as d]]}
             [db root path match-sha1 recursive]
             (let [path-entry (:db/fn (d/entity db :davstore.fn/path-entry))]
               (if-let [{:keys [:davstore.entry/sha1
                                :davstore.container/children
                                :davstore.container/_children] :as entry}
                        (path-entry db root path)]
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
                       :else (cons [:davstore.fn/rm-entry entry]
                                   (map #(vector :davstore.fn/update-child-sha1
                                                 (:db/id %) match-sha1 nil)
                                        _children)))
                 (throw (ex-info "Entry missing"
                                 {:error :missing/entry
                                  :missing/path path})))))])
