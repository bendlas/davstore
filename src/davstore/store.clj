(ns davstore.store
  (:import javax.xml.bind.DatatypeConverter
           (java.io ByteArrayOutputStream OutputStreamWriter
                    InputStream OutputStream)
           (java.security MessageDigest)
           (java.util UUID Date)
           datomic.db.Db)
  (:require [davstore.schema :refer [ensure-schema! alias-ns]]
            [davstore.blob :as blob :refer [make-store store-file get-file]]
            [clojure.tools.logging :as log]
            [webnf.datomic.query :refer [reify-entity entity-1 id-1 id-list by-attr by-value]]
            [clojure.repl :refer :all]
            [clojure.pprint :refer :all]
            [datomic.api :as d :refer [q tempid transact transact-async create-database connect]]))

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

(alias-ns
 de  davstore.entry
 det davstore.entry.type
 des davstore.entry.snapshot
 dr  davstore.root
 dd  davstore.dir
 dfc davstore.file.content
 dfn davstore.fn)

;; ## SHA-1 Stuff

(def zero-sha1 (apply str (repeat 40 \0)))

(defn xor-bytes [^bytes a1 ^bytes a2]
  (amap a1 i _ (unchecked-byte (bit-xor (aget a1 i)
                                        (aget a2 i)))))


;; ## File Store

(defn open-db [{:keys [db conn] :as store}]
  (when db
    (log/warn "Store already opened, reopening with new db"))
  (assoc store :db (d/db conn)))

(defn store-db [{:keys [db conn]}]
  (if db
    db
    (do (log/debug "Store not opened, getting current snapshot")
        (d/db conn))))

(defn postwalk-entries [fd ff e]
  (letfn [(pw [{:as e' et ::de/type id :db/id ch ::dd/children}]
            (let [e'' (into {:db/id id} e')]
              (if (= et ::det/dir)
                (fd (assoc e'' ::dd/children (map pw ch)))
                (ff e''))))]
    (pw e)))

(defn dir-child [db dir-id child-name]
  (when-let [name-entries (seq (d/q '[:find ?id :in $ ?root ?name :where
                                      [?root :davstore.dir/children ?id]
                                      [?id :davstore.entry/name ?name]]
                                    db dir-id child-name))]
    (assert (= 1 (count name-entries)))
    (ffirst name-entries)))

(defn path-entries
  "Resolve path entries from root"
  [db root path]
  (loop [{id :db/id :as entry} (::dr/dir (d/entity db root))
         [fname & names] (seq path)
         res [entry]]
    (assert entry)
    (if fname
      (if-let [ch (dir-child db id fname)]
        (let [e (d/entity db ch)]
          (recur e names (conj res e)))
        (do (log/debug "No entry at path" root path)
            nil))
      res)))

(defn path-entry
  "Get entry at path"
  [db root path]
  (last (path-entries db root path)))


(defn get-entry [{:keys [root] :as store} path]
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

(defn entry-info! [db root path]
  (let [dir-path (butlast path)
        file-name (last path)
        _ (when (empty? file-name)
            (throw (ex-info "Cannot refer to unnamed file"
                            {:error :missing/name
                             :missing/path path})))
        dir-entries (path-entries db [:davstore.root/id root] dir-path)
        _ (when-not dir-entries
            (throw (ex-info "Directory missing"
                            {:error :missing/directory
                             :missing/path dir-path})))
        {parent-id :db/id :as parent} (last dir-entries)
        entry (entity-1 '[:find ?id :in $ ?parent ?name :where
                          [?parent ::dd/children ?id]
                          [?id ::de/name ?name]]
                        db parent-id file-name)
        cas-entry (fn [parent-id ch-attr id name type]
                    [[::dfn/assert-val parent-id ch-attr id]
                     [::dfn/assert-val id ::de/name name]
                     [::dfn/assert-val id ::de/type type]])]
    {:dir-entries dir-entries
     :path path
     :parent parent
     :cas-tx (loop [[{:keys [:db/id ::de/type ::de/name]} & entries'] dir-entries
                    parent [::dr/id root]
                    ch-attr ::dr/dir
                    tx []]
               (if id
                 (recur entries'
                        id ::dd/children
                        (into tx (cas-entry parent ch-attr id name type)))
                 (into tx (if-let [{:keys [db/id ::de/type ::de/name]} entry]
                            (cas-entry parent ch-attr id name type)
                            [[::dfn/assert-available parent file-name]]))))
     :current-entry entry}))

(defn match-entry! [{:keys [::dfc/sha-1 ::de/type db/id] :as current-entry}
                    match-sha-1 match-type]
  (when-not (or (= :current match-sha-1)
                (= match-sha-1 sha-1))
    (throw (ex-info "SHA-1 mismatch"
                    {:error :cas/mismatch
                     :cas/attribute ::de/sha-1
                     :cas/expected match-sha-1
                     :cas/current sha-1})))
  (when (and (not= :current match-type)
             current-entry
             (not= match-type type))
    (throw (ex-info "Target type mismatch"
                    {:error :cas/mismatch
                     :cas/attribute ::de/type
                     :cas/expected match-type
                     :cas/current type}))))

(deffileop touch! "PUT" [{:keys [conn root] :as store} path mime-type sha-1* match-sha-1]
  (let [db (store-db store)
        {:keys [dir-entries cas-tx]
         {parent-id :db/id} :parent
         {:as current-entry :keys [db/id]} :current-entry}
        (entry-info! db root path)
        _ (match-entry! current-entry match-sha-1 ::det/file)
        id* (if current-entry id (tempid :db.part/davstore.entries))
        cur-date (Date.)
        tx (concat cas-tx
                   [[:db/add id* ::dfc/mime-type (or mime-type "application/octet-stream")]
                    [:db/add id* ::de/last-modified cur-date]
                    [:db/add parent-id ::de/last-modified cur-date]]
                   (when (or (not current-entry)
                             (= match-sha-1 :current))
                     [[:db/add id* ::de/created cur-date]])
                   (if current-entry
                     [[:db.fn/cas id* ::dfc/sha-1
                       (if (= :current match-sha-1)
                         (::dfc/sha-1 current-entry)
                         match-sha-1)
                       sha-1*]]
                     [{:db/id id*
                       ::dd/_children parent-id
                       ::de/type ::det/file
                       ::dfc/sha-1 sha-1*
                       ::de/name (last path)}]))
        res @(transact conn tx)]
    (log/debug "File touch success" res)
    (if current-entry
      {:success :updated}
      {:success :created})))

(deffileop mkdir! "MKCOL" [{:as store :keys [conn root]} path]
  (let [db (store-db store)
        {:keys [dir-entries cas-tx]
         {parent-id :db/id} :parent
         {:as current-entry :keys [db/id de/type]} :current-entry}
        (entry-info! db root path)
        _ (when current-entry
            (throw (ex-info "Entry exists"
                            {:error :cas/mismatch
                             :path path
                             :parent parent-id
                             :conflict-entry id
                             :cas/attribute ::de/type
                             :cas/expected nil
                             :cas/current type})))
        id* (tempid :db.part/davstore.entries)
        cur-date (Date.)
        tx (cons {:db/id (tempid :db.part/davstore.entries)
                  ::dd/_children parent-id
                  ::de/name (last path)
                  ::de/type ::det/dir
                  ::de/created cur-date
                  ::de/last-modified cur-date}
                 cas-tx)
        res @(transact conn tx)]
    (log/debug "Mkdir success" res)
    {:success :created}))

(deffileop rm! "DELETE" [{:keys [conn root] :as store} path match-sha-1 recursive]
  (let [db (store-db store)
        {:keys [dir-entries cas-tx]
         {parent-id :db/id} :parent
         {:as current-entry :keys [db/id de/type dd/children]} :current-entry}
        (entry-info! db root path)
        _ (match-entry! current-entry match-sha-1 :current)
        _ (when (and (not recursive) (seq children))
            (throw (ex-info "Directory not empty"
                            {:error :dir-not-empty
                             :path path})))
        tx (concat [[:db/add parent-id ::de/last-modified (Date.)]
                    [:db.fn/retractEntity id]]
                   (when-not recursive
                     [[::dfn/assert-val id ::dd/children nil]])
                   cas-tx)
        res @(transact conn tx)]
    (log/debug "rm success" res)
    {:success :deleted}))

(defn cp-cas [{{:as from type ::de/type} :current-entry
               from-cas :cas-tx
               from-path :path}
              {to :current-entry
               to-cas :cas-tx
               to-path :path}
              recursive overwrite]
  (when-not from
    (throw (ex-info "Not found" {:error :not-found :path from-path})))
  (when-not (or recursive (= :det/file type))
    (throw (ex-info "Copy source is directory"
                    {:error :cas/mismatch
                     :cas/attribute ::de/type
                     :cas/expected ::det/file
                     :cas/current type})))
  (when-not (or overwrite (not to))
    (throw (ex-info "Copy target exists"
                    {:error :target-exists :path to-path})))
  (seq (into (set from-cas) to-cas)))

(defn cp-tx [parent-id entry]
  (let [tid (d/tempid :db.part/davstore.entries)]
    (list* (dissoc (into {:db/id tid} entry) ::dd/children)
           [:db/add parent-id ::dd/children tid]
           [:db/add parent-id ::de/last-modified (Date.)]
           (mapcat #(cp-tx tid %) (::dd/children entry)))))

(deffileop cp! "COPY" [{:keys [conn root] :as store}
                       from-path to-path recursive overwrite]
  (let [db (store-db store)
        {:as from from-entry :current-entry} (entry-info! db root from-path)
        {:as to to-parent :parent to-entry :current-entry} (entry-info! db root to-path)
        tx (concat (cp-cas from to recursive overwrite)
                   (when to-entry
                     [[:db.fn/retractEntity (:db/id to-entry)]])
                   (cp-tx (:db/id to-parent)
                          (-> (into {} from-entry)
                              (assoc ::de/name (last to-path)))))
        res @(transact conn tx)]
    (log/debug "cp success" res)
    (if to-entry
      {:success :copied
       :result :overwritten}
      {:success :copied
       :result :created})))

(defn mv-tx [from-parent-id to-parent-id entry-id new-name]
  (let [cur-date (Date.)]
    (concat [[:db/add entry-id ::de/name new-name]
             [:db/add from-parent-id ::de/last-modified cur-date]
             [:db/add to-parent-id ::de/last-modified cur-date]]
            (when-not (= from-parent-id to-parent-id)
              [[:db/retract from-parent-id ::dd/children entry-id]
               [:db/add to-parent-id ::dd/children entry-id]]))))

(deffileop mv! "MOVE" [{:keys [conn root] :as store}
                       from-path to-path recursive overwrite]
  (when (= from-path (take (count from-path) to-path))
    (throw (ex-info "Cannot move entry into itself"
                    {:error :target-removed :path to-path})))
  (let [db (store-db store)
        {:as from from-parent :parent from-entry :current-entry} (entry-info! db root from-path)
        {:as to to-parent :parent to-entry :current-entry} (entry-info! db root to-path)
        tx (concat (cp-cas from to recursive overwrite)
                   (when to-entry
                     [[:db.fn/retractEntity (:db/id to-entry)]])
                   (mv-tx (:db/id from-parent) (:db/id to-parent) (:db/id from-entry) (last to-path)))
        res @(transact conn tx)]
    (log/debug "mv success" res)
    (if to-entry
      {:success :moved
       :result :overwritten}
      {:success :moved
       :result :created})))

(defn blob-file [{bs :blob-store} {sha1 ::dfc/sha-1}]
  (get-file bs sha1))

(defn- ls-seq
  [store {:keys [::dd/children ::de/type] :as e} dir depth]
  (cons (cond-> (assoc (reify-entity e)
                  :davstore.ls/path dir)
                (= ::det/file type) (assoc :davstore.ls/blob-file (blob-file store e)))
        (when (pos? depth)
          (mapcat #(ls-seq store % (conj dir (::de/name %)) (dec depth)) children))))

(defn ls [store path depth]
  (when-let [e (get-entry store path)]
    (ls-seq store e (vec path) depth)))

;; ### File Store Init

(def root-id :davstore.container/root)

(defn create-root!
  ([conn uuid] (create-root! conn uuid {}))
  ([conn uuid root-entity]
   (assert (not (d/entity (d/db conn) [:davstore.root/id uuid])) "Root exists")
   (let [root-id (tempid :db.part/user)
         rdir-id (tempid :db.part/davstore.entries)
         cur-date (Date.)
         root-dir {:db/id rdir-id
                   ::de/name (str \{ uuid \})
                   ::de/type :davstore.entry.type/dir
                   ::de/created cur-date
                   ::de/last-modified cur-date}
         tx [(assoc root-entity
                    :db/doc (str "File root {" uuid "}")
                    :db/id root-id
                    ::dr/id uuid
                    ::dr/dir rdir-id)
             root-dir]
         {:keys [db-after]} @(transact conn tx)]
     (d/entity db-after [::dr/id uuid]))))

(defn open-root! [{:keys [conn] :as store} uuid create-if-missing]
  (let [db (d/db conn)
        root' (d/entity db [:davstore.root/id uuid])
        root (cond
              root' root'
              create-if-missing (create-root! conn uuid create-if-missing)
              :else (throw (ex-info (str "No store {" uuid "}")
                                    {:conn conn :uuid uuid})))]
    (assoc store :root uuid)))

(defn db-fn [db id]
  (let [res (:db/fn (d/entity db id))]
    (assert res)
    res))

(defn init-store! 
  ([db-uri blob-store] (init-store! db-uri blob-store (d/squuid) true))
  ([db-uri blob-store main-root-uuid create-if-missing]
     (let [created (create-database db-uri)
           conn (connect db-uri)
           _ (ensure-schema! conn)
           db (d/db conn)]
       (assoc (open-root! {:conn conn} main-root-uuid (when create-if-missing
                                                        (if created
                                                          {:db/ident root-id}
                                                          {})))
         :store-id main-root-uuid
         :blob-store blob-store))))

;; # Testing and maintenance

(typ/tc-ignore
 ;; maintenance


 ;; dev

 (set! *warn-on-reflection* true)

 (defn cat [store path]
   (when-let [sha1 (:davstore.file.content/sha-1 (get-entry store path))]
     (println (slurp (blob-file store sha1)))))

 (defn- store-str [{:keys [blob-store]} ^String s]
   (store-file blob-store (java.io.ByteArrayInputStream.
                           (.getBytes s "UTF-8"))))

 (defn store-tp [store path content]
   (touch! store path "text/plain; charset=utf-8" (store-str store content) nil))

 (defn write! [store path content]
   (touch! store path "text/plain" (store-str store content) nil))

 (defn insert-testdata [{:keys [conn] :as store}]
   (store-tp store ["a"] "a's content")
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
 (defmethod print-entry :davstore.entry.type/dir
   [{:keys [:db/id ::de/name ::dd/children]} depth]
   (apply concat
          (repeat depth "  ")
          [name "/ #" id "\n"]
          (map #(print-entry % (inc depth)) children)))

 (defmethod print-entry :davstore.entry.type/file
   [{:keys [:db/id ::de/name ::dfc/sha-1]} depth]
   (concat
    (repeat depth "  ")
    [name " #" id " - CH: " sha-1 "\n"]))

 (defn pr-tree [store]
   (doseq [s (print-entry (get-entry store []) 0)]
     (print s))))

(defn pprint-tx [db datoms]
  (reduce (fn [out [e a v]]
            (let [il (dec (count out))
                  lst (when-not (neg? il) (nth out il))
                  akw (d/ident db a)]
              (cond
               (and (map? lst)
                    (= e (:db/id lst)))
               (assoc out il (assoc lst akw v))
               (= e (first lst))
               (assoc out il {:db/id e
                              (nth lst 1) (nth lst 2)
                              akw v})
               :else (conj out [e akw v]))))
          [] (sort-by :e datoms)))
