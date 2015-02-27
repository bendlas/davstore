(ns davstore.dav
  (:require [clojure.core.match :refer [match]]
            [clojure.data.xml :as xml]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [davstore.blob :as blob]
            [davstore.dav.xml :as dav]
            [davstore.store :as store]
            [davstore.schema :refer [alias-ns]]
            [datomic.api :as d]
            [ring.util.response :refer [created]]
            [webnf.base :refer [pprint-str]]
            [webnf.kv :refer [map-vals assoc-when*]]
            [webnf.date :as date])
  (:import java.io.File
           java.net.URI
           java.nio.file.Files
           java.util.Date))

(defn entry-status [{:as want-props :keys [::dav/all ::dav/names-only]}
                    {:as entry :keys [:davstore.ls/path :davstore.file.content/mime-type
                                      :davstore.entry/type :davstore.file.content/sha-1
                                      :davstore.entry/name :davstore.ls/blob-file
                                      :davstore.entry/created :davstore.entry/last-modified]}]
  (let [props (assoc-when* (fn* ([k] (or all (contains? want-props k)))
                                ([k v] (not (nil? v))))
                           {}
                           ::dav/displayname name
                           ::dav/getcontenttype mime-type
                           ::dav/getetag (when sha-1 (str \" sha-1 \"))
                           ::dav/getlastmodified (date/format-http (or last-modified (Date. 0)))
                           ::dav/creationdate (date/format-http (or created (Date. 0)))
                           ::dav/resourcetype
                           (case type
                             ;; here you can see, how to refer to xml names externally
                             :davstore.entry.type/dir (xml/element ::dav/collection)
                             :davstore.entry.type/file (xml/element ::dav/bendlas:file))
                           ::dav/getcontentlength (and blob-file (str (.length ^File blob-file))))]
    (if names-only
      (map-vals (constantly nil) props)
      props)))

(defn pjoin [^String root-dir & [path & pathes]]
  (let [sb (StringBuilder. root-dir)]
    (when-not (= \/ (.charAt root-dir (dec (count root-dir))))
      (.append sb \/))
    (when path
      (.append sb path)
      (reduce (fn [_ p] (.append sb \/) (.append sb p))
              nil pathes))
    (str sb)))

(defn propfind-status [^String root-dir files {:as want-props :keys [::dav/all ::dav/names-only]}]
  (reduce (fn [m {:keys [:davstore.ls/path] :as entry}]
            (assoc m
              (apply pjoin root-dir path)
              (dav/propstat 200 (entry-status want-props entry))))
          {} files))

(def path-matcher
  (memoize (fn [^String root-dir]
             (fn [^String uri]
               (when (zero? (.indexOf uri root-dir))
                 (str/split (subs uri (count root-dir)) #"/"))))))

(defn to-path [{{:keys [root-dir]
                 :or {root-dir "/"}} :davstore.app/store
                 {host "host"} :headers}
               uri]
  (let [uri* (URI/create uri)
        uhost (.getAuthority uri*)
        path (.getRawPath uri*)]
    (when (and uhost (not= host uhost))
      (throw (ex-info "Cannot manipulate files across hosts"
                      {:error :foreign-file
                       :target uri
                       :host host})))
    (->> (or (seq ((path-matcher root-dir) path))
             (throw (ex-info "Invalid Prefix"
                             {:error :user-error
                              :allowed-root root-dir
                              :request-uri uri})))
         (remove str/blank?)
         (map #(java.net.URLDecoder/decode % "UTF-8"))
         vec)))

(defn parse-etag [etag]
  (when-let [[_ res] (and etag (re-matches #"\"([^\"]+)\"" etag))]
    res))

(def mime-overrides
  {"css" "text/css"
   "js" "text/javascript"})

(defn infer-type [^File file name]
  (let [ext (last (str/split name #"\."))]
    (or (mime-overrides ext)
        (Files/probeContentType (.toPath file)))))

(defmacro defhandler [name [route-info-sym request-sym :as args] & body]
  (assert (= 2 (count args)) "Handler must take route-info and request")
  `(defn ~name [~route-info-sym]
     (fn [~request-sym]
       ~@body)))

;; Handlers

(alias-ns
 de  davstore.entry
 det davstore.entry.type
 des davstore.entry.snapshot
 dr  davstore.root
 dd  davstore.dir
 dfc davstore.file.content
 dfn davstore.fn)

(defhandler options [_ _]
  {:status 200
   :headers {"DAV" "2"}})

(defhandler propfind [path {:as req
                            store :davstore.app/store
                            {:strs [depth content-length]} :headers
                            uri :uri}]
;  (log/info "PROPFIND" uri (pr-str path) "depth" depth)
  (if-let [fs (seq (store/ls store (remove str/blank? path) 
                             (case depth
                               "0" 0
                               "1" 1
                               "infinity" 65536)))]
    (let [want-props (if (= "0" content-length)
                       {::dav/all true}
                       (dav/parse-propfind (xml/parse* 'davstore.dav.xml (:body req))))]
      {:status 207 :headers {"content-type" "text/xml; charset=utf-8" "dav" "1"}
       :body (dav/emit (dav/multistatus
                        (propfind-status (:root-dir store) fs want-props)))})
    {:status 404}))

(defhandler read [path {:as req store :davstore.app/store uri :uri}]
;  (log/info "GET" uri (pr-str path))
  (let [db (store/store-db store)]
    (loop [{:as entry
            :keys [::dfc/mime-type ::de/type ::dfc/sha-1 ::dd/index-file db/id]}
           (store/get-entry store (remove str/blank? path))]
      (if entry
        (if (= ::det/file type)
          {:status 200
           :headers {"Content-Type" mime-type
                     "ETag" (str \" sha-1 \")}
           :body (store/blob-file store entry)}
          (if index-file
            (recur (d/entity db (store/dir-child db id index-file)))
            {:status 405 :body (str uri " is a directory")}))
        {:status 404 :body (str "File " uri " not found")}))))

(defhandler mkcol [path {:as req uri :uri store :davstore.app/store}]
  ;; FIXME normalize path for all
;  (log/info "MKCOL" uri (pr-str path))
  (store/mkdir! store (remove str/blank? path))
  (created uri))

(defhandler delete [path {:as req store :davstore.app/store
                          {etag "if-match"
                           depth "depth"} :headers}]
;  (log/info "DELETE" (:uri req) (pr-str path))
  (store/rm! store (remove str/blank? path) (or (parse-etag etag) :current)
             (case depth
               "0" false
               "infinity" true
               nil true))
  {:status 204})

(defhandler move [path {:as req store :davstore.app/store
                        {:strs [depth overwrite destination]} :headers
                        uri :uri}]
;  (log/info "MOVE" uri (pr-str path) "to" destination)
  (match [(store/mv! store (remove str/blank? path)
                     (to-path req destination)
                     (case depth
                       "0" false
                       "infinity" true
                       nil true)
                     (case overwrite
                       "T" true
                       "F" false
                       nil false))]
         [{:success :moved
           :result :overwritten}]
         {:status 204}
         [{:success :moved
           :result :created}]
         (created destination)))

(defhandler copy [path {:as req store :davstore.app/store
                        {:strs [depth overwrite destination]} :headers
                        uri :uri}]
;  (log/info "COPY" uri (pr-str path) "to" destination)
  (match [(store/cp! store (remove str/blank? path)
                     (to-path req destination)
                     (case depth
                       "0" false
                       "infinity" true
                       nil true)
                     (case overwrite
                       "T" true
                       "F" false
                       nil false))]
         [{:success :copied
           :result :overwritten}]
         {:status 204}
         [{:success :copied
           :result :created}]
         (created destination)))

(defhandler put [path {:as req store :davstore.app/store
                       body :body
                       {:strs [content-type if-match]} :headers
                       uri :uri}]
                                        ;  (log/info "PUT" uri (pr-str path))
  (let [blob-sha (blob/store-file (:blob-store store) body)
        path (remove str/blank? path)
        fname (last path)
        ctype (if (or (nil? content-type)
                      (= "application/octet-stream" content-type))
                (infer-type (blob/get-file (:blob-store store) blob-sha)
                            fname)
                content-type)]
    (match [(store/touch! store path
                          ctype
                          blob-sha
                          (or (parse-etag if-match) :current))]
           [{:success :updated}] {:status 204}
           [{:success :created}] (created uri))))

(defhandler proppatch [path {:as req store :davstore.app/store
                             body :body}]
  {:status 405})

(defhandler lock [path {:as req store :davstore.app/store
                        {:strs [depth]} :headers
                        body :body}]
  (let [entry (store/get-entry store (remove str/blank? path))
        info (dav/parse-lockinfo (xml/parse* 'davstore.dav.xml body))]
    ;; (log/debug "Lock Info\n" (pprint-str info))
    {:status (if entry 200 201)
     :body (dav/emit
            (dav/props
             {#xml/name ::dav/lockdiscovery
              (dav/activelock (assoc info
                                :depth depth
                                :timeout "Second-60"
                                :token (java.util.UUID/randomUUID)))}))}))

(defhandler unlock [path {:as req store :davstore.app/store
                          {:strs [lock-token]} :headers}]
  ;; (log/info "unlock token" lock-token)
  {:status 204})

(defn wrap-errors [h]
  (fn [req]
    (try
      (try (h req)
           (catch java.util.concurrent.ExecutionException e
             (throw (.getCause e))))
      (catch clojure.lang.ExceptionInfo e
        (log/debug e "Translating to status code" (pprint-str (ex-data e)))
        (match [(ex-data e)]
               [{:error :cas/mismatch}] {:status 412 :body "Precondition failed"}
               [{:error :dir-not-empty}] {:status 412 :body "Directory not empty"}
               [data] (do (log/error e "Unhandled Exception during" (:request-method req) (:uri req)
                                     "\nException Info:" (pr-str data))
                          {:status 500}))))))

