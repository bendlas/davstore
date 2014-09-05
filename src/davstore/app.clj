(ns davstore.app
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :as log]
            [datomic.api :refer [delete-database]]
            [davstore.blob :as blob]
            [davstore.dav :as dav]
            [davstore.store :as store]
            [net.cgrand.moustache :refer [app uri-segments path-info-segments uri]]
            [ring.middleware.head :refer [wrap-head]]
            [ring.util.response :refer [response header status content-type not-found]]))

;; Middlewares

(defn wrap-store [h blob-path db-uri root-uuid root-uri]
  (let [store (blob/make-store blob-path)
        dstore (assoc (store/init-store! db-uri store root-uuid true)
                 :root-dir root-uri)]
    (fn [req]
      (h (assoc req
           ::store (store/open-db dstore)
           ::bstore store)))))

(defn wrap-root [h]
  (fn [req]
    (let [uri-segs (uri-segments req)
          path-segs (path-info-segments req)
          n (- (count uri-segs) (count path-segs))]
      (h (assoc req :root-path (take n uri-segs))))))

;; Handlers

(def blob-handler
  (app
   [] {:post (fn [{body :body store ::bstore :as req}]
               (let [{:keys [tempfile sha-1] :as res} (blob/store-temp store body)
                     created (blob/merge-temp store res)
                     uri (dav/pjoin (:uri req) sha-1)]
                 (-> (response sha-1)
                     (header "location" uri)
                     (header "etag" (str \" sha-1 \"))
                     (status (if created 201 302)))))}
   [sha-1] {:get (fn [req]
                   (if-let [f (blob/get-file (::bstore req) sha-1)]
                     (-> (response f)
                         (content-type "application/octet-stream")
                         (header "etag" (str \" sha-1 \")))
                     (not-found "Blob not found")))}))

(def file-handler
  (app
   wrap-root
   wrap-head
   dav/wrap-errors
   [& path] {:options (dav/options path)
             :propfind (dav/propfind path)
             :get (dav/read path)
             :mkcol (dav/mkcol path)
             :copy (dav/copy path)
             :move (dav/move path)
             :delete (dav/delete path)
             :put (dav/put path)
             #_(:proppatch (dav/proppatch path)
                           )}))

(def blob-dir "/tmp/davstore-app")
(def db-uri "datomic:mem://davstore-app" #_"datomic:free://localhost:4334/davstore-app")
(def root-id #uuid "6c28aedd-6ada-44b4-9f2d-1c666926982f")

(defn wrap-log [h]
  (fn [req]
    (let [bos (java.io.ByteArrayOutputStream.)
          _ (when-let [body (:body req)] (io/copy body bos))
          bs (.toByteArray bos)
          _ (log/debug (:request-method req) (:uri req)
                       "\nHEADERS:" (with-out-str (pprint (:headers req)))
                       "BODY:" (str \' (String. bs "UTF-8") \'))
          resp (h (assoc req :body (java.io.ByteArrayInputStream. bs)))]
      (log/debug " => RESPONSE" (with-out-str (pprint resp)))
      resp)))

(def davstore
  (app
   (wrap-store blob-dir db-uri root-id "/files")
   ;; wrap-log
   ["blob" &] blob-handler
   ["files" &] file-handler
   ["debug"] (fn [req] {:status 400 :debug req})))

(defonce server (agent nil))

(require '[ring.adapter.jetty :as rj])
(defn start-server! []
  (send server
        (fn [s]
          (assert (not s))
          (rj/run-jetty #'davstore {:port 8082 :join? false}))))

(defn stop-server! []
  (send server
        (fn [s]
          (.stop s)
          nil)))

(defn get-handler-store
  ([] (get-handler-store davstore))
  ([h] (::store (:debug (h {:uri "/debug"})))))