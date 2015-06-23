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
  "Unused for WebDAV, but useful if you want to serve stable, infinitely
  cacheable names, say for a CDN."
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
             :proppatch (dav/proppatch path)
             :lock (dav/lock path)
             :unlock (dav/unlock path)}))

(def blob-dir "/tmp/davstore-app")
(def db-uri "datomic:mem://davstore-app")
(def root-id #uuid "7178245f-5e6c-30fb-8175-c265b9fe6cb8")

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

(defn wrap-log-light [h]
  (fn [req]
    (try
      (let [resp (h req)]
        (log/debug (.toUpperCase (name (:request-method req)))
                   (:uri req) " => " (:status resp))
        resp)
      (catch Exception e
        (log/error e (.toUpperCase (name (:request-method req)))
                   (:uri req) " => " 500)))))

(defn wrap-access-control [h allow-origin]
  (let [hdr {"access-control-allow-origin" allow-origin
             "access-control-allow-credentials" "true"
             "access-control-allow-methods" "OPTIONS,GET,PUT,DELETE,PROPFIND,REPORT,MOVE,COPY,PROPPATCH,MKCOL"
             "access-control-allow-headers" "Content-Type,Depth,Cache-Control,X-Requested-With,If-Modified-Since,If-Match,If,X-File-Name,X-File-Size,Destination,Overwrite,Authorization"
             "dav" "2"}]
    (fn [req]
      (if (= :options (:request-method req))
        {:status 204 :headers hdr}
        (update-in (h req) [:headers] #(merge hdr %))))))

;; Quick and embedded dav server

(require '[ring.adapter.jetty :as rj])
(declare davstore)
(defonce server (agent nil))

(defn start-server! []
  (def davstore
    (app
     (wrap-store blob-dir db-uri root-id "/files")
     wrap-log-light
     (wrap-access-control "http://localhost:8080")
     ["blob" &] blob-handler
     ["files" &] file-handler
     ["debug"] (fn [req] {:status 400 :debug req})))
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
  ([h] (::store (:debug (h {:request-method :debug :uri "/debug"})))))
