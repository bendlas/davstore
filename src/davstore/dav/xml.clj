(ns davstore.dav.xml
  (:require [clojure.data.xml :as xml]
            [clojure.core.match :refer [match]]
            [clojure.tools.logging :as log]))

(defn multi? [v]
  (or (list? v) (vector? v)))

(defn to-multi [v]
  (if (multi? v) v [v]))

;; XML output

#_(defn pipe [writer-f]
    (let [pis (java.io.PipedInputStream. 1024)
          wrt (future (with-open [pos (java.io.PipedOutputStream. pis)]
                        (try (writer-f pos)
                             (catch Exception e
                               (log/error e "In pipe writer")))))]
      pis))

(defn emit [xt]
  ;; (log/debug "BODY" (with-out-str (clojure.pprint/pprint xt)))
  (with-open [w (java.io.StringWriter. 1024)]
    (xml/emit
     (-> xt
         (assoc-in [:attrs :xmlns/d] "DAV:")
         (assoc-in [:attrs :xmlns/b] "//dav.bendlas.net/extension-elements"))
     w)
    (.toString w)))

(def get-status-phrase
  (into {}
        (for [^java.lang.reflect.Field f (.getFields java.net.HttpURLConnection)
              :let [name (.getName f)]
              :when (.startsWith name "HTTP_")
              :let [code (.get f java.net.HttpURLConnection)]]
          [code name])))

(xml/defns "DAV:"
  :bendlas "//dav.bendlas.net/extension-elements")

(defn- element [name content]
  (xml/element* (xml/xml-name name) nil content))

(defn- props [ps]
  (xml/element* ::prop nil
                (for [[n v] ps
                      :when v]
                  (if (multi? v)
                    (element n v)
                    (element n [v])))))

(defn- status [code]
  (xml/element ::status nil (if (number? code)
                              (str "HTTP/1.1 " code " " (get-status-phrase code))
                              (str code))))

(defn propstat [st ps]
  (xml/element ::propstat nil
               (props ps)
               (status st)))

(defn response [href status-or-propstat]
  (xml/element* ::response nil
                (cons (xml/element ::href nil href)
                      (to-multi status-or-propstat))))

(defn multistatus [href-s-o-ps]
  (xml/element* ::multistatus nil
                (for [[href s-o-ps] href-s-o-ps]
                  (response href s-o-ps))))

;; XML input

(defn error! [& {:as attrs}]
  (throw (ex-info (str "XML parsing error " attrs) attrs)))

(defn parse-props [props]
  (reduce (fn [pm prop]
            (match [prop]
                   [{:tag #xml/name ::allprop}]
                   (assoc pm ::all true)
                   [{:tag #xml/name ::propname}]
                   (assoc pm ::names-only true)
                   [{:tag #xml/name ::prop
                     :content content}]
                   (reduce (fn [pm {pn :tag pv :content}]
                             (assoc pm pn pv))
                           pm content)
                   :else (error! :no-match (pr-str prop))))
          {} props))

(defn parse-propfind [pf]
  (match [pf]
         [{:tag #xml/name ::propfind
           :content props}]
         (parse-props props)))

