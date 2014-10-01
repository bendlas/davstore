(ns davstore.dav.xml
  (:require [clojure.data.xml :as xml]
            [clojure.core.match :refer [match]]
            [clojure.tools.logging :as log]))

;; # Namespaced xml parsing

;; ## Set up runtime - global keyword prefixes
;; They can be used to denote namespaced xml names in as regular clojure keywords

(xml/defns "DAV:" ;; e.g. ::props -> :davstore.dav.xml/props -> {DAV:}props
  :bendlas "//dav.bendlas.net/extension-elements") ;; e.g. ::bendlas:regular-file

(defn multi? [v]
  (or (list? v) (vector? v)))

(defn to-multi [v]
  (if (multi? v) v [v]))

(defn error! [& {:as attrs}]
  (throw (ex-info (str "XML parsing error " attrs) attrs)))

(defn parse-props [props]
  (reduce (fn [pm prop]
            (match [prop]
                   ;; FIXME: The parser should be able to parse in a namespace-local mode
                   ;; to keywordize names mentioned in the defns clause
                   ;; But maybe not, because then equal parses from different ns
                   ;; wouldn't be = anymore
                   [{:tag ::allprop}]
                   (assoc pm ::all true)
                   [{:tag ::propname}]
                   (assoc pm ::names-only true)
                   [{:tag ::prop
                     :content content}]
                   (reduce (fn [pm {pn :tag pv :content}]
                             (assoc pm pn pv))
                           pm content)
                   :else (error! :no-match (pr-str prop))))
          {} props))

(defn parse-propfind [pf]
  (match [pf]
         [{:tag ::propfind
           :content props}]
         (parse-props props)))

(defn parse-lock [lock-props]
  (reduce (fn [lm prop]
            (match [prop]
                   [{:tag ::lockscope
                     :content ([scope] :seq)}]
                   (assoc lm :scope (match [scope]
                                           [{:tag ::exclusive}] :exclusive
                                           [{:tag ::shared}] :shared))
                   [{:tag ::locktype
                     :content ([{:tag ::write}] :seq)}]
                   (assoc lm :type :write)
                   [{:tag ::owner
                     :content owner-info}]
                   (assoc lm :owner owner-info)))
          {} lock-props))

(defn parse-lockinfo [li]
  (match [li]
         [{:tag ::lockinfo
           :content lock}]
         (parse-lock lock)))

;; # XML output

(defn emit [xt]
  ;; FIXME: Should the emitter be able to pick up a defns clause?
  ;; What about foreign names?
  ;; Auto shorten prefix?
  ;; Need to predeclare?
  ;; Declare inline in emitter, taking space cost?
  (with-open [w (java.io.StringWriter. 1024)]
    (xml/emit
     (-> xt
                                        ; DAV: needs to have a prefix
                                        ; (not default namespace)
                                        ; for windows compatibility
         (assoc-in [:attrs :xmlns/d] "DAV:")
                                        ; reserved for custom attributes
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

(defn- element [name content]
  (xml/element* (xml/xml-name name) nil (to-multi content)))

(defn props [ps]
  (xml/element* ::prop nil
                (for [[n v] ps
                      :when v]
                  (element n v))))

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

(defn- dav-prop [kw]
  {:tag (xml/xml-name "DAV:" (name kw))})

(defn activelock [{:keys [scope type owner depth timeout token]}]
  (element ::activelock
           [(element ::locktype (dav-prop type))
            (element ::lockscope (dav-prop scope))
            (element ::depth depth)
            (element ::owner owner)
            (element ::timeout timeout)
            (element ::locktoken (element ::href (str "urn:uuid:" token)))]))
