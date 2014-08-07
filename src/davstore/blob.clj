(ns davstore.blob
  (:import java.io.File
           (java.security MessageDigest DigestInputStream)
           javax.xml.bind.DatatypeConverter)
  (:require 
   [clojure.edn :as edn]
   [clojure.java.io :refer [file input-stream output-stream copy] :as io]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [webnf.base :refer [pprint-str forcat]]))

(set! *warn-on-reflection* true)

(defn make-store [root-path]
  (let [rf (.getAbsoluteFile (file root-path))]
    (.mkdirs rf)
    {:root-dir rf}))

(defn store-temp [{:keys [root-dir]} input-stream]
  (let [tf (File/createTempFile "temp-" ".part" root-dir)
        digest (MessageDigest/getInstance "SHA-1")
        dis (DigestInputStream. input-stream digest)]
    (with-open [os (output-stream tf)]
      (copy dis os)
      {:sha-1 (.toLowerCase (DatatypeConverter/printHexBinary
                               (.digest digest)))
       :tempfile tf})))

(defn sha-file ^File [{:keys [root-dir]} ^String sha-1]
  (let [sha (.toLowerCase sha-1)
        dir (file root-dir (subs sha 0 2))]
    (file dir (subs sha 2))))

(defn open-file [store sha-1]
  (let [f (sha-file store sha-1)]
    (when (.isFile f)
      (input-stream f))))

(defn merge-temp [store {:keys [^File tempfile sha-1]}]
  (let [dest (sha-file store sha-1)]
    (if (.isFile dest)
      (do (.delete tempfile)
          false)
      (do (.mkdir (.getParentFile dest))
          (.renameTo tempfile dest)
          true))))

(defn store-file [store input-stream]
  (let [{:keys [sha-1] :as res} (store-temp store input-stream)]
    (merge-temp store res)
    sha-1))
