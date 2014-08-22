(ns davstore.blob
  (:import java.io.File
           (java.security MessageDigest DigestInputStream DigestOutputStream)
           javax.xml.bind.DatatypeConverter)
  (:require 
   [clojure.edn :as edn]
   [clojure.java.io :refer [file input-stream output-stream copy] :as io]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [webnf.base :refer [pprint-str forcat]]
   [clojure.core.typed :as typ :refer
    [ann cf defalias List HMap Set Value Fn Option Rec I U
     non-nil-return]]))

(set! *warn-on-reflection* true)

(ann clojure.java.io/file [(U String File) (U String File) * -> File])
(ann clojure.java.io/copy [(U java.io.File
                              java.io.InputStream
                              java.io.Reader
                              String)
                           (U File
                              java.io.OutputStream
                              java.io.Writer)
                           -> nil])
(ann clojure.java.io/output-stream [(U String File) -> java.io.OutputStream])
(ann clojure.java.io/input-stream [(U String File) -> java.io.InputStream])
(non-nil-return java.lang.String/toLowerCase :all)
(non-nil-return java.io.File/getAbsoluteFile :all)
(non-nil-return java.io.File/createTempFile :all)
(non-nil-return java.security.MessageDigest/getInstance :all)
(non-nil-return java.security.MessageDigest/digest :all)
(non-nil-return javax.xml.bind.DatatypeConverter/printHexBinary :all)
(non-nil-return javax.xml.bind.DatatypeConverter/parseHexBinary :all)

(defalias BlobStore
  (HMap :mandatory {:root-dir File}
        :complete? true))

(defalias TempFile
  (HMap :mandatory {:sha-1 String
                    :tempfile File}
        :complete? true))

(defalias Sha1 String)

(ann make-store [(U String File) -> BlobStore])
(defn make-store [root-path]
  (let [rf (.getAbsoluteFile (file root-path))]
    (assert rf)
    (.mkdirs rf)
    {:root-dir rf}))

(ann store-temp [BlobStore java.io.InputStream -> TempFile])
(defn store-temp [{:keys [root-dir]} input-stream]
  (let [tf (File/createTempFile "temp-" ".part" root-dir)
        digest (MessageDigest/getInstance "SHA-1")
        dis (DigestInputStream. input-stream digest)]
    (with-open [os (output-stream tf)]
      (copy dis os)
      {:sha-1 (.toLowerCase (DatatypeConverter/printHexBinary
                             (.digest digest)))
       :tempfile tf})))

(ann sha-file [BlobStore Sha1 -> File])
(defn- sha-file ^File [{:keys [root-dir]} ^String sha-1]
  (let [sha (.toLowerCase sha-1)
        dir (file root-dir (subs sha 0 2))]
    (file dir (subs sha 2))))

(ann get-file [BlobStore Sha1 -> (Option File)])
(defn get-file [store sha-1]
  (let [f (sha-file store sha-1)]
    (when (.isFile f) f)))

(ann open-file [BlobStore Sha1 -> (Option java.io.InputStream)])
(defn open-file [store sha-1]
  (when-let [f (get-file store sha-1)]
    (input-stream f)))

(ann merge-temp [BlobStore TempFile -> Boolean])
(defn merge-temp [store {:keys [^File tempfile sha-1]}]
  (let [dest (sha-file store sha-1)]
    (if (.isFile dest)
      (do (.delete tempfile)
          false)
      (let [p (.getParentFile dest)]
        (when-not p (throw (ex-info "Root dir" {:path dest})))
        (.mkdir p)
        (.renameTo tempfile dest)
        true))))

(ann store-file [BlobStore java.io.InputStream -> Sha1])
(defn store-file [store input-stream]
  (let [{:keys [sha-1] :as res} (store-temp store input-stream)]
    (merge-temp store res)
    sha-1))

(ann stream-temp [BlobStore [java.io.OutputStream -> nil] -> TempFile])
(defn stream-temp [{:keys [root-dir]} f]
  (let [tf (File/createTempFile "temp-" ".part" root-dir)
        digest (MessageDigest/getInstance "SHA-1")]
    (with-open [os (DigestOutputStream. (output-stream tf) digest)]
      (f os)
      {:sha-1 (.toLowerCase (DatatypeConverter/printHexBinary
                             (.digest digest)))
       :tempfile tf})))

(ann stream-file [BlobStore [java.io.OutputStream -> nil] -> Sha1])
(defn stream-file [store f]
  (let [{:keys [sha-1] :as res} (stream-temp store f)]
    (merge-temp store res)
    sha-1))

(ann write-file [BlobStore [java.io.PrintWriter -> nil] -> Sha1])
(defn write-file [store f]
  (stream-file store #(with-open [w (java.io.PrintWriter.
                                     (java.io.OutputStreamWriter. %))]
                        (f w))))
