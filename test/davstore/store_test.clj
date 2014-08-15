(ns davstore.store-test
  (:require [clojure.test :refer :all]
            [davstore.store :refer :all]
            [davstore.blob :as blob]
            [datomic.api :as d]
            [clojure.pprint :refer :all]
            [clojure.repl :refer :all]))

(def ^:dynamic *store*)

(use-fixtures
  :once (fn [f]
          (let [uuid (d/squuid)
                db-uri (str "datomic:mem://davstore-test-" uuid)
                blob-path (str "/tmp/davstore-test-" uuid)]
            (try
              (let [blobstore (blob/make-store blob-path)]
                (binding [*store* (init-store! db-uri blobstore uuid true)]
                  ;; (pprint [:once (dissoc *store* :path-entry)])
                  (insert-testdata *store*)
                  (f)))
              (finally
                (d/delete-database db-uri)
                (org.apache.commons.io.FileUtils/deleteDirectory
                 (java.io.File. blob-path)))))))
(use-fixtures
    :each (fn [f]
            (let [uuid (d/squuid)]
              (binding [*store* (open-root! *store* uuid {})]
                ;; (pprint [:each (dissoc *store* :path-entry)])
                 (insert-testdata *store*)
                (f)))))

(defn test-verify-store
  ([] (test-verify-store true))
  ([expect-succcess]
;     (pr-tree *store*)
     (if expect-succcess
       (is (= nil (seq (verify-store *store*))))
       (is (seq (verify-store *store*))))))

(deftest verification
  (println (keys *store*) (:conn *store*) (store-db *store*))
  (test-verify-store)
  @(d/transact (:conn *store*)
               [[:db/add (:db/id (get-entry *store* ["d" "c"]))
                 :davstore.entry/name "C"]])
  (test-verify-store false))
