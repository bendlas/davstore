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

(def ^:dynamic ^{:doc "testdata-ref-tree"} *trt*)

(defn store-content [sha1]
  (when-let [f (and sha1 (blob/get-file (:blob-store *store*) sha1))]
    (slurp f)))

(defn match-tree
  ([[entry]] (match-tree entry *trt*))
  ([{:keys [davstore.container/children davstore.file.content/sha1]} tree]
     (cond
      (string? tree) (is (= tree (store-content sha1)))
      (map? tree) (let [ch (into {} (map (juxt :davstore.entry/name identity) children))]
                    (is (= (count ch) (count children)) "Multiple entries with same name")
                    (reduce-kv
                     (fn [_ fname content]
                       (let [c (get ch fname)]
                         (is c (str "File " fname " not present"))
                         (match-tree c content)))
                     nil tree))
      :else (throw))))

(defn match-root
  ([] (match-root *trt*))
  ([tree]
     (match-tree (get-entry *store* []) tree)))

(defn update-root! [f]
  (set! *trt* (f *trt*))
  (match-root))

(def testdata-ref-tree
  {"a" "a's content"
   "b" "b's content"
   "d" {"c" "d/c's content"}})

(deftest file-ops
  (binding [*trt* testdata-ref-tree]
    (match-root)
    (testing "Copy file"
      (cp! *store* ["a"] ["A"] false false)
      (update-root! #(assoc % "A" (% "a")))
      (is (thrown? clojure.lang.ExceptionInfo
                   (cp! *store* ["b"] ["A"] false false)))
      (cp! *store* ["b"] ["A"] false true)
      (update-root! #(assoc % "A" (% "b")))
      (is (thrown? clojure.lang.ExceptionInfo
                   (cp! *store* ["d"] ["D"] false false)))
      (cp! *store* ["d"] ["D"] true false)
      (update-root! #(assoc % "D" (% "d"))))
    (testing "Copy dir"
      (is (thrown? clojure.lang.ExceptionInfo
                   (cp! *store* ["d"] ["D"] true false)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (cp! *store* ["d"] ["D"] false true)))
      (cp! *store* ["d"] ["D"] true true)
      (match-root))
    (testing "Move file"
      (mv! *store* ["A"] ["A'"] false false)
      (update-root! #(-> % (assoc "A'" (% "A")) (dissoc "A")))
      (is (thrown? clojure.lang.ExceptionInfo
                   (mv! *store* ["b"] ["A'"] false false)))
      (mv! *store* ["a"] ["d" "a"] false false)
      (update-root! #(-> % (assoc-in ["d" "a"] (% "a")) (dissoc "a")))
      (is (thrown? clojure.lang.ExceptionInfo
                   (mv! *store* ["b"] ["d" "a"] false false)))
      (mv! *store* ["b"] ["d" "a"] false true)
      (update-root! #(-> % (assoc-in ["d" "a"] (% "b")) (dissoc "b"))))
    (testing "Move dir"
      (is (thrown? clojure.lang.ExceptionInfo
                   (mv! *store* ["d"] ["D"] true false)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (mv! *store* ["d"] ["D"] false true)))
      (mv! *store* ["d"] ["D"] true true)
      (update-root! #(dissoc % "d")))))

(deftest self-referential
  (binding [*trt* testdata-ref-tree]
    (testing "Copy into itself"
      (cp! *store* ["d"] ["e"] true false)
      (cp! *store* ["d"] ["d" "D"] true false)
      (cp! *store* ["d"] ["d" "E"] true false)
      (cp! *store* ["d"] ["e" "D"] true false)
      (cp! *store* ["d"] ["e" "E"] true false)
      (update-root! #(-> %
                         (assoc "e" (% "d"))
                         (assoc-in ["d" "D"] (% "d"))
                         (assoc-in ["d" "E"] (% "d"))
                         (assoc-in ["e" "D"] (% "d"))
                         (assoc-in ["e" "E"] (% "d")))))
    (testing "Move into itself"
      (is (thrown? clojure.lang.ExceptionInfo
                   (mv! *store* ["d"] ["d" "D" "d'"] true false)))
      (mv! *store* ["d"] ["e" "D" "d'"] true false)
      (update-root! #(-> % (assoc-in ["e" "D" "d'"] (% "d")) (dissoc "d"))))))
