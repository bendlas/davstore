(ns davstore.type-check-test
  (:require [clojure.core.typed :refer [check-ns]]
            [clojure.test :refer :all]))

(defn check [ns]
  (is (= :ok (check-ns ns)) (str "Typecheck for " ns)))

(deftest main-typecheck
  (check 'davstore.store))

