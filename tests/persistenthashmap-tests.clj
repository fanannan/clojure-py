(ns tests.persistenthashmap-tests
    (:use [clojure.lang.persistenthashmap :only [PersistentHashMap]])
    (:require [tests.assertions :as assertions])
    (:use [tests.utils :only [deftest]]))

(def testmap (-> (PersistentHashMap 0 nil false nil)
                 (.assoc "a" 1)
                 (.assoc "b" 2)))

(deftest equality-tests
    (assertions/assert-false (= {} []))
    (assertions/assert-false (= {} #{})))

(deftest assoc-tests
    (assertions/assert-true (.containsKey testmap "a")))

(deftest without-tests
    (assertions/assert-false (-> testmap
                                 (.without "a")
                                 (.containsKey "a"))))
(deftest entryAt-tests
    (assertions/assert-false (-> testmap
                                 (.without "a")
                                 (.entryAt "a"))))
