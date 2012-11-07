(ns tests.string-tests
    (:require [tests.assertions :as assertions])
    (:use [tests.utils :only [deftest]])
    (:require [clojure.string :as string]))

;; Please note that just the reference implementation, passing faulty arguments results in undefined behaviour
;; Therefore, no such test is present

(deftest reverse-tests
    (assertions/assert-equal (string/reverse "spam") "maps")
    (assertions/assert-equal (string/reverse "racecar") "racecar")
    (assertions/assert-equal (string/reverse "") ""))

(deftest replace-tests
    (assertions/assert-equal (string/replace "This parrot is dead." "dead" "resting") "This parrot is resting.")
    (assertions/assert-equal (string/replace "" "" "") "")
    (assertions/assert-equal (string/replace "This parrot is dead." #"dead" "resting") "This parrot is resting."))

(deftest replace-first-tests
    (assertions/assert-equal (string/replace-first "surprise, surprise, ruthless efficiency" "surprise" "fear") "fear, surprise, ruthless efficiency")
    (assertions/assert-equal (string/replace-first "" "" "") "")
    (assertions/assert-equal (string/replace-first "string1 string2 string3" #"string\d" "string") "string string2 string3"))

(deftest replace-first-tests
    (assertions/assert-equal (string/replace-first "surprise, surprise, ruthless efficiency" "surprise" "fear") "fear, surprise, ruthless efficiency")
    (assertions/assert-equal (string/replace-first "" "" "") "")
    (assertions/assert-equal (string/replace-first "string1 string2 string3" #"string\d" "string") "string string2 string3"))

(deftest join-tests
    (assertions/assert-equal (string/join (map str (range 10))) "0123456789")
    (assertions/assert-equal (string/join ", " (map str (range 10))) "0, 1, 2, 3, 4, 5, 6, 7, 8, 9"))

(deftest capitalize-tests
    (assertions/assert-equal (string/capitalize "clojure") "Clojure")
    (assertions/assert-equal (string/capitalize "pypy vm") "Pypy vm"))

(deftest upper-case-tests
    (assertions/assert-equal (string/upper-case "abc") "ABC"))

(deftest lower-case-tests
    (assertions/assert-equal (string/lower-case "ABC") "abc"))

(deftest split-tests
    (assertions/assert-equal (string/split "a b c\td e f" #"\s") ["a" "b" "c" "d" "e" "f"])
    (assertions/assert-equal (string/split "a,b,c,d,e,f" #"," 3) ["a" "b" "c,d,e,f"]))

(deftest split-lines
    (assertions/assert-equal
        (string/split-lines "He's not the Messiah.\nHe's a very naughty boy!") ["He's not the Messiah" "He's a very naughty boy!"]))

(deftest trim-tests
    (assertions/assert-equal (string/trim "  test  ") "test")
    (assertions/assert-equal (string/triml "  test  ") "test  ")
    (assertions/assert-equal (string/trimr "  test  ") "  test")
    (assertions/assert-equal (string/trim "  test  ") (string/triml (string/trimr"  test  "))))

(deftest trim-newline-tests
    (assertions/assert-equal (string/trim-newline "chomp\r\n") "chomp")
    (assertions/assert-equal (string/trim-newline "chomp") "chomp"))

(deftest blank?-tests
    (assertions/assert-true (string/blank? nil))
    (assertions/assert-true (string/blank? ""))
    (assertions/assert-true (string/blank? "   "))
    (assertions/assert-true (string/blank? "\n\t"))
    (assertions/assert-false (string/blank? "  .  ")))

(deftest escape-tests
    (assertions/assert-equal (string/escape "int a =  b + c;" {" " "&nbsp;", ";" "&amp;"}) "int&nbsp;a&nbsp;=&nbsp;&nbsp;b&nbsp;+&nbsp;c&amp;"))
