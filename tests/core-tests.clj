(ns tests.core-tests
    (:require [tests.assertions :as a])
    (:use [tests.utils :only [deftest]]))

(deftest try-tests
;  (a/assert-true (= nil (try)))

;  (a/assert-true (= 20 (try (py.bytecode/BINARY_ADD 10 10 ))))

  (a/assert-true (= 20
    (try (py.bytecode/BINARY_ADD 10 10)
      (finally (py/print "finally")))))

  (a/assert-true (= 20
          (try
            (py.bytecode/BINARY_ADD 10 10)
            (catch IllegalStateException e "py/print exception"))))

  (a/assert-true (= 20
    (try
      (py.bytecode/BINARY_ADD 10 10)
            (catch IllegalArgumentException iae "py/print illegalargument")
            (catch IllegalStateException e "py/print exception"))))

  (a/assert-true (= 20
    (try
      (py.bytecode/BINARY_ADD 10 10)
            (catch IllegalArgumentException iae "py/print illegalargument")
            (catch IllegalStateException e "py/print exception")
            (finally 3))))

  (a/assert-false (= 1
                     (try
                       (py.bytecode/BINARY_ADD 10 10)
                       (catch py/Exception e e)
                       (finally 1))))

  (a/assert-equal 20
      (try (py.bytecode/BINARY_ADD 10 10)
          (finally (py/print "finally")))))

(deftest true?-and-false?-tests
  (a/assert-true (true? true))
  (a/assert-false (true? false))
  (a/assert-true (false? false))
  (a/assert-false (false? true)))

(deftest if-not-tests
    (a/assert-true (if-not false true))
    (a/assert-false (if-not true true false))
    (a/assert-true (if-not true false true)))

(deftest and-tests
    (a/assert-true (and true true true))
    (a/assert-false (and true false true))
    (a/assert-false (and nil true)))

(deftest identical?-tests
    (a/assert-true (identical? 1 1))
    (a/assert-false (identical? 1 2))
    (a/assert-false (identical? '() '()))
    (let [tmp '()]
         (a/assert-true (identical? tmp tmp))))

(deftest compare-tests
    (a/assert-lesser (compare 0 1) 0)
    (a/assert-lesser (compare -1 0) 0))

(deftest or-tests
    (a/assert-true (or true false false))
    (a/assert-false (or false false false))
    (a/assert-true (or false true false)))

(deftest zero?-tests
    (a/assert-true (zero? 0))
    (a/assert-false (zero? 1)))

(deftest count-tests
    (a/assert-equal (count '()) 0)
    (a/assert-equal (count '(1)) 1))

(deftest int-tests
    (a/assert-equal (int "1") 1)
    (a/assert-equal (int "-1") -1))

(deftest <-tests
    (a/assert-true (< 1))
    (a/assert-true (< 1 2))
    (a/assert-true (< 1 2 3))
    (a/assert-true (< 1 2 3 4))
    (a/assert-false (< 1 3 2 4)))

(deftest reduce1-tests
    (a/assert-equal (reduce1 (fn [x y] (py.bytecode/BINARY_ADD x y))
                            '(1 2 3 4)) 10)
    (a/assert-equal (reduce1 (fn [x y] (py.bytecode/BINARY_ADD x y))
                            5 '(1 2 3 4)) 15))
(deftest reverse-tests
    (a/assert-equal (reverse [1 2 3 4 5]) [5 4 3 2 1]))

(deftest >1?-tests
    (a/assert-true (>1? 3))
    (a/assert-false (>1? 0)))

(deftest >0?-tests
    (a/assert-true (>0? 2))
    (a/assert-false (>0? -1)))


(deftest +-tests
    (a/assert-equal (+) 0)
    (a/assert-equal (+ 1) 1)
    (a/assert-equal (+ 1 2) 3)
    (a/assert-equal (+ 1 2 3 4) 10))

(deftest *-tests
    (a/assert-equal (*) 1)
    (a/assert-equal (* 1) 1)
    (a/assert-equal (* 1 2) 2)
    (a/assert-equal (* 1 2 3 4) 24))

(deftest /-tests
    (a/assert-equal (/ 1) 1)
    (a/assert-equal (/ 2 2) 1)
    (a/assert-equal (/ 20 5 2) 2))

(deftest /-tests
    (a/assert-equal (- 1) -1)
    (a/assert-equal (- 2 2) 0)
    (a/assert-equal (- 20 5 2) 13))

(deftest <=-tests
    (a/assert-true (<= 1))
    (a/assert-true (<= 1 2))
    (a/assert-true (<= 1 1 3 4))
    (a/assert-false (<= 2 1 1 1)))

(deftest >-tests
    (a/assert-true (> 4))
    (a/assert-true (> 4 3))
    (a/assert-true (> 4 3 2 1))
    (a/assert-false (> 4 3 2 2)))

(deftest >=-tests
    (a/assert-true (>= 4))
    (a/assert-true (>= 4 3))
    (a/assert-true (>= 4 3 3))
    (a/assert-false (>= 3 4 2 1)))

(deftest ==-tests
    (a/assert-true (== 1))
    (a/assert-true (== 1 1))
    (a/assert-true (== 1 1 1 1 1))
    (a/assert-false (== 1 2 1 1 1)))

(deftest max-tests
    (a/assert-equal (max 1) 1)
    (a/assert-equal (max 1 2) 2)
    (a/assert-equal (max 3 2 1) 3))

(deftest min-tests
    (a/assert-equal (min 1) 1)
    (a/assert-equal (min 1 2) 1)
    (a/assert-equal (min 3 2 1) 1))

(deftest pos?-tests
    (a/assert-true (pos? 1))
    (a/assert-false (pos? -1))
    (a/assert-false (pos? 0)))

(deftest neg?-tests
    (a/assert-true (neg? -1))
    (a/assert-false (neg? 1))
    (a/assert-false (neg? 0)))

(deftest quot-tests
    (a/assert-equal (quot 23 7) 3)
    (a/assert-equal (quot 4 2) 2)
    (a/assert-equal (quot 3 2) 1)
    (a/assert-equal (quot 6 4) 1)
    (a/assert-equal (quot 0 5) 0)

    ;(a/assert-equal (quot 2 1/2) 4)
    ;(a/assert-equal (quot 2/3 1/2) 1)
    ;(a/assert-equal (quot 1 2/3) 1)

    (a/assert-equal (quot 4.0 2.0) 2.0)
    (a/assert-equal (quot 4.5 2.0) 2.0)
    ; |num| > |div|, num != k * div
    (a/assert-equal (quot 42 5) 8)     ; (8 * 5) + 2 == 42
    (a/assert-equal (quot 42 -5) -8)   ; (-8 * -5) + 2 == 42
    (a/assert-equal (quot -42 5) -8)   ; (-8 * 5) + -2 == -42
    (a/assert-equal (quot -42 -5) 8)   ; (8 * -5) + -2 == -42
    ; |num| > |div|, num = k * div
    (a/assert-equal (quot 9 3) 3)
    (a/assert-equal (quot 9 -3) -3)
    (a/assert-equal (quot -9 3) -3)
    (a/assert-equal (quot -9 -3) 3)
    ; |num| < |div|
    (a/assert-equal (quot 2 5) 0)
    (a/assert-equal (quot 2 -5) 0)
    (a/assert-equal (quot -2 5) 0)
    (a/assert-equal (quot -2 -5) 0)

    ; num = 0, div != 0
    (a/assert-equal (quot 0 3) 0)
    (a/assert-equal (quot 0 -3) 0)
    )


(deftest rem-tests
    (a/assert-equal (rem 23 7) 2)
    (a/assert-equal (rem 4 2) 0)
    (a/assert-equal (rem 3 2) 1)
    (a/assert-equal (rem 6 4) 2)
    (a/assert-equal (rem 0 5) 0)

    ;(a/assert-equal (rem 2 1/2) 0)
    ;(a/assert-equal (rem 2/3 1/2) 1/6)
    ;(a/assert-equal (rem 1 2/3) 1/3)

    (a/assert-equal (rem 4.0 2.0) 0.0)
    (a/assert-equal (rem 4.5 2.0) 0.5)

    ; |num| > |div|, num != k * div
    (a/assert-equal (rem 42 5) 2)      ; (8 * 5) + 2 == 42
    (a/assert-equal (rem 42 -5) 2)     ; (-8 * -5) + 2 == 42
    (a/assert-equal (rem -42 5) -2)    ; (-8 * 5) + -2 == -42
    (a/assert-equal (rem -42 -5) -2)   ; (8 * -5) + -2 == -42

    ; |num| > |div|, num = k * div
    (a/assert-equal (rem 9 3) 0)
    (a/assert-equal (rem 9 -3) 0)
    (a/assert-equal (rem -9 3) 0)
    (a/assert-equal (rem -9 -3) 0)

    )

(deftest mod-tests
    (a/assert-equal (rem 23 7) 2)
    (a/assert-equal (mod 4 2) 0)
    (a/assert-equal (mod 3 2) 1)
    (a/assert-equal (mod 6 4) 2)
    (a/assert-equal (mod 0 5) 0)

    ;(a/assert-equal (mod 2 1/2) 0)
    ;(a/assert-equal (mod 2/3 1/2) 1/6)
    ;(a/assert-equal (mod 1 2/3) 1/3)

    (a/assert-equal (mod 4.0 2.0) 0.0)
    (a/assert-equal (mod 4.5 2.0) 0.5)

    ; |num| > |div|, num != k * div
    (a/assert-equal (mod 42 5) 2)      ; (42 / 5) * 5 + (42 mod 5)        = 8 * 5 + 2        = 42
    (a/assert-equal (mod 42 -5) -3)    ; (42 / -5) * (-5) + (42 mod -5)   = -9 * (-5) + (-3) = 42
    (a/assert-equal (mod -42 5) 3)     ; (-42 / 5) * 5 + (-42 mod 5)      = -9 * 5 + 3       = -42
    (a/assert-equal (mod -42 -5) -2)  ; (-42 / -5) * (-5) + (-42 mod -5) = 8 * (-5) + (-2)  = -42

    ; |num| > |div|, num = k * div
    (a/assert-equal (mod 9 3) 0)      ; (9 / 3) * 3 + (9 mod 3) = 3 * 3 + 0 = 9
    (a/assert-equal (mod 9 -3) 0)
    (a/assert-equal (mod -9 3) 0)
    (a/assert-equal (mod -9 -3) 0)

    ; |num| < |div|
    (a/assert-equal (mod 2 5) 2)       ; (2 / 5) * 5 + (2 mod 5)        = 0 * 5 + 2          = 2
    (a/assert-equal (mod 2 -5) -3)     ; (2 / -5) * (-5) + (2 mod -5)   = (-1) * (-5) + (-3) = 2
    (a/assert-equal (mod -2 5) 3)      ; (-2 / 5) * 5 + (-2 mod 5)      = (-1) * 5 + 3       = -2
    (a/assert-equal (mod -2 -5) -2)    ; (-2 / -5) * (-5) + (-2 mod -5) = 0 * (-5) + (-2)    = -2

    ; num = 0, div != 0
    (a/assert-equal (mod 0 3) 0)       ; (0 / 3) * 3 + (0 mod 3) = 0 * 3 + 0 = 0
    (a/assert-equal (mod 0 -3) 0)

    ; large args
    (a/assert-equal (mod 3216478362187432 432143214) 120355456)
)


(deftest bit-not-tests
    (a/assert-equal (bit-not 5) -6))

(deftest bit-and-tests
    (a/assert-equal (bit-and 5 4) 4)
    (a/assert-equal (bit-and 5 4 1) 0))

(deftest bit-or-tests
    (a/assert-equal (bit-or 6 5 4 2) 7))

(deftest bit-xor-tests
    (a/assert-equal (bit-xor 2 3 4) 5))

(deftest bit-and-not-tests
    (a/assert-equal (bit-and-not 3 1 2) 0))

(deftest bit-shift-left-tests
    (a/assert-equal (bit-shift-left 1 3) 8))

(deftest bit-shift-right-tests
    (a/assert-equal (bit-shift-right 8 3) 1))

(deftest bit-clear-tests
    (a/assert-equal (bit-clear 3 1) 1))

(deftest bit-set-tests
    (a/assert-equal (bit-set 0 1) 2))

(deftest bit-flip-tests
    (a/assert-equal (bit-flip 0 1) 2)
    (a/assert-equal (bit-flip 2 1) 0))

(deftest bit-flip-tests
    (a/assert-true (bit-test 3 1))
    (a/assert-false (bit-test 1 1)))

(deftest integer?-tests
    (a/assert-true (integer? 1))
    (a/assert-false (integer? "1")))

(deftest even?-tests
    (a/assert-true (even? 2))
    (a/assert-false (even? 1)))

(deftest odd?-tests
    (a/assert-true (odd? 1))
    (a/assert-false (odd? 2)))

(deftest complement-tests
    (a/assert-true ((complement (fn [] false))))
    (a/assert-true ((complement (fn [x] false)) 1))
    (a/assert-true ((complement (fn [x y] false)) 1 2))
    (a/assert-true ((complement (fn [x y z] false)) 1 2 3)))

(deftest constantly-tests
    (a/assert-equal ((constantly 1) 1 2 3 4 5) 1))

(deftest identityi-tests
    (a/assert-equal (identity 3) 3)
    (a/assert-equal (identity 4) 4))

(deftest peek-tests
    (a/assert-equal (peek '(1 2)) 1)
    (a/assert-equal (peek nil) nil))

(deftest pop-tests
    (a/assert-equal (pop '(1 2)) '(2))
    (a/assert-equal (pop nil) nil))

;;map stuff

(deftest contains?-tests
    (a/assert-true (contains? [4 4 4 4] 3))
    (a/assert-true (contains? {:a 1 :b 2} :a))
    (a/assert-false (contains? [1 1 1] 4))
    (a/assert-false (contains? {:a 4} :b)))

(deftest conj-tests
    (a/assert-equal (conj [1] 2) [1 2])
    (a/assert-equal (conj '(1) 2) '(2 1))
    (a/assert-equal (conj nil 1) '(1)))

(deftest get-tests
    (a/assert-equal (get {:a 1} :a) 1)
    (a/assert-equal (get "abc" 1) "b"))

(deftest dissoc-tests
    (a/assert-equal (dissoc {:a 1 :b 2} :b) {:a 1})
    (a/assert-equal (dissoc {:a 1 :b 2} :a :b) {}))

(deftest disj-tests
    (a/assert-equal (disj #{:a :b :c} :a) #{:b :c}))

(deftest set-tests
    (a/assert-equal #{} (set []))
    (a/assert-equal #{"foo"} (set ["foo"]))
    (a/assert-equal #{1 2 3} #{1 3 2})
    ; FIXME vector/map find (a/assert-equal #{#{1 2 3} [4 5 6] {7 8} 9 10} #{10 9 [4 5 6] {7 8} #{1 2 3}})
    (a/assert-equal #{#{1 2 3} 9 10} #{10 9 #{1 2 3}})
    ;(a/assert-not-equal #{nil [] {} 0 #{}} #{})
    ;(a/assert-equal (count #{nil [] {} 0 #{}}) 5)
    (a/assert-equal (conj #{1} 1) #{1})
    (a/assert-equal (conj #{1} 2) #{2 1})
    (a/assert-equal (reduce + #{1 2 3 4 5}) 15)
    (a/assert-equal 4 (get #{1 2 3 4} 4))
    (a/assert-true (contains? #{1 2 3 4} 4))
    ;(a/assert-true (contains? #{[] nil 0 {} #{}} {}))
    (a/assert-true (contains? #{[1 2 3]} [1 2 3]))
    ; FIXME (a/assert-false (= [] {}))
    (a/assert-false (= () #{}))
    (a/assert-equal () [])
    (a/assert-equal [] ())
    (a/assert-equal #{1 2 3} #{1 2 3})
    (a/assert-equal #{#{1 2 3}} #{#{1 2 3}})
    (a/assert-equal #{[4 5 6]} #{[4 5 6]}))

(deftest find-tests
    (a/assert-equal (.getKey (find {:a 1} :a)) :a)
    (a/assert-equal (.getValue (find {:a 1} :a)) 1))

(deftest select-keys-tests
    (a/assert-equal (select-keys {:a 1 :b 2 :c 3} [:a]) {:a 1}))

(deftest keys-tests
    (a/assert-equal (keys {:a 1}) [:a]))

(deftest vals-tests
    (a/assert-equal (vals {:a 1}) [1]))

(deftest key-tests
    (a/assert-equal (key (find {:a 1 :b 2} :b)) :b))

(deftest val-tests
    (a/assert-equal (val (find {:a 1 :b 2} :b)) 2))

(deftest name-tests
    (a/assert-equal (name 'Foo) "Foo")
    (a/assert-equal (name "Foo") "Foo"))

(deftest namespace-tests
    (a/assert-equal (namespace 'baz/Foo) "baz")
    (a/assert-equal (namespace 'Foo) nil))

; broken need to fix
;(deftest dot-dot-tests
;    (a/assert-equal (.. :foo (.-sym) (.-name)) ":foo"))

(deftest ->-tests
    (a/assert-equal (-> " baz " (.rstrip) (.lstrip)) "baz"))

;(deftest ->>-tests ; haven't a clue how to test this
;    )


;;; var stuff


;;; if-let and when-let tests are from
;;; http://blog.jayfields.com/2011/03/clojure-if-let-and-when-let.html

(deftest if-let-tests
        (a/assert-equal (if-let [a 4] (+ a 4) (+ 10 10)) 8)
        (a/assert-equal (if-let [a nil] (+ a 4) (+ 10 10)) 20))


(deftest when-let-tests
    (a/assert-equal (when-let [a 9] (+ a 4))  13)
    (a/assert-equal (when-let [a nil] (+ a 4)) nil))

;;; functional stuff

(deftest comp-tests
    (a/assert-equal ((comp str +) 8 8 8) "24"))

(deftest juxt-tests
    (a/assert-equal ((juxt :a :b :c :d) {:a 1 :b 2 :c 3 :d 4}) [1 2 3 4]))

(deftest partial-tests
    (a/assert-equal ((partial + 1) 1) 2))

;;; sequence stuff

(deftest sequence-tests
    (a/assert-equal (sequence [1 2 3]) '(1 2 3)))

(deftest every?-tests
    (a/assert-true (every? even? '(2 4 6)))
    (a/assert-false (every? even? '(1 4 6))))

(deftest every?-tests
    (a/assert-false (not-every? even? '(2 4 6)))
    (a/assert-true (not-every? even? '(1 4 6))))

(deftest some-tests
    (a/assert-true (some even? '(1 2 3 4)))
    (a/assert-equal (some even? '(1 3 5 7)) nil))

(deftest not-any?-tests
    (a/assert-true (not-any? odd? '(2 4 6)))
    (a/assert-false (not-any? odd? '(1 2 3))))

;(deftest dotimes-tests
;    (dotimes [n 5] (a/assert-true (and (>= n 0) (< n 5)))))

(deftest map-tests
    (a/assert-equal (map inc [1 2 3 4 5]) (seq [2 3 4 5 6])))

(deftest mapcat-tests
    (a/assert-equal (mapcat reverse [[3 2 1 0] [6 5 4] [9 8 7]]) [0 1 2 3 4 5 6 7 8 9]))

(deftest filter-tests
    (a/assert-equal (filter even? [1 2 3 4 5]) [2 4]))

(deftest remove-tests
    (a/assert-equal (remove even? [1 2 3 4 5]) [1 3 5]))

(deftest take-tests
    (a/assert-equal (take 2 [1 2 3 4]) [1 2]))

(deftest take-while-tests
    (a/assert-equal (take-while even? [2 2 1 1]) [2 2]))

(deftest drop-tests
    (a/assert-equal (drop 1 [1 2 3]) [2 3]))

(deftest drop-last-tests
    (a/assert-equal (drop-last 2 [1 2 3 4]) [1 2]))

(deftest take-last-tests
    (a/assert-equal (take-last 3 [1 2 3 4]) [2 3 4]))

(deftest drop-while-tests
    (a/assert-equal (drop-while even? [2 4 6 1 2 3]) [1 2 3]))

(deftest cycle-tests
    (a/assert-equal (take 6 (cycle [1 2 3])) [1 2 3 1 2 3]))

(deftest split-at-tests
    (a/assert-equal (split-at 3 [1 2 3 4 5]) [[1 2 3] [4 5]]))

(deftest split-with-tests
    (a/assert-equal (split-with odd? [1 1 1 1 2 2 2 2]) [[1 1 1 1] [2 2 2 2]]))

(deftest repeat-tests
    (a/assert-equal (repeat 3 1) [1 1 1]))

(deftest interate-tests
    (a/assert-equal (take 3 (iterate inc 0)) [0 1 2]))

(deftest range-tests
    (a/assert-equal (range 0 8 2) [0 2 4 6]))

(deftest merge-tests
    (a/assert-equal (merge {:a 1 :b 2} {:a 3 :c 4}) {:a 3 :b 2 :c 4}))

(deftest merge-with-tests
    (a/assert-equal (merge-with +
                   {:a 1  :b 2}
                   {:a 9  :b 98 :c 0})
                  {:c 0, :a 10, :b 100}))


(deftest zipmap-tests
    (a/assert-equal (zipmap [:a :b :c :d :e] [1 2 3 4 5])
                    {:e 5, :d 4, :c 3, :b 2, :a 1}))


(deftest sort-tests
    (a/assert-equal (sort [3 1 2 4]) [1 2 3 4])
    (a/assert-equal (sort > (vals {:foo 5, :bar 2, :baz 10})) [10 5 2]))

(deftest sort-by-tests
    (a/assert-equal (sort-by first > [[1 2] [2 2] [2 3]]) [[2 2] [2 3] [1 2]]))

(deftype Accum [i]
    ISeq  ; Bit of a hack until we get definterface implemented
    (inc [self] (py/setattr self "i" (inc i))))


(deftest dorun-tests
    (let [accum (Accum 0)]
         (dorun (map (fn [x] (.inc accum))
                     (range 10)))
         (a/assert-equal (.-i accum) 10)))

(deftest nthnext-tests
    (a/assert-equal (nthnext (range 10) 3) '(3 4 5 6 7 8 9)))

(deftest nth-tests
    (a/assert-equal (nth (list 1 2 3) 1) 2)
    (a/assert-equal (nth [1 2 3] 1) 2))

(deftest partition-tests
    (a/assert-equal (partition 4 (range 20))
                  '((0 1 2 3) (4 5 6 7) (8 9 10 11) (12 13 14 15) (16 17 18 19)))
    (a/assert-equal (partition 4 6 ["a" "b" "c" "d"] (range 20))
                  '((0 1 2 3) (6 7 8 9) (12 13 14 15) (18 19 "a" "b"))))

(deftest eval-tests
    (a/assert-equal (eval '(+ 1 2)) 3))

(deftest doseq-tests
    (doseq [x [1 2 3]
                          y [1 2 3]]
                         (py/print (* x y))))
;; prints
;;[1 2 3 2 4 6 3 6 9]

(deftest do-times
    (let [accum (Accum 0)]
         (dotimes [i 5]
             (a/assert-equal (.-i accum) i)
             (.inc accum))))

(deftest class-tests
    (a/assert-equal (class "f") py/str))

(deftest num-tests
    (a/assert-equal (num "1") 1))

(deftest num-tests
    (a/assert-true (float? (num "inf"))))

(deftest number?-tests
    (a/assert-true (number? 1)))

(deftest read-string-tests
    (a/assert-equal (read-string "12") 12))

(deftest subvec-tests
    (a/assert-equal (subvec [1 2 3 4] 1 3) [2 3]))

(deftest doto-tests
    (a/assert-equal (doto (py/list) (.append "foo") (.append "bar")) ["foo" "bar"]))

(deftest memfn-tests
    (a/assert-equal (let [f (memfn join ch)]
                       (f "," ["1" "2"]))
                  "1,2"))

(deftest find-ns-tests
    (a/assert-true (not (nil? (find-ns 'clojure.core)))))

(deftest create-ns-tests
    (a/assert-true (identical? (find-ns 'clojure.core) (create-ns 'clojure.core)))
    (a/assert-true (not (nil? (create-ns 'foo.bar)))))

(deftest ns-name-tests
    (a/assert-equal (ns-name 'clojure.core) 'clojure.core))

(deftest let-tests
    (let [[x & y] [1 2 3]]
         (a/assert-equal x 1)
         (a/assert-equal y [2 3])))

(deftest let-tests
    ((fn td [[x & y]]
         (a/assert-equal x 1)
         (a/assert-equal y [2 3])) [ 1 2 3]))

(deftest loop-tests
    (loop [[x & y] [1 2 3]]
         (a/assert-equal x 1)
         (a/assert-equal y [2 3])))

(deftest when-first-tests
    (a/assert-equal (when-first [a [1 2 3]] a) 1)
    (a/assert-equal (when-first [a []] a) nil)
    (a/assert-equal (when-first [a nil] a) nil))

(deftest lazy-cat-tests
    (a/assert-equal (lazy-cat [1 2 3] [4 5 6]) [1 2 3 4 5 6]))

(deftest for-tests
    (a/assert-equal (for [x [1 2 3]] x) [1 2 3]))

(deftest destructure-tests
    (a/assert-equal (map (fn [[k v]] k) {:1 1}) [:1])
    (a/assert-equal (map (fn [[k v]] v) {:1 1}) [1]))

(deftest map-entry-tests
    (a/assert-equal (-> {:1 :2} first first) :1)
    (a/assert-equal (-> {:1 :2} first second) :2))


(deftest reduce-tests
    (a/assert-equal (reduce + '(1 2 3 4)) 10)
    (a/assert-equal (reduce + 5 '(1 2 3 4)) 15))

(deftest empty?-tests
    (a/assert-true (empty? []))
    (a/assert-false (empty? [1])))



(deftest do-tests
    (a/assert-equal (do) nil)
    (a/assert-equal (do 1) 1)
    (a/assert-equal (do 1 2) 2))

(deftest lazy-seq-tests
    (.more (range 1))) ; would throw an error before fix to Issue #45

(deftest comment-tests
    (comment (a/assert-true false)))

(deftest vec-tests
    (a/assert-equal ((fn [& y] (vec y)) 1 2) [1 2]))

(deftest reify-tests
    (let [f (fn [y] (reify ISeq
                           (seq [self] self)
                           (first [self] y)))]
         (a/assert-equal (first (f 42)) 42)))

(deftest defprotocol-tests
    (defprotocol Foo "Foo Protocol"
        (foo [self] "Foo_foo")))

(deftest truthiness-tests
    (a/assert-true (if true true false))
    (a/assert-true (if 0 true false))
    (a/assert-true (if '() true false))
    (a/assert-true (if '(1) true false))
    (a/assert-true (if [] true false))
    (a/assert-true (if [1] true false))
    (a/assert-true (if {} true false))
    (a/assert-true (if {1 2} true false))
    (a/assert-true (if #{} true false))
    (a/assert-true (if #{1} true false))
    (a/assert-true (if "" true false))
    (a/assert-true (if 1 true false))
    (a/assert-true (if :spam true false))
    (a/assert-false (if false true false))
    (a/assert-false (if nil true false)))

(deftest defrecord-tests
    (defrecord FooRecord [x y] IDeref (deref [self] 42))
    (let [foo (FooRecord 1 2)]
         (a/assert-equal (:x foo) 1)
         (a/assert-equal (:y foo) 2)
         (a/assert-equal (get foo "x") 1)
         (a/assert-equal (get foo 'x) 1)
         (a/assert-equal (vec (keys foo)) ["x" "y"])
         (a/assert-equal (count foo) 2)
         (a/assert-equal (:x (.without foo "x")) nil)
         (a/assert-equal (deref foo) 42)
         (a/assert-equal (FooRecord 1 2) foo)
         (a/assert-not-equal (FooRecord 2 2) foo)
         (a/assert-equal (py/hash (FooRecord 1 2))
                         (py/hash foo))
         (a/assert-not-equal (py/hash (FooRecord 2 2))
                             (py/hash foo)))
    (let [bar (->FooRecord 1 2)]
         (a/assert-equal (:x bar) 1)
         (a/assert-equal (:y bar) 2)
         (a/assert-equal (get bar "x") 1)
         (a/assert-equal (get bar 'x) 1)
         (a/assert-equal (vec (keys bar)) ["x" "y"])
         (a/assert-equal (count bar) 2)
         (a/assert-equal (:x (.without bar "x")) nil)
         (a/assert-equal (deref bar) 42)
         (a/assert-equal (FooRecord 1 2) bar)
         (a/assert-not-equal (FooRecord 2 2) bar)
         (a/assert-equal (py/hash (FooRecord 1 2))
                         (py/hash bar))
         (a/assert-not-equal (py/hash (FooRecord 2 2))
                             (py/hash bar))))

(deftest extend-tests
    (extend py/int ISeq {:seq (fn [self] 42)})
    (a/assert-equal (seq 1) 42))

(deftest binding-tests
    (def x 0)
    (def y 0)
    (binding [x 1 y 2]
        (a/assert-equal (+ x y) 3))
    (a/assert-equal (+ x y) 0))

(deftest var-tests
    (a/assert-true (py/hasattr #'cons "deref")))

(deftest doc-tests
    (defn baz "This is a test" [] nil)
    (doc baz))

(definterface IClosable
        (__enter__ [self])
        (__exit__ [self]))

(deftype MutatableCloser [state]
        IClosable
        (__enter__ [self]
            (py/setattr self "state" :enter))
        (__exit__ [self]
            (py/setattr self "state" :exit)))

(deftest with-open-tests
    (let [mc (MutatableCloser :unknown)]
        (a/assert-equal (.-state mc) :unknown)
        (with-open [obj mc]
             (a/assert-equal (.-state obj) :enter))
        (a/assert-equal (.-state mc) :exit)))

(deftest generator-seq-tests
    (let [gen (fn [times]
                (dotimes [x times]
                    (py.bytecode/YIELD_VALUE x)))]
         (a/assert-equal (seq (gen 3)) [0 1 2])))

(deftest bases-tests
    (a/assert-equal (bases ISeq)
                             [IPersistentCollection]))

(deftest supers-tests
    (a/assert-equal (supers ISeq)
                             [IPersistentCollection Seqable py/object]))

(deftest not-empty-tests
    (a/assert-equal (not-empty []) nil)
    (a/assert-equal (not-empty [1]) [1]))

(def AlterVarInt 0)

(deftest alter-var-root-tests
        (a/assert-equal AlterVarInt 0)
        (alter-var-root #'AlterVarInt inc)
        (a/assert-equal AlterVarInt 1))

(def ^:dynamic unbound)
(def bound 1)

(deftest bound?-tests
        (a/assert-true (bound? #'bound))
        (a/assert-false (bound? #'bound #'unbound)))

(deftest thread-bound?-tests
        (a/assert-false (thread-bound? #'unbound))
        (binding [unbound 1]
                (a/assert-true (thread-bound? #'unbound))
                (a/assert-false (thread-bound? #'unbound #'bound))))

(deftest isa?-tests
        (a/assert-true (isa? ISeq Seqable))
        (a/assert-false (isa? Seqable ISeq)))

(defmulti factorial identity)

(defmethod factorial 0 [_]  1)
(defmethod factorial :default [num]
    (* num (factorial (dec num))))



(derive ::rect ::shape)

(defmulti bar (fn [x y] [x y]))
(defmethod bar [::rect ::shape] [x y] :rect-shape)
(defmethod bar [::shape ::rect] [x y] :shape-rect)
(defmethod bar [::circle ::foo] [x y] :circle-foo)

(prefer-method bar [::rect ::shape] [::shape ::rect])

(deftest mult-method-tests
    (a/assert-equal (factorial 0) 1)
    (a/assert-equal (factorial 1) 1)
    (a/assert-equal (factorial 3) 6)
    (a/assert-equal (factorial 7) 5040)
    (a/assert-equal (bar ::rect ::rect) :rect-shape)
    (a/assert-equal (bar ::shape ::rect) :shape-rect)
    (a/assert-equal (bar ::circle ::foo) :circle-foo))

(deftest extends?-tests
    (a/assert-true (extends? (type '()) ISeq))
    (a/assert-false (extends? (type 1) ISeq)))

(deftest satisfies?-tests
    (a/assert-true (satisfies? ISeq '()))
    (a/assert-false (satisfies? ISeq 1)))

(deftest letfn-tests
    (letfn [(twice [x]
                 (* x 2))
               (six-times [y]
                 (* (twice y) 3))]
         (a/assert-equal (twice 15) 30)
         (a/assert-equal (six-times 15) 90)))

(deftest atom-tests
  (let [x (atom {})]
    (a/assert-equal @x {})
    (a/assert-equal (reset! x []) [])
    (a/assert-equal (swap! x conj 1) [1])
    (a/assert-equal (swap! x conj 2 3 4 5 6) [1 2 3 4 5 6])))

(deftest replace-tests
    (let [smap {:a "z" :b "y"}]
        (= (apply hash-map (apply concat (replace {:a "z" :b "y"} {:one "one" :a "a"}))) {:one "one" :a "a"})
        (a/assert-equal (replace smap [:one "one" :a "a"]) [:one "one" "z" "a"])
        (a/assert-equal (replace smap [:one "one" :c "c"]) [:one "one" :c "c"])))

(deftest reductions-tests
    (a/assert-equal (reductions + [1 2 3 4 5]) [1 3 6 10 15])
    (a/assert-equal (reductions * 2 [1 2 3 4 5]) [2 2 4 12 48 240]))

(deftest try-body-tests
    (a/assert-equal (try) nil)
    (a/assert-equal (try :a) :a)
    (a/assert-equal (try :a :b :c) :c)
    (a/assert-equal (try #(:a) :b :c (finally :d)) :c)
    (a/assert-equal (try :a :b :c (finally :d)) :c)
    (a/assert-equal (try :a :b :c (finally :d)) :c)
    (a/assert-equal (try :a (catch Exception e :c)) :a)
    (a/assert-equal (try :a :b (catch Exception e :c)) :b)
    (a/assert-equal (try (throw Exception) (catch Exception e :b)) :b)
    (a/assert-equal (try :a (throw Exception) (catch Exception e :b)) :b)
    (a/assert-equal (try :a :b (finally :z)) :b)
    (a/assert-equal (try :a :b (catch Exception e :c) (finally :z)) :b)
    (a/assert-equal (try :a :b (throw Exception) (catch Exception e :c) (finally :z)) :c))

(deftest with-redefs-tests
    (a/assert-equal (class []) (type []))
    (a/assert-equal (with-redefs [class (constantly nil)]
                      (class [])) nil)
    (a/assert-equal (class []) (type []))) ; Make sure variables are restored properly by with-redefs

(deftest ArrayChunk-tests
    ; range creates an ArrayChunk and filter and map-indexed are using the .nth attribute
    ; throws before fix ... AttributeError: 'ArrayChunk' object has no attribute nth 
    (a/assert-equal (filter (fn [x] (zero? (mod x 3))) (range 10)) [0 3 6 9])
    (a/assert-equal (map-indexed (fn [idx x] [idx x]) (range 2)) [[0 0] [1 1]]))
