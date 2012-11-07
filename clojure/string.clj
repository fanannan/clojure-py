(ns clojure.string
  (:require [re]))

(defn reverse
  ; Returns s with its characters reversed.
  [s]
  (apply str (py/reversed s)))

(defn replace
  ; Replaces all instance of match with replacement in s.
  ; Match can be a regular expression or a string-like object.

  ; If match is a regular expression then replacement can be either a string or a function.
  ; If replacement is a function, it is called for every non-overlapping occurrence of pattern.

  ; See also replace-first.
  [s match replacement]
  (if (py/hasattr match "replace") (.replace s match replacement) (re/sub match replacement s)))

(defn replace-first
  ; Replaces the first instance of match with replacement in s.

  ; See also replace.
  [s match replacement]
  (if (py/hasattr match "replace") (.replace s match replacement 1) (re/sub match replacement s 1)))


(defn join
  ; Returns a string of all elements in coll, as returned by (seq coll),
  ; separated by an optional separator.
  ([coll]
     (apply str coll))
  ([separator coll]
     (.join separator coll)))

(defn capitalize [s]
  ; Capitalizes string.
  (.capitalize s))

(defn upper-case [s]
  ; Converts string to all upper-case.
  (.upper s))

(defn lower-case [s]
  ; Converts string to all lower-case.
  (.lower s))

(defn split
  ; Splits string on a regular expression.  Optional argument limit is
  ; the maximum number of splits.
  ([s regex]
     (seq (re/split regex s)))
  ([s regex limit]
     (seq (re/split regex s (dec limit)))))

(defn split-lines
  ; Splits s on \\n or \\r\\n.
  [s]
  (seq (.splitlines s)))

(defn trim [s] 
  ; Remove whitespace at the left and right of the string.
  (.strip s))

(defn triml [s]
  ; Remove whitespace at the left of the string
  (.lstrip s))

(defn trimr [s]
  ; Remove whitespace at the right of the string
  (.rstrip s))

(defn trim-newline
  ; Removes all trailing newline \\n or return \\r characters from
  ; string.  Similar to Perl's chomp.
  [s]
  (re/sub #"(?:\\n|\\r)+$" "" s))

(defn blank?
  ; True if s is nil, empty, or contains only whitespace.
  [s]
  (or (nil? s) (= "" (trim s))))

(defn escape
  ; Return a new string, using cmap to escape each character ch
  ; from s as follows:
   
  ; If (cmap ch) is nil, append ch to the new string.
  ; If (cmap ch) is non-nil, append (str (cmap ch)) instead.
  [s cmap]
  (apply str (map #(get cmap % %) s)))
