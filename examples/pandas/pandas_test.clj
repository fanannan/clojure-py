(ns pandas_test
    (:require [numpy :as np] ; use :require instead of :import
;        [numpy.random :as random]
;        [scipy.stats :as stats]
        [datetime]
;        [pandas :as pd]
        [pandas.io.data :as web]
;        [pandas.tools.plotting :as plotting]
;        [matplotlib :as mpl]
        [matplotlib.pyplot :as plt]
 ))


(defn chart [ticker source start]
    (let [historical-data (web/DataReader ticker source start)
           close-prices (get historical-data "Adj Close")]
           (.plot close-prices)
           (plt/show)))

(chart "^GSPC" "yahoo" (datetime/datetime. 1990 1 1))

