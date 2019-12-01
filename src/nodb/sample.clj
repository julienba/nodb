(ns nodb.sample
  "Show usage of the library"
  (:require [nodb.core :as core]))

(def config
  {:date-format "yyyy-MM-dd"
   :date-round-by :days
   :aggregation/enum-size 10})

(def data-path "/home/jba/Documents/datasets/Data/Stocks/fb.us.txt")


(comment
 (core/ingest-csv
  "/home/jba/Documents/datasets/Data/Stocks/fb.us.txt"
  "/tmp/test-fb.edn"
  config)

 (core/ingest-csv
  "/home/jba/Documents/datasets/Data/Stocks/goog.us.txt"
  "/tmp/test-goog.edn"
  config))
