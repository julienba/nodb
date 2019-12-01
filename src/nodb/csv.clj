(ns nodb.csv
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]))

(defn file-size [path]
  (.length (io/file path)))

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data) ;; First row is the header
            (map keyword) ;; Drop if you want string keys instead
            repeat)
    (rest csv-data)))

(defn- read*
  "NOTE it's an eager evaluation, so you can load only what your memory allow.
In future this need to be lazy and map with the data-processing."
  [path opts]
  (with-open [reader (io/reader path)]
    (doall
      (csv/read-csv reader))))

(defn read->maps
  [path opts]
  (csv-data->maps (read* path opts)))


;(take 2 (read->maps "/home/jba/Documents/datasets/Data/Stocks/fb.us.txt" {}))
