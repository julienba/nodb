(ns nodb.core
  (:require [java-time :as time]
            [nodb.analyze :as analyze]
            [nodb.csv :as csv]
            [nodb.cast :as cast]
            [nodb.io.edn :as edn]
            [nodb.transformation :as transformation]))

(defn- convert
  "Read source file and convert it to the target schema"
  [source-path schema opts]
  (let [source-data    (csv/read->maps source-path opts)
        schema-map     (into {} (map (fn [{:keys [field type]}] {field type}) schema))
        cast-fns       (into {} (map (fn [{:keys [type fn]}] {type fn}) (cast/casts-fns opts)))
        transform-data (reduce
                        (fn [acc e]
                          (conj acc
                                (reduce-kv
                                 (fn [m k v]
                                   (assoc m k ((get cast-fns (get schema-map k)) v)))
                                 {}
                                 e)))
                        []
                        source-data)]
    transform-data))

(defn ingest-csv
  "Read a CSV file and store it as EDN"
  [source-path dest-path opts]
  (let [source-data (csv/read->maps source-path opts)
        schema      (analyze/analyze source-data opts)
        dest-data   (convert source-path schema opts)]
    (.store (edn/->EDN)
            dest-path
            schema
            dest-data
            opts)))
