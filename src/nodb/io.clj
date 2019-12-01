(ns nodb.io
  (:require [clojure.string :as string]
            [nodb.csv :as csv]))

(defn write-edn
  [path type-maps entries opts]
  (doseq [e entries]
    (spit path (str (pr-str e) "\n") :append true))
  (doseq [t type-maps]
    (spit (str path ".meta.edn") (pr-str t) :append true))
  (spit (str path ".config.edn") (pr-str opts)))

; (write-edn
;  "/tmp/test.edn"
;  [{:field :a, :types {:long 2}, :meta {:enum? false}, :type :long}]
;  [{:a 2} {:a 1}]
;  {:aggregation/enum-size 10})

(defn read-edn-data [path]
  (map read-string (string/split-lines (slurp path))))

(defn read-edn-meta [path]
  (read-edn-data (str path ".meta.edn")))

(defn read-edn-config [path]
  (read-edn-data (str path ".config.edn")))


; (store
;   "/tmp/test.edn"
;   [{:field :a, :types {:long 2}, :meta {:enum? false}, :type :long}]
;   [{:a 2} {:a 1}]
;   {:aggregation/enum-size 10})
