(ns nodb.io.edn
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [java-time :as time]
            nodb.io.protocol)
  (:import java.time.LocalDateTime
           nodb.io.protocol.IO))

(defmethod print-method java.time.LocalDateTime
  [dt out]
  (.write out (str "#time/inst \"" (.toString dt) "\"")))

(defmethod print-dup java.time.LocalDateTime
  [dt out]
  (.write out (str "#time/inst \"" (.toString dt) "\"")))

;(.toString (time/local-date-time))

(defn write-edn
  [path type-maps entries opts]
  (doseq [e entries]
    (spit path (str (pr-str e) "\n") :append true))
  (doseq [t type-maps]
    (spit (str path ".meta.edn") (str (pr-str t) "\n") :append true))
  (spit (str path ".config.edn") (pr-str opts)))

(comment
  (write-edn
   "/tmp/test.edn"
   [{:field :a, :types {:long 2}, :meta {:enum? false}, :type :long}]
   [{:a 2} {:a 1}]
   {:aggregation/enum-size 10}))

(defn str->date [s]
  (time/local-date-time s))

(defn read-edn [path]
  (with-open [rdr (clojure.java.io/reader path)]
             (doall (map #(edn/read-string {:readers {'time/inst str->date}} %)
                         (line-seq rdr)))))

(defn read-edn-data [path]
  (read-edn path))

;(read-edn "/tmp/test.edn")

(defn read-edn-meta [path]
  (read-edn (str path ".meta.edn")))

;(read-edn-meta "/tmp/test-fb.edn")

(defn read-edn-config [path]
  (read-edn-data (str path ".config.edn")))

(defrecord EDN []
  IO
  (store [_ path type-maps entries opts]
         (write-edn path type-maps entries opts))
  (get-meta [_ path] (read-edn-meta path))
  (get-data [_ path] (read-edn-data path))
  (get-config [_ path] (read-edn-config path))
  (get-all [_ path] {:data (read-edn-data path)
                     :meta (read-edn-meta path)
                     :config (read-edn-config path)}))

(comment
 (.store (->EDN)
         "/tmp/test.edn"
         [{:field :a, :types {:long 2}, :meta {:enum? false}, :type :long}
          {:field :b :types {:date 2} :meta {:enum? false} :type :long}]

         [{:a 2 :b (time/local-date-time)} {:a 1 :b (time/local-date-time)}]
         {:aggregation/enum-size 10})

 (.get-all (->EDN) "/tmp/test.edn")
 (.get-meta (->EDN) "/tmp/test-fb.edn"))
