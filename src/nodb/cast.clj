(ns nodb.cast
  (:require [java-time :as time]
            [nodb.date :as date]))

(defn str->num [x]
  (try
    (read-string x)
    (catch Exception e nil)))

(defn str->boolean [x]
  (Boolean/valueOf x))

(defn str->date
  "From string to date. Date are round by :days :hours :minutes :seconds"
  [x date-format round-by]
  (-> (time/local-date date-format x)
      time/local-date-time
      (date/round-date round-by)))

(defn guess-type
  "Transform a string into a more specific type if possible"
  [x {:keys [date-format date-round-by] :as opts}]
  (try
    (or
     (try (str->date x date-format date-round-by) (catch Exception e nil))
     (str->num x)
     ;(str->boolean x)
     (if (= java.lang.String (type x)) x nil)
     nil)
    (catch Exception e :nil)))

(defn type->avro-types
  [t]
  (condp = t
    java.lang.Long :long
    java.lang.Double :long
    java.lang.String :string
    java.lang.Boolean :boolean
    java.time.LocalDateTime :date
    nil))

(defn guess
  "Transform a string into a more specific type if possible. Return a map of :value and :type"
  [str-value opts]
  (let [v (guess-type str-value opts)]
    {:value v :type (type->avro-types (type v))}))

(defn casts-fns
  "Return functions for casting string"
  [{:keys [date-format date-round-by] :as opts}]
  (assert date-format)
  [{:type :long :fn str->num}
   {:type :date :fn #(str->date % date-format date-round-by)}
   {:type :bool :fn str->boolean}
   {:type :str  :fn identity}])

(defn- analyze-row
  [row opts]
  (map (fn [[k v]] (assoc (guess v opts) :field k)) row))

(defn analyze
  "Take a list of maps and aggregate it by fields.
Return something like {:field_name {:values [1 ] :types [:long]}"
  [rows typed? opts]
  (reduce
   (fn [acc row]
     (reduce
      (fn [acc2 {:keys [field type value]}]
        (-> acc2
            (update-in [field :values] #(conj % value))
            (update-in [field :types] #(conj % type))))
      acc
      (if typed?
        (map (fn [[k v]] {:value v :type (type->avro-types (type v)) :field k})
             row)
        (analyze-row row opts))))
   {}
   rows))

(defn type-summary
  "Analyze a list of maps"
  [rows typed? opts]
  (let [result (analyze rows typed? opts)]
    (reduce
     (fn [acc [field {:keys [types values]}]]
       (conj acc {:field field :types (frequencies types) :values (frequencies values)}))
     []
     result)))
