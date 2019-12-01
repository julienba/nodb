(ns nodb.transformation
  (:require clojure.set
            [java-time :as t]
            [nodb.analyze :as analyze]
            [nodb.date :as date]
            [tesser.core :as tesser]
            [tesser.math :as tesser.math]))

(defn- prefix-keyword [prefix k]
  (keyword (name prefix) (name k)))

(defn leftJoin
  "We are assuming that ds1 and ds2 are already sorted and contains the same pivot-key"
  [pivot-key ds1 ds2]
  (assert (get (first (:data ds1)) pivot-key) "Pivot field is missing")
  (assert (get (first (:data ds2)) pivot-key) "Pivot field is missing")
  (let [id1 (:id ds1)
        id2 (:id ds2)
        _ (assert (not= id1 id2))
        meta  (concat
               (remove (fn [{:keys [field]}] (= field pivot-key)) (:meta ds1))
               (:meta ds2))
        data (map (fn [a b]
                    (merge {pivot-key (get a pivot-key)}
                           (into {} (map (fn [[k v]] {(prefix-keyword id1 k) v}) (dissoc a pivot-key)))
                           (into {} (map (fn [[k v]] {(prefix-keyword id2 k) v}) (dissoc b pivot-key)))))
               (:data ds1)
               (:data ds2))]
    {:id (keyword (str (name id1) "+" (name id2)))
     :meta meta
     :data data}))

(defn- select-data [data filter-map]
  (filter
   #(if (= (:scope filter-map) :global)
      ((:fn filter-map) %)
      ((:fn filter-map) (get % (second (:scope filter-map)))))
   data))

(defn select
  [{:keys [id data] :as ds} filters opts]
  (let [new-data (reduce
                  (fn [acc e] (select-data acc e))
                  data
                  filters)]
   {:id id
    :meta (analyze/analyze new-data true opts)
    :data new-data}))

(defn- merge-fields
  [f maps target-fields]
  (->> maps
       (map #(select-keys % target-fields))
       (apply merge-with f)))

; (merge-fields
;  +
;  [{:a 1 :b 1}
;   {:a 2 :b 2}]
;  [:a])

(defn aggregate-by-date*
  [ds merge-fn {:keys [date-field target-fields]} {:keys [by step] :as interval}]
  (let [data         (:data ds)
        first-date   (-> data first date-field)
        last-date    (-> data last date-field)
        target-dates (date/all-interval-date
                      first-date
                      last-date
                      by
                      step)]
    (loop [data (map #(select-keys % (conj target-fields date-field)) data)
           [start end & other] target-dates
           results []]
      (if (nil? end)
        (conj results (merge
                       {date-field start}
                       (merge-fields merge-fn data target-fields)))
        (let [part (take-while #(t/before? (date-field %) end) data)]
          (recur
            (drop (count part) data)
            (cons end other)
            (conj results (merge
                           {date-field start}
                           (merge-fields merge-fn part target-fields)))))))))

(defn aggregate-by-date
  [{:keys [id] :as ds} merge-fn field-map interval-map opts]
  (let [new-data (aggregate-by-date* ds merge-fn field-map interval-map)]
    {:id id
     :meta (analyze/analyze new-data true opts)
     :data new-data}))

(defn correlate
  "Corrolate every long fields in a dataset"
  [{:keys [meta data] :as ds}]
  (let [fields (->> meta
                    (filter (fn [{:keys [type]}] (= type :long)))
                   (map :field))
        fields-map (into {} (map (fn [x] {x x}) fields))]
    fields-map
    (->> (tesser/fuse {:corrs (tesser.math/correlation-matrix fields-map)})
         (tesser/tesser (partition-all 100 data)))))
