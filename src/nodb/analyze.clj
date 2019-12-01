(ns nodb.analyze
 (:require [nodb.cast :as cast]))

(defn mean [xs]
  (/ (reduce + xs) (count xs)))

; NOTE
;; multimethod is not really extensible or parametric using only configuration
(defmulti meta-analyze :type)

(defmethod meta-analyze :default [type-map] type-map)

(defmethod meta-analyze :long [{:keys [values] :as type-map}]
  (let [only-values (map first values)
        average (float (mean only-values))]
    (update type-map :meta #(merge % {:average average
                                      :min (apply min only-values)
                                      :max (apply max only-values)}))))

(defn analyze-type
  [{:keys [values types] :as type-map} {:keys [aggregation/enum-size] :as opts}]
  (assert enum-size)
  (let [nb-value (count values)
        enum? (< nb-value enum-size)
        type (if (count types) (ffirst types) (map first types))]
    (-> type-map
        (assoc-in [:meta :enum?] enum?)
        (assoc :type type)
        meta-analyze
        (dissoc :values))))

(defn analyze [rows typed? opts]
  (let [types (cast/type-summary rows typed? opts)]
    (map #(analyze-type % opts) types)))
