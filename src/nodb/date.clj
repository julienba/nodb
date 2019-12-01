(ns nodb.date
  (:require [java-time :as t]))

(defn round-date
  "By can by days, hours, minutes"
  [date by]
  (assert (#{:days :hours :minutes :seconds} by) "Cannot round date")
  (t/truncate-to date by))

(defn all-interval-date [start-date end-date by step]
  (assert (t/before? start-date end-date) "Start date should be before end")
  (let [start-date (round-date start-date by)
        end-date   (round-date end-date by)
        by-fn (condp = by
                :days     t/days
                :hours    t/hours
                :minutes  t/minutes
                :seconds  t/seconds)]
    (loop [result [start-date]]
      (let [l (last result)
            d (t/plus l (by-fn step))]
        (if (t/before? d end-date)
          (recur
            (conj result d))
          result)))))

; (all-interval-date
;  (t/local-date-time)
;  (t/plus (t/local-date-time) (t/days 4))
;  :days
;  2)


(defn generate-week-days [start-date end-date]
  (let [days (all-interval-date start-date end-date :days)
        western-week-days #{"MONDAY" "TUESDAY" "WEDNESDAY" "THURSDAY" "FRIDAY"}]
    (filter (fn [d] (western-week-days (str (t/day-of-week d)))) days)))


; (generate-week-days
;  (t/local-date-time)
;  (t/plus (t/local-date-time) (t/days 10)))


;(t/as-map (t/local-date-time))
