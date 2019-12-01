(ns nodb.cast-test
  (:require [clojure.test :refer :all]
            [java-time :as time]
            [nodb.cast :refer :all]))

(deftest conversion
  (testing "Date"
    (is (= "2012-05-18T00:00"
           (str (str->date "2012-05-18" "yyyy-MM-dd" :days)))))
  (testing "Number"
    (is (= 42.25 (str->num "42.25")))))

(deftest guess-test
  (testing "Date"
    (is (= "2012-05-18T00:00"
           (str (guess-type "2012-05-18" {:date-format "yyyy-MM-dd" :date-round-by :days})))))
  (testing "Number"
    (is (= 1 (guess-type "1" {})))))

(deftest analyze-test
  (testing "Simple values"
    (is (= {:a {:values [1 1], :types [:long :long]},
            :b {:values [1 2], :types [:long :long]}}
           (analyze [{:a "1" :b "2"} {:a "1" :b "1"}] false {})
           (analyze [{:a 1 :b 2} {:a 1 :b 1}] true {})))))

(deftest summary-test
  (testing "Simple values"
    (is (= [{:field :a, :types {:long 2}, :values {1 2}}
            {:field :b, :types {:long 2}, :values {1 1, 2 1}}]
           (type-summary [{:a "1" :b "2"} {:a "1" :b "1"}] false {}))))
  (testing "Multi type"
    (is (= [{:field :Date, :types {:date 1}, :values {(time/local-date-time 2012 05 18) 1}}
            {:field :Open, :types {:long 1}, :values {42.05 1}}]
           (type-summary [{:Date "2012-05-18", :Open "42.05"}]
                         false
                         {:date-format "yyyy-MM-dd" :date-round-by :days})))))
