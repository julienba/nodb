(ns nodb.analyze-test
  (:require [clojure.test :refer :all]
            [nodb.analyze :refer :all]))

(deftest meta-analyze-test
  (testing "Long analyze"
    (is (= {:type :long, :values {1 2, 3 1}, :meta {:average 2.0, :min 1, :max 3}}
           (meta-analyze {:type :long :values (frequencies [1 1 3])})))))

(def cfg
  {:date-format "yyyy-MM-dd"
   :date-round-by :days
   :aggregation/enum-size 10})

(deftest analyze-type-test
  (testing "Long analyze"
    (is (= {:field :a,
            :types {:long 2},
            :meta {:enum? true, :average 1.5, :min 1, :max 2},
            :type :long}
           (analyze-type
            {:field :a, :types {:long 2}, :values {2 1, 1 1}}
            cfg)))))

(deftest meta-analyze-test
  (testing "Long analyze"
    (is (= [{:field :a,
             :types {:long 2},
             :meta {:enum? true, :average 1.5, :min 1, :max 2},
             :type :long}]
          (analyze [{:a "1"} {:a "2"}]
                   false
                   {:date-format "yyyy-MM-dd"
                    :date-round-by :days
                    :aggregation/enum-size 10})))))
