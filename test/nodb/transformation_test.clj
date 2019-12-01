(ns nodb.transformation-test
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [nodb.transformation :refer :all]))

(def alice-ds
  {:id :alice
   :meta [{:field :date :types {:long 2} :meta {:enum? false :average 1.5 :min 1 :max 2} :type :long}
          {:field :a :types {:long 2} :meta {:enum? false :average 2 :min 1 :max 4} :type :long}]
   :data [{:date 1 :a 1}
          {:date 2 :a 4}]})

(def bob-ds
  {:id :bob
   :meta [{:field :date :types {:long 2} :meta {:enum? false :average 1.5 :min 1 :max 2} :type :long}
          {:field :b :types {:long 2} :meta {:enum? false :average 3 :min 2 :max 4} :type :long}]
   :data [{:date 1 :b 2}
          {:date 2 :b 4}]})

(deftest join-test
  (testing "Left"
    (is (= {:id :alice+bob,
            :meta [{:field :a, :types {:long 2}, :meta {:enum? false, :average 2, :min 1, :max 4}, :type :long}
                   {:field :date, :types {:long 2}, :meta {:enum? false, :average 1.5, :min 1, :max 2}, :type :long}
                   {:field :b, :types {:long 2}, :meta {:enum? false, :average 3, :min 2, :max 4}, :type :long}],
            :data [{:date 1, :alice/a 1, :bob/b 2}
                   {:date 2, :alice/a 4, :bob/b 4}]}

           (leftJoin
            :date
            alice-ds
            bob-ds)))))

(def filter-global-sample
  {:scope :global :fn (fn [{:keys [a]}] (= a 4))})

(def filter-field-sample
  {:scope [:field :a] :fn (fn [a] (= a 1))})

(def DS
  {:data [{:a 1 :b 1} {:a 2 :b 1}]})

(def config
  {:date-format "yyyy-MM-dd"
   :date-round-by :days
   :aggregation/enum-size 10})

(deftest select-test
  (testing "Filter by field"
    (is (= {:id :alice,
            :meta [{:field :date, :types {:long 1}, :meta {:enum? true, :average 1.0, :min 1, :max 1}, :type :long}
                   {:field :a, :types {:long 1}, :meta {:enum? true, :average 1.0, :min 1, :max 1}, :type :long}]
            :data [{:date 1, :a 1}]}
           (select alice-ds [filter-field-sample] config)))
    (is (= {:id :alice
            :meta [{:field :date, :types {:long 1}, :meta {:enum? true, :average 2.0, :min 2, :max 2}, :type :long}
                   {:field :a, :types {:long 1}, :meta {:enum? true, :average 4.0, :min 4, :max 4}, :type :long}],
            :data [{:date 2, :a 4}]}
           (select alice-ds [filter-global-sample] config)))))

(defn- generate-data
  "Helper to build a dataset "
  [nb]
  (let [starting-date (t/local-date-time 2015 1 1)]
    (for [i (range nb)]
      {:date (t/plus starting-date (t/days i))
       :a i
       :aa (inc i)
       :b (+ i 10)})))

(deftest aggegate-by-date-test
  (testing "Simple case: group date by 2 consecutive days"
    (is (= [{:date (t/local-date-time 2015 1 1) :a (apply + [0 1])}
            {:date (t/local-date-time 2015 1 3) :a (apply + [2 3])}
            {:date (t/local-date-time 2015 1 5) :a (apply + [4 5])}
            {:date (t/local-date-time 2015 1 7) :a (apply + [6 7])}
            {:date (t/local-date-time 2015 1 9) :a (apply + [8 9])}])
        (aggregate-by-date
         {:id :test
          :meta [{:field :date, :types {:date 10}, :meta {}, :type :date}
                 {:field :a, :types {:long 10}, :meta {:enum? false, :average 5.0, :min 1, :max 10}, :type :long}]
          :data (generate-data 10)}
         +
         {:date-field :date
          :target-fields [:a]}
         {:by :days
          :step 2}
         config))))

(deftest aggegate-by-date-test
  (testing "Similar data"
    (is (= {:corrs {[:a :aa] 1N,
                    [:a :b] 1N,
                    [:aa :b] 1N,
                    [:aa :a] 1N,
                    [:b :a] 1N,
                    [:b :aa] 1N}}
          (correlate
            {:id :test
             :meta [{:field :date, :types {:date 10}, :meta {}, :type :date}
                    {:field :a, :types {:long 10}, :meta {:enum? false, :average 5.0, :min 1, :max 10}, :type :long}
                    {:field :aa, :types {:long 10}, :meta {:enum? false, :average 6.0, :min 1, :max 10}, :type :long}
                    {:field :b, :types {:long 10}, :meta {:enum? false, :average 14.5, :min 1, :max 10}, :type :long}]
             :data (generate-data 10)}))))

  (testing "Different data"
    (is (= {:corrs {[:a :b] -0.32732683535398854,
                    [:b :a] -0.32732683535398854}}
           (correlate
            {:meta [{:field :a :type :long}
                    {:field :b :type :long}]
             :data [{:a 1 :b 2}
                    {:a 1 :b 4.5}
                    {:a 2 :b 2.5}]})))))
