(ns metabase.query-processor.pivot-test
  "Tests for pivot table actions for the query processor"
  (:require [clojure.set :as set]
            [clojure.test :refer :all]
            [metabase.query-processor.pivot :as pivot]
            [metabase.test :as mt]))

(def ^:private applicable-drivers
  ;; Redshift takes A LONG TIME to insert the sample-dataset, so do not
  ;; run these tests against Redshift (for now?)
  ;;TODO: refactor Redshift testing to support a bulk COPY or something
  ;; other than INSERT INTO statements
  (disj (mt/normal-drivers-with-feature :expressions :left-join) :redshift))

(deftest generate-queries-test
  (mt/test-drivers applicable-drivers
    (mt/dataset sample-dataset
      (let [request {:database   (mt/db)
                     :query      (mt/$ids orders
                                   {:source-table $$orders
                                    :aggregation  [[:count] [:sum $orders.quantity]]
                                    :breakout     [$orders.user_id->people.state
                                                   $orders.user_id->people.source
                                                   $orders.product_id->products.category]})
                     :type       :query
                     :parameters []
                     :pivot-rows [1 0]
                     :pivot-cols [2]}]
        (testing "can generate queries for each new breakout"
          (let [expected (mt/$ids orders
                           [{:query {:breakout    [$orders.user_id->people.source
                                                   $orders.product_id->products.category
                                                   [:expression "pivot-grouping"]]
                                     :expressions {"pivot-grouping" [:abs 1]}}}
                            {:query {:breakout    [$orders.product_id->products.category
                                                   [:expression "pivot-grouping"]]
                                     :expressions {"pivot-grouping" [:abs 3]}}}
                            {:query {:breakout    [$orders.user_id->people.source
                                                   $orders.user_id->people.state
                                                   [:expression "pivot-grouping"]]
                                     :expressions {"pivot-grouping" [:abs 4]}}}
                            {:query {:breakout    [$orders.user_id->people.source
                                                   [:expression "pivot-grouping"]]
                                     :expressions {"pivot-grouping" [:abs 5]}}}

                            {:query {:breakout    [[:expression "pivot-grouping"]]
                                     :expressions {"pivot-grouping" [:abs 7]}}}])
                expected (map (fn [expected-val] (-> expected-val
                                                     (assoc :type       :query
                                                            :parameters []
                                                            :pivot-rows [1 0]
                                                            :pivot-cols [2])
                                                     (assoc-in [:query :fields] [[:expression "pivot-grouping"]])
                                                     (assoc-in [:query :aggregation] [[:count] [:sum (mt/$ids $orders.quantity)]])
                                                     (assoc-in [:query :source-table] (mt/$ids $$orders)))) expected)
                actual   (map (fn [actual-val] (dissoc actual-val :database)) (#'pivot/generate-queries request))]
            (is (= 5 (count actual)))
            (is (= (map #(get-in % [:query :expressions "pivot-grouping"]) expected)
                   (map #(get-in % [:query :expressions "pivot-grouping"]) actual)))
            (is (= expected actual))))))))

(deftest breakout-combinations-test
  (testing "Should return the combos that Paul specified in (#14329)"
    (is (= [[0 1 2]
            [0 1  ]
            [0    ]
            [     ]]
           (#'pivot/breakout-combinations 3 [0 1 2] [])))
    (is (= (sort-by
            (partial #'pivot/group-number 4)
            [ ;; subtotal rows
             [0   3]
             [0 1 3]
             ;; row totals
             [0 1 2]
             ;; subtotal rows within "row totals"
             [0    ]
             [0 1  ]
             ;; "grand totals" row
             [    3]
             ;; bottom right corner
             [     ]])
           (#'pivot/breakout-combinations 4 [0 1 2] [3])))))

(deftest group-number-test
  (doseq [[indecies expected] {[0]     6
                               [0 1]   4
                               [0 1 2] 0
                               []      7}]
    (is (= expected
           (#'pivot/group-number 3 indecies)))))

(deftest validate-pivot-rows-cols-test
  (testing "Should throw an Exception if you pass in invalid pivot-rows"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid pivot-rows: specified breakout at index 3, but we only have 3 breakouts"
         (#'pivot/breakout-combinations 3 [0 1 2 3] []))))
  (testing "Should throw an Exception if you pass in invalid pivot-cols"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Invalid pivot-cols: specified breakout at index 3, but we only have 3 breakouts"
         (#'pivot/breakout-combinations 3 [] [0 1 2 3]))))
  ;; TODO -- we should require these columns to be distinct as well (I think?)
  ;; TODO -- require all numbers to be positive
  ;; TODO -- can you specify something in both pivot-rows and pivot-cols?
  )

(defn- test-query []
  (mt/dataset sample-dataset
    (mt/$ids orders
      {:database     (mt/id)
       :type         :query
       :query        {:source-table $$orders
                      :aggregation  [[:count]]
                      :breakout     [$product_id->products.category
                                     $user_id->people.source
                                     !year.created_at]
                      :filter       [:and
                                     [:= $user_id->people.source "Facebook" "Google"]
                                     [:= $product_id->products.category "Doohickey" "Gizmo"]
                                     [:time-interval $created_at -2 :year {}]]}
       :pivot-rows [0 1 2]
       :pivot-cols []})))

(deftest allow-snake-case-test
  (testing "make sure the stuff works with either normal lisp-case keys or snake case. It should only ever see lisp-case"
    (is (= (mt/rows (pivot/run-pivot-query (test-query)))
           (mt/rows (pivot/run-pivot-query (set/rename-keys (test-query)
                                                            {:pivot-rows :pivot_rows, :pivot-cols :pivot_cols})))))))

(deftest dont-return-too-many-rows-test
  (testing "Make sure pivot queries don't return too many rows (#14329)"
    (mt/dataset sample-dataset
      (let [results (pivot/run-pivot-query (test-query))
            rows    (mt/rows results)]
        ;; TODO - fix me
        #_(is (= ["Category"
                  "Source"
                  "Created At"
                  "pivot-grouping"
                  "Count"]
                 (map :display_name (mt/cols results))))
        (is (apply distinct? rows))
        ;; OLD RESULTS are commented out below
        (is (= [;; ["Doohickey" "Facebook" "2019-01-01T00:00:00Z" 0  263 ]
                ;; ["Doohickey" "Facebook" "2020-01-01T00:00:00Z" 0  89  ]
                ;; ["Doohickey" "Google"   "2019-01-01T00:00:00Z" 0  276 ]
                ;; ["Doohickey" "Google"   "2020-01-01T00:00:00Z" 0  100 ]
                ;; ["Gizmo"     "Facebook" "2019-01-01T00:00:00Z" 0  361 ]
                ;; ["Gizmo"     "Facebook" "2020-01-01T00:00:00Z" 0  113 ]
                ;; ["Gizmo"     "Google"   "2019-01-01T00:00:00Z" 0  325 ]
                ;; ["Gizmo"     "Google"   "2020-01-01T00:00:00Z" 0  101 ]
                ;; [nil         nil        nil                    7  1628]
                ["Doohickey" "Facebook" "2019-01-01T00:00:00Z" 0  263 ]
                ["Doohickey" "Facebook" "2020-01-01T00:00:00Z" 0  89  ]
                ["Doohickey" "Google"   "2019-01-01T00:00:00Z" 0  276 ]
                ["Doohickey" "Google"   "2020-01-01T00:00:00Z" 0  100 ]
                ["Gizmo"     "Facebook" "2019-01-01T00:00:00Z" 0  361 ]
                ["Gizmo"     "Facebook" "2020-01-01T00:00:00Z" 0  113 ]
                ["Gizmo"     "Google"   "2019-01-01T00:00:00Z" 0  325 ]
                ["Gizmo"     "Google"   "2020-01-01T00:00:00Z" 0  101 ]
                ;; [nil         "Facebook" "2019-01-01T00:00:00Z" 1  624 ]
                ;; [nil         "Facebook" "2020-01-01T00:00:00Z" 1  202 ]
                ;; [nil         "Google"   "2019-01-01T00:00:00Z" 1  601 ]
                ;; [nil         "Google"   "2020-01-01T00:00:00Z" 1  201 ]
                ;; ["Doohickey" nil        "2019-01-01T00:00:00Z" 2  539 ]
                ;; ["Doohickey" nil        "2020-01-01T00:00:00Z" 2  189 ]
                ;; ["Gizmo"     nil        "2019-01-01T00:00:00Z" 2  686 ]
                ;; ["Gizmo"     nil        "2020-01-01T00:00:00Z" 2  214 ]
                ;; [nil         nil        "2019-01-01T00:00:00Z" 3  1225]
                ;; [nil         nil        "2020-01-01T00:00:00Z" 3  403 ]
                ["Doohickey" "Facebook" nil                    4  352 ]
                ["Doohickey" "Google"   nil                    4  376 ]
                ["Gizmo"     "Facebook" nil                    4  474 ]
                ["Gizmo"     "Google"   nil                    4  426 ]
                ;; [nil         "Facebook" nil                    5  826 ]
                ;; [nil         "Google"   nil                    5  802 ]
                ["Doohickey" nil        nil                    6  728 ]
                ["Gizmo"     nil        nil                    6  900 ]
                [nil         nil        nil                    7  1628]]
               rows))))))
