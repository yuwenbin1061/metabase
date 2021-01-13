(ns metabase.test.generate
  "Facilities for generating random instances of our various models."
  (:require [metabase.models :refer [Card Dashboard DashboardCard User Collection Pulse Table Field Metric FieldValues
  Dimension ImportantField]]
            [metabase.test :as mt]
            [methodical.core :as m]
            [toucan.db :as db]
            [toucan.util.test :as tt]))

(m/defmulti create-random!*
  {:arglists '([model property-overrides])}
  (fn [model _]
    (class model)))

(m/defmethod create-random!* :default
  [model property-overrides]
  (let [properties (merge (tt/with-temp-defaults model)
                          property-overrides)]
    (db/insert! model properties)))

(defn- coin-toss?
  "Maybe should be overridable"
  []
  (zero? (rand-int 2)))

(def max-children 5)

(defn- random-query []
  ;; TODO -- something more random
  (cond-> (mt/mbql-query venues)
    ;; 50% chance to add an aggregation
    (coin-toss?) (assoc-in [:query :aggregation] [[:count]])
    ;; 50% chance to add a filter on `price`
    (coin-toss?) (assoc-in [:query :filter] [:=
                                             [:field-id (mt/id :venues :price)]
                                             (inc (rand-int 4))])))

(m/defmethod create-random!* :before (class Card)
  [_ property-overrides]
  (merge {:dataset_query (random-query)}
         property-overrides))

(m/defmethod create-random!* :after (class Card)
  [_ card]
  (dotimes [_ (rand-int max-children)]
    ))


(m/defmethod create-random!* :after (class Dashboard)
  [_ dashboard]
  ;; create 0-4 Cards and add to a Dashboard when creating a Dashboard
  (dotimes [_ (rand-int max-children)]
    (let [card (create-random!* Card nil)]
      (println "Creating random Card with query" (pr-str (:dataset_query card)))
      (create-random!* DashboardCard {:dashboard_id (:id dashboard), :card_id (:id card)})))
  dashboard)

(m/defmethod create-random!* :after (class User)
  [_ user]
  (create-random!* Collection {:personal_owner_id (:id user)})
  user)

(m/defmethod create-random!* :after (class Collection)
  [_ collection]
  (dotimes [_ (rand-int max-children)]
    (create-random!* Pulse {:creator_id (:personal_owner_id collection)
                            :collection_id (:id collection)})))

(defn create-random!
  {:arglists '([model] [n model] [model property-overrides] [n model property-overrides])}
  ([model]
   (create-random!* model nil))
  ([x y]
   (if (integer? x)
     (create-random! x y nil)
     (create-random!* x y)))
  ([n model property-overrides]
   (vec (for [_ (range n)]
          (create-random!* model property-overrides)))))


(defn x []
  (mt/with-model-cleanup [Card Dashboard DashboardCard Collection Pulse Table Field
                          Metric FieldValues Dimension ImportantField] ;; create a random Dashboard.
    (let [user (create-random! User)
          dash (create-random! Dashboard {:creator_id (:id user)})]
      (println "CREATED" (db/count DashboardCard :dashboard_id (:id dash)) "CARDS")
      (println "CREATED" (db/count Pulse) "PULSES")
      (println "CREATED" (db/count Collection) "Collections")
      dash)))
