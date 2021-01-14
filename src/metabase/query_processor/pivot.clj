(ns metabase.query-processor.pivot
  "Pivot table actions for the query processor"
  (:require [clojure.core.async :as a]
            [metabase.mbql.normalize :as normalize]
            [metabase.query-processor :as qp]
            [metabase.query-processor.context :as qp.context]
            [metabase.query-processor.context.default :as context.default]
            [metabase.query-processor.error-type :as qp.error-type]
            [metabase.query-processor.store :as qp.store]
            [metabase.util.i18n :refer [tru]]
            [schema.core :as s]))

(defn- group-number
  "Come up with a display name given a combination of breakout `indecies` e.g.

    (group-number 3 [1])   ; -> 5
    (group-number 3 [1 2]) ; -> 1"
  [num-breakouts indecies]
  (transduce
   (map (partial bit-shift-left 1))
   (completing bit-xor)
   (int (dec (Math/pow 2 num-breakouts)))
   indecies))

(defn- breakout-combinations
  "Return a sequence of all breakout combinations (by index) we should generate queries for.

    (breakout-combinations 3 [1 2] nil) ;; -> [[0 1 2] [] [1 2] [2] [1]]"
  [num-breakouts pivot-rows pivot-cols]
  ;; validate pivot-rows/pivot-cols
  (doseq [[k pivots] {:pivot-rows pivot-rows, :pivot-cols pivot-cols}
          i          pivots]
    (when (>= i num-breakouts)
      (throw (ex-info (tru "Invalid {0}: specified breakout at index {1}, but we only have {2} breakouts"
                           (name k) i num-breakouts)
                      {:type          qp.error-type/invalid-query
                       :num-breakouts num-breakouts
                       :pivot-rows    pivot-rows
                       :pivot-cols    pivot-cols}))))
  (sort-by
   (partial group-number num-breakouts)
   (distinct
    (map
     vec
     (concat
      ;; e.g. given num-breakouts = 4; pivot-rows = [0 1 2]; pivot-cols = [3]
      ;; subtotal rows
      ;; _.range(1, pivotRows.length).map(i => [...pivotRow.slice(0, i), ...pivotCols])
      ;;  => [0 _ _ 3] [0 1 _ 3] => 0110 0100 => Group #6, #4
      (for [i (range 1 (count pivot-rows))]
        (concat (take i pivot-rows) pivot-cols))
      ;; “row totals” on the right
      ;; pivotRows
      ;; => [0 1 2 _] => 1000 => Group #8
      [pivot-rows]
      ;; subtotal rows within “row totals”
      ;; _.range(1, pivotRows.length).map(i => pivotRow.slice(0, i))
      ;; => [0 _ _ _] [0 1 _ _] => 1110 1100 => Group #14, #12
      (for [i (range 1 (count pivot-rows))]
        (take i pivot-rows))
      ;; “grand totals” row
      ;; pivotCols
      ;; => [_ _ _ 3] => 0111 => Group #7
      [pivot-cols]
      ;; bottom right corner [_ _ _ _] => 1111 => Group #15
      [[]])))))

(s/defn ^:private add-grouping-field
  "Add the grouping field and expression to the query"
  [outer-query breakouts group-number :- s/Int]
  (let [new-query (-> outer-query
                      ;;TODO: `pivot-grouping` is not "magic" enough to mark it as an internal thing
                      (update-in [:query :fields]
                                 #(conj % [:expression "pivot-grouping"]))
                      (update-in [:query :expressions]
                                 #(assoc % "pivot-grouping" [:abs group-number])))]
    ;; in PostgreSQL and most other databases, all the expressions must be present in the breakouts
    (assoc-in new-query [:query :breakout]
              (concat breakouts
                      (map (fn [expr] [:expression (name expr)])
                           (keys (get-in new-query [:query :expressions])))))))

(defn- generate-queries
  "Generate the additional queries to perform a generic pivot table"
  [{{all-breakouts :breakout} :query, :keys [pivot-rows pivot-cols query], :as outer-query}]
  (try
    (for [breakout-indecies (breakout-combinations (count all-breakouts) pivot-rows pivot-cols)
          :let              [group-number    (group-number (count all-breakouts) breakout-indecies)
                             new-breakouts (for [i breakout-indecies]
                                             (nth all-breakouts i))]]
      (add-grouping-field outer-query new-breakouts group-number))
    (catch Throwable e
      (throw (ex-info (tru "Error generating pivot queries")
                      {:type qp.error-type/qp, :query query}
                      e)))))

;; this function needs to be executed at the start of every new query to
;; determine the mapping for maintaining query shape
(defn- map-cols
  {:arglists '([query context])}
  [query {all-expected-cols ::all-expected-columns}]
  (assert (seq all-expected-cols))
  (let [query-cols (map-indexed vector (qp/query->expected-cols query))]
    (map (fn [item]
           (some #(when (= (:name item) (:name (second %)))
                    (first %))
                 query-cols))
         all-expected-cols)))

;; this function needs to be called for each row so that it can actually
;; shape the row according to the `map-cols` fn above
(defn- map-row
  [row cols]
  ;; the first query doesn't need any special mapping, it already has all the columns
  (if cols
    (for [i cols]
      (when i
        (nth row i)))
    row))

(defn- process-query-append-results
  "Reduce the results of a single `query` using `rf` and initial value `init`."
  [query rf init context]
  (if (a/poll! (qp.context/canceled-chan context))
    (ensure-reduced init)
    (let [cols (map-cols query context)]
      (qp/process-query-sync
       query
       {:canceled-chan (qp.context/canceled-chan context)
        :rff           (fn [_]
                         (fn
                           ([]        init)
                           ([acc]     acc)
                           ([acc row] (rf acc (map-row row cols)))))}))))

(defn- process-queries-append-results
  "Reduce the results of a sequence of `queries` using `rf` and initial value `init`."
  [queries rf init context]
  (reduce
   (fn [acc query]
     (process-query-append-results query rf acc context))
   init
   queries))

(defn- append-queries-context
  "Update Query Processor `context` so it appends the rows fetched when running `more-queries`."
  [context more-queries]
  (cond-> context
    (seq more-queries)
    (update :rff (fn [rff]
                   (fn [metadata]
                     (let [rf (rff metadata)]
                       (fn
                         ([]        (rf))
                         ([acc]     (rf (process-queries-append-results more-queries rf acc context)))
                         ([acc row] (rf acc row)))))))))

(defn process-multiple-queries
  "Allows the query processor to handle multiple queries, stitched together to appear as one"
  [[first-query & more-queries] context]
  (qp/process-query first-query (append-queries-context context more-queries)))

(defn run-pivot-query
  "Run the pivot query. Unlike many query execution functions, this takes `context` as the first parameter to support
   its application via `partial`.

   You are expected to wrap this call in `qp.streaming/streaming-response` yourself."
  ([query]
   (run-pivot-query query nil))
  ([query info]
   (run-pivot-query query info (context.default/default-context)))
  ([query info context]
   ;; make sure the query keys are normalized to lisp-case.
   (let [query (normalize/normalize query)]
     (qp.store/with-store
       (let [main-breakout           (:breakout (:query query))
             col-determination-query (add-grouping-field query main-breakout 0)
             all-expected-cols       (qp/query->expected-cols col-determination-query)
             all-queries             (generate-queries query)]
         (process-multiple-queries
          all-queries
          (assoc context
                 :info (assoc info :context context)
                 ::all-expected-columns all-expected-cols)))))))
