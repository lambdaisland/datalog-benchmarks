(ns datalog-benchmarks.scratch
  (:require [asami.core :as asami]
            [datascript.core :as ds]
            [datomic.api :as datomic]))

(defn random-person []
  #:person{:name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
           :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
           :alias     (vec
                       (repeatedly (rand-int 10) #(rand-nth ["A. C. Q. W." "A. J. Finn" "A.A. Fair" "Aapeli" "Aaron Wolfe" "Abigail Van Buren" "Jeanne Phillips" "Abram Tertz" "Abu Nuwas" "Acton Bell" "Adunis"])))
           :sex       (rand-nth [:sex/male :sex/female])
           :age       (rand-int 100)
           :salary    (rand-int 100000)})

(def people (repeatedly random-person))

(def people20k (shuffle (take 20000 people)))

(def ^:dynamic *warmup-t* 5000)
(def ^:dynamic *bench-t*  10000)
(def ^:dynamic *step*     5)
(def ^:dynamic *repeats*  1)

#?(:cljs (defn ^number now [] (js/performance.now))
   :clj  (defn ^Long   now [] (/ (System/nanoTime) 1000000.0)))

(defn to-fixed [n places]
  #?(:cljs (.toFixed n places)
     :clj  (format (str "%." places "f") (double n))))

(defn ^:export round [n]
  (cond
    (> n 1)        (to-fixed n 1)
    (> n 0.1)      (to-fixed n 2)
    (> n 0.001)    (to-fixed n 2)
    ;;     (> n 0.000001) (to-fixed n 7)
    :else          n))

(defn percentile [xs n]
  (->
   (sort xs)
   (nth (min (dec (count xs))
             (int (* n (count xs)))))))

#?(:clj
   (defmacro dotime [duration & body]
     `(let [start-t# (now)
            end-t#   (+ ~duration start-t#)]
        (loop [iterations# *step*]
          (dotimes [_# *step*] ~@body)
          (let [now# (now)]
            (if (< now# end-t#)
              (recur (+ *step* iterations#))
              (double (/ (- now# start-t#) iterations#)))))))) ;; ms / iteration

#?(:clj
   (defmacro bench [& body]
     `(let [_#       (dotime *warmup-t* ~@body)
            results# (into []
                           (for [_# (range *repeats*)]
                             (dotime *bench-t* ~@body)))
                                        ; min#     (reduce min results#)
            med#     (percentile results# 0.5)
                                        ; max#     (reduce max results#)
            ]
        med#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Datomic

(def datomic-schema
  (for [[ident type & tags] [[:person/name :db.type/string]
                             [:person/last-name :db.type/string]
                             [:person/alias :db.type/string :many]
                             [:person/sex :db.type/ref]
                             [:person/age :db.type/long]
                             [:person/salary :db.type/long]]
        :let [tags (set tags)]]
    (cond-> {:db/ident ident
             :db/valueType type
             :db/cardinality :db.cardinality/one}
      (:many tags)
      (assoc :db/cardinality :db.cardinality/many))))

(def datomic-enums
  [{:db/ident :sex/male}
   {:db/ident :sex/female}])

(def datomic-db-uri "datomic:mem://dbname")
(datomic/create-database datomic-db-uri)

(def datomic-conn (datomic/connect datomic-db-uri))

@(datomic/transact datomic-conn (concat datomic-schema datomic-enums))
@(datomic/transact datomic-conn people20k)

(def datomic-db (datomic/db datomic-conn))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Asami

(def asami-db-uri "asami:mem://dbname")
(asami/create-database asami-db-uri)

;; Create a connection to the database
(def asami-conn (asami/connect asami-db-uri))

@(asami/transact asami-conn {:tx-data people20k})

(def asami-db (asami/db asami-conn))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Datascript

(def ds-db
  (ds/db-with
   (ds/empty-db
    {:person/alias {:db/cardinality :db.cardinality/many}})
   people20k))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Bench

(def targets [[:asami asami/q asami-db]
              [:datascript ds/q ds-db]
              [:datomic datomic/q datomic-db]])

(for [[t q db] targets]
  [t (bench
      (q '[:find ?e
           :where [?e :person/name "Ivan"]]
         db))])
;; => ([:asami 0.055361949128050786]
;;     [:datascript 4.500557351911001]

;; Numbers are median millisecond per iteration

(for [[t q db] targets]
  [t (bench
      (q '[:find ?e ?l ?a
           :where
           [?e :person/name "Ivan"]
           [?e :person/last-name ?l]
           [?e :person/age ?a]
           [?e :person/sex :sex/male]]
         db))])
;; => ([:asami 0.2681985143738926]
;;     [:datascript 23.34473799302481]
