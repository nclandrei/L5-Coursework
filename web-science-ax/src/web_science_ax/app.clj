(ns web-science-ax.app
  (:require [clojure.data.csv :as csv]
            [clojure.set :as set]
           [clojure.java.io :as io]))

(def csvKeys [:cluster_id :cluster_name_entity
           :tweet_id :timestamp_ms :user_id :tweet_tokens :tweet_text])

(defn csv-data->maps [csv-data]
  "Reads in a CSV file and outputs a map with the keys defined in csvKeys"
  (map zipmap
       (->> csvKeys
            (map keyword)
            repeat)
       csv-data))

(defn filter-out-small-clusters [docMap]
  "Filters out the clusters having less than 10 documents"
  (into {} (filter (fn [[k v]] (> (count v) 10)) docMap)))

(defn group-clusters [docMap]
  "Groups clusters by cluster_id"
  (group-by :cluster_id docMap))

(defn compute-mean-time [docs]
  "Computes mean time for documents in a specific cluster"
  (double (/ (reduce + (map (fn [x] (Long/parseLong x)) (map :timestamp_ms docs))) (count docs))))

(defn add-centroid-times [docMap]
  "Creates a new map with key as centroid time and value as an array of
  the corresponding documents for that cluster"
  (set/rename-keys docMap (zipmap (keys docMap) (map (fn [[k v]] (compute-mean-time v)) docMap))))
