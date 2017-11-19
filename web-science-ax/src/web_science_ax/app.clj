(ns web-science-ax.app
  (:require [clojure.data.csv :as csv]
            [clojure.set :as set]
           [clojure.java.io :as io]))

(def csvKeys [:cluster_id :cluster_name_entity
           :tweet_id :timestamp_ms :user_id :tweet_tokens :tweet_text])

(def windowIntervals [450000, 900000, 1800000, 3600000, 7200000, 14400000])

(defn read-csv->maps [csv-data]
  "Reads in a CSV file and outputs a map with the keys defined in csvKeys"
  (map zipmap
       (->> csvKeys
            (map keyword)
            repeat)
       csv-data))

(defn write-maps->csv [docMap to]
  (with-open [writer (io/writer to)]
  "Writes map back to a new csv"
  (->> (map #(vals %) (flatten (vals docMap)))
       (csv/write-csv writer))))

(defn filter-out-small-clusters [docMap numberOfTweets]
  "Filters out the clusters having less tweets than numberOfTweets"
  (into [] (vals (into {} (filter (fn [[k v]] (> (count v) numberOfTweets)) docMap)))))

(defn group-clusters [docMap groupByKeyword]
  "Groups clusters by keyword (e.g. clusterID, namedEntity)"
  (group-by groupByKeyword docMap))

(defn compute-mean-time [docMap]
  "Computes mean time for documents in a specific cluster"
  (double (/ (reduce + (map (fn [x] (Long/parseLong x)) (map :timestamp_ms docMap))) (count docMap))))

(defn add-centroid-times [docMap]
  "Creates a new map with key as centroid time and value as an array of
  the corresponding documents for that cluster"
  (set/rename-keys docMap (zipmap (keys docMap) (map (fn [[k v]] (compute-mean-time v)) docMap))))

(defn merge-named-entities [docMap windowInterval numberOfTweets]
  "Merges the clusters who reference the same named entity and which have more than numberOfTweets tweets"
  (filter (fn [v]
            (> (count (flatten (vals v))) numberOfTweets))
          (into '() (map #(group-by :cluster_id %)
                         (vals (group-by :cluster_name_entity docMap))))))