(ns web-science-ax.app
  (:require [clojure.data.csv :as csv]
            [clojure.set :as set]
           [clojure.java.io :as io]))

(def csvKeys [:cluster_id :cluster_name_entity
           :tweet_id :timestamp_ms :user_id :tweet_tokens :tweet_text])

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

(defn remove-small-clusters [docMap numberOfTweets]
  "Filters out the clusters having less tweets than numberOfTweets"
  (into {} (filter (fn [[k v]] (> (count v) numberOfTweets)) docMap)))

(defn group-clusters [docMap groupByKeyword]
  "Groups clusters by keyword (e.g. clusterID, namedEntity)"
  (group-by groupByKeyword docMap))

(defn perform-merge [docMap windowInterval numberOfTweets]
  "Merges the clusters who reference the same named entity and which have more than numberOfTweets tweets"
  (filter (fn [v]
            (> (count (flatten (vals v))) numberOfTweets))
          (into '() (map #(group-by :cluster_id %)
                         (vals (group-by :cluster_name_entity docMap))))))

(defn merge-named-entities [from to windowInterval numberOfTweets]
  "Writes a CSV using the named entity merging technique"
  (csv/write-csv (io/writer to)
                 (map #(vals %) (flatten (map #(vals %)
                                              (perform-merge (read-csv->maps
                                                                      (csv/read-csv
                                                                        (io/reader from)))
                                                                    windowInterval numberOfTweets))))))

(defn filter-clusters [from to numberOfTweets]
  "Writes a CSV using the cluster filtering technique"
  (write-maps->csv (remove-small-clusters
                     (group-clusters
                       (read-csv->maps
                         (csv/read-csv (io/reader from))) :cluster_id) numberOfTweets)
                   to))