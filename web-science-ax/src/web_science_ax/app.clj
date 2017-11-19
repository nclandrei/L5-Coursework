(ns web-science-ax.app
  (:require [clojure.data.csv :as csv]
            [clojure.set :as set]
           [clojure.java.io :as io]
            [web-science-ax.io :as ax-io]))


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
                                              (perform-merge (ax-io/read-csv->maps
                                                                      (csv/read-csv
                                                                        (io/reader from)))
                                                                    windowInterval numberOfTweets))))))

(defn filter-clusters [from to numberOfTweets]
  "Writes a CSV using the cluster filtering techniques"
  (ax-io/write-maps->csv (remove-small-clusters
                     (group-clusters
                       (ax-io/read-csv->maps
                         (csv/read-csv (io/reader from))) :cluster_id) numberOfTweets)
                   to))