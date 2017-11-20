(ns event_detection.util)

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
