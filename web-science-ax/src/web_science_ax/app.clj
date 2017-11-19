(ns web-science-ax.app
  (:require [clojure.data.csv :as csv]
            [clojure.set :as set]
            [clojure.java.io :as sys-io]
            [web-science-ax.io :as ax-io]
            [web-science-ax.util :as util]))

(defn merge-named-entities [from to windowInterval numberOfTweets]
  "Writes a CSV using the named entity merging technique"
  (csv/write-csv (sys-io/writer to)
                 (map #(vals %) (flatten (map #(vals %)
                                              (util/perform-merge (ax-io/read-csv->maps
                                                                      (csv/read-csv
                                                                        (sys-io/reader from)))
                                                                    windowInterval numberOfTweets))))))

(defn filter-clusters [from to numberOfTweets]
  "Writes a CSV using the cluster filtering techniques"
  (ax-io/write-maps->csv (util/remove-small-clusters
                     (util/group-clusters
                       (ax-io/read-csv->maps
                         (csv/read-csv (sys-io/reader from))) :cluster_id) numberOfTweets)
                   to))