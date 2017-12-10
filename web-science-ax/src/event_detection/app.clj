(ns event_detection.app
  (:require [clojure.data.csv :as csv]
            [clojure.set :as set]
            [clojure.java.io :as sys-io]
            [event_detection.io :as ax-io]
            [event_detection.util :as util]))

(defn merge-named-entities [from to windowInterval numberOfTweets]
  "Writes a CSV using the named entity merging technique
    from - location of CSV to read from (e.g. ./resources/<file_name>.csv)
    to - location of CSV to read from (e.g. ./resources/<file_name>.csv)
    windowInterval - number of milliseconds in a window
    numberOfTweets - min number of tweets in a cluster"
  (csv/write-csv (sys-io/writer to)
                 (map #(vals %) (flatten (map #(vals %)
                                              (util/perform-merge (ax-io/read-csv->maps
                                                                      (csv/read-csv
                                                                        (sys-io/reader from)))
                                                                    windowInterval numberOfTweets))))))

(defn filter-clusters [from to numberOfTweets]
  "Writes a CSV using the cluster filtering techniques
    from - location of CSV to read from (e.g. ./resources/<file_name>.csv)
    to - location of CSV to read from (e.g. ./resources/<file_name>.csv)
    numberOfTweets - min number of tweets in a cluster"
  (ax-io/write-maps->csv (util/remove-small-clusters
                     (util/group-clusters
                       (ax-io/read-csv->maps
                         (csv/read-csv (sys-io/reader from))) :cluster_id) numberOfTweets)
                   to))