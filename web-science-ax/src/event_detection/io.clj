(ns event_detection.io
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]))


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
