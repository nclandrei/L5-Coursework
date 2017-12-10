(ns event_detection.driver
  (:require [event_detection.app :as app])
  (:gen-class)
  (:import (java.io BufferedReader)))

(defn -main
  []
  (do
    (println "Hello! Please select 1 for filtering clusters method
              or 2 for event merging technique...")
    (def input (read-line))
    (if (= input "1") (do
                      (println "Please input where to get csv from (e.g. ./resources/7days/clusters.sortedby.clusterid.csv)")
                      (def fromLocation (read-line))
                      (println "Please input where to write csv to")
                      (def toLocation (read-line))
                      (println "Please input minimum number of tweets per cluster")
                      (def numberOfTweets (read-line))
                      (app/filter-clusters fromLocation toLocation (Integer/parseInt numberOfTweets)))
                    (if (= input "2") (do
                                      (println "Please input where to get csv from (e.g. ./resources/7days/clusters.sortedby.clusterid.csv)")
                                      (def fromLocation (read-line))
                                      (println "Please input where to write csv to")
                                      (def toLocation (read-line))
                                      (println "Please input minimum number of tweets per cluster")
                                      (def numberOfTweets (read-line))
                                      (println "Please input window interval")
                                      (def windowInterval (read-line))
                                      (app/merge-named-entities fromLocation toLocation (Integer/parseInt windowInterval)
                                                                (Integer/parseInt numberOfTweets)))
                                    (println "You have not typed either 1 nor 2. Please try again.")))))
