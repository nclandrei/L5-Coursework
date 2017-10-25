(ns web-science-ax.app
  (:require [clojure.data.csv :as csv]
           [clojure.java.io :as io]))

(defn csv-data->maps [csv-data]
  "Reads in a CSV file and outputs a map"
  (map zipmap
       (->> (first csv-data) ;; First row is the header
            (map keyword) ;; Drop if you want string keys instead
            repeat)
       (rest csv-data)))

