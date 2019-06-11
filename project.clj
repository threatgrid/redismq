(defproject threatgrid/redismq "0.1.2-SNAPSHOT"
  :description "Redis-based message queue"
  :url "http://github.com/threatgrid/redismq"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [cheshire "5.8.1"]
                 [com.taoensso/carmine "2.19.1"]])
