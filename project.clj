(defproject metabase/odps-driver "v0.2"
  :min-lein-version "2.5.0"

  :include-drivers-dependencies [#"^odps-jdbc-\d+(\.\d+)+.jar$"]

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.10.1"]
     ;; can't ship it as part of MB!
     [com.aliyun.odps/odps-jdbc "3.2.8"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot           :all
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "odps.metabase-driver.jar"}})
  