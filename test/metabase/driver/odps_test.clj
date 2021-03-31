(ns metabase.driver.odps-test
  "Tests for specific behavior of the Maxcompute driver."
  (:require [clojure.java.jdbc :as jdbc]
            [expectations :refer [expect]]
            [honeysql.core :as hsql]
            [metabase
             [driver :as driver]
             [query-processor :as qp]
             [query-processor-test :as qp.test]
             [util :as u]]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.models
             [field :refer [Field]]
             [table :refer [Table]]]
            [metabase.query-processor.test-util :as qp.test-util]
            [metabase.test
             [data :as data]
             [util :as tu]]
            [metabase.test.data
             [datasets :refer [expect-with-driver]]
             [odps :as odps.tx]
             [sql :as sql.tx]]
            [metabase.test.util.log :as tu.log]
            [metabase.util.honeysql-extensions :as hx]
            [toucan.util.test :as tt]))


(expect
  (let [engine  :odps
        details {:ssl            false
                 :password       ""
                 :host           "https://service.odps.aliyun.com/api?project="
                 :dbname         "projectName"
                 :user           ""}]
    (tu.log/suppress-output
      (driver/can-connect? :odps.tx details))))
