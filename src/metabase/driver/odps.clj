(ns metabase.driver.odps
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [honeysql
             [core :as hsql]
             [helpers :as h]
             [format :as hformat]]
            [metabase
             [config :as config]
             [driver :as driver]
             [util :as u]]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.sql-jdbc
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.util
             [date :as du]
             [honeysql-extensions :as hx]]
            [metabase.mbql.util :as mbql.u]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor
             [store :as qp.store]
             [util :as qputil]])
  (:import [java.sql ResultSet Types]
           [java.sql PreparedStatement Time]
           java.util.Date))

(driver/register! :odps, :parent :sql-jdbc)

(def ^:private source-table-alias
  "Default alias for all source tables. (Not for source queries; those still use the default SQL QP alias of `source`.)"
  "t1")

(defmethod sql-jdbc.sync/database-type->base-type :odps [_ database-type]
  ({:BIGINT    :type/BigInteger
    :TINYINT   :type/Integer
    :SMALLINT  :type/Integer
    :INT       :type/Integer
    :FLOAT     :type/Float
    :DOUBLE    :type/Float
    :DECIMAL   :type/Decimal
    :VARCHAR   :type/Text
    :STRING    :type/Text
    :BINARY    :type/*
    :DATETIME  :type/DateTime
    :TIMESTAMP :type/DateTime
    :BOOLEAN   :type/Boolean
    :ARRAY     :type/*
    :STRUCT    :type/*
    :MAP       :type/*
    } database-type))

(defmethod sql-jdbc.conn/connection-details->spec :odps [_ {:keys [host dbname]
                                                            :or   {host "https://service.odps.aliyun.com/api?project=", dbname ""}
                                                            :as   details}]
  (merge
    {:classname   "com.aliyun.odps.jdbc.OdpsDriver"
     :subprotocol "odps"
     :subname     (str host dbname "&charset=UTF-8")}

    (dissoc details :host :dbname)))

(defn- run-query
  "Run the query itself."
  [{sql :query, :keys [params remark max-rows]} connection]
  (let [sql (str "-- " remark "\n" sql)
        options {:identifiers identity
                 :as-arrays?  true
                 :max-rows    max-rows}]
    (with-open [connection (jdbc/get-connection connection)]
      (with-open [^PreparedStatement statement (jdbc/prepare-statement connection sql options)]
        (let [statement (into [statement] params)
              [columns & rows] (jdbc/query connection statement options)]
          {:rows    (or rows [])
           :columns (map u/keyword->qualified-name columns)})))))

(defn run-query-without-timezone
  "Runs the given query without trying to set a timezone"
  [_ _ connection query]
  (run-query query connection))


(defmethod driver/execute-query :odps
  [driver {:keys [database settings], query :native, :as outer-query}]
  (let [query (-> (assoc query
                    :remark (qputil/query->remark outer-query)
                    :query (if (seq (:params query))
                             (unprepare/unprepare driver (cons (:query query) (:params query)))
                             (:query query))
                    :max-rows (mbql.u/query->max-rows-limit outer-query))
                  (dissoc :params))]
    (sql-jdbc.execute/do-with-try-catch
      (fn []
        (let [db-connection (sql-jdbc.conn/db->pooled-connection-spec database)]
          (run-query-without-timezone driver settings db-connection query))))))

(defmethod sql.qp/quote-style :odps [_] :mysql)

(defmethod sql.qp/current-datetime-fn :odps [_] (hsql/raw "getdate()"))

(defmethod sql.qp/unix-timestamp->timestamp [:odps :seconds] [_ _ expr]
  (hx/->timestamp (hsql/call :from_unixtime expr)))

(defn- date-format [format-str expr]
  (hsql/call :to_char expr (hx/literal format-str)))

(defn- str-to-date [format-str expr] (hsql/call :to_date expr (hx/literal format-str)))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:odps :minute]          [_ _ expr] (trunc-with-format "yyyy-mm-dd hh:mi" expr))
(defmethod sql.qp/date [:odps :minute-of-hour]  [_ _ expr] (hsql/call :datepart expr (hx/literal "mi")))
(defmethod sql.qp/date [:odps :hour]            [_ _ expr] (trunc-with-format "yyyy-mm-dd hh" expr))
(defmethod sql.qp/date [:odps :hour-of-day]     [_ _ expr] (hsql/call :datepart expr (hx/literal "hh")))
(defmethod sql.qp/date [:odps :day]             [_ _ expr] (trunc-with-format "yyyy-mm-dd" expr))
(defmethod sql.qp/date [:odps :day-of-week]     [_ _ expr] (hx/+ (hsql/call :weekday expr) 1))
(defmethod sql.qp/date [:odps :day-of-month]    [_ _ expr] (hsql/call :datepart expr (hx/literal "dd")))
(defmethod sql.qp/date [:odps :week-of-year]    [_ _ expr] (hsql/call :weekofyear expr))
(defmethod sql.qp/date [:odps :month]           [_ _ expr] (trunc-with-format "yyyy-mm" expr))
(defmethod sql.qp/date [:odps :month-of-year]   [_ _ expr] (hsql/call :datepart expr (hx/literal "mm")))
(defmethod sql.qp/date [:odps :year]            [_ _ expr] (trunc-with-format "yyyy" expr))


;;;(defmethod sql.qp/date [:odps :week] [_ _ expr]
;;;           (hsql/call :date_sub
;;;                      (hx/+ (hx/->timestamp expr)
;;;                            (hsql/raw "interval '1' day"))
;;;                      (date-format "u"
;;;                                   (hx/+ (hx/->timestamp expr)
;;;                                         (hsql/raw "interval '1' day")))))

(defmethod sql.qp/date [:odps :quarter] [_ _ expr]
  (hsql/call :dateadd (hsql/call :datetrunc expr (hx/literal "yyyy"))
             (hx/* (hx/- ((hsql/call :quarter expr)) 1) 3)
             (hx/literal "mm")
             ))

(defmethod sql.qp/date [:odps :quarter-of-year] [_ _ expr]
           (hsql/call :ceil (hx// (hsql/call :datepart expr (hx/literal "mm")) 3)))

(defmethod driver/date-interval :odps [_ unit amount]
  (hsql/raw (format "dateadd(getdate(), %d, '%s')" (case unit
                                                           :second  (int amount)
                                                           :minute  (int amount)
                                                           :hour    (int amount)
                                                           :day     (int amount)
                                                           :week    (int (hx/* amount 7))
                                                           :month   (int amount)
                                                           :quarter (int (hx/* amount 3))
                                                           :year    (int amount))
                                                     (case unit
                                                          :second  (name "ss")
                                                          :minute  (name "mi")
                                                          :hour    (name "hh")
                                                          :day     (name "dd")
                                                          :week    (name "dd")
                                                          :month   (name "mm")
                                                          :quarter (name "mm")
                                                          :year    (name "yyyy")))))

(defmethod unprepare/unprepare-value [:odps Date] [_ value]
           (hformat/to-sql
             (hsql/call :to_date (hx/literal (du/date->iso-8601 value)) (hx/literal "yyyy-mm-ddThh:mi:ss.ff3Z"))))

(prefer-method unprepare/unprepare-value [:sql Time] [:odps Date])

(defmethod unprepare/unprepare-value [:odps String] [_ value]
           (str \' (str/replace value "'" "\\\\'") \'))
