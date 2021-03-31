(ns metabase.driver.odps
"Driver for MaxCompute databases"
  (:require [clojure
             [set :as set]
             [string :as str]]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [honeysql
             [core :as hsql]
             [format :as hformat]]
            [java-time :as t]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.db.spec :as dbspec]
            [metabase.driver.common :as driver.common]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.mbql.util :as mbql.u]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor
             [store :as qp.store]
             [timezone :as qp.timezone]
             [util :as qputil]]
            [metabase.util
             [date-2 :as u.date]
             [honeysql-extensions :as hx]
             [i18n :refer [trs]]])
  (:import [java.sql Connection DatabaseMetaData ResultSetMetaData ResultSet Types]
           [java.sql PreparedStatement Time]
           [java.time LocalDate LocalDateTime OffsetDateTime ZonedDateTime]
           java.util.Date))


(driver/register! :odps, :parent :sql-jdbc)

;(driver/register! :odps :parent #{:sql-jdbc ::legacy/use-legacy-classes-for-read-and-set} :abstract? true)

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
    :BINARY    :type/Binary
    :DATETIME  :type/DateTime
    :TIMESTAMP :type/Timestamp
    :BOOLEAN   :type/Boolean
    :ARRAY     :type/*
    :STRUCT    :type/*
    :MAP       :type/*
    } database-type))

(defmethod sql-jdbc.conn/connection-details->spec :odps 
  [_ {:keys [host dbname] 
      :or   {host "https://service.odps.aliyun.com/api?project=", dbname "projectName"} 
      :as   details}]
  (merge {:classname   "com.aliyun.odps.jdbc.OdpsDriver"
     :subprotocol "odps"
     :subname     (str host dbname "&charset=UTF-8")}
    (dissoc details :host :dbname)))

(defn keyword->qualified-name
  "Return keyword K as a string, including its namespace, if any (unlike `name`).

     (keyword->qualified-name :type/FK) ->  \"type/FK\""
  [k]
  (when k
    (str/replace (str k) #"^:" "")))

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
           :columns (map keyword->qualified-name columns)})))))

(defn run-query-without-timezone
  "Runs the given query without trying to set a timezone"
  [_ _ connection query]
  (run-query query connection))

(def ^:dynamic *param-splice-style*
  "How we should splice params into SQL (i.e. 'unprepare' the SQL). Either `:friendly` (the default) or `:paranoid`.
  `:friendly` makes a best-effort attempt to escape strings and generate SQL that is nice to look at, but should not
  be considered safe against all SQL injection -- use this for 'convert to SQL' functionality. `:paranoid` hex-encodes
  strings so SQL injection is impossible; this isn't nice to look at, so use this for actually running a query."
  :friendly)

(defmethod driver/execute-reducible-query :odps
  [driver {:keys [database settings], {sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [inner-query (-> (assoc inner-query
                               :remark (qputil/query->remark :sparksql outer-query)
                               :query  (if (seq params)
                                         (binding [*param-splice-style* :paranoid]
                                           (unprepare/unprepare driver (cons sql params)))
                                         sql)
                               :max-rows (mbql.u/query->max-rows-limit outer-query))
                        (dissoc :params))
        query       (assoc outer-query :native inner-query)]
    ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond)))

(defmethod sql.qp/current-datetime-honeysql-form :odps [_] (hsql/raw "getdate()"))

(defmethod sql.qp/quote-style :odps [_] :mysql)

(defmethod sql.qp/current-datetime-honeysql-form :odps [_] (hsql/raw "getdate()"))

(defmethod sql.qp/unix-timestamp->honeysql [:odps :seconds]
           [_ _ expr]
           (hx/->timestamp (hsql/call :from_unixtime expr)))

(defn- date-format [format-str expr]
       (hsql/call :to_char expr (hx/literal format-str)))

(defn- str-to-date [format-str expr] (hsql/call :to_date expr (hx/literal format-str)))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str (hx/cast :DATETIME expr))))

(defmethod sql.qp/date [:odps :second]          [_ _ expr] (trunc-with-format "yyyy-mm-dd hh:mi:ss" expr))
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


(defmethod sql.qp/date [:odps :quarter] [_ _ expr]
  (hsql/call :dateadd (hsql/call :datetrunc expr (hx/literal "yyyy"))
             (hx/* (hx/- ((hsql/call :quarter expr)) 1) 3)
             (hx/literal "mm")
             ))

(defmethod sql.qp/date [:odps :quarter-of-year] [_ _ expr]
           (hsql/call :ceil (hx// (hsql/call :datepart expr (hx/literal "mm")) 3)))

(defmethod unprepare/unprepare-value [:odps Date] [_ value]
           (hformat/to-sql
             (hsql/call :to_date (hx/literal (u.date/format-sql value)) (hx/literal "yyyy-mm-ddThh:mi:ss.ff3Z"))))

(prefer-method unprepare/unprepare-value [:sql Time] [:odps Date])

(defmethod unprepare/unprepare-value [:odps String] [_ value]
           (str \' (str/replace value "'" "\\\\'") \'))

;; 1.  SparkSQL doesn't support setting holdability type to `CLOSE_CURSORS_AT_COMMIT`
(defmethod sql-jdbc.execute/prepared-statement :odps
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))

(defmethod unprepare/unprepare-value [:odps LocalDate]
  [driver t]
  (unprepare/unprepare-value driver (t/local-date-time t (t/local-time 0))))

(defmethod unprepare/unprepare-value [:odps LocalDateTime]
  [_ t]
  (format "to_date('%s', 'yyyy-mm-dd hh:mi:ss')" (u.date/format-sql (t/local-date-time t))))

(defmethod unprepare/unprepare-value [:odps OffsetDateTime]
  [_ t]
  (format "to_date('%s', 'yyyy-mm-dd hh:mi:ss')" (u.date/format-sql (t/local-date-time t))))

(defmethod unprepare/unprepare-value [:odps ZonedDateTime]
  [_ t]
  (format "to_date('%s', 'yyyy-mm-dd hh:mi:ss')" (u.date/format-sql (t/local-date-time t))))

;; Hive/Spark SQL doesn't seem to like DATEs so convert it to a DATETIME first
(defmethod sql-jdbc.execute/set-parameter [:odps LocalDate]
  [driver ps i t]
  (sql-jdbc.execute/set-parameter driver ps i (t/local-date-time t (t/local-time 0))))

;; TIMEZONE FIXME — not sure what timezone the results actually come back as
(defmethod sql-jdbc.execute/read-column-thunk [:odps Types/TIME]
  [_ ^ResultSet rs rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/offset-time (t/local-time t) (t/zone-offset 0)))))

(defmethod sql-jdbc.execute/read-column-thunk [:odps Types/DATE]
  [_ ^ResultSet rs rsmeta ^Integer i]
  (fn []
    (when-let [t (.getDate rs i)]
      (t/zoned-date-time (t/local-date t) (t/local-time 0) (t/zone-id)))))

(defmethod sql-jdbc.execute/read-column-thunk [:odps Types/TIMESTAMP]
  [_ ^ResultSet rs rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/zoned-date-time (t/local-date-time t) (t/zone-id)))))
