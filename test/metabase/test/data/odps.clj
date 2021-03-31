(ns metabase.test.data.ODPS
  (:require [clojure.java.jdbc :as jdbc]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.test.data
             [interface :as tx]
             [sql :as sql.tx]
             [sql-jdbc :as sql-jdbc.tx]]
            [metabase.test.data.sql-jdbc
             [execute :as execute]
             [load-data :as load-data]]
            [metabase.util :as u]))

(sql-jdbc.tx/add-test-extensions! :odps)

;; e.g.
;; H2 Tests                   | ODPS Tests
;; ---------------------------+------------------------------------------------
;; PUBLIC.VENUES.ID           | CAM_195.test_data_venues.id
;; PUBLIC.CHECKINS.USER_ID    | CAM_195.test_data_checkins.user_id
;; PUBLIC.INCIDENTS.TIMESTAMP | CAM_195.sad_toucan_incidents.timestamp
(defonce ^:private session-schema-number (rand-int 200))
(defonce           session-schema        (str "CAM_" session-schema-number))
(defonce ^:private session-password      (apply str (repeatedly 16 #(rand-nth (map char (range (int \a) (inc (int \z))))))))
;; Session password is only used when creating session user, not anywhere else

(def ^:private connection-details
  (delay
   {:host     (tx/db-test-env-var-or-throw :ODPS :host)
    :port     (Integer/parseInt (tx/db-test-env-var-or-throw :ODPS :port "1521"))
    :user     (tx/db-test-env-var-or-throw :ODPS :user)
    :password (tx/db-test-env-var-or-throw :ODPS :password)
    :sid      (tx/db-test-env-var-or-throw :ODPS :sid)}))

(defmethod tx/dbdef->connection-details :ODPS [& _] @connection-details)

(defmethod tx/sorts-nil-first? :ODPS [_] false)

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

;; If someone tries to run Time column tests with ODPS give them a heads up that ODPS does not support it
(defmethod sql.tx/field-base-type->sql-type [:ODPS :type/Time] [_ _]
  (throw (UnsupportedOperationException. "ODPS does not have a TIME data type.")))

(defmethod tx/expected-base-type->actual :ODPS [_ base-type]
  ;; ODPS doesn't have INTEGERs
  (if (isa? base-type :type/Integer)
    :type/Decimal
    base-type))

(defmethod sql.tx/create-db-sql :ODPS [& _] nil)

(defmethod sql.tx/drop-db-if-exists-sql :ODPS [& _] nil)

(defmethod execute/execute-sql! :ODPS [& args]
  (apply execute/sequentially-execute-sql! args))

;; Now that connections are reüsed doing this sequentially actually seems to be faster than parallel
(defmethod load-data/load-data! :ODPS [& args]
  (apply load-data/load-data-one-at-a-time! args))

(defmethod sql.tx/qualified-name-components :ODPS [& args]
  (apply tx/single-db-qualified-name-components session-schema args))

(defmethod tx/id-field-type :ODPS [_] :type/Decimal)

(defmethod tx/has-questionable-timezone-support? :ODPS [_] true)
