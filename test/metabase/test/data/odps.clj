(ns metabase.test.data.odps
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

(defonce ^:private session-schema-number (rand-int 200))
(defonce           session-schema        (str "CAM_" session-schema-number))
(defonce ^:private session-password      (apply str (repeatedly 16 #(rand-nth (map char (range (int \a) (inc (int \z))))))))
;; Session password is only used when creating session user, not anywhere else

(def ^:private connection-details
  (delay
   {:host     (tx/db-test-env-var-or-throw :odps :host)
    :project  (tx/db-test-env-var-or-throw :odps :project)
    :user     (tx/db-test-env-var-or-throw :odps :user)
    :password (tx/db-test-env-var-or-throw :odps :password)}))

(defmethod tx/dbdef->connection-details :odps [& _] @connection-details)

(defmethod tx/sorts-nil-first? :odps [_] false)

;; If someone tries to run Time column tests with odps give them a heads up that odps does not support it
(defmethod sql.tx/field-base-type->sql-type [:odps :type/Time] [_ _]
  (throw (UnsupportedOperationException. "odps does not have a TIME data type.")))

(defmethod tx/expected-base-type->actual :odps [_ base-type]
  ;; odps doesn't have INTEGERs
  (if (isa? base-type :type/Integer)
    :type/Decimal
    base-type))

(defmethod sql.tx/create-db-sql :odps [& _] nil)

(defmethod sql.tx/drop-db-if-exists-sql :odps [& _] nil)

(defmethod execute/execute-sql! :odps [& args]
  (apply execute/sequentially-execute-sql! args))

;; Now that connections are reüsed doing this sequentially actually seems to be faster than parallel
(defmethod load-data/load-data! :odps [& args]
  (apply load-data/load-data-one-at-a-time! args))

(defmethod sql.tx/qualified-name-components :odps [& args]
  (apply tx/single-db-qualified-name-components session-schema args))

(defmethod tx/id-field-type :odps [_] :type/Decimal)

(defmethod tx/has-questionable-timezone-support? :odps [_] true)

