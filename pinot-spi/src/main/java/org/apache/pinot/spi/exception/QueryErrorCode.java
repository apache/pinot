/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.exception;

import java.util.EnumMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public enum QueryErrorCode {
  /// Request in JSON format is incorrect. For example, doesn't contain the expected 'sql' field.
  JSON_PARSING(100, "JsonParsingError"),
  /// Error detected at parsing time. For example, syntax error.
  SQL_PARSING(150, "SQLParsingError"),
  SQL_RUNTIME(160, "SQLRuntimeError"),
  ACCESS_DENIED(180, "AccessDenied"),
  TABLE_DOES_NOT_EXIST(190, "TableDoesNotExistError"),
  TABLE_IS_DISABLED(191, "TableIsDisabledError"),
  /// Error at query execution time. For example, String column cast to int when contains a non-integer value.
  QUERY_EXECUTION(200, "QueryExecutionError"),
  SERVER_SHUTTING_DOWN(210, "ServerShuttingDown"),
  SERVER_OUT_OF_CAPACITY(211, "ServerOutOfCapacity"),
  SERVER_TABLE_MISSING(230, "ServerTableMissing"),
  SERVER_SEGMENT_MISSING(235, "ServerSegmentMissing"),
  QUERY_SCHEDULING_TIMEOUT(240, "QuerySchedulingTimeoutError"),
  SERVER_RESOURCE_LIMIT_EXCEEDED(245, "ServerResourceLimitExceededError"),
  EXECUTION_TIMEOUT(250, "ExecutionTimeoutError"),
  BROKER_SEGMENT_UNAVAILABLE(305, ""),
  BROKER_TIMEOUT(400, "BrokerTimeoutError"),
  BROKER_RESOURCE_MISSING(410, "BrokerResourceMissingError"),
  BROKER_INSTANCE_MISSING(420, "BrokerInstanceMissingError"),
  BROKER_REQUEST_SEND(425, "BrokerRequestSend"),
  SERVER_NOT_RESPONDING(427, "ServerNotResponding"),
  TOO_MANY_REQUESTS(429, "TooManyRequests"),
  INTERNAL(450, "InternalError"),
  MERGE_RESPONSE(500, "MergeResponseError"),
  QUERY_CANCELLATION(503, "QueryCancellationError"),
  /// Error detected at validation time. For example, type mismatch.
  QUERY_VALIDATION(700, "QueryValidationError"),
  UNKNOWN_COLUMN(710, "UnknownColumnError"),
  ///  Error while planning the query. For example, trying to run a colocated join on non-colocated tables.
  QUERY_PLANNING(720, "QueryPlanningError"),
  ///  Query already errored out.
  UNKNOWN(1000, "UnknownError");
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryErrorCode.class);

  private static final QueryErrorCode[] BY_ID;

  static {
    int maxId = -1;
    for (QueryErrorCode queryErrorCode : QueryErrorCode.values()) {
      maxId = Math.max(maxId, queryErrorCode.getId());
    }
    BY_ID = new QueryErrorCode[maxId + 1];
    for (QueryErrorCode queryErrorCode : QueryErrorCode.values()) {
      BY_ID[queryErrorCode.getId()] = queryErrorCode;
    }
  }
  private final int _id;
  private final String _defaultMessage;

  QueryErrorCode(int errorCode, String defaultMessage) {
    _id = errorCode;
    _defaultMessage = defaultMessage;
  }

  public int getId() {
    return _id;
  }

  public String getDefaultMessage() {
    return _defaultMessage;
  }

  /**
   * Returns the {@link QueryErrorCode} for the given error code.
   *
   * @param errorCode Error code
   * @return {@link QueryErrorCode} for the given error code or {@link QueryErrorCode#UNKNOWN} if the error code is not
   *        recognized
   */
  public static QueryErrorCode fromErrorCode(int errorCode) {
    if (errorCode < 0 || errorCode >= BY_ID.length) {
      throw new IllegalArgumentException("Invalid error code: " + errorCode);
    }
    QueryErrorCode queryErrorCode = BY_ID[errorCode];
    if (queryErrorCode == null) {
      LOGGER.warn("No QueryErrorCode found for error code: {}", errorCode);
      return UNKNOWN;
    }
    return queryErrorCode;
  }

  public static <T> Map<QueryErrorCode, T> fromKeyMap(Map<Integer, T> originalMap) {
    EnumMap<QueryErrorCode, T> newMap = new EnumMap<>(QueryErrorCode.class);
    for (Map.Entry<Integer, T> entry : originalMap.entrySet()) {
      newMap.put(QueryErrorCode.fromErrorCode(entry.getKey()), entry.getValue());
    }
    return newMap;
  }

  public QueryException asException() {
    return new QueryException(this);
  }

  public QueryException asException(String message) {
    return new QueryException(this, message);
  }

  public QueryException asException(Throwable cause) {
    return new QueryException(this, cause);
  }

  public QueryException asException(String message, Throwable cause) {
    return new QueryException(this, message, cause);
  }

  public boolean isClientError() {
    switch (this) {
      // NOTE: QueryException.BROKER_RESOURCE_MISSING_ERROR can be triggered due to issues
      // with EV updates. For cases where access to tables is controlled via ACLs, for an
      // incorrect table name we expect ACCESS_DENIED_ERROR to be thrown. Hence, we currently
      // don't treat BROKER_RESOURCE_MISSING_ERROR as client error.
      case ACCESS_DENIED:
      case JSON_PARSING:
      case QUERY_CANCELLATION:
      case QUERY_VALIDATION:
      case SERVER_OUT_OF_CAPACITY:
      case SERVER_RESOURCE_LIMIT_EXCEEDED:
      case SQL_PARSING:
      case TOO_MANY_REQUESTS:
      case TABLE_DOES_NOT_EXIST:
      case TABLE_IS_DISABLED:
      case UNKNOWN_COLUMN:
        return true;
      default:
        return false;
    }
  }

  public boolean isIncludeStackTrace() {
    switch (this) {
      case INTERNAL:
      case UNKNOWN:
        return true;
      default:
        return false;
    }
  }
}
