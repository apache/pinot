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
import java.util.EnumSet;
import java.util.Map;
import javax.annotation.Nonnegative;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// The error codes used by Pinot queries.
///
/// When updated, remember to update Query.tsx in pinot-ui as well.
public enum QueryErrorCode {
  /// Request in JSON format is incorrect. For example, doesn't contain the expected 'sql' field.
  JSON_PARSING(100, "JsonParsingError", Response.Status.BAD_REQUEST),
  /// Error detected at parsing time. For example, syntax error.
  SQL_PARSING(150, "SQLParsingError", Response.Status.BAD_REQUEST),
  TIMESERIES_PARSING(155, "TimeseriesParsingError", Response.Status.BAD_REQUEST),
  SQL_RUNTIME(160, "SQLRuntimeError", Response.Status.INTERNAL_SERVER_ERROR),
  ACCESS_DENIED(180, "AccessDenied", Response.Status.FORBIDDEN),
  TABLE_DOES_NOT_EXIST(190, "TableDoesNotExistError", Response.Status.NOT_FOUND),
  TABLE_IS_DISABLED(191, "TableIsDisabledError", Response.Status.NOT_FOUND),
  /// Error at query execution time. For example, String column cast to int when contains a non-integer value.
  QUERY_EXECUTION(200, "QueryExecutionError", Response.Status.INTERNAL_SERVER_ERROR),
  SERVER_SHUTTING_DOWN(210, "ServerShuttingDown", Response.Status.SERVICE_UNAVAILABLE),
  SERVER_OUT_OF_CAPACITY(211, "ServerOutOfCapacity", Response.Status.SERVICE_UNAVAILABLE),
  SERVER_TABLE_MISSING(230, "ServerTableMissing", Response.Status.NOT_FOUND),
  SERVER_SEGMENT_MISSING(235, "ServerSegmentMissing", Response.Status.NOT_FOUND),
  QUERY_SCHEDULING_TIMEOUT(240, "QuerySchedulingTimeoutError", Response.Status.REQUEST_TIMEOUT),
  SERVER_RESOURCE_LIMIT_EXCEEDED(245, "ServerResourceLimitExceededError", Response.Status.SERVICE_UNAVAILABLE),
  EXECUTION_TIMEOUT(250, "ExecutionTimeoutError", Response.Status.REQUEST_TIMEOUT),
  BROKER_SEGMENT_UNAVAILABLE(305, "", Response.Status.SERVICE_UNAVAILABLE),
  BROKER_TIMEOUT(400, "BrokerTimeoutError", Response.Status.REQUEST_TIMEOUT),
  BROKER_RESOURCE_MISSING(410, "BrokerResourceMissingError", Response.Status.SERVICE_UNAVAILABLE),
  BROKER_INSTANCE_MISSING(420, "BrokerInstanceMissingError", Response.Status.SERVICE_UNAVAILABLE),
  BROKER_REQUEST_SEND(425, "BrokerRequestSend", Response.Status.SERVICE_UNAVAILABLE),
  SERVER_NOT_RESPONDING(427, "ServerNotResponding", Response.Status.SERVICE_UNAVAILABLE),
  TOO_MANY_REQUESTS(429, "TooManyRequests", Response.Status.TOO_MANY_REQUESTS),
  WORKLOAD_BUDGET_EXCEEDED(429, "WorkloadBudgetExceededError", Response.Status.TOO_MANY_REQUESTS),
  INTERNAL(450, "InternalError", Response.Status.INTERNAL_SERVER_ERROR),
  MERGE_RESPONSE(500, "MergeResponseError", Response.Status.INTERNAL_SERVER_ERROR),
  QUERY_CANCELLATION(503, "QueryCancellationError", Response.Status.SERVICE_UNAVAILABLE),
  REMOTE_CLUSTER_UNAVAILABLE(510, "RemoteClusterUnavailable", Response.Status.SERVICE_UNAVAILABLE),
  /// Error detected at validation time. For example, type mismatch.
  QUERY_VALIDATION(700, "QueryValidationError", Response.Status.BAD_REQUEST),
  UNKNOWN_COLUMN(710, "UnknownColumnError", Response.Status.BAD_REQUEST),
  ///  Error while planning the query. For example, trying to run a colocated join on non-colocated tables.
  QUERY_PLANNING(720, "QueryPlanningError", Response.Status.BAD_REQUEST),
  UNKNOWN(1000, "UnknownError", Response.Status.INTERNAL_SERVER_ERROR);
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryErrorCode.class);

  private static final QueryErrorCode[] BY_ID;

  // Static set of SLA-critical (system) error codes
  private static final EnumSet<QueryErrorCode> CRITICAL_ERROR_CODES = EnumSet.of(
      SQL_RUNTIME,
      INTERNAL,
      QUERY_SCHEDULING_TIMEOUT,
      EXECUTION_TIMEOUT,
      BROKER_TIMEOUT,
      SERVER_SEGMENT_MISSING,
      BROKER_SEGMENT_UNAVAILABLE,
      SERVER_NOT_RESPONDING,
      BROKER_REQUEST_SEND,
      MERGE_RESPONSE,
      QUERY_CANCELLATION,
      SERVER_SHUTTING_DOWN,
      QUERY_PLANNING
  );

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
  @Nonnegative
  private final int _id;
  private final String _defaultMessage;
  private final Response.Status _httpResponseStatus;

  QueryErrorCode(int errorCode, String defaultMessage) {
    this(errorCode, defaultMessage, Response.Status.INTERNAL_SERVER_ERROR);
  }

  QueryErrorCode(int errorCode, String defaultMessage, Response.Status httpResponseStatus) {
    _id = errorCode;
    _defaultMessage = defaultMessage;
    _httpResponseStatus = httpResponseStatus;
  }

  public int getId() {
    return _id;
  }

  public String getDefaultMessage() {
    return _defaultMessage;
  }

  public Response.Status getHttpResponseStatus() {
    return _httpResponseStatus;
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
      if (entry.getKey() < 0) {
        continue;
      }
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

  /**
   * Returns true if the error is considered critical for SLA accounting.
   * Critical errors represent system-side failures (timeouts, internal errors, infra issues, etc.).
   */
  public boolean isCriticalError() {
    return CRITICAL_ERROR_CODES.contains(this);
  }
}
