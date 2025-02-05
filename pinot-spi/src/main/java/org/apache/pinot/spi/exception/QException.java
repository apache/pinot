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

/**
 * The base exception for query processing in Pinot.
 *
 * This exception is captured by the query engine and converted to a user-friendly message in a standardized format
 * (ie converted into a JSON payload in an HTTP response).
 *
 * Contrary to {@code org.apache.pinot.common.response.ProcessingException}, which is usually designed to be thrown by
 * {@code org.apache.pinot.common.exception.QueryException} in a non-allocating, non-throwing and stack-trace-less
 * exception, this exception is designed to be thrown and caught by the query engine as any other standard Java
 * exception.
 */
public class QException extends PinotRuntimeException {
  public static final int JSON_PARSING_ERROR_CODE = 100;
  public static final int SQL_PARSING_ERROR_CODE = 150;
  public static final int SQL_RUNTIME_ERROR_CODE = 160;
  public static final int ACCESS_DENIED_ERROR_CODE = 180;
  public static final int TABLE_DOES_NOT_EXIST_ERROR_CODE = 190;
  public static final int TABLE_IS_DISABLED_ERROR_CODE = 191;
  public static final int QUERY_EXECUTION_ERROR_CODE = 200;
  public static final int QUERY_CANCELLATION_ERROR_CODE = 503;
  public static final int SERVER_SHUTTING_DOWN_ERROR_CODE = 210;
  public static final int SERVER_OUT_OF_CAPACITY_ERROR_CODE = 211;
  public static final int SERVER_TABLE_MISSING_ERROR_CODE = 230;
  public static final int SERVER_SEGMENT_MISSING_ERROR_CODE = 235;
  public static final int QUERY_SCHEDULING_TIMEOUT_ERROR_CODE = 240;
  public static final int SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE = 245;
  public static final int EXECUTION_TIMEOUT_ERROR_CODE = 250;
  public static final int BROKER_SEGMENT_UNAVAILABLE_ERROR_CODE = 305;
  public static final int BROKER_TIMEOUT_ERROR_CODE = 400;
  public static final int BROKER_RESOURCE_MISSING_ERROR_CODE = 410;
  public static final int BROKER_INSTANCE_MISSING_ERROR_CODE = 420;
  public static final int BROKER_REQUEST_SEND_ERROR_CODE = 425;
  public static final int SERVER_NOT_RESPONDING_ERROR_CODE = 427;
  public static final int TOO_MANY_REQUESTS_ERROR_CODE = 429;
  public static final int INTERNAL_ERROR_CODE = 450;
  public static final int MERGE_RESPONSE_ERROR_CODE = 500;
  public static final int QUERY_VALIDATION_ERROR_CODE = 700;
  public static final int UNKNOWN_COLUMN_ERROR_CODE = 710;
  public static final int QUERY_PLANNING_ERROR_CODE = 720;
  public static final int UNKNOWN_ERROR_CODE = 1000;

  /**
   * The error code for the exception.
   */
  private final int _errorCode;

  public QException() {
    _errorCode = UNKNOWN_ERROR_CODE;
  }

  public QException(int errorCode) {
    _errorCode = errorCode;
  }

  public QException(String message) {
    super(message);
    _errorCode = UNKNOWN_ERROR_CODE;
  }

  public QException(int errorCode, String message) {
    super(message);
    _errorCode = errorCode;
  }

  public QException(String message, Throwable cause) {
    super(message, cause);
    _errorCode = UNKNOWN_ERROR_CODE;
  }

  public QException(int errorCode, String message, Throwable cause) {
    super(message, cause);
    _errorCode = errorCode;
  }

  public QException(Throwable cause) {
    super(cause);
    _errorCode = UNKNOWN_ERROR_CODE;
  }

  public QException(int errorCode, Throwable cause) {
    super(cause);
    _errorCode = errorCode;
  }

  public int getErrorCode() {
    return _errorCode;
  }
}
