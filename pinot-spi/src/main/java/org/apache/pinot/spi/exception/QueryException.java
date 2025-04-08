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
 * Contrary to {@code org.apache.pinot.common.response.ProcessingException}, which is static and originally allocated
 * in a non-throwing and stack-trace-less exception, this exception is designed to be thrown and caught by the query
 * engine as any other standard Java exception. It also doesn't include any thrift-specific boilerplate.
 */
public class QueryException extends PinotRuntimeException {

  /**
   * The error code for the exception.
   */
  private final QueryErrorCode _errorCode;

  public QueryException(QueryErrorCode errorCode) {
    super(errorCode.getDefaultMessage());
    _errorCode = errorCode;
  }

  public QueryException(QueryErrorCode errorCode, String message) {
    super(message);
    _errorCode = errorCode;
  }

  public QueryException(QueryErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    _errorCode = errorCode;
  }

  public QueryException(QueryErrorCode errorCode, Throwable cause) {
    super(cause.getMessage(), cause);
    _errorCode = errorCode;
  }

  public QueryException(QueryErrorCode errorCode, String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
    _errorCode = errorCode;
  }

  public QueryErrorCode getErrorCode() {
    return _errorCode;
  }
}
