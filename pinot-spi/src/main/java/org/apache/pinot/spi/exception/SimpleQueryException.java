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
 * A Pinot exception where contextual information is not relevant.
 *
 * This exception is used in scenarios where the stack trace is not relevant to the exception. For example, when a
 * mailbox is not capable of offering or polling data from a queue. On these scenarios the stack trace we already know
 * what the code was doing, and we don't need to flood logs with unnecessary stack traces.
 */
public class SimpleQueryException extends QueryException {

  public SimpleQueryException(QueryErrorCode errorCode, String message) {
    super(errorCode, message, null, false, false);
  }

  public SimpleQueryException(String message) {
    this(QueryErrorCode.UNKNOWN, message);
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

  @Override
  public String toString() {
    return getClass().getName() + ": " + getMessage();
  }
}
