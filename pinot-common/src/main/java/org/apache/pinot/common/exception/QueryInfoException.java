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
package org.apache.pinot.common.exception;

import org.apache.pinot.common.response.ProcessingException;


/**
 * Exception to contain info about QueryException errors.
 * Throwable version of {@link org.apache.pinot.common.response.broker.QueryProcessingException}
 */
public class QueryInfoException extends RuntimeException {
  private ProcessingException _processingException;

  public QueryInfoException(String message) {
    super(message);
  }

  public QueryInfoException(String message, Throwable cause) {
    super(message, cause);
  }

  public QueryInfoException(Throwable cause) {
    super(cause);
  }

  public ProcessingException getProcessingException() {
    return _processingException;
  }

  public void setProcessingException(ProcessingException processingException) {
    _processingException = processingException;
  }
}
