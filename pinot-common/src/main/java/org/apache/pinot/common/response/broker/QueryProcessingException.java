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
package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * This class represents an exception using a message and an error code.
 */
// TODO: Rename as QueryErrorMessage or something like that to avoid confusion with java.lang.Exception
public class QueryProcessingException {
  private int _errorCode;
  private String _message;

  public QueryProcessingException() {
  }

  public QueryProcessingException(int errorCode, String message) {
    _errorCode = errorCode;
    _message = message;
  }

  @JsonProperty("errorCode")
  public int getErrorCode() {
    return _errorCode;
  }

  @JsonProperty("errorCode")
  public void setErrorCode(int errorCode) {
    _errorCode = errorCode;
  }

  @JsonProperty("message")
  public String getMessage() {
    return _message;
  }

  @JsonProperty("message")
  public void setMessage(String message) {
    _message = message;
  }

  @Override
  public String toString() {
    return "{" + _errorCode + "=" + _message + '}';
  }
}
