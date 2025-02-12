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
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryErrorMessage;


/**
 * This class represents an exception using a message and an error code.
 *
 * This is only used to serialize the error message and error code when a broker sends an error message to the client.
 * In other cases use {@link QueryErrorMessage} instead.
 */
public class BrokerQueryErrorMessage {
  private int _errorCode;
  private String _message;

  public BrokerQueryErrorMessage(int errorCode, String message) {
    _errorCode = errorCode;
    _message = message;
  }

  public BrokerQueryErrorMessage(QueryErrorCode errorCode, String message) {
    _errorCode = errorCode.getId();
    _message = message == null ? errorCode.getDefaultMessage() : message;
  }

  public static BrokerQueryErrorMessage fromQueryErrorMessage(QueryErrorMessage queryErrorMessage) {
    return new BrokerQueryErrorMessage(queryErrorMessage.getErrCode(), queryErrorMessage.getUsrMsg());
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
