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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


public class QueryErrorMessage {
  /**
   * Error code for the exception, usually obtained from {@link QueryException}.
   */
  private final QueryErrorCode _errCode;
  /**
   * User facing error message.
   *
   * This message is intended to be shown to the user and should not contain implementation details (including stack
   * traces or class names).
   */
  private final String _usrMsg;
  /**
   * Log message for the exception.
   *
   * This message is intended to be logged and may contain implementation details (including stack traces or class
   * names) but it must never contain any sensitive information (including user data).
   */
  private final String _logMsg;

  @JsonCreator
  public QueryErrorMessage(
      @JsonProperty("code") QueryErrorCode errCode,
      @JsonProperty("usr") @Nullable String usrMsg,
      @JsonProperty("log") @Nullable String logMsg) {
    _errCode = errCode;
    _usrMsg = usrMsg != null ? usrMsg : errCode.getDefaultMessage();
    _logMsg = logMsg != null ? logMsg : errCode.getDefaultMessage();
  }

  /**
   * Create a new QueryErrorMessage with the given error code and user message.
   *
   * Remember that the message must be safe to show to the user and should not contain any sensitive information.
   */
  public static QueryErrorMessage safeMsg(QueryErrorCode errorCode, @Nullable String safeMessage) {
    return new QueryErrorMessage(errorCode, safeMessage, safeMessage);
  }

  @JsonProperty("code")
  public QueryErrorCode getErrCode() {
    return _errCode;
  }

  @JsonProperty("usr")
  public String getUsrMsg() {
    return _usrMsg;
  }

  @JsonProperty("log")
  public String getLogMsg() {
    return _logMsg;
  }

  @Override
  public String toString() {
    ObjectNode object = JsonUtils.newObjectNode()
        .put("code", _errCode.name())
        .put("log", _logMsg);
    try {
      return JsonUtils.objectToString(object);
    } catch (JsonProcessingException e) {
      return "{\"e\": " + _errCode + "}";
    }
  }
}
