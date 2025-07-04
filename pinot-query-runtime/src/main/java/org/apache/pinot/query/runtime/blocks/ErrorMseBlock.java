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
package org.apache.pinot.query.runtime.blocks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.ExceptionUtils;
import org.apache.pinot.query.MseWorkerThreadContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.JsonUtils;


/// A block that represents a failed execution.
///
public class ErrorMseBlock implements MseBlock.Eos {
  private final int _stageId;
  private final int _workerId;
  private final String _serverId;
  private final EnumMap<QueryErrorCode, String> _errorMessages;

  /// Kept for backward compatibility.
  ///
  /// @deprecated Use [#fromMap(Map)] instead
  @Deprecated
  public ErrorMseBlock(Map<QueryErrorCode, String> errorMessages) {
    this(-1, -1, null, errorMessages);
  }

  public ErrorMseBlock(int stageId, int workerId, String serverId, Map<QueryErrorCode, String> errorMessages) {
    _stageId = stageId;
    _workerId = workerId;
    _serverId = serverId;
    Preconditions.checkArgument(!errorMessages.isEmpty(), "Error messages cannot be empty");
    _errorMessages = new EnumMap<>(errorMessages);
  }

  public static ErrorMseBlock fromMap(Map<QueryErrorCode, String> errorMessages) {
    int stage;
    int worker;
    String server = QueryThreadContext.isInitialized() ? QueryThreadContext.getInstanceId() : "unknown";
    if (MseWorkerThreadContext.isInitialized()) {
      stage = MseWorkerThreadContext.getStageId();
      worker = MseWorkerThreadContext.getWorkerId();
    } else {
      stage = -1; // Default value when not initialized
      worker = -1; // Default value when not initialized
    }
    return new ErrorMseBlock(stage, worker, server, errorMessages);
  }

  public static ErrorMseBlock fromException(Exception e) {
    QueryErrorCode errorCode;
    boolean extractTrace;
    if (e instanceof QueryException) {
      errorCode = ((QueryException) e).getErrorCode();
      extractTrace = false;
    } else if (e instanceof ProcessingException) {
      errorCode = QueryErrorCode.fromErrorCode(((ProcessingException) e).getErrorCode());
      extractTrace = true;
    } else {
      errorCode = QueryErrorCode.UNKNOWN;
      extractTrace = true;
    }
    String errorMessage = extractTrace ? ExceptionUtils.consolidateExceptionMessages(e) : e.getMessage();
    return fromMap(Collections.singletonMap(errorCode, errorMessage));
  }

  public static ErrorMseBlock fromError(QueryErrorCode errorCode, String errorMessage) {
    return fromMap(Collections.singletonMap(errorCode, errorMessage));
  }

  @Override
  public boolean isError() {
    return true;
  }

  /// The error messages associated with the block.
  /// The keys are the error codes and the values are the error messages.
  /// It is guaranteed that the map is not empty.
  public Map<QueryErrorCode, String> getErrorMessages() {
    return _errorMessages;
  }

  /// Returns the stage where the error occurred, or -1 if the server wasn't able to calculate that.
  public int getStageId() {
    return _stageId;
  }

  /// Returns the worker where the error occurred, or -1 if the server wasn't able to calculate that.
  public int getWorkerId() {
    return _workerId;
  }

  /// Returns the server ID where the error occurred, or null if the server wasn't able to calculate that.
  @Nullable
  public String getServerId() {
    return _serverId;
  }

  @Override
  public <R, A> R accept(Visitor<R, A> visitor, A arg) {
    return visitor.visit(this, arg);
  }

  @Override
  public String toString() {
    try {
      ObjectNode root = JsonUtils.newObjectNode();
      root.put("type", "error");
      root.put("errorMessages", JsonUtils.objectToJsonNode(_errorMessages));
      return JsonUtils.objectToString(root);
    } catch (JsonProcessingException e) {
      return "{\"type\": \"error\", \"errorMessages\": \"not serializable\"}";
    }
  }
}
