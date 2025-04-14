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
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.JsonUtils;


/// A block that represents a failed execution.
///
public class ErrorMseBlock implements MseBlock.Eos {
  private final EnumMap<QueryErrorCode, String> _errorMessages;

  public ErrorMseBlock(Map<QueryErrorCode, String> errorMessages) {
    Preconditions.checkArgument(!errorMessages.isEmpty(), "Error messages cannot be empty");
    _errorMessages = new EnumMap<>(errorMessages);
  }

  public static ErrorMseBlock fromException(Exception e) {
    QueryErrorCode errorCode;
    if (e instanceof QueryException) {
      errorCode = ((QueryException) e).getErrorCode();
    } else if (e instanceof ProcessingException) {
      errorCode = QueryErrorCode.fromErrorCode(((ProcessingException) e).getErrorCode());
    } else {
      errorCode = QueryErrorCode.UNKNOWN;
    }
    String errorMessage = errorCode.isIncludeStackTrace() ? DataBlockUtils.extractErrorMsg(e) : e.getMessage();
    return new ErrorMseBlock(Collections.singletonMap(errorCode, errorMessage));
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
