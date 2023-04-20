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
package org.apache.pinot.common.utils.request;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Joiner;
import javax.annotation.Nullable;

public class OperatorId implements Comparable<OperatorId> {
  public static final String SEPERATOR = "__";
  private final long _requestId;
  private final int _stageId;
  private final String _server;
  private final String _operatorName;
  private Integer _operatorIndex;

  public OperatorId(long requestId, int stageId, String server, String operatorName, @Nullable Integer operatorIndex) {
    _requestId = requestId;
    _stageId = stageId;
    _operatorIndex = operatorIndex;
    _server = server;
    _operatorName = operatorName;
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getStageId() {
    return _stageId;
  }

  public Integer getOperatorIndex() {
    return _operatorIndex;
  }

  public String getServer() {
    return _server;
  }

  public String getOperatorName() {
    return _operatorName;
  }

  public void setOperatorIndex(Integer operatorIndex) {
    _operatorIndex = operatorIndex;
  }

  public static OperatorId fromString(String str) {
    String[] parts = str.split(SEPERATOR);
    if (parts.length < 4) {
      throw new IllegalArgumentException("Invalid string representation of OperatorId");
    }

    String operatorName = parts[0];

    long requestId = Long.parseLong(parts[1]);
    int stageId = Integer.parseInt(parts[2]);
    String server = parts[3];

    Integer operatorIndex = null;
    if (parts.length == 5) {
      operatorIndex = Integer.parseInt(parts[4]);
    }

    return new OperatorId(requestId, stageId, server, operatorName, operatorIndex);
  }

  @JsonValue
  @Override
  public String toString() {
    if (_operatorIndex == null) {
      return Joiner.on("__").join(_operatorName, _requestId, _stageId, _server);
    } else {
      return Joiner.on("__").join(_operatorName, _requestId, _stageId, _server, _operatorIndex);
    }
  }

  @Override
  public int compareTo(OperatorId other) {
    // Compare stageId first
    int stageIdComparison = Integer.compare(_stageId, other.getStageId());
    if (stageIdComparison != 0) {
      return stageIdComparison;
    }

    // Compare server third
    int serverComparison = _server.compareTo(other.getServer());
    if (serverComparison != 0) {
      return serverComparison;
    }

    // Compare operatorIndex second
    if (_operatorIndex != null && other.getOperatorIndex() != null) {
      int operatorIndexComparison = _operatorIndex.compareTo(other.getOperatorIndex());
      if (operatorIndexComparison != 0) {
        return operatorIndexComparison;
      }
    }

    // Compare operatorName fourth
    int operatorNameComparison = _operatorName.compareTo(other.getOperatorName());
    if (operatorNameComparison != 0) {
      return operatorNameComparison;
    }

    // Compare requestId last
    return Long.compare(_requestId, other.getRequestId());
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof OperatorId) {
      return compareTo((OperatorId) obj) == 0;
    }
    return false;
  }
}
