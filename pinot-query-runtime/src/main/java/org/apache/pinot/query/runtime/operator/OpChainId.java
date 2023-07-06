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
package org.apache.pinot.query.runtime.operator;

import java.util.Objects;


public class OpChainId {
  final long _requestId;
  final int _virtualServerId;
  final int _stageId;

  public OpChainId(long requestId, int virtualServerId, int stageId) {
    _requestId = requestId;
    _virtualServerId = virtualServerId;
    _stageId = stageId;
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getVirtualServerId() {
    return _virtualServerId;
  }

  @Override
  public String toString() {
    return String.format("%s_%s_%s", _requestId, _virtualServerId, _stageId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OpChainId opChainId = (OpChainId) o;
    return _requestId == opChainId._requestId && _virtualServerId == opChainId._virtualServerId
        && _stageId == opChainId._stageId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_requestId, _virtualServerId, _stageId);
  }
}
