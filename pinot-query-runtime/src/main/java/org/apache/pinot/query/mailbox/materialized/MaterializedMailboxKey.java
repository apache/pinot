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
package org.apache.pinot.query.mailbox.materialized;

import java.util.Objects;


/// Consumer-independent identity of one producer-local materialized partition.
public final class MaterializedMailboxKey {
  private final long _requestId;
  private final int _producerStageId;
  private final int _producerWorkerId;
  private final int _logicalPartitionId;

  public MaterializedMailboxKey(long requestId, int producerStageId, int producerWorkerId, int logicalPartitionId) {
    _requestId = requestId;
    _producerStageId = producerStageId;
    _producerWorkerId = producerWorkerId;
    _logicalPartitionId = logicalPartitionId;
  }

  public long getRequestId() {
    return _requestId;
  }

  public int getProducerStageId() {
    return _producerStageId;
  }

  public int getProducerWorkerId() {
    return _producerWorkerId;
  }

  public int getLogicalPartitionId() {
    return _logicalPartitionId;
  }

  public String toOpaqueFileId() {
    return _requestId + "/" + _producerStageId + "/" + _producerWorkerId + "/" + _logicalPartitionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MaterializedMailboxKey)) {
      return false;
    }
    MaterializedMailboxKey that = (MaterializedMailboxKey) o;
    return _requestId == that._requestId && _producerStageId == that._producerStageId
        && _producerWorkerId == that._producerWorkerId && _logicalPartitionId == that._logicalPartitionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_requestId, _producerStageId, _producerWorkerId, _logicalPartitionId);
  }

  @Override
  public String toString() {
    return toOpaqueFileId();
  }
}
