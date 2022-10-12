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
package org.apache.pinot.spi.stream;

import javax.annotation.Nullable;


/**
 * Container for holding the current consumption state at a per-partition level
 */
public class ConsumerPartitionState {
  private final String _partitionId;
  private final StreamPartitionMsgOffset _currentOffset;
  private final long _lastProcessedTimeMs;
  private final StreamPartitionMsgOffset _upstreamLatestOffset;
  private final RowMetadata _lastProcessedRowMetadata;

  public ConsumerPartitionState(String partitionId, StreamPartitionMsgOffset currentOffset, long lastProcessedTimeMs,
      @Nullable StreamPartitionMsgOffset upstreamLatestOffset, @Nullable RowMetadata lastProcessedRowMetadata) {
    _partitionId = partitionId;
    _currentOffset = currentOffset;
    _lastProcessedTimeMs = lastProcessedTimeMs;
    _upstreamLatestOffset = upstreamLatestOffset;
    _lastProcessedRowMetadata = lastProcessedRowMetadata;
  }

  public StreamPartitionMsgOffset getCurrentOffset() {
    return _currentOffset;
  }

  public long getLastProcessedTimeMs() {
    return _lastProcessedTimeMs;
  }

  public String getPartitionId() {
    return _partitionId;
  }

  public StreamPartitionMsgOffset getUpstreamLatestOffset() {
    return _upstreamLatestOffset;
  }

  public RowMetadata getLastProcessedRowMetadata() {
    return _lastProcessedRowMetadata;
  }
}
