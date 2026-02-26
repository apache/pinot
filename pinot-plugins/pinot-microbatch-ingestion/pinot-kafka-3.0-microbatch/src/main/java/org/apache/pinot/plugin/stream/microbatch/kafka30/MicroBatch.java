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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import org.apache.pinot.spi.stream.StreamMessageMetadata;

/**
 * Represents a microbatch message with protocol information
 */
public class MicroBatch {
  private final byte[] _key;
  private final int _unfilteredMessageCount;
  private final MicroBatchProtocol _protocol;
  private final StreamMessageMetadata _messageMetadata;
  private final boolean _hasDataLoss;
  private final int _recordsToSkip;

  public MicroBatch(
      byte[] key,
      int unfilteredMessageCount,
      MicroBatchProtocol protocol,
      StreamMessageMetadata messageMetadata,
      boolean hasDataLoss) {
    this(key, unfilteredMessageCount, protocol, messageMetadata, hasDataLoss, 0);
  }

  public MicroBatch(
      byte[] key,
      int unfilteredMessageCount,
      MicroBatchProtocol protocol,
      StreamMessageMetadata messageMetadata,
      boolean hasDataLoss,
      int recordsToSkip) {
    _key = key;
    _unfilteredMessageCount = unfilteredMessageCount;
    _protocol = protocol;
    _messageMetadata = messageMetadata;
    _hasDataLoss = hasDataLoss;
    _recordsToSkip = recordsToSkip;
  }

  public byte[] getKey() {
    return _key;
  }

  public MicroBatchProtocol getProtocol() {
    return _protocol;
  }

  public StreamMessageMetadata getMessageMetadata() {
    return _messageMetadata;
  }

  public int getUnfilteredMessageCount() {
    return _unfilteredMessageCount;
  }

  public boolean hasDataLoss() {
    return _hasDataLoss;
  }

  /**
   * Number of records to skip when reading this microbatch.
   * Used for mid-batch resume after a segment commit.
   */
  public int getRecordsToSkip() {
    return _recordsToSkip;
  }
}
