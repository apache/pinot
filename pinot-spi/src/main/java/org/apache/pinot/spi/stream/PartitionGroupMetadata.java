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

/**
 * A PartitionGroup is a group of partitions/shards that the same consumer should consume from.
 * This class is a container for the metadata regarding a partition group, that is needed by a consumer to start
 * consumption.
 * It consists of:
 * 1. A unique partition group id for this partition group
 * 2. The start offset to begin consumption for this partition group
 * 3. The sequence number for the consuming segment (used when creating segments with designated offsets/sequences)
 * 4. The id of the topic (stream config) this partition group belongs to, for tables with multiple topics
 */
public class PartitionGroupMetadata {

  private static final int DEFAULT_SEQUENCE_NUMBER = -1;

  private final int _partitionGroupId;
  private final StreamPartitionMsgOffset _startOffset;
  private final int _sequenceNumber;
  private final int _topicId;

  public PartitionGroupMetadata(int partitionGroupId, StreamPartitionMsgOffset startOffset) {
    this(partitionGroupId, startOffset, DEFAULT_SEQUENCE_NUMBER);
  }

  public PartitionGroupMetadata(int partitionGroupId, StreamPartitionMsgOffset startOffset, int sequenceNumber) {
    this(partitionGroupId, 0, startOffset, sequenceNumber);
  }

  public PartitionGroupMetadata(int partitionGroupId, int topicId, StreamPartitionMsgOffset startOffset,
      int sequenceNumber) {
    _partitionGroupId = partitionGroupId;
    _topicId = topicId;
    _startOffset = startOffset;
    _sequenceNumber = sequenceNumber;
  }

  public int getPartitionGroupId() {
    return _partitionGroupId;
  }

  public int getTopicId() {
    return _topicId;
  }

  public StreamPartitionMsgOffset getStartOffset() {
    return _startOffset;
  }

  public int getSequenceNumber() {
    return _sequenceNumber;
  }
}
