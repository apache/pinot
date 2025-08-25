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
 * A PartitionGroup is a group of partitions/shards that the same consumer should consume from.
 * This class is a container for the metadata regarding a partition group, that is needed by a consumer to start
 * consumption.
 * It consists of
 * 1. A unique partition group id for this partition group
 * 2. The start offset to begin consumption for this partition group
 */
public class PartitionGroupMetadata {

  private static final String SEPARATOR = "__";
  private final int _partitionGroupId;
  @Nullable
  private final String _topicName;
  private final StreamPartitionMsgOffset _startOffset;

  public PartitionGroupMetadata(int partitionGroupId, StreamPartitionMsgOffset startOffset) {
    this(null, partitionGroupId, startOffset);
  }

  public PartitionGroupMetadata(@Nullable String topicName, int partitionGroupId,
      StreamPartitionMsgOffset startOffset) {
    _topicName = topicName;
    _partitionGroupId = partitionGroupId;
    _startOffset = startOffset;
  }

  public int getPartitionGroupId() {
    return _partitionGroupId;
  }

  public String getTopicName() {
    if (_topicName == null || _topicName.isEmpty()) {
      return null;
    }
    return _topicName;
  }

  public String getPartitionGroupTopicAndId() {
    if (_topicName == null || _topicName.isEmpty()) {
      return String.valueOf(_partitionGroupId);
    } else {
      return _topicName + SEPARATOR + _partitionGroupId;
    }
  }

  public StreamPartitionMsgOffset getStartOffset() {
    return _startOffset;
  }
}
