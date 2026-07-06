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
package org.apache.pinot.common.utils;

import com.google.common.base.Preconditions;
import java.util.Objects;


/**
 * Identifies a partition within a (possibly multi-topic) real-time table.
 *
 * <p>For single-topic tables, {@code topicId} is 0 and {@code partitionId} is the stream partition number.
 * For multi-topic tables, {@code topicId} distinguishes the topic and {@code partitionId} is the stream partition
 * number within that topic.
 *
 * <p>Thread-safe: instances are immutable.
 */
public final class TopicPartitionId implements Comparable<TopicPartitionId> {
  // Must match IngestionConfigUtils.PARTITION_PADDING_OFFSET in pinot-spi
  public static final int PARTITION_PADDING_OFFSET = 10000;

  private final int _topicId;
  private final int _partitionId;

  public TopicPartitionId(int topicId, int partitionId) {
    _topicId = topicId;
    _partitionId = partitionId;
  }

  public TopicPartitionId(int partitionId) {
    this(0, partitionId);
  }

  public int getTopicId() {
    return _topicId;
  }

  public int getPartitionId() {
    return _partitionId;
  }

  @Override
  public int compareTo(TopicPartitionId other) {
    int cmp = Integer.compare(_topicId, other._topicId);
    return cmp != 0 ? cmp : Integer.compare(_partitionId, other._partitionId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TopicPartitionId)) {
      return false;
    }
    TopicPartitionId that = (TopicPartitionId) o;
    return _topicId == that._topicId && _partitionId == that._partitionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_topicId, _partitionId);
  }

  /** Recomposes the composite Pinot partition ID: {@code topicId * 10000 + partitionId}. */
  public int toMultiTopicPinotPartitionId() {
    return _topicId * PARTITION_PADDING_OFFSET + _partitionId;
  }

  /** Decomposes a composite Pinot partition ID into its topic and partition components. */
  public static TopicPartitionId fromMultiTopicPinotPartitionId(int pinotPartitionId) {
    Preconditions.checkArgument(pinotPartitionId >= 0, "Negative Pinot partition ID: %s", pinotPartitionId);
    return new TopicPartitionId(pinotPartitionId / PARTITION_PADDING_OFFSET,
        pinotPartitionId % PARTITION_PADDING_OFFSET);
  }

  /**
   * Wraps a partition group ID from stream metadata into a {@link TopicPartitionId}.
   * When {@code hasMultipleStreams} is true and the ID is composite-encoded (>= 10000),
   * decomposes it into topic and partition components.
   */
  public static TopicPartitionId fromPartitionGroupMetadata(int partitionGroupId, boolean hasMultipleStreams) {
    if (hasMultipleStreams && partitionGroupId >= PARTITION_PADDING_OFFSET) {
      return fromMultiTopicPinotPartitionId(partitionGroupId);
    }
    return new TopicPartitionId(partitionGroupId);
  }

  @Override
  public String toString() {
    return _topicId + ":" + _partitionId;
  }
}
