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

import com.google.common.base.Preconditions;
import java.util.Objects;


/// Composite identity for one logical stream partition.
///
/// V1 identities wrap the integer persisted in a 4-part LLC name. That integer may be a raw stream
/// partition or a packed `streamIndex * 10000 + partitionId`. This class does not pack, unpad, or
/// guess which case it is.
///
/// V2 identities are `(topicId, partitionId)` as stored in a `table__v2__...` name. Topic id is the
/// stable registry id, not the current `streamConfigMaps` list index.
///
/// Equality is injective: V1 packed `10000`, V2 `(0, 10000)`, and V2 `(1, 0)` are three values.
/// Use this type as a map key instead of a packed int. Do not add a
/// `topicId * 10000 + partitionId` helper here. `IngestionConfigUtils.PARTITION_PADDING_OFFSET`
/// stays on the V1 write path only.
///
/// `LLCSegmentName.getPartitionGroupId()` remains an `int` and is V1-only. This type is the
/// wrapper not to return from that getter (see apache/pinot#18913). Prefer this type at new call
/// sites; do not add a parallel `TopicPartitionId` type.
///
/// Immutable and safe to share across threads.
public final class StreamPartitionIdentity implements Comparable<StreamPartitionIdentity> {
  public static final int FORMAT_VERSION_V1 = 1;
  public static final int FORMAT_VERSION_V2 = 2;

  /// Sentinel topic id for V1 names, which do not store a topic id. Never infer a topic from
  /// `{@code partitionGroupId % 10000}`.
  public static final int UNKNOWN_TOPIC_ID = -1;

  /// Sentinel raw partition id on V1 names. The persisted value lives in [getPartitionId] as the
  /// V1 partition-group integer.
  public static final int UNKNOWN_PARTITION_ID = -1;

  private final int _formatVersion;
  private final int _topicId;
  private final int _partitionId;

  private StreamPartitionIdentity(int formatVersion, int topicId, int partitionId) {
    _formatVersion = formatVersion;
    _topicId = topicId;
    _partitionId = partitionId;
  }

  /// V1 identity for a persisted partition-group int. `partitionGroupId` is stored as-is.
  public static StreamPartitionIdentity v1(int partitionGroupId) {
    return new StreamPartitionIdentity(FORMAT_VERSION_V1, UNKNOWN_TOPIC_ID, partitionGroupId);
  }

  /// V2 identity. Both ids are non-negative decimal ints from the segment name.
  public static StreamPartitionIdentity v2(int topicId, int partitionId) {
    Preconditions.checkArgument(topicId >= 0, "V2 topicId must be non-negative: %s", topicId);
    Preconditions.checkArgument(partitionId >= 0, "V2 partitionId must be non-negative: %s", partitionId);
    return new StreamPartitionIdentity(FORMAT_VERSION_V2, topicId, partitionId);
  }

  public int getFormatVersion() {
    return _formatVersion;
  }

  public boolean isV2() {
    return _formatVersion == FORMAT_VERSION_V2;
  }

  /// Topic id for V2, or [UNKNOWN_TOPIC_ID] for V1.
  public int getTopicId() {
    return _topicId;
  }

  /// Raw stream partition for V2, or the persisted V1 partition-group int.
  public int getPartitionId() {
    return _partitionId;
  }

  @Override
  public int compareTo(StreamPartitionIdentity other) {
    int versionCmp = Integer.compare(_formatVersion, other._formatVersion);
    if (versionCmp != 0) {
      return versionCmp;
    }
    int topicCmp = Integer.compare(_topicId, other._topicId);
    if (topicCmp != 0) {
      return topicCmp;
    }
    return Integer.compare(_partitionId, other._partitionId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamPartitionIdentity)) {
      return false;
    }
    StreamPartitionIdentity that = (StreamPartitionIdentity) o;
    return _formatVersion == that._formatVersion && _topicId == that._topicId && _partitionId == that._partitionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_formatVersion, _topicId, _partitionId);
  }

  @Override
  public String toString() {
    if (isV2()) {
      return "v2:topicId=" + _topicId + ",partitionId=" + _partitionId;
    }
    return "v1:partitionGroupId=" + _partitionId;
  }
}
