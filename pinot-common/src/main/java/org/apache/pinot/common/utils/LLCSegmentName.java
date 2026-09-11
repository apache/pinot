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

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.stream.StreamPartitionIdentity;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/// Low-level consumer segment name.
///
/// V1 (4 tokens): `{rawTable}__{partitionGroupId}__{sequence}__{creationTime}`
///
/// V2 (6 tokens): `{rawTable}__v2__{topicId}__{partitionId}__{sequence}__{creationTime}`
///
/// The version token is the literal `v2`, so a name is self-describing without ZK. Uploaded
/// realtime names stay 5 tokens (`prefix__table__partition__time__suffix`) and are not LLC.
/// Do not accept a 5-token `table__configId__partition__seq__time` name (apache/pinot#18830).
///
/// V1 constructors and generated names are unchanged. Production writers must keep using the
/// 4-arg constructor. [formatV2] exists for parse/format tests and a later writer PR.
///
/// `getPartitionGroupId()` stays an `int` and is V1-only. New call sites should use
/// [getStreamPartitionIdentity] rather than a packed int or a `TopicPartitionId` wrapper
/// (apache/pinot#18913).
public class LLCSegmentName implements Comparable<LLCSegmentName> {
  public static final int FORMAT_VERSION_V1 = StreamPartitionIdentity.FORMAT_VERSION_V1;
  public static final int FORMAT_VERSION_V2 = StreamPartitionIdentity.FORMAT_VERSION_V2;

  private static final String SEPARATOR = "__";
  private static final String V2_TOKEN = "v2";
  private static final String DATE_FORMAT = "yyyyMMdd'T'HHmm'Z'";
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT).withZoneUTC();

  private final int _formatVersion;
  private final String _tableName;
  private final int _partitionGroupId;
  private final int _topicId;
  private final int _partitionId;
  private final int _sequenceNumber;
  private final String _creationTime;
  private final String _segmentName;

  public LLCSegmentName(String segmentName) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    if (parts.length == 4) {
      _formatVersion = FORMAT_VERSION_V1;
      _tableName = parts[0];
      _partitionGroupId = Integer.parseInt(parts[1]);
      _topicId = StreamPartitionIdentity.UNKNOWN_TOPIC_ID;
      _partitionId = StreamPartitionIdentity.UNKNOWN_PARTITION_ID;
      _sequenceNumber = Integer.parseInt(parts[2]);
      _creationTime = parts[3];
      _segmentName = segmentName;
      return;
    }
    Preconditions.checkArgument(isV2Parts(parts), "Invalid LLC segment name: %s", segmentName);
    _formatVersion = FORMAT_VERSION_V2;
    _tableName = parts[0];
    _partitionGroupId = Integer.MIN_VALUE;
    _topicId = parseNonNegativeInt(parts[2], "topicId", segmentName);
    _partitionId = parseNonNegativeInt(parts[3], "partitionId", segmentName);
    _sequenceNumber = Integer.parseInt(parts[4]);
    _creationTime = parts[5];
    _segmentName = segmentName;
  }

  public LLCSegmentName(String tableName, int partitionGroupId, int sequenceNumber, long msSinceEpoch) {
    Preconditions.checkArgument(!tableName.contains(SEPARATOR), "Illegal table name: %s", tableName);
    _formatVersion = FORMAT_VERSION_V1;
    _tableName = tableName;
    _partitionGroupId = partitionGroupId;
    _topicId = StreamPartitionIdentity.UNKNOWN_TOPIC_ID;
    _partitionId = StreamPartitionIdentity.UNKNOWN_PARTITION_ID;
    _sequenceNumber = sequenceNumber;
    // ISO8601 date: 20160120T1234Z
    _creationTime = DATE_FORMATTER.print(msSinceEpoch);
    _segmentName = tableName + SEPARATOR + partitionGroupId + SEPARATOR + sequenceNumber + SEPARATOR + _creationTime;
  }

  /// Formats a V2 LLC name for tests. Production writers must not emit V2 names.
  static String formatV2(String tableName, int topicId, int partitionId, int sequenceNumber, long msSinceEpoch) {
    Preconditions.checkArgument(!tableName.contains(SEPARATOR), "Illegal table name: %s", tableName);
    Preconditions.checkArgument(topicId >= 0, "V2 topicId must be non-negative: %s", topicId);
    Preconditions.checkArgument(partitionId >= 0, "V2 partitionId must be non-negative: %s", partitionId);
    String creationTime = DATE_FORMATTER.print(msSinceEpoch);
    return tableName + SEPARATOR + V2_TOKEN + SEPARATOR + topicId + SEPARATOR + partitionId + SEPARATOR + sequenceNumber
        + SEPARATOR + creationTime;
  }

  /// Returns the [LLCSegmentName] for the given V1 or V2 segment name, or `null` if the given
  /// segment name does not represent an LLC segment.
  @Nullable
  public static LLCSegmentName of(String segmentName) {
    try {
      return new LLCSegmentName(segmentName);
    } catch (Exception e) {
      return null;
    }
  }

  /// Returns whether the given segment name represents an LLC segment.
  ///
  /// V1 names have 3 `__` separators. V2 names have 5 and token 2 is the literal `v2`.
  /// Five-token uploaded names and 5-part `table__configId__partition__seq__time` names are not LLC.
  public static boolean isLLCSegment(String segmentName) {
    int numSeparators = 0;
    int index = 0;
    while ((index = segmentName.indexOf(SEPARATOR, index)) != -1) {
      numSeparators++;
      index += 2; // SEPARATOR.length()
    }
    if (numSeparators == 3) {
      return true;
    }
    if (numSeparators == 5) {
      return isV2Parts(StringUtils.splitByWholeSeparator(segmentName, SEPARATOR));
    }
    return false;
  }

  /// Returns the sequence number of the given V1 or V2 LLC segment name.
  public static int getSequenceNumber(String segmentName) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    if (parts.length == 4) {
      return Integer.parseInt(parts[2]);
    }
    Preconditions.checkArgument(isV2Parts(parts), "Invalid LLC segment name: %s", segmentName);
    return Integer.parseInt(parts[4]);
  }

  private static boolean isV2Parts(String[] parts) {
    return parts.length == 6 && V2_TOKEN.equals(parts[1]);
  }

  private static int parseNonNegativeInt(String raw, String fieldName, String segmentName) {
    int value = Integer.parseInt(raw);
    Preconditions.checkArgument(value >= 0, "Invalid LLC segment name %s: %s must be non-negative", segmentName,
        fieldName);
    return value;
  }

  public String getTableName() {
    return _tableName;
  }

  public int getFormatVersion() {
    return _formatVersion;
  }

  public boolean isV2() {
    return _formatVersion == FORMAT_VERSION_V2;
  }

  /// V1 packed or raw partition-group int. V2 names have no packed id.
  public int getPartitionGroupId() {
    Preconditions.checkState(!isV2(), "V2 LLC segment name has no packed partition group id: %s", _segmentName);
    return _partitionGroupId;
  }

  /// Registry topic id. V1 names do not store one; do not infer it from `% 10000`.
  public int getTopicId() {
    Preconditions.checkState(isV2(), "V1 LLC segment name has no topic id: %s", _segmentName);
    return _topicId;
  }

  /// Raw stream partition id from a V2 name.
  public int getPartitionId() {
    Preconditions.checkState(isV2(), "V1 LLC segment name has no raw partition id: %s", _segmentName);
    return _partitionId;
  }

  public StreamPartitionIdentity getStreamPartitionIdentity() {
    return isV2() ? StreamPartitionIdentity.v2(_topicId, _partitionId) : StreamPartitionIdentity.v1(_partitionGroupId);
  }

  public int getSequenceNumber() {
    return _sequenceNumber;
  }

  public String getCreationTime() {
    return _creationTime;
  }

  public long getCreationTimeMs() {
    DateTime dateTime = DATE_FORMATTER.parseDateTime(_creationTime);
    return dateTime.getMillis();
  }

  @JsonValue
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public int compareTo(LLCSegmentName other) {
    Preconditions.checkArgument(_tableName.equals(other._tableName),
        "Cannot compare segment names from different table: %s, %s", _segmentName, other.getSegmentName());
    int identityCmp = getStreamPartitionIdentity().compareTo(other.getStreamPartitionIdentity());
    if (identityCmp != 0) {
      return identityCmp;
    }
    return Integer.compare(_sequenceNumber, other._sequenceNumber);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LLCSegmentName)) {
      return false;
    }
    LLCSegmentName that = (LLCSegmentName) o;
    return _segmentName.equals(that._segmentName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentName);
  }

  @Override
  public String toString() {
    return _segmentName;
  }
}
