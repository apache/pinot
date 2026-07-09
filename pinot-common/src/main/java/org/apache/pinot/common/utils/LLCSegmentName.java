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
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * Represents an LLC (Low-Level Consumer) segment name in the format:
 * {@code {tableName}__{partitionGroupId}__{sequenceNumber}__{date}}
 */
public class LLCSegmentName implements Comparable<LLCSegmentName> {
  private static final String SEPARATOR = "__";
  private static final String DATE_FORMAT = "yyyyMMdd'T'HHmm'Z'";
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT).withZoneUTC();

  private final String _tableName;
  private final TopicPartitionId _topicPartitionId;
  private final int _sequenceNumber;
  private final String _creationTime;
  private final String _segmentName;

  /**
   * Parses a segment name string.
   *
   * <p>When {@code hasMultipleStreams} is true, composite partition IDs (>= 10000)
   * are decomposed into their topic and partition components. This ensures that
   * segment {@code table__10003__5__date} produces {@code TopicPartitionId(1, 3)}
   * instead of {@code TopicPartitionId(0, 10003)}.
   */
  public LLCSegmentName(String segmentName, boolean hasMultipleStreams) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    Preconditions.checkArgument(parts.length == 4, "Invalid LLC segment name: %s", segmentName);
    _tableName = parts[0];
    int rawId = Integer.parseInt(parts[1]);
    if (hasMultipleStreams && rawId >= TopicPartitionId.PARTITION_PADDING_OFFSET) {
      _topicPartitionId = TopicPartitionId.fromMultiTopicPinotPartitionId(rawId);
    } else {
      _topicPartitionId = new TopicPartitionId(rawId);
    }
    _sequenceNumber = Integer.parseInt(parts[2]);
    _creationTime = parts[3];
    _segmentName = segmentName;
  }

  /** @deprecated Use {@link #LLCSegmentName(String, boolean)} to provide multi-stream context. */
  @Deprecated
  public LLCSegmentName(String segmentName) {
    this(segmentName, false);
  }

  @Deprecated
  public LLCSegmentName(String tableName, int partitionGroupId, int sequenceNumber, long msSinceEpoch) {
    Preconditions.checkArgument(!tableName.contains(SEPARATOR), "Illegal table name: %s", tableName);
    _tableName = tableName;
    _topicPartitionId = new TopicPartitionId(partitionGroupId);
    _sequenceNumber = sequenceNumber;
    _creationTime = DATE_FORMATTER.print(msSinceEpoch);
    _segmentName = tableName + SEPARATOR + partitionGroupId + SEPARATOR + sequenceNumber + SEPARATOR + _creationTime;
  }

  @Nullable
  public static LLCSegmentName of(String segmentName, boolean hasMultipleStreams) {
    try {
      return new LLCSegmentName(segmentName, hasMultipleStreams);
    } catch (Exception e) {
      return null;
    }
  }

  /** @deprecated Use {@link #of(String, boolean)} to provide multi-stream context. */
  @Deprecated
  @Nullable
  public static LLCSegmentName of(String segmentName) {
    return of(segmentName, false);
  }

  /**
   * Returns whether the given segment name represents an LLC segment.
   */
  public static boolean isLLCSegment(String segmentName) {
    int numSeparators = 0;
    int index = 0;
    while ((index = segmentName.indexOf(SEPARATOR, index)) != -1) {
      numSeparators++;
      index += 2; // SEPARATOR.length()
    }
    return numSeparators == 3;
  }

  @Deprecated
  public static boolean isLowLevelConsumerSegmentName(String segmentName) {
    return isLLCSegment(segmentName);
  }

  /**
   * Returns the sequence number of the given segment name.
   */
  public static int getSequenceNumber(String segmentName) {
    return Integer.parseInt(StringUtils.splitByWholeSeparator(segmentName, SEPARATOR)[2]);
  }

  public String getTableName() {
    return _tableName;
  }

  public TopicPartitionId getTopicPartitionId() {
    return _topicPartitionId;
  }

  @Deprecated
  public int getPartitionGroupId() {
    return _topicPartitionId.getPartitionId();
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
    int cmp = _topicPartitionId.compareTo(other._topicPartitionId);
    if (cmp != 0) {
      return cmp;
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
