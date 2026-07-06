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
 * Represents an LLC (Low-Level Consumer) segment name in one of two formats:
 * <ul>
 *   <li>Old format (3 separators): {@code {tableName}__{partitionGroupId}__{sequenceNumber}__{date}}</li>
 *   <li>New multi-topic format (4 separators):
 *       {@code {tableName}__{topicId}__{partitionGroupId}__{sequenceNumber}__{date}}</li>
 * </ul>
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
  private final boolean _isMultiTopicFormat;

  /**
   * Parses a segment name string in either old (4-part) or new (5-part) format.
   *
   * <p>When {@code hasMultipleStreams} is true, old-format composite partition IDs (>= 10000)
   * are decomposed into their topic and partition components. This ensures that old-format
   * segment {@code table__10003__5__date} and new-format segment {@code table__1__3__5__date}
   * produce the same {@link TopicPartitionId}.
   */
  public LLCSegmentName(String segmentName, boolean hasMultipleStreams) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    if (parts.length == 4) {
      _tableName = parts[0];
      int rawId = Integer.parseInt(parts[1]);
      if (hasMultipleStreams && rawId >= TopicPartitionId.PARTITION_PADDING_OFFSET) {
        _topicPartitionId = TopicPartitionId.fromMultiTopicPinotPartitionId(rawId);
      } else {
        _topicPartitionId = new TopicPartitionId(rawId);
      }
      _sequenceNumber = Integer.parseInt(parts[2]);
      _creationTime = parts[3];
      _isMultiTopicFormat = false;
    } else if (parts.length == 5) {
      _tableName = parts[0];
      int topicId = Integer.parseInt(parts[1]);
      int partitionId = Integer.parseInt(parts[2]);
      _topicPartitionId = new TopicPartitionId(topicId, partitionId);
      _sequenceNumber = Integer.parseInt(parts[3]);
      _creationTime = parts[4];
      _isMultiTopicFormat = true;
    } else {
      throw new IllegalArgumentException("Invalid LLC segment name: " + segmentName);
    }
    _segmentName = segmentName;
  }

  /** @deprecated Use {@link #LLCSegmentName(String, boolean)} to provide multi-stream context. */
  @Deprecated
  public LLCSegmentName(String segmentName) {
    this(segmentName, false);
  }

  /** Constructs a segment name from components. The format is determined by {@code useMultiTopicFormat}. */
  public LLCSegmentName(String tableName, TopicPartitionId topicPartitionId, int sequenceNumber, long msSinceEpoch,
      boolean useMultiTopicFormat) {
    Preconditions.checkArgument(!tableName.contains(SEPARATOR), "Illegal table name: %s", tableName);
    _tableName = tableName;
    _topicPartitionId = topicPartitionId;
    _sequenceNumber = sequenceNumber;
    _creationTime = DATE_FORMATTER.print(msSinceEpoch);
    _isMultiTopicFormat = useMultiTopicFormat;
    if (useMultiTopicFormat) {
      _segmentName = tableName + SEPARATOR + topicPartitionId.getTopicId() + SEPARATOR
          + topicPartitionId.getPartitionId() + SEPARATOR + sequenceNumber + SEPARATOR + _creationTime;
    } else {
      _segmentName = tableName + SEPARATOR + topicPartitionId.getPartitionId() + SEPARATOR
          + sequenceNumber + SEPARATOR + _creationTime;
    }
  }

  @Deprecated
  public LLCSegmentName(String tableName, int partitionGroupId, int sequenceNumber, long msSinceEpoch) {
    this(tableName, new TopicPartitionId(partitionGroupId), sequenceNumber, msSinceEpoch, false);
  }

  /**
   * Creates the next segment name, handling format transitions between old and new formats.
   *
   * <p>The previous segment must have been parsed with the correct {@code hasMultipleStreams}
   * context so that its {@link TopicPartitionId} is already canonical.
   *
   * <p>Handles all four transitions:
   * <ul>
   *   <li>old → old: continue old format, partition ID unchanged</li>
   *   <li>new → new: continue new format, partition ID unchanged</li>
   *   <li>old → new: partition ID already decomposed at parse time</li>
   *   <li>new → old: recompose topicId + partitionId into composite</li>
   * </ul>
   */
  public static LLCSegmentName createNextSegment(LLCSegmentName previous, boolean useMultiTopicFormat,
      long creationTimeMs) {
    int nextSeq = previous._sequenceNumber + 1;
    TopicPartitionId tpId = previous._topicPartitionId;

    if (!useMultiTopicFormat && previous._isMultiTopicFormat) {
      // new → old: recompose to composite
      tpId = new TopicPartitionId(tpId.toMultiTopicPinotPartitionId());
    }

    return new LLCSegmentName(previous._tableName, tpId, nextSeq, creationTimeMs, useMultiTopicFormat);
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
   * Returns whether the given segment name represents an LLC segment (old or new format).
   */
  public static boolean isLLCSegment(String segmentName) {
    int numSeparators = 0;
    int index = 0;
    while ((index = segmentName.indexOf(SEPARATOR, index)) != -1) {
      numSeparators++;
      index += 2;
    }
    if (numSeparators == 3) {
      return true;
    }
    if (numSeparators == 4) {
      // Disambiguate from UploadedRealtimeSegmentName: parts[3] is integer (seq) for LLC, date string for uploaded
      String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
      return isNumeric(parts[3]);
    }
    return false;
  }

  @Deprecated
  public static boolean isLowLevelConsumerSegmentName(String segmentName) {
    return isLLCSegment(segmentName);
  }

  /** Returns the sequence number from a segment name string (handles both formats). */
  public static int getSequenceNumber(String segmentName) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    return parts.length == 4 ? Integer.parseInt(parts[2]) : Integer.parseInt(parts[3]);
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

  public boolean isMultiTopicFormat() {
    return _isMultiTopicFormat;
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

  private static boolean isNumeric(String s) {
    if (s == null || s.isEmpty()) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }
}
