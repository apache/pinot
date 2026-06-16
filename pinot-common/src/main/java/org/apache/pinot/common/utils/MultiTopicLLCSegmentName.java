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
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * Segment name for multi-topic LLC realtime segments with 5-part format:
 * {@code tableName__configId__streamPartitionId__sequenceNumber__creationTime}
 *
 * <p>This format stores the stream config ID and stream partition ID as separate fields
 * rather than encoding them into a single partition group ID ({@code configId * 10000 + partitionId}).
 * This eliminates collision and overflow risks from the encoding scheme.
 *
 * <p>Existing 4-part segments ({@link LLCSegmentName}) are not affected. Both formats can
 * coexist in the same table. Use {@link #of(String)} to parse a segment name that may be
 * in either format.
 */
public class MultiTopicLLCSegmentName implements Comparable<MultiTopicLLCSegmentName> {
  private static final String SEPARATOR = "__";
  private static final String DATE_FORMAT = "yyyyMMdd'T'HHmm'Z'";
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT).withZoneUTC();

  private final String _tableName;
  private final int _configId;
  private final int _streamPartitionId;
  private final int _sequenceNumber;
  private final String _creationTime;
  private final String _segmentName;

  public MultiTopicLLCSegmentName(String segmentName) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    Preconditions.checkArgument(parts.length == 5, "Invalid multi-topic LLC segment name: %s", segmentName);
    _tableName = parts[0];
    _configId = Integer.parseInt(parts[1]);
    _streamPartitionId = Integer.parseInt(parts[2]);
    _sequenceNumber = Integer.parseInt(parts[3]);
    _creationTime = parts[4];
    _segmentName = segmentName;
  }

  public MultiTopicLLCSegmentName(String tableName, int configId, int streamPartitionId, int sequenceNumber,
      long msSinceEpoch) {
    Preconditions.checkArgument(!tableName.contains(SEPARATOR), "Illegal table name: %s", tableName);
    _tableName = tableName;
    _configId = configId;
    _streamPartitionId = streamPartitionId;
    _sequenceNumber = sequenceNumber;
    _creationTime = DATE_FORMATTER.print(msSinceEpoch);
    _segmentName = tableName + SEPARATOR + configId + SEPARATOR + streamPartitionId + SEPARATOR + sequenceNumber
        + SEPARATOR + _creationTime;
  }

  @Nullable
  public static MultiTopicLLCSegmentName of(String segmentName) {
    try {
      return new MultiTopicLLCSegmentName(segmentName);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Returns whether the given segment name is a multi-topic LLC segment (5-part format where
   * parts[1] and parts[3] are integers, distinguishing it from UploadedRealtimeSegmentName).
   */
  public static boolean isMultiTopicLLCSegment(String segmentName) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    if (parts.length != 5) {
      return false;
    }
    try {
      Integer.parseInt(parts[1]); // configId
      Integer.parseInt(parts[2]); // streamPartitionId
      Integer.parseInt(parts[3]); // sequenceNumber
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public String getTableName() {
    return _tableName;
  }

  public int getConfigId() {
    return _configId;
  }

  public int getStreamPartitionId() {
    return _streamPartitionId;
  }

  /// Returns the encoded partition group ID for backward compatibility with code
  /// that expects the single-integer encoding ({@code configId * 10000 + streamPartitionId}).
  public int getPartitionGroupId() {
    return IngestionConfigUtils.getPinotPartitionIdFromConfigId(_streamPartitionId, _configId);
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

  /// Converts this multi-topic segment name to an equivalent {@link LLCSegmentName} using the
  /// encoded partition group ID. Useful for interacting with code that only understands the 4-part format.
  public LLCSegmentName toLLCSegmentName() {
    return new LLCSegmentName(_tableName, getPartitionGroupId(), _sequenceNumber, getCreationTimeMs());
  }

  @Override
  public int compareTo(MultiTopicLLCSegmentName other) {
    Preconditions.checkArgument(_tableName.equals(other._tableName),
        "Cannot compare segment names from different table: %s, %s", _segmentName, other.getSegmentName());
    if (_configId != other._configId) {
      return Integer.compare(_configId, other._configId);
    }
    if (_streamPartitionId != other._streamPartitionId) {
      return Integer.compare(_streamPartitionId, other._streamPartitionId);
    }
    return Integer.compare(_sequenceNumber, other._sequenceNumber);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MultiTopicLLCSegmentName)) {
      return false;
    }
    MultiTopicLLCSegmentName that = (MultiTopicLLCSegmentName) o;
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
