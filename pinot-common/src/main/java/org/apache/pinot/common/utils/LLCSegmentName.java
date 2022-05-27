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
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class LLCSegmentName extends SegmentName implements Comparable {
  private final static String DATE_FORMAT = "yyyyMMdd'T'HHmm'Z'";
  private final static DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT).withZoneUTC();

  private final String _tableName;
  private final int _partitionGroupId;
  private final int _sequenceNumber;
  private final String _creationTime;
  private final String _segmentName;

  public LLCSegmentName(String segmentName) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    Preconditions.checkArgument(parts.length == 4, "Invalid LLC segment name: %s", segmentName);
    _tableName = parts[0];
    _partitionGroupId = Integer.parseInt(parts[1]);
    _sequenceNumber = Integer.parseInt(parts[2]);
    _creationTime = parts[3];
    _segmentName = segmentName;
  }

  public LLCSegmentName(String tableName, int partitionGroupId, int sequenceNumber, long msSinceEpoch) {
    if (!isValidComponentName(tableName)) {
      throw new RuntimeException("Invalid table name " + tableName);
    }
    _tableName = tableName;
    _partitionGroupId = partitionGroupId;
    _sequenceNumber = sequenceNumber;
    // ISO8601 date: 20160120T1234Z
    _creationTime = DATE_FORMATTER.print(msSinceEpoch);
    _segmentName = tableName + SEPARATOR + partitionGroupId + SEPARATOR + sequenceNumber + SEPARATOR + _creationTime;
  }

  private LLCSegmentName(String tableName, int partitionGroupId, int sequenceNumber, String creationTime,
      String segmentName) {
    _tableName = tableName;
    _partitionGroupId = partitionGroupId;
    _sequenceNumber = sequenceNumber;
    _creationTime = creationTime;
    _segmentName = segmentName;
  }

  /**
   * Returns the {@link LLCSegmentName} for the given segment name, or {@code null} if the given segment name does not
   * represent an LLC segment.
   */
  @Nullable
  public static LLCSegmentName of(String segmentName) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    if (parts.length != 4) {
      return null;
    }
    return new LLCSegmentName(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), parts[3], segmentName);
  }

  /**
   * Returns the sequence number of the given segment name.
   */
  public static int getSequenceNumber(String segmentName) {
    return Integer.parseInt(StringUtils.splitByWholeSeparator(segmentName, SEPARATOR)[2]);
  }

  @Override
  public RealtimeSegmentType getSegmentType() {
    return RealtimeSegmentType.LLC;
  }

  @Override
  public String getTableName() {
    return _tableName;
  }

  @Override
  public int getPartitionGroupId() {
    return _partitionGroupId;
  }

  @Override
  public String getPartitionRange() {
    return Integer.toString(getPartitionGroupId());
  }

  @Override
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

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public String getSequenceNumberStr() {
    return Integer.toString(_sequenceNumber);
  }

  @Override
  public int compareTo(Object o) {
    LLCSegmentName other = (LLCSegmentName) o;
    if (!this.getTableName().equals(other.getTableName())) {
      throw new RuntimeException(
          "Cannot compare segment names " + this.getSegmentName() + " and " + other.getSegmentName());
    }
    if (this.getPartitionGroupId() > other.getPartitionGroupId()) {
      return 1;
    } else if (this.getPartitionGroupId() < other.getPartitionGroupId()) {
      return -1;
    } else {
      if (this.getSequenceNumber() > other.getSequenceNumber()) {
        return 1;
      } else if (this.getSequenceNumber() < other.getSequenceNumber()) {
        return -1;
      } else {
        if (!this.getCreationTime().equals(other.getCreationTime())) {
          // If sequence number is the same, time cannot be different.
          throw new RuntimeException(
              "Cannot compare segment names " + this.getSegmentName() + " and " + other.getSegmentName());
        }
        return 0;
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
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
    return getSegmentName();
  }
}
