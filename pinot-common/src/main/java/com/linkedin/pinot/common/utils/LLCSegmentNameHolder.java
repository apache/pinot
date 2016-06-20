/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.common.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


public class LLCSegmentNameHolder extends  SegmentNameHolder implements Comparable {
  private final static String DATE_FORMAT = "yyyyMMdd'T'HHmm'Z'";
  private final String _tableName;
  private final int _partitionId;
  private final int _sequenceNumber;
  private final String _creationTime;
  private final String _segmentName;

  public LLCSegmentNameHolder(String segmentName) {
    if (segmentName.endsWith(SEPARATOR) || segmentName.startsWith(SEPARATOR)) {
      throw new RuntimeException(segmentName + " is not a Low level consumer segment name");
    }
    String[] parts =  segmentName.split(SEPARATOR);
    if (parts.length != 4) {
      throw new RuntimeException(segmentName + " is not a Low level consumer segment name");
    }
    _segmentName = segmentName;
    _tableName = parts[0];
    _partitionId = Integer.valueOf(parts[1]);
    _sequenceNumber = Integer.valueOf(parts[2]);
    _creationTime = parts[3];
  }

  public LLCSegmentNameHolder(String tableName, int partitionId, int sequenceNumber, long msSinceEpoch) {
    if (!isValidComponentName(tableName)) {
      throw new RuntimeException("Invalid table name " + tableName);
    }
    _tableName = tableName;
    _partitionId = partitionId;
    _sequenceNumber = sequenceNumber;
    // ISO8601 date: 20160120T1234Z
    DateTime dateTime = new DateTime(msSinceEpoch, DateTimeZone.UTC);
    _creationTime = dateTime.toString("yyyyMMdd'T'HHmm'Z'");
    _segmentName = tableName + SEPARATOR + partitionId + SEPARATOR + sequenceNumber + SEPARATOR + _creationTime;
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
  public int getPartitionId() {
    return _partitionId;
  }

  @Override
  public int getSequenceNumber() {
    return _sequenceNumber;
  }

  public String getCreationTime() {
    return _creationTime;
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
    LLCSegmentNameHolder other = (LLCSegmentNameHolder)o;
    if (!this.getTableName().equals(other.getTableName())) {
      throw new RuntimeException("Cannot compare segment names " + this.getSegmentName() + " and " + other.getSegmentName());
    }
    if (this.getPartitionId() > other.getPartitionId()) {
      return 1;
    } else if (this.getPartitionId() < other.getPartitionId()) {
      return -1;
    } else {
      if (this.getSequenceNumber() > other.getSequenceNumber()) {
        return 1;
      } else if (this.getSequenceNumber() < other.getSequenceNumber()) {
        return -1;
      } else {
        if (!this.getCreationTime().equals(other.getCreationTime())) {
          // If sequence number is the same, time cannot be different.
          throw new RuntimeException("Cannot compare segment names " + this.getSegmentName() + " and " + other.getSegmentName());
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

    LLCSegmentNameHolder holder = (LLCSegmentNameHolder) o;

    if (_partitionId != holder._partitionId) {
      return false;
    }
    if (_sequenceNumber != holder._sequenceNumber) {
      return false;
    }
    if (_tableName != null ? !_tableName.equals(holder._tableName) : holder._tableName != null) {
      return false;
    }
    if (_creationTime != null ? !_creationTime.equals(holder._creationTime) : holder._creationTime != null) {
      return false;
    }
    return !(_segmentName != null ? !_segmentName.equals(holder._segmentName) : holder._segmentName != null);
  }

  @Override
  public int hashCode() {
    int result = _tableName != null ? _tableName.hashCode() : 0;
    result = 31 * result + _partitionId;
    result = 31 * result + _sequenceNumber;
    result = 31 * result + (_creationTime != null ? _creationTime.hashCode() : 0);
    result = 31 * result + (_segmentName != null ? _segmentName.hashCode() : 0);
    return result;
  }
}
