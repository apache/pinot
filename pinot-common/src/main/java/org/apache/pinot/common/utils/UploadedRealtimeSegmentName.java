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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


/**
 * Class to represent segment names like: uploaded__{tableName}__{partitionId}__{sequenceId}__{creationTime}__{
 * optionalSuffix}
 *
 * <p>This naming convention is adopted to represent a segment uploaded to a realtime table. The naming
 * convention has been kept semantically similar to {@link LLCSegmentName} to but differs in following ways:
 *
 * <li> compulsory prefix updated, to quickly identify the segment is an uplaoded realtime segment
 * <li> sequenceId is used to uniquely identify the segment created at the same time
 * <li> optional suffix to encode any additional information about the segment
 */
public class UploadedRealtimeSegmentName implements Comparable<UploadedRealtimeSegmentName> {

  private static final String UPLOADED_PREFIX = "uploaded";
  private static final String SEPARATOR = "__";
  private static final String DATE_FORMAT = "yyyyMMdd'T'HHmm'Z'";
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT).withZoneUTC();
  private final String _tableName;
  private final int _partitionId;
  private final int _sequenceId;
  private final String _creationTime;
  private final String _segmentName;

  @Nullable
  private String _suffix = null;

  public UploadedRealtimeSegmentName(String segmentName) {

    // split the segment name by the separator and get creation time, sequence id, partition id and table name from
    // the end and validate segment name starts with prefix uploaded_
    try {
      String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
      Preconditions.checkState((parts.length == 5 || parts.length == 6) && parts[0].equals(UPLOADED_PREFIX),
          "Uploaded segment name must be of the format "
              + "uploaded__{tableName}__{partitionId}__{sequenceId}__{creationTime}");
      _tableName = parts[1];
      _partitionId = Integer.parseInt(parts[2]);
      _sequenceId = Integer.parseInt(parts[3]);
      _creationTime = parts[4];
      if (parts.length == 6) {
        _suffix = parts[5];
      }
      _segmentName = segmentName;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid segment name: " + segmentName, e);
    }
  }

  /**
   * Constructor to create a segment name from the table name, partition id, sequence id, creation time and optional
   * suffix
   * @param tableName
   * @param partitionId
   * @param sequenceId
   * @param msSinceEpoch
   * @param suffix
   */
  public UploadedRealtimeSegmentName(String tableName, int partitionId, int sequenceId, long msSinceEpoch,
      @Nullable String suffix) {
    _tableName = tableName;
    _partitionId = partitionId;
    _sequenceId = sequenceId;
    _creationTime = DATE_FORMATTER.print(msSinceEpoch);
    _suffix = suffix;
    _segmentName = Joiner.on(SEPARATOR).skipNulls()
        .join(UPLOADED_PREFIX, tableName, partitionId, sequenceId, _creationTime, suffix);
  }

  public static boolean isUploadedRealtimeSegmentName(String segmentName) {
    String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
    if (!(parts.length == 5 || parts.length == 6)) {
      return false;
    }
    if (!parts[0].equals(UPLOADED_PREFIX)) {
      return false;
    }

    int idx = parts.length == 5 ? parts.length - 1 : parts.length - 2;
    // validate creation time is of format yyyyMMdd'T'HHmm'Z'
    try {
      DATE_FORMATTER.parseDateTime(parts[idx--]);
    } catch (IllegalArgumentException e) {
      return false;
    }

    // return false if sequenceId and partitionId are not int
    try {
      Integer.parseInt(parts[idx]);
      Integer.parseInt(parts[idx - 1]);
    } catch (NumberFormatException e) {
      return false;
    }

    return true;
  }

  @Nullable
  public static UploadedRealtimeSegmentName of(String segmentName) {
    try {
      return new UploadedRealtimeSegmentName(segmentName);
    } catch (Exception e) {
      return null;
    }
  }

  public String getTableName() {
    return _tableName;
  }

  public int getPartitionId() {
    return _partitionId;
  }

  public int getSequenceId() {
    return _sequenceId;
  }

  /**
   * Returns the creation time in the format yyyyMMdd'T'HHmm'Z'
   * To be used for only human readability and not for any computation
   * @return
   */
  public String getCreationTime() {
    return _creationTime;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  @Nullable
  public String getSuffix() {
    return _suffix;
  }

  @Override
  public int compareTo(UploadedRealtimeSegmentName other) {
    Preconditions.checkState(_tableName.equals(other._tableName),
        "Cannot compare segment names from different table: %s, %s", _segmentName, other.getSegmentName());
    if (_partitionId != other._partitionId) {
      return Integer.compare(_partitionId, other._partitionId);
    }
    return Integer.compare(_sequenceId, other._sequenceId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UploadedRealtimeSegmentName)) {
      return false;
    }
    UploadedRealtimeSegmentName that = (UploadedRealtimeSegmentName) o;
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
