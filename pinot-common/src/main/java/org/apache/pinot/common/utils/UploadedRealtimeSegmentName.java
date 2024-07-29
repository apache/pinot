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
 * Class to represent segment names like: {prefix}__{tableName}__{partitionId}__{creationTime}__{suffix}
 *
 * <p>This naming convention is adopted to represent a segment uploaded to a realtime table. The naming
 * convention has been kept semantically similar to {@link LLCSegmentName} but differs in following ways:
 *
 * <li> prefix to quickly identify the type/source of segment e.g. "uploaded"/"minion"
 * <li> name of the table to which the segment belongs
 * <li> partitionId which should be consistent as the stream partitioning in case of upsert realtime tables.
 * <li> creationTime creation time of segment of the format yyyyMMdd'T'HHmm'Z'
 * <li> suffix to uniquely identify segments created at the same time.
 *
 * Use {@link org.apache.pinot.segment.spi.creator.name.UploadedRealtimeSegmentNameGenerator} to generate segment names.
 */
public class UploadedRealtimeSegmentName implements Comparable<UploadedRealtimeSegmentName> {

  private static final String SEPARATOR = "__";
  private static final String DATE_FORMAT = "yyyyMMdd'T'HHmm'Z'";
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT).withZoneUTC();
  private final String _prefix;
  private final String _tableName;
  private final int _partitionId;
  private final String _creationTime;
  private final String _segmentName;
  private final String _suffix;

  public UploadedRealtimeSegmentName(String segmentName) {
    try {
      String[] parts = StringUtils.splitByWholeSeparator(segmentName, SEPARATOR);
      Preconditions.checkState(parts.length == 5,
          "Uploaded segment name must be of the format {prefix}__{tableName}__{partitionId}__{creationTime}__{suffix}");
      _prefix = parts[0];
      _tableName = parts[1];
      _partitionId = Integer.parseInt(parts[2]);
      _creationTime = parts[3];
      _suffix = parts[4];
      _segmentName = segmentName;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid segment name: " + segmentName, e);
    }
  }

  /**
   * Constructor for UploadedRealtimeSegmentName.
   * @param tableName
   * @param partitionId
   * @param msSinceEpoch
   * @param prefix
   * @param suffix
   */
  public UploadedRealtimeSegmentName(String tableName, int partitionId, long msSinceEpoch, String prefix,
      String suffix) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(tableName) && !tableName.contains(SEPARATOR) && StringUtils.isNotBlank(prefix)
            && !prefix.contains(SEPARATOR) && StringUtils.isNotBlank(suffix) && !suffix.contains(SEPARATOR),
        "tableName, prefix and suffix must be non-null, non-empty and not contain '__'");
    _tableName = tableName;
    _partitionId = partitionId;
    _creationTime = DATE_FORMATTER.print(msSinceEpoch);
    _prefix = prefix;
    _suffix = suffix;
    _segmentName = Joiner.on(SEPARATOR).join(prefix, tableName, partitionId, _creationTime, suffix);
  }

  /**
   * Checks if the segment name is of the format: {prefix}__{tableName}__{partitionId}__{creationTime}__{suffix}
   * @param segmentName
   * @return boolean true if the segment name is of the format: {prefix}__{tableName}__{partitionId}__{creationTime}
   * __{suffix}
   */
  public static boolean isUploadedRealtimeSegmentName(String segmentName) {
    int numSeparators = 0;
    int index = 0;
    while ((index = segmentName.indexOf(SEPARATOR, index)) != -1) {
      numSeparators++;
      index += 2; // SEPARATOR.length()
    }
    return numSeparators == 4;
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

  public String getPrefix() {
    return _prefix;
  }

  public String getSuffix() {
    return _suffix;
  }

  /**
   * Compares the string representation of the segment name.
   * @param other the object to be compared.
   * @return
   */
  @Override
  public int compareTo(UploadedRealtimeSegmentName other) {
    Preconditions.checkState(_tableName.equals(other._tableName),
        "Cannot compare segment names from different table: %s, %s", _segmentName, other.getSegmentName());
    return _segmentName.compareTo(other._segmentName);
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
