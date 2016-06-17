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

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;


/**
 * Segment name builder for realtime segments.
 */

public class SegmentNameBuilder {
  public static final String REALTIME_SUFFIX = "_REALTIME";
  private static final int REALTIME_SUFFIX_LENGTH = REALTIME_SUFFIX.length();

  public static String buildBasic(String tableName, Object minTimeValue, Object maxTimeValue,
      String prefix) {
    return StringUtil.join("_", tableName, minTimeValue.toString(), maxTimeValue.toString(), prefix);
  }

  public static String buildBasic(String tableName, String prefix) {
    return StringUtil.join("_", tableName, prefix);
  }

  public static class Realtime {
    public static final String ALL_PARTITIONS = "ALL";

    public static String buildHighLevelConsumerSegmentName(String groupId, String partitionRange,
        String sequenceNumber) {
      // old style name
      //  return StringUtils.join(
      //      Lists.newArrayList(tableName, instanceName, groupId, partitionName, sequenceNumber), "__");

      // shorter name: {groupId}__{partitionRange}__{sequenceNumber}
      // groupId contains the realtime table name, amongst other things
      // see com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder#getGroupIdFromRealtimeDataTable for details on the groupId

      if (isValidSegmentComponent(groupId) && isValidSegmentComponent(partitionRange) &&
          isValidSegmentComponent(sequenceNumber)) {
        return StringUtils.join(Lists.newArrayList(groupId, partitionRange, sequenceNumber), "__");
      } else {
        throw new IllegalArgumentException("Invalid group id (" + groupId + "), partition range (" + partitionRange +
            ") or sequence number (" + sequenceNumber + ")");
      }
    }

    public static String buildLowLevelConsumerSegmentName(String tableName, String partitionId, String sequenceNumber,
        long millisSinceEpoch) {
      // ISO8601 date: 20160120T1234Z
      DateTime dateTime = new DateTime(millisSinceEpoch, DateTimeZone.UTC);
      String date = dateTime.toString("yyyyMMdd'T'HHmm'Z'");

      // New realtime consumer v2 name: {tableName}__{partitionId}__{sequenceNumber}__{date}
      if (isValidSegmentComponent(tableName) && isValidSegmentComponent(partitionId) &&
          isValidSegmentComponent(sequenceNumber) && isValidSegmentComponent(date)) {
        return StringUtils.join(Lists.newArrayList(tableName, partitionId, sequenceNumber, date), "__");
      } else {
        throw new IllegalArgumentException("Invalid table name (" + tableName + "), partition range (" + partitionId +
            "), sequence number (" + sequenceNumber + ") or date (" + date + ")");
      }
    }

    public static String extractTableName(String segmentId) {
      if (isOldV1StyleName(segmentId) || isRealtimeV2Name(segmentId)) {
        return segmentId.split("__")[0];
      } else if (isShortV1StyleName(segmentId)) {
        // Table name is the first part of the Kafka consumer group id
        String groupId = extractGroupIdName(segmentId);
        return groupId.substring(0, groupId.indexOf(REALTIME_SUFFIX) + REALTIME_SUFFIX_LENGTH);
      } else {
        throw new RuntimeException("Unable to parse segment name " + segmentId);
      }
    }

    public static String extractGroupIdName(String segmentId) {
      if (isOldV1StyleName(segmentId)) {
        return segmentId.split("__")[2];
      } else if (isShortV1StyleName(segmentId)) {
        return segmentId.split("__")[0];
      } else if (isRealtimeV2Name(segmentId)){
        throw new RuntimeException("Realtime v2 segments don't have a consumer group id");
      } else {
        throw new RuntimeException("Unable to parse segment name " + segmentId);
      }
    }

    public static String extractPartitionRange(String segmentId) {
      if (isOldV1StyleName(segmentId)) {
        return segmentId.split("__")[3];
      } else if (isShortV1StyleName(segmentId)) {
        return segmentId.split("__")[1];
      } else if (isRealtimeV2Name(segmentId)){
        return segmentId.split("__")[1];
      } else {
        throw new RuntimeException("Unable to parse segment name " + segmentId);
      }
    }

    public static String extractSequenceNumber(String segmentId) {
      if (isOldV1StyleName(segmentId)) {
        return segmentId.split("__")[4];
      } else if (isShortV1StyleName(segmentId)) {
        return segmentId.split("__")[2];
      } else if (isRealtimeV2Name(segmentId)){
        return segmentId.split("__")[2];
      } else {
        throw new RuntimeException("Unable to parse segment name " + segmentId);
      }

    }

    public static boolean isRealtimeV1Name(String segmentId) {
      int namePartCount = segmentId.split("__").length;
      // Realtime v1 segment names have either:
      // - Five parts (old style name: tableName, instanceName, groupId, partitionName, sequenceNumber)
      // - Three parts (shorter name : groupId, partitionName, sequenceNumber)
      return namePartCount == 5 || namePartCount == 3;
    }

    public static boolean isRealtimeV2Name(String segmentId) {
      int namePartCount = segmentId.split("__").length;
      // Realtime v2 segment names have four parts (tableName, partitionId, sequenceNumber, date)
      return namePartCount == 4;
    }

    private static boolean isOldV1StyleName(String segmentId) {
      return segmentId.split("__").length == 5;
    }

    private static boolean isShortV1StyleName(String segmentId) {
      return segmentId.split("__").length == 3;
    }

    private static boolean isValidSegmentComponent(String component) {
      return component != null && !component.contains("__");
    }
  }
}
