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

    /**
     * @deprecated  Use {@link HLCSegmentName}
     * @param groupId groupid
     * @param partitionRange partitionRange
     * @param sequenceNumber sequenceNumber
     * @return segmentName
     */
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

    /**
     * @deprecated  Use {@link HLCSegmentName}
     * @param segmentId segmentName
     * @return tablename
     */
    public static String extractTableName(String segmentId) {
      if (isOldV1StyleName(segmentId)) {
        return segmentId.split("__")[0];
      } else if (isShortV1StyleName(segmentId)) {
        // Table name is the first part of the Kafka consumer group id
        String groupId = extractGroupIdName(segmentId);
        return groupId.substring(0, groupId.indexOf(REALTIME_SUFFIX) + REALTIME_SUFFIX_LENGTH);
      } else {
        throw new RuntimeException("Unable to parse segment name " + segmentId);
      }
    }

    /**
     * @deprecated  Use {@link HLCSegmentName}
     * @param segmentId segmentname
     * @return groupname
     */
    public static String extractGroupIdName(String segmentId) {
      if (isOldV1StyleName(segmentId)) {
        return segmentId.split("__")[2];
      } else if (isShortV1StyleName(segmentId)) {
        return segmentId.split("__")[0];
      } else {
        throw new RuntimeException("Unable to parse segment name " + segmentId);
      }
    }

    /**
     * @deprecated  Use {@link HLCSegmentName}
     * @param segmentId segment name
     * @return partitionrange
     */
    public static String extractPartitionRange(String segmentId) {
      if (isOldV1StyleName(segmentId)) {
        return segmentId.split("__")[3];
      } else if (isShortV1StyleName(segmentId)) {
        return segmentId.split("__")[1];
      } else {
        throw new RuntimeException("Unable to parse segment name " + segmentId);
      }
    }

    /**
     * @deprecated  Use {@link HLCSegmentName}
     * @param segmentId segment name
     * @return sequence number
     */
    public static String extractSequenceNumber(String segmentId) {
      if (isOldV1StyleName(segmentId)) {
        return segmentId.split("__")[4];
      } else if (isShortV1StyleName(segmentId)) {
        return segmentId.split("__")[2];
      } else {
        throw new RuntimeException("Unable to parse segment name " + segmentId);
      }

    }

    /**
     * @deprecated  Use {@link HLCSegmentName}
     * @param segmentId segment name
     * @return true if realtime long segment name
     */
    public static boolean isRealtimeV1Name(String segmentId) {
      int namePartCount = segmentId.split("__").length;
      // Realtime v1 segment names have either:
      // - Five parts (old style name: tableName, instanceName, groupId, partitionName, sequenceNumber)
      // - Three parts (shorter name : groupId, partitionName, sequenceNumber)
      return namePartCount == 5 || namePartCount == 3;
    }

    /**
     * @deprecated  Use {@link HLCSegmentName}
     * @param segmentId segment name
     * @return true if realtime short segment name
     */
    public static boolean isRealtimeV2Name(String segmentId) {
      try {
        LLCSegmentName holder = new LLCSegmentName(segmentId);
      } catch (Exception e) {
        return false;
      }
      return true;
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
