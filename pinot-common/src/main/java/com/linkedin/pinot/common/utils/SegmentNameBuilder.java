/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
 * Nov 11, 2014
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

    public static String build(String groupId, String partitionRange, String sequenceNumber) {
      // old style name
      //  return StringUtils.join(
      //      Lists.newArrayList(tableName, instanceName, groupId, partitionName, sequenceNumber), "__");
      
      // shorter name: {groupId}__{partitionRange}__{sequenceNumber}
      // groupId contains the realtime table name, amongst other things
      // see com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder#getGroupIdFromRealtimeDataTable for details on the groupId
      return StringUtils.join(Lists.newArrayList(groupId, partitionRange, sequenceNumber), "__");
    }

    public static String extractTableName(String segmentId) {
      if (!isOldStyleName(segmentId)) {
        // Table name is the first part of the Kafka consumer group id
        String groupId = extractGroupIdName(segmentId);
        return groupId.substring(0, groupId.indexOf(REALTIME_SUFFIX) + REALTIME_SUFFIX_LENGTH);
      } else {
        return segmentId.split("__")[0];
      }
    }

    public static String extractGroupIdName(String segmentId) {
      if (!isOldStyleName(segmentId)) {
        return segmentId.split("__")[0];
      } else {
        return segmentId.split("__")[2];
      }
    }

    public static String extractPartitionRange(String segmentId) {
      if (!isOldStyleName(segmentId)) {
        return segmentId.split("__")[1];
      } else {
        return segmentId.split("__")[3];
      }
    }

    private static boolean isOldStyleName(String segmentId) {
      return segmentId.split("__").length == 5;
    }
  }
}
