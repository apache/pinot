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
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 11, 2014
 */

public class SegmentNameBuilder {

  public static String buildBasic(String resourceName, String tableName, Object minTimeValue, Object maxTimeValue,
      String prefix) {
    return StringUtil.join("_", resourceName, tableName, minTimeValue.toString(), maxTimeValue.toString(), prefix);
  }

  public static String buildBasic(String resourceName, String tableName, String prefix) {
    return StringUtil.join("_", resourceName, tableName, prefix);
  }

  public static class Realtime {
    public static String build(String resourceName, String tableName, String insatanceName, String groupId,
        String partitionId, String sequenceNumber) {
      return StringUtils.join(
          Lists.newArrayList(resourceName, tableName, insatanceName, groupId, partitionId, sequenceNumber), "__");
    }

    public static String extractResourceName(String segmentId) {
      return segmentId.split("__")[0];
    }

    public static String extractTableName(String segmentId) {
      return segmentId.split("__")[1];
    }

    public static String extractInstanceName(String segmentId) {
      return segmentId.split("__")[2];
    }

    public static String extractGroupIdName(String segmentId) {
      return segmentId.split("__")[3];
    }

    public static String extractPartitionName(String segmentId) {
      return segmentId.split("__")[4];
    }

  }
}
