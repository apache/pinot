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

public abstract class SegmentName {
  public static final String SEPARATOR = "__";
  public static final String REALTIME_SUFFIX = "_REALTIME";
  public static final int REALTIME_SUFFIX_LENGTH = REALTIME_SUFFIX.length();

  public enum RealtimeSegmentType {
    UNSUPPORTED,
    HLC_LONG,
    HLC_SHORT,
    LLC,
  }

  public static RealtimeSegmentType getSegmentType(String segmentName) {
    if (isHighLevelConsumerSegmentName(segmentName)) {
      HLCSegmentName segName = new HLCSegmentName(segmentName);

      if (segName.isOldStyleNaming()) {
        return RealtimeSegmentType.HLC_LONG;
      } else {
        return RealtimeSegmentType.HLC_SHORT;
      }
    }

    if (isLowLevelConsumerSegmentName(segmentName)) {
      return RealtimeSegmentType.LLC;
    }

    return RealtimeSegmentType.UNSUPPORTED;
  }

  protected boolean isValidComponentName(String string) {
    if (string.contains("__")) {
      return false;
    }
    return true;
  }

  public abstract String getTableName();

  public abstract String getSequenceNumberStr();

  public abstract int getSequenceNumber();

  public abstract String getSegmentName();

  public abstract RealtimeSegmentType getSegmentType();

  public String getGroupId() {
    throw new RuntimeException("No groupId in " + getSegmentName());
  }

  public int getPartitionId() {
    throw new RuntimeException("No partitionId in " + getSegmentName());
  }

  public String getPartitionRange() {
    throw new RuntimeException("No partitionRange in " + getSegmentName());
  }

  public static boolean isHighLevelConsumerSegmentName(String segmentName) {
    if (segmentName.endsWith(SEPARATOR) || segmentName.startsWith(SEPARATOR)) {
      return false;
    }

    String[] parts =  segmentName.split(SEPARATOR);
    if (parts.length != 3 && parts.length != 5) {
      return false;
    }

    return true;
  }

  public static boolean isLowLevelConsumerSegmentName(String segmentName) {
    if (segmentName.endsWith(SEPARATOR) || segmentName.startsWith(SEPARATOR)) {
      return false;
    }

    String[] parts =  segmentName.split(SEPARATOR);
    if (parts.length != 4) {
      return false;
    }

    return true;
  }
}
