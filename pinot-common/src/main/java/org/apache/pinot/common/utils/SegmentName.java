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

public abstract class SegmentName {
  public static final String SEPARATOR = "__";
  public static final String REALTIME_SUFFIX = "_REALTIME";
  public static final int REALTIME_SUFFIX_LENGTH = REALTIME_SUFFIX.length();

  public enum RealtimeSegmentType {
    UNSUPPORTED, HLC_LONG, HLC_SHORT, LLC,
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
    return !string.contains(SEPARATOR);
  }

  public abstract String getTableName();

  public abstract String getSequenceNumberStr();

  public abstract int getSequenceNumber();

  public abstract String getSegmentName();

  public abstract RealtimeSegmentType getSegmentType();

  public String getGroupId() {
    throw new RuntimeException("No groupId in " + getSegmentName());
  }

  public int getPartitionGroupId() {
    throw new RuntimeException("No partitionGroupId in " + getSegmentName());
  }

  public String getPartitionRange() {
    throw new RuntimeException("No partitionRange in " + getSegmentName());
  }

  public static boolean isHighLevelConsumerSegmentName(String segmentName) {
    int numSeparators = getNumSeparators(segmentName);
    return numSeparators == 2 || numSeparators == 4;
  }

  public static boolean isLowLevelConsumerSegmentName(String segmentName) {
    return getNumSeparators(segmentName) == 3;
  }

  public static boolean isRealtimeSegmentName(String segmentName) {
    int numSeparators = getNumSeparators(segmentName);
    return numSeparators >= 2 && numSeparators <= 4;
  }

  private static int getNumSeparators(String segmentName) {
    int numSeparators = 0;
    int index = 0;
    while ((index = segmentName.indexOf(SEPARATOR, index)) != -1) {
      numSeparators++;
      index += 2; // SEPARATOR.length()
    }
    return numSeparators;
  }
}
