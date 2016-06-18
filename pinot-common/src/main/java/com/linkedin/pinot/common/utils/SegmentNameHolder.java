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

public abstract class SegmentNameHolder {
  protected static final String SEPARATOR = "__";

  public static enum RealtimeSegmentType {
    UNSUPPORTED,
    HLC_LONG,
    HLC_SHORT,
    LLC,
  }

  public RealtimeSegmentType getSegmentType(String segmentName) {
    try {
      if (SegmentNameBuilder.Realtime.isRealtimeV1Name(segmentName)) {
        HLCSegmentNameHolder holder = new HLCSegmentNameHolder(segmentName);
        if (holder.isOldStyleNaming()) {
          return RealtimeSegmentType.HLC_LONG;
        } else {
          return RealtimeSegmentType.HLC_SHORT;
        }
      } else if (SegmentNameBuilder.Realtime.isRealtimeV2Name(segmentName)) {
        return RealtimeSegmentType.LLC;
      } else {
        return RealtimeSegmentType.UNSUPPORTED;
      }
    } catch (Exception e) {
      return RealtimeSegmentType.UNSUPPORTED;
    }
  }

  protected boolean isValidComponentName(String string) {
    if (string.contains("__")) {
      return false;
    }
    return true;
  }
}
