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
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import java.util.UUID;
import org.restlet.data.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentCompletionUtils {
  private static Logger LOGGER = LoggerFactory.getLogger(
      SegmentCompletionUtils.class);
  // Used to create temporary segment file names
  private static final String TMP = ".tmp.";

  static SegmentCompletionProtocol.Request.Params extractParams(Reference reference) {
    final String offsetStr = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_OFFSET);
    final String segmentName = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_SEGMENT_NAME);
    final String instanceId = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_INSTANCE_ID);
    final String segmentLocation = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_SEGMENT_LOCATION);
    final String reason = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_REASON);
    final String extraTimeSecStr = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_EXTRA_TIME_SEC);
    final String rowCountStr = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_ROW_COUNT);
    final String buildTimeMillisStr = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_BUILD_TIME_MILLIS);
    final String waitTimeMillisStr = reference.getQueryAsForm().getValues(SegmentCompletionProtocol.PARAM_WAIT_TIME_MILLIS);

    if (offsetStr == null || segmentName == null || instanceId == null) {
      LOGGER.error("Invalid call: offset={}, segmentName={}, instanceId={}", offsetStr, segmentName, instanceId);
      return null;
    }

    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withSegmentName(segmentName).withInstanceId(instanceId);

    try {
      long offset = Long.valueOf(offsetStr);
      params.withOffset(offset);

    } catch (NumberFormatException e) {
      LOGGER.error("Invalid offset {} for segment {} from instance {}", offsetStr, segmentName, instanceId);
      return null;
    }

    int extraTimeSec = SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds();
    if (extraTimeSecStr != null) {
      try {
        extraTimeSec = Integer.valueOf(extraTimeSecStr);

      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid extraTimeSec {} for segment {} from instance {}", extraTimeSecStr, segmentName, instanceId);
      }
    }
    params.withExtraTimeSec(extraTimeSec);

    if (rowCountStr != null) {
      try {
        int rowCount = Integer.valueOf(rowCountStr);
        params.withNumRows(rowCount);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid rowCount {} for segment {} from instance {}", rowCountStr, segmentName, instanceId);
      }
    }

    if (buildTimeMillisStr != null) {
      try {
        long buildTimeMillis = Long.valueOf(buildTimeMillisStr);
        params.withBuildTimeMillis(buildTimeMillis);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid buildTimeMillis {} for segment {} from instance {}", buildTimeMillisStr, segmentName, instanceId);
      }
    }

    if (waitTimeMillisStr != null) {
      try {
        long waitTimeMillis = Long.valueOf(waitTimeMillisStr);
        params.withWaitTimeMillis(waitTimeMillis);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid waitTimeMillis {} for segment {} from instance {}", waitTimeMillisStr, segmentName, instanceId);
      }
    }

    if (reason != null) {
      params.withReason(reason);
    }

    if (segmentLocation != null) {
      params.withSegmentLocation(segmentLocation);
    }

    return params;
  }

  /**
   * Takes in a segment name, and returns a file name prefix that is used to store all attempted uploads of this
   * segment when a segment is uploaded using split commit. Each attempt has a unique file name suffix
   * @param segmentName segment name
   * @return
   */
  public static String getSegmentNamePrefix(String segmentName) {
    return segmentName + TMP;
  }

  public static String generateSegmentFileName(String segmentNameStr) {
    return getSegmentNamePrefix(segmentNameStr) + UUID.randomUUID().toString();
  }
}
