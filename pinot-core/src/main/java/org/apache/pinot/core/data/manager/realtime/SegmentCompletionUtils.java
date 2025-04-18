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
package org.apache.pinot.core.data.manager.realtime;

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;

import java.util.UUID;
import java.util.concurrent.TimeUnit;


public class SegmentCompletionUtils {
  private SegmentCompletionUtils() {
  }

  // Used to create temporary segment file names
  private static final String TMP = ".tmp.";

  /**
   * Takes in a segment name, and returns a file name prefix that is used to store all attempted uploads of this
   * segment when a segment is uploaded using split commit. Each attempt has a unique file name suffix
   * @param segmentName segment name
   * @return
   */
  public static String getTmpSegmentNamePrefix(String segmentName) {
    return segmentName + TMP;
  }

  public static String generateTmpSegmentFileName(String segmentNameStr) {
    return getTmpSegmentNamePrefix(segmentNameStr) + UUID.randomUUID();
  }

  public static boolean isTmpFile(String uri) {
    String[] splits = StringUtils.splitByWholeSeparator(uri, TMP);
    if (splits.length < 2) {
      return false;
    }
    try {
      UUID.fromString(splits[splits.length - 1]);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static void updateActiveUploadSegmentCount(ServerMetrics serverMetrics, String rawTableName, long activeUploadCount) {
    serverMetrics.setOrUpdateTableGauge(rawTableName, ServerGauge.SEGMENT_UPLOAD_COUNT, activeUploadCount);
    serverMetrics.setValueOfGlobalGauge(ServerGauge.SEGMENT_UPLOAD_COUNT, activeUploadCount);
  }

  public static void raiseSegmentUploadMetrics(ServerMetrics serverMetrics, String rawTableName, long duration,
                                               long segmentSize) {
    // Upload duration
    serverMetrics.addTimedTableValue(rawTableName, ServerTimer.SEGMENT_UPLOAD_TIME_MS, duration, TimeUnit.MILLISECONDS);
    serverMetrics.addTimedValue(ServerTimer.SEGMENT_UPLOAD_TIME_MS, duration, TimeUnit.MILLISECONDS);
    // Upload size
    serverMetrics.addMeteredTableValue(rawTableName, ServerMeter.SEGMENT_UPLOAD_SIZE_BYTES, segmentSize);
    serverMetrics.addMeteredGlobalValue(ServerMeter.SEGMENT_UPLOAD_SIZE_BYTES, segmentSize);
  }
}
