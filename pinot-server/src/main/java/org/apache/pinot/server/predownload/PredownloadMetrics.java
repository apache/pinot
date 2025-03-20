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
package org.apache.pinot.server.predownload;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.metrics.ServerTimer;


public class PredownloadMetrics {
  private static final String INSTANCE_ID_TAG = "instance_id";
  private static final String SEGMENT_ID_TAG = "segment_id";
  private static final long BYTES_TO_MB = 1024 * 1024;

  private final ServerMetrics _serverMetrics;

  public PredownloadMetrics() {
    _serverMetrics = ServerMetrics.get();
  }

  public void segmentDownloaded(boolean succeed, String segmentName, long segmentSizeBytes, long downloadTimeMs) {
    if (succeed) {
      _serverMetrics.addMeteredGlobalValue(ServerMeter.PREDOWNLOAD_SEGMENT_DOWNLOAD_COUNT, 1);
      // Report download speed in MB/s, avoid divide by 0
      _serverMetrics.setValueOfGlobalGauge(ServerGauge.SEGMENT_DOWNLOAD_SPEED,
          (segmentSizeBytes / BYTES_TO_MB) / (downloadTimeMs / 1000 + 1));
    } else {
      _serverMetrics.addMeteredGlobalValue(ServerMeter.PREDOWNLOAD_SEGMENT_DOWNLOAD_FAILURE_COUNT, 1);
    }
  }

  public void preDownloadSucceed(long totalSegmentSizeBytes, long totalDownloadTimeMs) {
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.PREDOWNLOAD_SPEED,
        (totalSegmentSizeBytes / BYTES_TO_MB) / (totalDownloadTimeMs / 1000 + 1));
    _serverMetrics.addTimedValue(ServerTimer.PREDOWNLOAD_TIME, totalDownloadTimeMs, TimeUnit.MILLISECONDS);
  }

  public void preDownloadComplete(PredownloadCompletionReason reason) {
    _serverMetrics.addMeteredGlobalValue(
        reason.isSucceed() ? ServerMeter.PREDOWNLOAD_SUCCEED : ServerMeter.PREDOWNLOAD_FAILED, 1);
  }
}
