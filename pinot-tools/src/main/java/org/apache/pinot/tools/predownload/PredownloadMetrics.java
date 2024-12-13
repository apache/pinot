package org.apache.pinot.tools.predownload;

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
      _serverMetrics.addMeteredGlobalValue(ServerMeter.SEGMENT_DOWNLOAD_COUNT, 1);
      // Report download speed in MB/s, avoid divide by 0
      _serverMetrics.setValueOfGlobalGauge(ServerGauge.SEGMENT_DOWNLOAD_SPEED,
          (segmentSizeBytes / BYTES_TO_MB) / (downloadTimeMs / 1000 + 1));
    } else {
      _serverMetrics.addMeteredGlobalValue(ServerMeter.SEGMENT_DOWNLOAD_FAILURE_COUNT, 1);
    }
  }

  public void preDownloadSucceed(long totalSegmentSizeBytes, long totalDownloadTimeMs) {
    _serverMetrics.setValueOfGlobalGauge(ServerGauge.PREDOWNLOAD_SPEED,
        (totalSegmentSizeBytes / BYTES_TO_MB) / (totalDownloadTimeMs / 1000 + 1));
    _serverMetrics.addTimedValue(ServerTimer.PREDOWNLOAD_TIME, totalDownloadTimeMs, TimeUnit.MILLISECONDS);
  }

  public void preDownloadComplete(PredownloadCompleteReason reason) {
    if (reason.isSucceed()) {
      _serverMetrics.addMeteredGlobalValue(ServerMeter.PREDOWNLOAD_SUCCEED, 1);
    } else {
      _serverMetrics.addMeteredGlobalValue(ServerMeter.PREDOWNLOAD_FAILED, 1);
    }
  }
}
