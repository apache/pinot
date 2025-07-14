package org.apache.pinot.segment.local.utils;

import java.util.Locale;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;


/**
 * Utils for metrics
 */
public class MetricUtils {


  private MetricUtils() {
  }

  public static void updateIndexingErrorMetric(String tableNameWithType, String indexDisplayName) {
    String metricKeyName =
        tableNameWithType + "-" + indexDisplayName.toUpperCase(Locale.US) + "-indexingError";
    ServerMetrics.get().addMeteredTableValue(metricKeyName, ServerMeter.INDEXING_FAILURES, 1);
  }
}
