package com.linkedin.thirdeye.anomaly.utils;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;


public class ThirdeyeMetricsUtil {
  private static MetricsRegistry metricsRegistry = new MetricsRegistry();
  private static JmxReporter jmxReporter = new JmxReporter(metricsRegistry);

  static {
    jmxReporter.start();
  }

  private ThirdeyeMetricsUtil() {
  }

  public static final Counter detectionTaskCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "detectionTaskCounter");

  public static final Counter detectionTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "detectionTaskSuccessCounter");

  public static final Counter alertTaskSuccessCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "alertTaskSuccessCounter");

  public static final Counter dbCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbCallCounter");

  public static final Counter dbReadCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbReadCallCounter");

  public static final Counter dbReadByteCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbReadByteCounter");

  public static final Counter dbReadDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbReadDurationCounter");

  public static final Counter dbWriteCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbWriteCallCounter");

  public static final Counter dbWriteByteCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbWriteByteCounter");

  public static final Counter dbWriteDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "dbWriteDurationCounter");

  public static final Counter datasourceCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "datasourceCallCounter");

  public static final Counter datasourceDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "datasourceDurationCounter");

  public static final Counter pinotCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "pinotCallCounter");

  public static final Counter pinotDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "pinotDurationCounter");

  public static final Counter rcaPipelineCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaPipelineCallCounter");

  public static final Counter rcaPipelineDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaPipelineDurationCounter");

  public static final Counter rcaFrameworkCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaFrameworkCallCounter");

  public static final Counter rcaFrameworkDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "rcaFrameworkDurationCounter");

  public static final Counter cubeCallCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "cubeCallCounter");

  public static final Counter cubeDurationCounter =
      metricsRegistry.newCounter(ThirdeyeMetricsUtil.class, "cubeDurationCounter");

  private static final ConcurrentLinkedDeque<RequestLogEntry> dataSourceRequests = new ConcurrentLinkedDeque<>();

  public static MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }

  /**
   * Append statistics for a successful data source request to the performance log
   *
   * @param datasource data source name
   * @param dataset data set name
   * @param metric metric name
   * @param start request start time in ns
   * @param end request end time in ns
   */
  public static void logRequestSuccess(String datasource, String dataset, String metric, long start, long end) {
    dataSourceRequests.add(new RequestLogEntry(datasource, dataset, metric, true, start, end, null));
  }

  /**
   * Append statistics for a failed data source request to the performance log
   *
   * @param datasource data source name
   * @param dataset data set name
   * @param metric metric name
   * @param start request start time in ns
   * @param end request end time in ns
   * @param exception exception
   */
  public static void logRequestFailure(String datasource, String dataset, String metric, long start, long end, Exception exception) {
    dataSourceRequests.add(new RequestLogEntry(datasource, dataset, metric, false, start, end, exception));
  }

  /**
   * Trim the performance log to the minimum timestamp provided. Weakly consistent.
   *
   * @param minTimestamp lower time bound for performance log entries to keep
   */
  public static void truncateRequestLog(long minTimestamp) {
    Iterator<RequestLogEntry> itReq = dataSourceRequests.iterator();

    // weakly consistent iteration
    while (itReq.hasNext()) {
      if (itReq.next().start < minTimestamp) {
        itReq.remove();
      } else {
        break;
      }
    }
  }

  /**
   * Return aggregate data source performance statistics up to the given timestamp. Weakly consistent.
   *
   * @param maxTimestamp upper time bound for performance log entries to process
   * @return aggregated performance statistics
   */
  public static RequestStatistics getRequestStatistics(long maxTimestamp) {
    Map<String, Long> requestsPerDatasource = new HashMap<>();
    Map<String, Long> requestsPerDataset = new HashMap<>();
    Map<String, Long> requestsPerMetric = new HashMap<>();
    long requestsTotal = 0;

    Map<String, Long> successPerDatasource = new HashMap<>();
    Map<String, Long> successPerDataset = new HashMap<>();
    Map<String, Long> successPerMetric = new HashMap<>();
    long successTotal = 0;

    Map<String, Long> failurePerDatasource = new HashMap<>();
    Map<String, Long> failurePerDataset = new HashMap<>();
    Map<String, Long> failurePerMetric = new HashMap<>();
    long failureTotal = 0;

    Map<String, Long> durationPerDatasource = new HashMap<>();
    Map<String, Long> durationPerDataset = new HashMap<>();
    Map<String, Long> durationPerMetric = new HashMap<>();
    long durationTotal = 0;

    // weakly consistent iteration
    for (RequestLogEntry req : dataSourceRequests) {
      if (req.start > maxTimestamp) {
        break;
      }

      final String datasource = req.datasource;
      final String dataset = req.dataset;
      final String metric = req.dataset + "::" + req.metric;

      increment(requestsPerDatasource, datasource, 1);
      increment(requestsPerDataset, dataset, 1);
      increment(requestsPerMetric, metric, 1);

      if (req.success) {
        increment(successPerDatasource, datasource, 1);
        increment(successPerDataset, dataset, 1);
        increment(successPerMetric, metric, 1);
        successTotal++;
      } else {
        increment(failurePerDatasource, datasource, 1);
        increment(failurePerDataset, dataset, 1);
        increment(failurePerMetric, metric, 1);
        failureTotal++;
      }

      final long duration = Math.max(req.end - req.start, 0);
      increment(durationPerDatasource, datasource, duration);
      increment(durationPerDataset, dataset, duration);
      increment(durationPerMetric, metric, duration);
      durationTotal += duration;

      requestsTotal++;
    }

    RequestStatistics stats = new RequestStatistics();
    stats.setRequestsPerDatasource(requestsPerDatasource);
    stats.setRequestsPerDataset(requestsPerDataset);
    stats.setRequestsPerMetric(requestsPerMetric);
    stats.setRequestsTotal(requestsTotal);
    stats.setSuccessPerDatasource(successPerDatasource);
    stats.setSuccessPerDataset(successPerDataset);
    stats.setSuccessPerMetric(successPerMetric);
    stats.setSuccessTotal(successTotal);
    stats.setFailurePerDatasource(failurePerDatasource);
    stats.setFailurePerDataset(failurePerDataset);
    stats.setFailurePerMetric(failurePerMetric);
    stats.setFailureTotal(failureTotal);
    stats.setDurationPerDatasource(durationPerDatasource);
    stats.setDurationPerDataset(durationPerDataset);
    stats.setDurationPerMetric(durationPerMetric);
    stats.setDurationTotal(durationTotal);

    return stats;
  }

  /**
   * Helper to increment (possibly non-existing) map value by given value
   *
   * @param map map
   * @param key key
   * @param byValue value to increment by
   */
  private static void increment(Map<String, Long> map, String key, long byValue) {
    if (!map.containsKey(key)) {
      map.put(key, 0L);
    }
    map.put(key, map.get(key) + byValue);
  }
}
