package com.linkedin.thirdeye.anomaly.utils;

import com.linkedin.thirdeye.api.TimeGranularity;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RequestStatisticsLogger implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RequestStatisticsLogger.class);

  private ScheduledExecutorService scheduledExecutorService;
  private TimeGranularity runFrequency;

  public RequestStatisticsLogger(TimeGranularity runFrequency) {
    this.runFrequency = runFrequency;
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void run() {
    long timestamp = System.nanoTime();
    RequestStatistics stats = ThirdeyeMetricsUtil.getRequestStatistics(timestamp);
    ThirdeyeMetricsUtil.truncateRequestLog(timestamp);

    RequestStatisticsFormatter formatter = new RequestStatisticsFormatter();
    LOG.info("Recent request performance statistics:\n{}", formatter.format(stats));
  }

  public void start() {
    this.scheduledExecutorService.scheduleWithFixedDelay(this,
        this.runFrequency.getSize(), this.runFrequency.getSize(), this.runFrequency.getUnit());
  }

  public void shutdown() {
    this.scheduledExecutorService.shutdown();
  }
}
