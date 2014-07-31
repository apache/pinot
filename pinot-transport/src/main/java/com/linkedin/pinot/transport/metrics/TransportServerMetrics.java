package com.linkedin.pinot.transport.metrics;

import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;

public interface TransportServerMetrics {

  /**
   * Get total requests received by this server handler
   * @return
   */
  public long getTotalRequests();

  /**
   * Get total response bytes sent by this server instance
   * @return
   */
  public long getTotalBytesSent();

  /**
   * Get total request bytes received by this server instance
   * @return
   */
  public long getTotalBytesReceived();

  /**
   * Get total errors
   * @return
   */
  public long getTotalErrors();

  /**
   * Get Latency Metric for flushing the response
   * @return
   */
  public <T extends Sampling & Summarizable> LatencyMetric<T> getSendResponseLatencyMs();

  /**
   * Get Latency metric  for processing the request (including the time to write the response)
   * @return
   */
  public <T extends Sampling & Summarizable> LatencyMetric<T> getProcessingLatencyMs();

}
