package com.linkedin.pinot.transport.metrics;

import com.linkedin.pinot.metrics.common.LatencyMetric;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;

public interface TransportClientMetrics {

  /**
   * Get total number of requests sent by this client
   * @return
   */
  public long getTotalRequests();

  /**
   * Get total number of bytes sent by this client
   * @return
   */
  public long getTotalBytesSent();

  /**
   * Get total number of bytes received by this client
   * @return
   */
  public long getTotalBytesReceived();

  /**
   * Get total errors seen by this client
   * @return
   */
  public long getTotalErrors();

  /**
   * Get Latency metrics for flushing a request
   * @return
   */
  public <T extends Sampling & Summarizable> LatencyMetric<T> getSendRequestLatencyMs();

  /**
   * Get round-trip latency metric for the request
   * @return
   */
  public <T extends Sampling & Summarizable> LatencyMetric<T> getResponseLatencyMs();

  /**
   * Get Connect time in Ms
   * @return
   */
  public long getConnectTimeMs();
}
