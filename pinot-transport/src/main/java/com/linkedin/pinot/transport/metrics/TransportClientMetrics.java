/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.transport.metrics;

import com.linkedin.pinot.common.metrics.LatencyMetric;
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
