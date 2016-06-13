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
