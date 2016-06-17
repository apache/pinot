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

import com.linkedin.pinot.common.metrics.AggregatedCounter;
import com.linkedin.pinot.common.metrics.AggregatedHistogram;
import com.linkedin.pinot.common.metrics.AggregatedMetricsRegistry;
import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;


/**
 *
 * Aggregated Transport Server Metrics. Provides multi-level aggregation.
 *
 */
public class AggregatedTransportServerMetrics implements TransportServerMetrics {

  public static final String REQUESTS_RECEIVED = "Requests-Sent";
  public static final String BYTES_SENT = "bytes-Sent";
  public static final String BYTES_RECEIVED = "bytes-received";
  public static final String SEND_RESPONSE_MS = "Send-Response-MS";
  public static final String PROCESSING_LATENCY_MS = "Processing-Latency-MS";
  public static final String ERRORS = "errors";

  // Num Requests
  private final AggregatedCounter _requestsReceived;

  // Request bytes
  private final AggregatedCounter _bytesSent;

  // Response Bytes
  private final AggregatedCounter _bytesReceived;

  // Errors
  private final AggregatedCounter _errors;

  // Latency for sending response
  private final AggregatedHistogram<Sampling> _sendResponseMsHistogram;

  // Total processing latency including that of sending response
  private final AggregatedHistogram<Sampling> _processingLatencyMsHistogram;

  public AggregatedTransportServerMetrics(AggregatedMetricsRegistry registry, String group) {
    _requestsReceived = MetricsHelper.newAggregatedCounter(registry, new MetricName(group, "", REQUESTS_RECEIVED));
    _bytesSent = MetricsHelper.newAggregatedCounter(registry, new MetricName(group, "", BYTES_SENT));
    _bytesReceived = MetricsHelper.newAggregatedCounter(registry, new MetricName(group, "", BYTES_RECEIVED));
    _errors = MetricsHelper.newAggregatedCounter(registry, new MetricName(group, "", ERRORS));
    _sendResponseMsHistogram =
        MetricsHelper.newAggregatedHistogram(registry, new MetricName(group, "", SEND_RESPONSE_MS));
    _processingLatencyMsHistogram =
        MetricsHelper.newAggregatedHistogram(registry, new MetricName(group, "", PROCESSING_LATENCY_MS));
  }

  /**
   * Add NettyServerMetrics to aggregated metrics
   * @param metric metric to be aggregated
   */
  public void addTransportClientMetrics(NettyServerMetrics metric) {
    _requestsReceived.add(metric.getRequestsReceived());
    _bytesSent.add(metric.getBytesSent());
    _bytesReceived.add(metric.getBytesReceived());
    _errors.add(metric.getErrors());
    _sendResponseMsHistogram.add(metric.getSendResponseMsHistogram());
    _processingLatencyMsHistogram.add(metric.getProcessingLatencyMsHistogram());
  }

  /**
   * Add another AggregatedTransportServerMetrics to this aggregated metrics to create
   * multi-level aggregation
   * @param metric metric to be aggregated
   */
  public void addTransportClientMetrics(AggregatedTransportServerMetrics metric) {
    _requestsReceived.add(metric.getRequestsReceived());
    _bytesSent.add(metric.getBytesSent());
    _bytesReceived.add(metric.getBytesReceived());
    _errors.add(metric.getErrors());
    _sendResponseMsHistogram.add(metric.getSendResponseMsHistogram());
    _processingLatencyMsHistogram.add(metric.getProcessingLatencyMsHistogram());
  }

  /**
   * Remove NettyServerMetrics to aggregated metrics
   * @param metric metric to be be removed
   */
  public void removeTransportClientMetrics(NettyServerMetrics metric) {
    _requestsReceived.remove(metric.getRequestsReceived());
    _bytesSent.remove(metric.getBytesSent());
    _bytesReceived.remove(metric.getBytesReceived());
    _errors.remove(metric.getErrors());
    _sendResponseMsHistogram.remove(metric.getSendResponseMsHistogram());
    _processingLatencyMsHistogram.remove(metric.getProcessingLatencyMsHistogram());
  }

  /**
   * Remove AggregatedTransportServerMetrics to aggregated metrics
   * @param metric metric to be be removed
   */
  public void removeTransportClientMetrics(AggregatedTransportServerMetrics metric) {
    _requestsReceived.remove(metric.getRequestsReceived());
    _bytesSent.remove(metric.getBytesSent());
    _bytesReceived.remove(metric.getBytesReceived());
    _errors.remove(metric.getErrors());
    _sendResponseMsHistogram.remove(metric.getSendResponseMsHistogram());
    _processingLatencyMsHistogram.remove(metric.getProcessingLatencyMsHistogram());
  }

  @Override
  public long getTotalRequests() {
    return _requestsReceived.count();
  }

  @Override
  public long getTotalBytesSent() {
    return _bytesSent.count();
  }

  @Override
  public long getTotalBytesReceived() {
    return _bytesReceived.count();
  }

  @Override
  public long getTotalErrors() {
    return _errors.count();
  }

  @Override
  public <T extends Sampling & Summarizable> LatencyMetric<T> getSendResponseLatencyMs() {
    return new LatencyMetric(_sendResponseMsHistogram);
  }

  @Override
  public <T extends Sampling & Summarizable> LatencyMetric<T> getProcessingLatencyMs() {
    return new LatencyMetric(_processingLatencyMsHistogram);
  }

  private AggregatedCounter getRequestsReceived() {
    return _requestsReceived;
  }

  private AggregatedCounter getBytesSent() {
    return _bytesSent;
  }

  private AggregatedCounter getBytesReceived() {
    return _bytesReceived;
  }

  private AggregatedCounter getErrors() {
    return _errors;
  }

  private AggregatedHistogram<Sampling> getSendResponseMsHistogram() {
    return _sendResponseMsHistogram;
  }

  private AggregatedHistogram<Sampling> getProcessingLatencyMsHistogram() {
    return _processingLatencyMsHistogram;
  }
}
