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

import com.linkedin.pinot.common.metrics.AggregatedCounter;
import com.linkedin.pinot.common.metrics.AggregatedHistogram;
import com.linkedin.pinot.common.metrics.AggregatedLongGauge;
import com.linkedin.pinot.common.metrics.AggregatedMetricsRegistry;
import com.linkedin.pinot.common.metrics.LatencyMetric;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;


/**
 *
 * Aggregated Transport Client Metrics. Provides multi-level aggregation.
 *
 */
public class AggregatedTransportClientMetrics implements TransportClientMetrics {

  public static final String CONNECT_TIME = "CONNECT-MS";
  public static final String REQUESTS_SENT = "Requests-Sent";
  public static final String BYTES_SENT = "bytes-Sent";
  public static final String BYTES_RECEIVED = "bytes-received";
  public static final String ERRORS = "errors";
  public static final String SEND_REQUEST_MS = "Send-Request-MS";
  public static final String RESPONSE_LATENCY_MS = "Latency-MS";

  // Number of Requests Sent
  private final AggregatedCounter _requestsSent;

  // Number of Request bytes sent
  private final AggregatedCounter _bytesSent;

  // Number of Response bytes sent
  private final AggregatedCounter _bytesReceived;

  // Number of Errors
  private final AggregatedCounter _errors;

  // Send Request Latency Ms
  private final AggregatedHistogram<Sampling> _sendRequestMsHistogram;

  // Response Latency Ms
  private final AggregatedHistogram<Sampling> _responseLatencyMsHistogram;

  // Connect Time (MS)
  private final AggregatedLongGauge<Long, Gauge<Long>> _connectMsGauge;

  public AggregatedTransportClientMetrics(AggregatedMetricsRegistry registry, String group) {
    _requestsSent = MetricsHelper.newAggregatedCounter(registry, new MetricName(group, "", REQUESTS_SENT));
    _bytesSent = MetricsHelper.newAggregatedCounter(registry, new MetricName(group, "", BYTES_SENT));
    _bytesReceived = MetricsHelper.newAggregatedCounter(registry, new MetricName(group, "", BYTES_RECEIVED));
    _errors = MetricsHelper.newAggregatedCounter(registry, new MetricName(group, "", ERRORS));
    _sendRequestMsHistogram =
        MetricsHelper.newAggregatedHistogram(registry, new MetricName(group, "", SEND_REQUEST_MS));
    _responseLatencyMsHistogram =
        MetricsHelper.newAggregatedHistogram(registry, new MetricName(group, "", RESPONSE_LATENCY_MS));
    _connectMsGauge = MetricsHelper.newAggregatedLongGauge(registry, new MetricName(group, "", CONNECT_TIME));
  }

  /**
   * Add NettyClientMetrics to aggregated metrics
   * @param metric metric to be aggregated
   */
  public void addTransportClientMetrics(NettyClientMetrics metric) {
    _requestsSent.add(metric.getRequestsSent());
    _bytesSent.add(metric.getBytesSent());
    _bytesReceived.add(metric.getBytesReceived());
    _errors.add(metric.getErrors());
    _connectMsGauge.add(metric.getConnectMsGauge());
    _sendRequestMsHistogram.add(metric.getSendRequestMsHistogram());
    _responseLatencyMsHistogram.add(metric.getResponseLatencyMsHistogram());
  }

  /**
   * Add another AggregatedTransportClientMetrics to this aggregated metrics to create
   * multi-level aggregation
   * @param metric metric to be aggregated
   */
  public void addTransportClientMetrics(AggregatedTransportClientMetrics metric) {
    _requestsSent.add(metric.getRequestsSent());
    _bytesSent.add(metric.getBytesSent());
    _bytesReceived.add(metric.getBytesReceived());
    _errors.add(metric.getErrors());
    _connectMsGauge.add(metric.getConnectMsGauge());
    _sendRequestMsHistogram.add(metric.getSendRequestMsHistogram());
    _responseLatencyMsHistogram.add(metric.getResponseLatencyMsHistogram());
  }

  /**
   * Remove NettyClientMetrics to aggregated metrics
   * @param metric metric to be be removed
   */
  public void removeTransportClientMetrics(NettyClientMetrics metric) {
    _requestsSent.remove(metric.getRequestsSent());
    _bytesSent.remove(metric.getBytesSent());
    _bytesReceived.remove(metric.getBytesReceived());
    _errors.remove(metric.getErrors());
    _connectMsGauge.remove(metric.getConnectMsGauge());
    _sendRequestMsHistogram.remove(metric.getSendRequestMsHistogram());
    _responseLatencyMsHistogram.remove(metric.getResponseLatencyMsHistogram());
  }

  /**
   * Remove AggregatedTransportClientMetrics to aggregated metrics
   * @param metric metric to be be removed
   */
  public void removeTransportClientMetrics(AggregatedTransportClientMetrics metric) {
    _requestsSent.remove(metric.getRequestsSent());
    _bytesSent.remove(metric.getBytesSent());
    _bytesReceived.remove(metric.getBytesReceived());
    _errors.remove(metric.getErrors());
    _connectMsGauge.remove(metric.getConnectMsGauge());
    _sendRequestMsHistogram.remove(metric.getSendRequestMsHistogram());
    _responseLatencyMsHistogram.remove(metric.getResponseLatencyMsHistogram());
  }

  @Override
  public long getTotalRequests() {
    return _requestsSent.count();
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
  public <T extends Sampling & Summarizable> LatencyMetric<T> getSendRequestLatencyMs() {
    return new LatencyMetric(_sendRequestMsHistogram);
  }

  @Override
  public <T extends Sampling & Summarizable> LatencyMetric<T> getResponseLatencyMs() {
    return new LatencyMetric(_responseLatencyMsHistogram);
  }

  @Override
  public long getConnectTimeMs() {
    return _connectMsGauge.value();
  }

  private AggregatedCounter getRequestsSent() {
    return _requestsSent;
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

  private AggregatedHistogram<Sampling> getSendRequestMsHistogram() {
    return _sendRequestMsHistogram;
  }

  private AggregatedHistogram<Sampling> getResponseLatencyMsHistogram() {
    return _responseLatencyMsHistogram;
  }

  private AggregatedLongGauge<Long, Gauge<Long>> getConnectMsGauge() {
    return _connectMsGauge;
  }
}
