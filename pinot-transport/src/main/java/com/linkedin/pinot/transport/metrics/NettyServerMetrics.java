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
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;


public class NettyServerMetrics implements TransportServerMetrics {
  public static final String REQUESTS_RECEIVED = "Requests-Sent";
  public static final String BYTES_SENT = "bytes-Sent";
  public static final String BYTES_RECEIVED = "bytes-received";
  public static final String SEND_RESPONSE_MS = "Send-Response-MS";
  public static final String PROCESSING_LATENCY_MS = "Processing-Latency-MS";
  public static final String ERRORS = "errors";

  // Num Requests
  private final Counter _requestsReceived;

  // Request bytes
  private final Counter _bytesSent;

  // Response Bytes
  private final Counter _bytesReceived;

  // Errors
  private final Counter _errors;

  // Latency for sending response
  private final Histogram _sendResponseMsHistogram;

  // Total processing latency including that of sending response
  private final Histogram _processingLatencyMsHistogram;

  public NettyServerMetrics(MetricsRegistry registry, String group) {
    _requestsReceived = MetricsHelper.newCounter(registry, new MetricName(group, "", REQUESTS_RECEIVED));
    _bytesSent = MetricsHelper.newCounter(registry, new MetricName(group, "", BYTES_SENT));
    _bytesReceived = MetricsHelper.newCounter(registry, new MetricName(group, "", BYTES_RECEIVED));
    _errors = MetricsHelper.newCounter(registry, new MetricName(group, "", ERRORS));
    _sendResponseMsHistogram = MetricsHelper.newHistogram(registry, new MetricName(group, "", SEND_RESPONSE_MS), false);
    _processingLatencyMsHistogram =
        MetricsHelper.newHistogram(registry, new MetricName(group, "", PROCESSING_LATENCY_MS), false);
  }

  public void addServingStats(long requestSize, long responseSize, long numRequests, boolean error,
      long processingLatencyMs, long sendResponseLatencyMs) {
    _requestsReceived.inc(numRequests);
    _bytesReceived.inc(requestSize);
    _bytesSent.inc(responseSize);
    if (error)
      _errors.inc();
    _sendResponseMsHistogram.update(sendResponseLatencyMs);
    _processingLatencyMsHistogram.update(processingLatencyMs);
  }

  public Counter getRequestsReceived() {
    return _requestsReceived;
  }

  public Counter getBytesSent() {
    return _bytesSent;
  }

  public Counter getBytesReceived() {
    return _bytesReceived;
  }

  public Counter getErrors() {
    return _errors;
  }

  public Histogram getSendResponseMsHistogram() {
    return _sendResponseMsHistogram;
  }

  public Histogram getProcessingLatencyMsHistogram() {
    return _processingLatencyMsHistogram;
  }

  @Override
  public String toString() {
    return "NettyServerMetric [_requestsReceived=" + _requestsReceived.count() + ", _bytesSent=" + _bytesSent.count()
        + ", _bytesReceived=" + _bytesReceived.count() + ", _errors=" + _errors.count() + ", _sendResponseMsGauge="
        + _sendResponseMsHistogram.count() + ", _processingLatencyMsGauge=" + _processingLatencyMsHistogram.count()
        + "]";
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
  public LatencyMetric<Histogram> getSendResponseLatencyMs() {
    return new LatencyMetric<Histogram>(_sendResponseMsHistogram);
  }

  @Override
  public LatencyMetric<Histogram> getProcessingLatencyMs() {
    return new LatencyMetric<Histogram>(_processingLatencyMsHistogram);
  }
}
