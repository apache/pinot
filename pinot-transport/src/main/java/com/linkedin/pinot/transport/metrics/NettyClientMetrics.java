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
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;


public class NettyClientMetrics implements TransportClientMetrics {

  public static final String CONNECT_TIME = "CONNECT-MS";
  public static final String REQUESTS_SENT = "Requests-Sent";
  public static final String BYTES_SENT = "bytes-Sent";
  public static final String BYTES_RECEIVED = "bytes-received";
  public static final String ERRORS = "errors";
  public static final String SEND_REQUEST_MS = "Send-Request-MS";
  public static final String RESPONSE_LATENCY_MS = "Latency-MS";

  // Number of Requests Sent
  private final Counter _requestsSent;

  // Number of Request bytes sent
  private final Counter _bytesSent;

  // Number of Response bytes sent
  private final Counter _bytesReceived;

  // Number of Errors
  private final Counter _errors;

  // Send Request Latency Ms
  private final Histogram _sendRequestMsHistogram;

  // Response Latency Ms
  private final Histogram _responseLatencyMsHistogram;

  // Connect Time (MS)
  private final Gauge<Long> _connectMsGauge;
  private long _connectMs;

  public NettyClientMetrics(MetricsRegistry registry, String group) {
    _requestsSent = MetricsHelper.newCounter(registry, new MetricName(group, "", REQUESTS_SENT));
    _bytesSent = MetricsHelper.newCounter(registry, new MetricName(group, "", BYTES_SENT));
    _bytesReceived = MetricsHelper.newCounter(registry, new MetricName(group, "", BYTES_RECEIVED));
    _errors = MetricsHelper.newCounter(registry, new MetricName(group, "", ERRORS));
    _sendRequestMsHistogram = MetricsHelper.newHistogram(registry, new MetricName(group, "", SEND_REQUEST_MS), false);
    _responseLatencyMsHistogram =
        MetricsHelper.newHistogram(registry, new MetricName(group, "", RESPONSE_LATENCY_MS), false);
    _connectMsGauge = MetricsHelper.newGauge(registry, new MetricName(group, "", CONNECT_TIME), new ConnectMsGauge());
  }

  public void addRequestResponseStats(long bytesSent, long numRequests, long bytesReceived,
      boolean isError, long sendRequestMs, long responseLatencyMs) {
    _requestsSent.inc(numRequests);
    _bytesSent.inc(bytesSent);
    _bytesReceived.inc(bytesReceived);

    if (isError)
      _errors.inc();

    _sendRequestMsHistogram.update(sendRequestMs);
    _responseLatencyMsHistogram.update(responseLatencyMs);
  }

  public void addConnectStats(long connectMs) {
    _connectMs = connectMs;
  }

  public Counter getRequestsSent() {
    return _requestsSent;
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

  public Histogram getSendRequestMsHistogram() {
    return _sendRequestMsHistogram;
  }

  public Histogram getResponseLatencyMsHistogram() {
    return _responseLatencyMsHistogram;
  }

  @Override
  public String toString() {
    return "NettyClientMetric [_requestsSent=" + _requestsSent.count() + ", _bytesSent=" + _bytesSent.count()
        + ", _bytesReceived=" + _bytesReceived.count() + ", _errors=" + _errors.count() + ", _sendRequestMsGauge="
        + _sendRequestMsHistogram.count() + ", _responseLatencyMsGauge=" + _responseLatencyMsHistogram.count()
        + ", _connectMsGauge=" + _connectMsGauge.value() + "]";
  }

  private class ConnectMsGauge extends Gauge<Long> {
    @Override
    public Long value() {
      return _connectMs;
    }
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
  public LatencyMetric<Histogram> getSendRequestLatencyMs() {
    return new LatencyMetric<Histogram>(_sendRequestMsHistogram);
  }

  @Override
  public LatencyMetric<Histogram> getResponseLatencyMs() {
    return new LatencyMetric<Histogram>(_responseLatencyMsHistogram);
  }

  @Override
  public long getConnectTimeMs() {
    return _connectMs;
  }

  public Gauge<Long> getConnectMsGauge() {
    return _connectMsGauge;
  }
}
