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

package com.linkedin.thirdeye.tracking;

import com.linkedin.thirdeye.auth.ThirdEyeAuthFilter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Concurrent request log. Tracks request success and failure events with deep information.
 * Support append and truncation plus an approximate capacity gauge.
 */
public class RequestLog {
  final int approximateCapacity;

  final ConcurrentLinkedDeque<RequestLogEntry> requestLog = new ConcurrentLinkedDeque<>();

  final AtomicInteger requestLogGauge = new AtomicInteger();

  public RequestLog(int approximateCapacity) {
    this.approximateCapacity = approximateCapacity;
  }

  /**
   * Append statistics for a successful data source request to the performance log. Discards oldest
   * log entries if capacity is exhausted.
   *
   * @param datasource data source name
   * @param dataset data set name
   * @param metric metric name
   * @param start request start time (in nanos)
   * @param end request end time (in nanos)
   */
  public void success(String datasource, String dataset, String metric, long start, long end) {
    if (this.requestLogGauge.getAndIncrement() >= this.approximateCapacity) {
      this.requestLog.removeFirst();
    }
    this.requestLog.add(new RequestLogEntry(datasource, dataset, metric, getPrincipal(), true, start, end, null));
  }

  /**
   * Append statistics for a failed data source request to the performance log. Discards oldest
   * log entries if capacity is exhausted.
   *
   * @param datasource data source name
   * @param dataset data set name
   * @param metric metric name
   * @param start request start time (in nanos)
   * @param end request end time (in nanos)
   * @param exception exception
   */
  public void failure(String datasource, String dataset, String metric, long start, long end, Exception exception) {
    if (this.requestLogGauge.getAndIncrement() >= this.approximateCapacity) {
      this.requestLog.removeFirst();
    }
    this.requestLog.add(new RequestLogEntry(datasource, dataset, metric, getPrincipal(), false, start, end, exception));
  }

  /**
   * Trim the performance log to the minimum timestamp provided. Weakly consistent.
   *
   * @param minTimestamp lower time bound for performance log entries to keep
   */
  public void truncate(long minTimestamp) {
    Iterator<RequestLogEntry> itReq = this.requestLog.iterator();

    // weakly consistent iteration
    while (itReq.hasNext()) {
      if (itReq.next().start < minTimestamp) {
        itReq.remove();
      } else {
        break;
      }
    }

    // weakly consistent limit
    this.requestLogGauge.set(this.requestLog.size());
  }

  /**
   * Return aggregate data source performance statistics up to the given timestamp. Weakly consistent.
   *
   * @param end upper time bound for performance log entries
   * @return aggregated performance statistics
   */
  public RequestStatistics getStatistics(long end) {
    Map<String, Long> requestsPerDatasource = new HashMap<>();
    Map<String, Long> requestsPerDataset = new HashMap<>();
    Map<String, Long> requestsPerMetric = new HashMap<>();
    Map<String, Long> requestsPerPrincipal = new HashMap<>();
    long requestsTotal = 0;

    Map<String, Long> successPerDatasource = new HashMap<>();
    Map<String, Long> successPerDataset = new HashMap<>();
    Map<String, Long> successPerMetric = new HashMap<>();
    Map<String, Long> successPerPrincipal = new HashMap<>();
    long successTotal = 0;

    Map<String, Long> failurePerDatasource = new HashMap<>();
    Map<String, Long> failurePerDataset = new HashMap<>();
    Map<String, Long> failurePerMetric = new HashMap<>();
    Map<String, Long> failurePerPrincipal = new HashMap<>();
    long failureTotal = 0;

    Map<String, Long> durationPerDatasource = new HashMap<>();
    Map<String, Long> durationPerDataset = new HashMap<>();
    Map<String, Long> durationPerMetric = new HashMap<>();
    Map<String, Long> durationPerPrincipal = new HashMap<>();
    long durationTotal = 0;

    // weakly consistent iteration
    for (RequestLogEntry req : this.requestLog) {
      if (req.start > end) {
        break;
      }

      final String datasource = req.datasource;
      final String dataset = req.dataset;
      final String metric = req.dataset + "::" + req.metric;
      final String principal = req.principal;

      increment(requestsPerDatasource, datasource);
      increment(requestsPerDataset, dataset);
      increment(requestsPerMetric, metric);
      increment(requestsPerPrincipal, principal);

      if (req.success) {
        increment(successPerDatasource, datasource);
        increment(successPerDataset, dataset);
        increment(successPerMetric, metric);
        increment(successPerPrincipal, principal);
        successTotal++;

      } else {
        increment(failurePerDatasource, datasource);
        increment(failurePerDataset, dataset);
        increment(failurePerMetric, metric);
        increment(failurePerPrincipal, principal);
        failureTotal++;
      }

      final long duration = Math.max(req.end - req.start, 0);
      increment(durationPerDatasource, datasource, duration);
      increment(durationPerDataset, dataset, duration);
      increment(durationPerMetric, metric, duration);
      increment(durationPerPrincipal, principal, duration);
      durationTotal += duration;

      requestsTotal++;
    }

    RequestStatistics stats = new RequestStatistics();
    stats.setRequestsPerDatasource(requestsPerDatasource);
    stats.setRequestsPerDataset(requestsPerDataset);
    stats.setRequestsPerMetric(requestsPerMetric);
    stats.setRequestsPerPrincipal(requestsPerPrincipal);
    stats.setRequestsTotal(requestsTotal);
    stats.setSuccessPerDatasource(successPerDatasource);
    stats.setSuccessPerDataset(successPerDataset);
    stats.setSuccessPerMetric(successPerMetric);
    stats.setSuccessPerPrincipal(successPerPrincipal);
    stats.setSuccessTotal(successTotal);
    stats.setFailurePerDatasource(failurePerDatasource);
    stats.setFailurePerDataset(failurePerDataset);
    stats.setFailurePerMetric(failurePerMetric);
    stats.setFailurePerPrincipal(failurePerPrincipal);
    stats.setFailureTotal(failureTotal);
    stats.setDurationPerDatasource(durationPerDatasource);
    stats.setDurationPerDataset(durationPerDataset);
    stats.setDurationPerMetric(durationPerMetric);
    stats.setDurationPerPrincipal(durationPerPrincipal);
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

  /**
   * Helper to increment (possibly non-existing) map value by one
   *
   * @param map map
   * @param key key
   */
  private static void increment(Map<String, Long> map, String key) {
    increment(map, key, 1);
  }

  /**
   * Helper to return current authenticated principal, if any.
   *
   * @return principal name
   */
  private static String getPrincipal() {
    if (ThirdEyeAuthFilter.getCurrentPrincipal() == null) {
      return "no-auth-user";
    }
    return ThirdEyeAuthFilter.getCurrentPrincipal().getName();
  }
}
