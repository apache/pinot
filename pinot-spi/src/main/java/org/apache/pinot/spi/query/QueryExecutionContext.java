/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys;
import org.apache.pinot.spi.utils.CommonConstants.Query.Request.MetadataKeys.TimeSeries;


/// The context for query execution. It should be shared across all the threads executing the same query
/// ([QueryThreadContext#getExecutionContext()] returns the same instance). It tracks the tasks (as [Future]) executing
/// the query, and provides a way to terminate the query by cancelling all the tasks.
///
/// It is made JSON serializable for debugging purpose only, and should never be serialized in production.
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryExecutionContext {

  public enum QueryType {
    SSE,  // Single-stage engine
    MSE,  // Multi-stage engine
    TSE   // Time-series engine
  }

  private final QueryType _queryType;
  private final long _requestId;
  private final String _cid;
  private final String _workloadName;
  private final long _startTimeMs;
  private final long _activeDeadlineMs;
  private final long _passiveDeadlineMs;
  private final String _brokerId;
  private final String _instanceId;
  private final String _queryHash;

  @GuardedBy("this")
  private final List<Future<?>> _tasks = new ArrayList<>();

  private volatile TerminationException _terminateException;

  /// Per-query scan cost accumulators for scan-based killing, tracking cumulative scan cost across all segments.
  @Nullable
  private volatile QueryScanCostContext _queryScanCostContext;

  @Nullable
  private volatile Object _cachedKillingStrategy;

  @Nullable
  private volatile String _tableName;

  @Nullable
  private volatile String _queryId;

  @Nullable
  private volatile Accounting.ScanKillingMode _effectiveScanKillingMode;

  /// Guards single-emission of the scan-based killing dry-run log line and metric for this query
  private final AtomicBoolean _scanKillingDryRunEmitted = new AtomicBoolean(false);

  public QueryExecutionContext(QueryType queryType, long requestId, String cid, String workloadName, long startTimeMs,
      long activeDeadlineMs, long passiveDeadlineMs, String brokerId, String instanceId, String queryHash) {
    _queryType = queryType;
    _requestId = requestId;
    _cid = cid;
    _workloadName = workloadName;
    _startTimeMs = startTimeMs;
    _activeDeadlineMs = activeDeadlineMs;
    _passiveDeadlineMs = passiveDeadlineMs;
    _brokerId = brokerId;
    _instanceId = instanceId;
    _queryHash = queryHash;
  }

  public static QueryExecutionContext forMseServerRequest(Map<String, String> requestMetadata, String instanceId) {
    long startTimeMs = System.currentTimeMillis();
    long requestId = Long.parseLong(requestMetadata.get(MetadataKeys.REQUEST_ID));
    String cid = requestMetadata.get(MetadataKeys.CORRELATION_ID);
    if (cid == null) {
      cid = Long.toString(requestId);
    }
    String workloadName = requestMetadata.getOrDefault(QueryOptionKey.WORKLOAD_NAME, Accounting.DEFAULT_WORKLOAD_NAME);
    long timeoutMs = Long.parseLong(requestMetadata.get(QueryOptionKey.TIMEOUT_MS));
    long extraPassiveTimeoutMs =
        Long.parseLong(requestMetadata.getOrDefault(QueryOptionKey.EXTRA_PASSIVE_TIMEOUT_MS, "0"));
    long activeDeadlineMs = startTimeMs + timeoutMs;
    long passiveDeadlineMs = activeDeadlineMs + extraPassiveTimeoutMs;
    String brokerId = requestMetadata.getOrDefault(MetadataKeys.BROKER_ID, "unknown");
    String queryHash = requestMetadata.getOrDefault(QueryOptionKey.QUERY_HASH, Broker.DEFAULT_QUERY_HASH);
    return new QueryExecutionContext(QueryType.MSE, requestId, cid, workloadName, startTimeMs, activeDeadlineMs,
        passiveDeadlineMs, brokerId, instanceId, queryHash);
  }

  public static QueryExecutionContext forTseServerRequest(Map<String, String> requestMetadata, String instanceId) {
    long startTimeMs = System.currentTimeMillis();
    long requestId = Long.parseLong(requestMetadata.get(MetadataKeys.REQUEST_ID));
    String cid = requestMetadata.get(MetadataKeys.CORRELATION_ID);
    if (cid == null) {
      cid = Long.toString(requestId);
    }
    String workloadName = requestMetadata.getOrDefault(QueryOptionKey.WORKLOAD_NAME, Accounting.DEFAULT_WORKLOAD_NAME);
    long deadlineMs = Long.parseLong(requestMetadata.get(TimeSeries.DEADLINE_MS));
    String brokerId = requestMetadata.getOrDefault(MetadataKeys.BROKER_ID, "unknown");
    String queryHash = requestMetadata.getOrDefault(QueryOptionKey.QUERY_HASH, Broker.DEFAULT_QUERY_HASH);
    return new QueryExecutionContext(QueryType.TSE, requestId, cid, workloadName, startTimeMs, deadlineMs, deadlineMs,
        brokerId, instanceId, queryHash);
  }

  @VisibleForTesting
  public static QueryExecutionContext forSseTest() {
    return new QueryExecutionContext(QueryType.SSE, 123L, "cid", Accounting.DEFAULT_WORKLOAD_NAME,
        System.currentTimeMillis(), Long.MAX_VALUE, Long.MAX_VALUE, "brokerId", "instanceId", "");
  }

  @VisibleForTesting
  public static QueryExecutionContext forMseTest() {
    return new QueryExecutionContext(QueryType.MSE, 123L, "cid", Accounting.DEFAULT_WORKLOAD_NAME,
        System.currentTimeMillis(), Long.MAX_VALUE, Long.MAX_VALUE, "brokerId", "instanceId", "");
  }

  public QueryType getQueryType() {
    return _queryType;
  }

  public long getRequestId() {
    return _requestId;
  }

  public String getCid() {
    return _cid;
  }

  public String getWorkloadName() {
    return _workloadName;
  }

  public long getStartTimeMs() {
    return _startTimeMs;
  }

  public long getActiveDeadlineMs() {
    return _activeDeadlineMs;
  }

  public long getPassiveDeadlineMs() {
    return _passiveDeadlineMs;
  }

  public String getBrokerId() {
    return _brokerId;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String getQueryHash() {
    return _queryHash;
  }

  /// Adds a task to the execution context. If the query has been terminated, cancels the task immediately without
  /// adding it to the context.
  public synchronized void addTask(Future<?> task) {
    if (_terminateException != null) {
      task.cancel(true);
    } else {
      _tasks.add(task);
    }
  }

  /// Terminates the query execution with the given error code and message. Cancels and clears all the tasks.
  public synchronized boolean terminate(QueryErrorCode errorCode, String message) {
    if (_terminateException != null) {
      return false;
    }
    _terminateException = new TerminationException(errorCode, message);
    for (Future<?> task : _tasks) {
      task.cancel(true);
    }
    _tasks.clear();
    return true;
  }

  /// Returns the [TerminationException] if the query has been terminated, or null otherwise.
  @Nullable
  public TerminationException getTerminateException() {
    return _terminateException;
  }

  @Nullable
  public QueryScanCostContext getQueryScanCostContext() {
    return _queryScanCostContext;
  }

  public void setQueryScanCostContext(@Nullable QueryScanCostContext queryScanCostContext) {
    _queryScanCostContext = queryScanCostContext;
  }

  @Nullable
  public Object getCachedKillingStrategy() {
    return _cachedKillingStrategy;
  }

  public void setCachedKillingStrategy(@Nullable Object cachedKillingStrategy) {
    _cachedKillingStrategy = cachedKillingStrategy;
  }

  @Nullable
  public String getTableName() {
    return _tableName;
  }

  public void setTableName(@Nullable String tableName) {
    _tableName = tableName;
  }

  @Nullable
  public String getQueryId() {
    return _queryId;
  }

  public void setQueryId(@Nullable String queryId) {
    _queryId = queryId;
  }

  /**
   * Returns the per-table scan killing mode override set for this query, or {@code null} if no
   * table-level override is configured. When {@code null}, the cluster-level mode from
   * {@link org.apache.pinot.spi.utils.CommonConstants.Accounting} applies.
   */
  @Nullable
  public Accounting.ScanKillingMode getEffectiveScanKillingMode() {
    return _effectiveScanKillingMode;
  }

  /**
   * Sets the per-table scan killing mode for this query. Pass {@code null} to fall back to the
   * cluster-level mode. Called once during query initialization; thread-safe via {@code volatile}.
   */
  public void setEffectiveScanKillingMode(@Nullable Accounting.ScanKillingMode effectiveScanKillingMode) {
    _effectiveScanKillingMode = effectiveScanKillingMode;
  }

  /**
   * Atomically marks that the scan-based killing dry-run signal has been emitted for this query
   */
  public boolean markScanKillingDryRunEmitted() {
    return _scanKillingDryRunEmitted.compareAndSet(false, true);
  }
}
