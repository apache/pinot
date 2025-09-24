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
package org.apache.pinot.controller.helix.core.rebalance.tenant;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceJobConstants;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Default implementation of TenantRebalanceContext that includes parallel and sequential queues
 * for managing tenant rebalance operations. This context is synchronized to ZK by `ZkBasedTenantRebalanceObserver`
 * to ensure consistency across controller restarts.
 */
public class TenantRebalanceContext {
  protected static final int INITIAL_ATTEMPT_ID = 1;
  @JsonProperty("jobId")
  private final String _jobId;
  @JsonProperty("originalJobId")
  private final String _originalJobId;
  @JsonProperty("config")
  private final TenantRebalanceConfig _config;
  @JsonProperty("attemptId")
  private final int _attemptId;
  // Ongoing jobs queue and parallel queue are accessed concurrently by multiple threads, where each worker thread
  // consumes a tenant-table-rebalance-job from the parallel queue, adds it to the ongoing jobs queue, processes it.
  // On the other hand, only a single thread consumes from the sequential queue.
  private final ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> _ongoingJobsQueue;
  private final ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> _parallelQueue;
  private final Queue<TenantRebalancer.TenantTableRebalanceJobContext> _sequentialQueue;

  // Default constructor for JSON deserialization
  public TenantRebalanceContext() {
    _jobId = null;
    _originalJobId = null;
    _config = null;
    _attemptId = INITIAL_ATTEMPT_ID;
    _parallelQueue = new ConcurrentLinkedDeque<>();
    _sequentialQueue = new LinkedList<>();
    _ongoingJobsQueue = new ConcurrentLinkedQueue<>();
  }

  public TenantRebalanceContext(TenantRebalanceContext context) {
    _jobId = context._jobId;
    _originalJobId = context._originalJobId;
    _config = context._config;
    _attemptId = context._attemptId;
    _parallelQueue = new ConcurrentLinkedDeque<>(context._parallelQueue);
    _sequentialQueue = new LinkedList<>(context._sequentialQueue);
    _ongoingJobsQueue = new ConcurrentLinkedQueue<>(context._ongoingJobsQueue);
  }

  public TenantRebalanceContext(String originalJobId, TenantRebalanceConfig config, int attemptId,
      ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue,
      Queue<TenantRebalancer.TenantTableRebalanceJobContext> sequentialQueue,
      ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> ongoingJobsQueue) {
    _jobId = createAttemptJobId(originalJobId, attemptId);
    _originalJobId = originalJobId;
    _config = config;
    _attemptId = attemptId;
    _parallelQueue = new ConcurrentLinkedDeque<>(parallelQueue);
    _sequentialQueue = new LinkedList<>(sequentialQueue);
    _ongoingJobsQueue = new ConcurrentLinkedQueue<>(ongoingJobsQueue);
  }

  public static TenantRebalanceContext forInitialRebalance(String originalJobId, TenantRebalanceConfig config,
      ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue,
      Queue<TenantRebalancer.TenantTableRebalanceJobContext> sequentialQueue) {
    return new TenantRebalanceContext(originalJobId, config, INITIAL_ATTEMPT_ID,
        parallelQueue, sequentialQueue, new ConcurrentLinkedQueue<>());
  }

  public static TenantRebalanceContext forRetry(String originalJobId, TenantRebalanceConfig config,
      int attemptId, ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue,
      Queue<TenantRebalancer.TenantTableRebalanceJobContext> sequentialQueue,
      ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> ongoingJobsQueue) {
    return new TenantRebalanceContext(originalJobId, config, attemptId,
        parallelQueue, sequentialQueue, ongoingJobsQueue);
  }

  public ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> getParallelQueue() {
    return _parallelQueue;
  }

  public Queue<TenantRebalancer.TenantTableRebalanceJobContext> getSequentialQueue() {
    return _sequentialQueue;
  }

  public ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> getOngoingJobsQueue() {
    return _ongoingJobsQueue;
  }

  public int getAttemptId() {
    return _attemptId;
  }

  public String getOriginalJobId() {
    return _originalJobId;
  }

  public String getJobId() {
    return _jobId;
  }

  public TenantRebalanceConfig getConfig() {
    return _config;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TenantRebalanceContext)) {
      return false;
    }
    TenantRebalanceContext that = (TenantRebalanceContext) o;
    return _attemptId == that._attemptId && Objects.equals(_jobId, that._jobId) && Objects.equals(
        _originalJobId, that._originalJobId) && Objects.equals(_config, that._config)
        && Arrays.equals(_ongoingJobsQueue.toArray(), that._ongoingJobsQueue.toArray()) && Arrays.equals(
        _sequentialQueue.toArray(), that._sequentialQueue.toArray()) && Arrays.equals(_parallelQueue.toArray(),
        that._parallelQueue.toArray());
  }

  @Override
  public int hashCode() {
    return Objects.hash(_jobId, _originalJobId, _config, _attemptId, _ongoingJobsQueue, _parallelQueue,
        _sequentialQueue);
  }

  public String toString() {
    return "TenantRebalanceContext{" + "jobId='" + getJobId() + '\'' + ", originalJobId='" + getOriginalJobId()
        + '\'' + ", attemptId=" + getAttemptId() + ", parallelQueueSize="
        + getParallelQueue().size() + ", sequentialQueueSize=" + getSequentialQueue().size() + ", ongoingJobsQueueSize="
        + getOngoingJobsQueue().size() + '}';
  }

  private static String createAttemptJobId(String originalJobId, int attemptId) {
    if (attemptId == INITIAL_ATTEMPT_ID) {
      return originalJobId;
    }
    return originalJobId + "_" + attemptId;
  }

  public static TenantRebalanceContext fromTenantRebalanceJobMetadata(Map<String, String> jobMetadata)
      throws JsonProcessingException {
    String tenantRebalanceContextStr = jobMetadata.get(RebalanceJobConstants.JOB_METADATA_KEY_REBALANCE_CONTEXT);
    return JsonUtils.stringToObject(tenantRebalanceContextStr, TenantRebalanceContext.class);
  }
}
