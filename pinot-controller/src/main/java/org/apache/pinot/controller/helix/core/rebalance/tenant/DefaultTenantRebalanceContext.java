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

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Queue;


/**
 * Default implementation of TenantRebalanceContext that includes parallel and sequential queues
 * for managing tenant rebalance operations.
 */
public class DefaultTenantRebalanceContext extends TenantRebalanceContext {
  private final ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> _parallelQueue;
  private final Queue<TenantRebalancer.TenantTableRebalanceJobContext> _sequentialQueue;
  private final ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> _ongoingJobsQueue;

  public DefaultTenantRebalanceContext() {
    super();
    _parallelQueue = new ConcurrentLinkedDeque<>();
    _sequentialQueue = new LinkedList<>();
    _ongoingJobsQueue = new ConcurrentLinkedQueue<>();
  }

  public DefaultTenantRebalanceContext(String originalJobId, TenantRebalanceConfig config, int attemptId,
      boolean allowRetries, ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue,
      Queue<TenantRebalancer.TenantTableRebalanceJobContext> sequentialQueue,
      ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> ongoingJobsQueue) {
    super(originalJobId, config, attemptId, allowRetries);
    _parallelQueue = parallelQueue;
    _sequentialQueue = sequentialQueue;
    _ongoingJobsQueue = ongoingJobsQueue;
  }

  public static DefaultTenantRebalanceContext forInitialRebalance(String originalJobId, TenantRebalanceConfig config,
      boolean allowRetries, ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue,
      Queue<TenantRebalancer.TenantTableRebalanceJobContext> sequentialQueue) {
    return new DefaultTenantRebalanceContext(originalJobId, config, INITIAL_ATTEMPT_ID, allowRetries,
        parallelQueue, sequentialQueue, new ConcurrentLinkedQueue<>());
  }

  public static DefaultTenantRebalanceContext forRetry(String originalJobId, TenantRebalanceConfig config,
      int attemptId, ConcurrentLinkedDeque<TenantRebalancer.TenantTableRebalanceJobContext> parallelQueue,
      Queue<TenantRebalancer.TenantTableRebalanceJobContext> sequentialQueue,
      ConcurrentLinkedQueue<TenantRebalancer.TenantTableRebalanceJobContext> ongoingJobsQueue) {
    return new DefaultTenantRebalanceContext(originalJobId, config, attemptId, true,
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

  @Override
  public String toString() {
    return "DefaultTenantRebalanceContext{" +
        "jobId='" + getJobId() + '\'' +
        ", originalJobId='" + getOriginalJobId() + '\'' +
        ", attemptId=" + getAttemptId() +
        ", allowRetries=" + getAllowRetries() +
        ", parallelQueueSize=" + getParallelQueue().size() +
        ", sequentialQueueSize=" + getSequentialQueue().size() +
        ", ongoingJobsQueueSize=" + getOngoingJobsQueue().size() +
        '}';
  }
}
