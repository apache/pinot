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
package org.apache.pinot.spi.accounting;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;


public interface ThreadResourceUsageAccountant {

  /**
   * clear thread accounting info when a task finishes execution on a thread
   */
  void clear();

  /**
   * check if the corresponding anchor thread of current thread is interrupted
   * so that when we preempt a task we only call interrupt on the anchor thread
   */
  boolean isAnchorThreadInterrupted();

  /**
   * This method has been deprecated and replaced by {@link setupRunner(String, int, ThreadExecutionContext.TaskType)}
   * and {@link setupWorker(int, ThreadExecutionContext.TaskType, ThreadExecutionContext)}.
   */
  @Deprecated
  void createExecutionContext(String queryId, int taskId, ThreadExecutionContext.TaskType taskType,
      @Nullable ThreadExecutionContext parentContext);

  /**
   * Set up the thread execution context for an anchor a.k.a runner thread.
   * @param queryId query id string
   * @param taskId a unique task id
   * @param taskType the type of the task - SSE or MSE
   */
  void setupRunner(String queryId, int taskId, ThreadExecutionContext.TaskType taskType);

  /**
   * Set up the thread execution context for a worker thread.
   * @param taskId a unique task id
   * @param taskType the type of the task - SSE or MSE
   * @param parentContext the parent execution context
   */
  void setupWorker(int taskId, ThreadExecutionContext.TaskType taskType,
      @Nullable ThreadExecutionContext parentContext);

  /**
   * get the executon context of current thread
   */
  @Nullable
  ThreadExecutionContext getThreadExecutionContext();

  /**
   * set resource usage provider
   */
  @Deprecated
  void setThreadResourceUsageProvider(ThreadResourceUsageProvider threadResourceUsageProvider);

  /**
   * call to sample usage
   */
  void sampleUsage();

  /**
   * Sample Usage for Multi-stage engine queries
   */
  void sampleUsageMSE();

  /**
   * special interface to aggregate usage to the stats store only once, it is used for response
   * ser/de threads where the thread execution context cannot be setup before hands as
   * queryId/taskId is unknown and the execution process is hard to instrument
   */
  void updateQueryUsageConcurrently(String queryId, long cpuTimeNs, long allocatedBytes);

  @Deprecated
  void updateQueryUsageConcurrently(String queryId);

  /**
   * start the periodical task
   */
  void startWatcherTask();

  @Nullable
  default PinotClusterConfigChangeListener getClusterConfigChangeListener() {
    return null;
  }

  /**
   * get error status if the query is preempted
   * @return empty string if N/A
   */
  Exception getErrorStatus();

  /**
   * Get all the ThreadResourceTrackers for all threads executing query tasks
   * @return A collection of ThreadResourceTracker objects
   */
  Collection<? extends ThreadResourceTracker> getThreadResources();

  /**
   * Get all the QueryResourceTrackers for all the queries executing in a broker or server.
   * @return A Map of String, QueryResourceTracker for all the queries.
   */
  Map<String, ? extends QueryResourceTracker> getQueryResources();
}
