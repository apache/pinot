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
package org.apache.pinot.core.query.scheduler.resources;

import com.google.common.base.Preconditions;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ResourceManager class to manage threadpools as per the configured policy
 */
public class PolicyBasedResourceManager extends ResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolicyBasedResourceManager.class);

  private final ResourceLimitPolicy _resourcePolicy;

  public PolicyBasedResourceManager(PinotConfiguration config) {
    super(config);
    _resourcePolicy = new ResourceLimitPolicy(config, _numQueryWorkerThreads);
  }

  /**
   * Returns an executor service that query executor can use like a dedicated
   * service for submitting jobs for parallel execution.
   * @param query
   * @param accountant Accountant for a scheduler group
   * @return BoundedAccountingExecutor service that limits the number of threads available
   * for query execution. Query execution can submit tasks for parallel execution without need
   * for limiting their parallelism.
   */
  @Override
  public QueryExecutorService getExecutorService(ServerQueryRequest query, SchedulerGroupAccountant accountant) {
    int numSegments = query.getSegmentsToQuery().size();
    int queryThreadLimit = Math.max(1, Math.min(_resourcePolicy.getMaxThreadsPerQuery(), numSegments));
    int spareThreads = _resourcePolicy.getTableThreadsHardLimit() - accountant.totalReservedThreads();
    if (spareThreads <= 0) {
      LOGGER.warn("UNEXPECTED: Attempt to schedule query uses more than the configured hard limit on threads");
      spareThreads = 1;
    } else {
      spareThreads = Math.min(spareThreads, queryThreadLimit);
    }
    Preconditions.checkState(spareThreads >= 1);
    // We do not bound number of threads here by total available threads. We can potentially
    // over-provision number of threads here. That is intentional and (potentially) good solution.
    // Queries don't use their workers all the time. So, reserving workers leads to suboptimal resource
    // utilization. We want to keep the pipe as full as possible for query workers. Overprovisioning is one
    // way to achieve that (in fact, only way for us).  There is a couter-argument to be made that overprovisioning
    // can impact cache-lines and memory in general.
    // We use this thread reservation only to determine priority based on resource utilization and not as a way to
    // improve system performance (because we don't have good insight on that yet)
    accountant.addReservedThreads(spareThreads);
    // TODO: For 1 thread we should have the query run in the same queryRunner thread
    // by supplying an executor service that similar to Guava' directExecutor()
    return new BoundedAccountingExecutor(_queryWorkers, spareThreads, accountant);
  }

  @Override
  public int getTableThreadsHardLimit() {
    return _resourcePolicy.getTableThreadsHardLimit();
  }

  @Override
  public int getTableThreadsSoftLimit() {
    return _resourcePolicy.getTableThreadsSoftLimit();
  }
}
