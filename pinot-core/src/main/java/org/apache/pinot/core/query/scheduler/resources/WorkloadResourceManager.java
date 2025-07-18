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

import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;

public class WorkloadResourceManager extends ResourceManager {
  private final ResourceLimitPolicy _resourcePolicy;

  /**
   * @param config configuration for initializing resource manager
   */
  public WorkloadResourceManager(PinotConfiguration config, ThreadResourceUsageAccountant resourceUsageAccountant) {
    super(config, resourceUsageAccountant);
    _resourcePolicy = new ResourceLimitPolicy(config, _numQueryWorkerThreads);
  }

  @Override
  public QueryExecutorService getExecutorService(ServerQueryRequest query, SchedulerGroupAccountant accountant) {
    return new QueryExecutorService() {
      @Override
      public void execute(Runnable command) {
        _queryWorkers.submit(command);
      }
    };
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
