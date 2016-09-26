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
package com.linkedin.pinot.core.query.scheduler.resources;

import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.commons.configuration.Configuration;


/**
 * Resource manager that does not limit resources for a query.
 * This resource manager maintains the legacy approach of
 * letting operators add parallel jobs to the same executor service.
 */
public class UnboundedResourceManager extends ResourceManager {

  public UnboundedResourceManager(Configuration config) {
    super(config);
  }

  @Override
  public QueryExecutorService getExecutorService(ServerQueryRequest query, SchedulerGroupAccountant accountant) {
    return new QueryExecutorService() {
      @Override
      public void execute(Runnable command) {
        queryWorkers.submit(command);
      }
    };
  }

  @Override
  public int getTableThreadsHardLimit() {
    return numQueryRunnerThreads + numQueryWorkerThreads;
  }

  @Override
  public int getTableThreadsSoftLimit() {
    return numQueryRunnerThreads + numQueryWorkerThreads;
  }
}
