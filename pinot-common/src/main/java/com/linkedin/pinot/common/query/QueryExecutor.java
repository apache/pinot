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
package com.linkedin.pinot.common.query;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import java.util.concurrent.ExecutorService;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.utils.DataTable;


public interface QueryExecutor {
  /**
   * Initialize query executor
   * @param queryExecutorConfig
   * @param dataManager
   * @param serverMetrics
   * @throws ConfigurationException
   */
  void init(Configuration queryExecutorConfig, DataManager dataManager, ServerMetrics serverMetrics) throws ConfigurationException;

  void start();

  /**
   * Execute a given query optionally parallelizing execution using provided
   * executor service
   * @param queryRequest Query to execute
   * @param executorService executor service to use for parallelizing query execution
   *                        across different segments
   *
   * @return
   */
  DataTable processQuery(ServerQueryRequest queryRequest, ExecutorService executorService);

  void shutDown();

  boolean isStarted();

  void updateResourceTimeOutInMs(String resource, long timeOutMs);
}
