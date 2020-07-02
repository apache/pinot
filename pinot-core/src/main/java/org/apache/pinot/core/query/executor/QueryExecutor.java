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
package org.apache.pinot.core.query.executor;

import java.util.concurrent.ExecutorService;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.query.request.ServerQueryRequest;


@ThreadSafe
public interface QueryExecutor {

  /**
   * Initializes the query executor.
   * <p>Should be called only once and before calling any other method.
   */
  void init(Configuration config, InstanceDataManager instanceDataManager, ServerMetrics serverMetrics)
      throws ConfigurationException;

  /**
   * Starts the query executor.
   * <p>Should be called only once after query executor gets initialized but before calling any other method.
   */
  void start();

  /**
   * Shuts down the query executor.
   * <p>Should be called only once. After calling shut down, no other method should be called.
   */
  void shutDown();

  /**
   * Processes the query with the given executor service.
   */
  DataTable processQuery(ServerQueryRequest queryRequest, ExecutorService executorService);
}
