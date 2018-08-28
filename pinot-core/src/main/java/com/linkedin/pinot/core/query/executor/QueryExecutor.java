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
package com.linkedin.pinot.core.query.executor;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.data.manager.InstanceDataManager;
import com.linkedin.pinot.core.query.config.QueryExecutorConfig;
import com.linkedin.pinot.core.query.context.ServerQueryContext;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nonnull;


public interface QueryExecutor {

  /**
   * Initializes the query executor.
   */
  void init(@Nonnull QueryExecutorConfig queryExecutorConfig, @Nonnull InstanceDataManager instanceDataManager,
      @Nonnull ServerMetrics serverMetrics);

  /**
   * Starts the query executor.
   */
  void start();

  /**
   * Shuts down the query executor.
   */
  void shutDown();

  /**
   * Process the query with the given executor service.
   */
  @Nonnull
  DataTable processQuery(@Nonnull ServerQueryContext queryContext, @Nonnull ExecutorService executorService);

  /**
   * Sets the timeout for the given table.
   */
  void setTableTimeoutMs(@Nonnull String tableNameWithType, long timeoutMs);
}
