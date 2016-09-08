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

package com.linkedin.pinot.core.query.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.utils.DataTable;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * First Come First Served(FCFS) query scheduler.
 * The FCFS policy applies across all tables.
 * This is similar to the existing query scheduling logic.
 */
public class FCFSQueryScheduler extends QueryScheduler {

  private static Logger LOGGER = LoggerFactory.getLogger(FCFSQueryScheduler.class);

  public FCFSQueryScheduler(@Nonnull Configuration schedulerConfig, @Nonnull QueryExecutor queryExecutor) {
    super(schedulerConfig, queryExecutor);
    Preconditions.checkNotNull(queryExecutor);
  }

  @Override
  public ListenableFuture<DataTable> submit(final QueryRequest queryRequest) {
    ListenableFuture<DataTable> queryResultFuture = queryRunners.submit(new Callable<DataTable>() {
      @Override
      public DataTable call() {
        return queryExecutor.processQuery(queryRequest);
      }
    });

    return queryResultFuture;
  }

}
