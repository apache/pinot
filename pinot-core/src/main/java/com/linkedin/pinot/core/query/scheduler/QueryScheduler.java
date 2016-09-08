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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.utils.DataTable;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;

/**
 * Abstract class providing common scheduler functionality
 * including query runner and query worker pool
 */
public abstract class QueryScheduler {

  public static final String QUERY_RUNNER_CONFIG_KEY = "query_runner_threads";
  public static final String QUERY_WORKER_CONFIG_KEY = "query_worker_threads";

  public static final int DEFAULT_QUERY_RUNNER_THREADS;
  public static final int DEFAULT_QUERY_WORKER_THREADS;

  int numQueryRunnerThreads;
  int numQueryWorkerThreads;

  // This executor service will run the "main" operation of query processing
  // including planning, distributing operators across threads, waiting and
  // reducing the results from the parallel set of operators (MCombineOperator)
  //
  protected ListeningExecutorService queryRunners;

  // TODO: in future, this should be driven by configured policy
  // like poolPerTable or QoS based pools etc
  // The policy should also determine how many threads we use per query

  // These are worker threads to parallelize execution of query operators
  // across groups of segments.
  protected ListeningExecutorService queryWorkers;

  final QueryExecutor queryExecutor;
  static {
    int numCores = Runtime.getRuntime().availableProcessors();
    // arbitrary...but not completely arbitrary
    DEFAULT_QUERY_RUNNER_THREADS = numCores;
    DEFAULT_QUERY_WORKER_THREADS = 2 * numCores;
  }

  public QueryScheduler(@Nullable QueryExecutor queryExecutor) {
    numQueryRunnerThreads = DEFAULT_QUERY_RUNNER_THREADS;
    numQueryWorkerThreads = DEFAULT_QUERY_WORKER_THREADS;
    this.queryExecutor = queryExecutor;
    queryRunners = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numQueryRunnerThreads));
    queryWorkers = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numQueryWorkerThreads));
  }

  public QueryScheduler(@Nonnull Configuration schedulerConfig, QueryExecutor queryExecutor) {
    Preconditions.checkNotNull(schedulerConfig);
    numQueryRunnerThreads = schedulerConfig.getInt(QUERY_RUNNER_CONFIG_KEY, DEFAULT_QUERY_RUNNER_THREADS);
    numQueryWorkerThreads = schedulerConfig.getInt(QUERY_WORKER_CONFIG_KEY, DEFAULT_QUERY_WORKER_THREADS);
    queryRunners = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numQueryRunnerThreads));
    queryWorkers = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numQueryWorkerThreads));
    this.queryExecutor = queryExecutor;
  }

  public abstract ListenableFuture<DataTable> submit(@Nullable QueryRequest queryRequest);

  public @Nullable QueryExecutor getQueryExecutor() {
    return queryExecutor;
  }
}
