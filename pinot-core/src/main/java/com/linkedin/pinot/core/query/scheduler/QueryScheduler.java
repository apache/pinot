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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.QueryRequest;
import com.linkedin.pinot.common.utils.DataTable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class providing common scheduler functionality
 * including query runner and query worker pool
 */
public abstract class QueryScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryScheduler.class);

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

  /**
   * Constructor to initialize QueryScheduler based on scheduler configuration.
   * The configuration variables can control the size of query executors per servers.
   * 'pinot.query.scheduler.query_runner_threads' : controls the number 'main' threads for executing queries.
   * These remain "blocked" for the duration of query execution and indicate the number of parallel queries
   * a server can run. (default: equal to the number of cores on the server)
   * 'pinot.query.scheduler.query_worker_threads' : controls the total number of workers for query execution.
   * Actual work of parallel processing of a query on each segment is done by these threads.
   * (Default: 2 * number of cores)
   */
  public QueryScheduler(@Nonnull Configuration schedulerConfig, @Nonnull  QueryExecutor queryExecutor) {
    Preconditions.checkNotNull(schedulerConfig);
    Preconditions.checkNotNull(queryExecutor);

    numQueryRunnerThreads = schedulerConfig.getInt(QUERY_RUNNER_CONFIG_KEY, DEFAULT_QUERY_RUNNER_THREADS);
    numQueryWorkerThreads = schedulerConfig.getInt(QUERY_WORKER_CONFIG_KEY, DEFAULT_QUERY_WORKER_THREADS);
    LOGGER.info("Initializing with {} query runner threads and {} worker threads", numQueryRunnerThreads,
        numQueryWorkerThreads);
    // pqr -> pinot query runner (to give short names)
    ThreadFactory queryRunnerFactory = new ThreadFactoryBuilder().setDaemon(false)
        .setPriority(7)
        .setNameFormat("pqr-%d")
        .build();
    queryRunners = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numQueryRunnerThreads, queryRunnerFactory));

    // pqw -> pinot query workers
    ThreadFactory queryWorkersFactory = new ThreadFactoryBuilder().setDaemon(false)
        .setPriority(Thread.NORM_PRIORITY)
        .setNameFormat("pqw-%d")
        .build();
    queryWorkers = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numQueryWorkerThreads, queryWorkersFactory));
    this.queryExecutor = queryExecutor;
  }

  public abstract ListenableFuture<DataTable> submit(@Nonnull QueryRequest queryRequest);

  public @Nullable QueryExecutor getQueryExecutor() {
    return queryExecutor;
  }

  public ExecutorService getWorkerExecutorService() { return queryWorkers; }
}
