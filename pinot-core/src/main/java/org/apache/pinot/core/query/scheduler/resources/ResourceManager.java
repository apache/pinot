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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstract class to manage all the server resources for query execution.
 * Currently this manages the threadpool for query execution.
 *
 * This class supports soft and hard limits on the number of threads. A
 * scheduler group will not get more than the hard_limit number of threads.
 */
// TODO: This class supports hard and soft thread limits. Potentially, we can make
// these limits dynamic - SchedulerGroups with low latency can have higher hard limit
// than the groups with high latency. That requires more experimentation and tuning
public abstract class ResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceManager.class);

  public static final String QUERY_RUNNER_CONFIG_KEY = "query_runner_threads";
  public static final String QUERY_WORKER_CONFIG_KEY = "query_worker_threads";
  public static final int DEFAULT_QUERY_RUNNER_THREADS;
  public static final int DEFAULT_QUERY_WORKER_THREADS;

  static {
    int numCores = Runtime.getRuntime().availableProcessors();
    // arbitrary...but not completely arbitrary
    DEFAULT_QUERY_RUNNER_THREADS = numCores;
    DEFAULT_QUERY_WORKER_THREADS = 2 * numCores;
  }

  // set the main query runner priority higher than NORM but lower than MAX
  // because if a query is complete we want to deserialize and return response as soon
  // as possible
  protected static final int QUERY_RUNNER_THREAD_PRIORITY = 7;
  // This executor service will run the "main" operation of query processing
  // including planning, distributing operators across threads, waiting and
  // reducing the results from the parallel set of operators (CombineOperator)
  //
  protected final ListeningExecutorService queryRunners;
  protected final ListeningExecutorService queryWorkers;
  protected final int numQueryRunnerThreads;
  protected final int numQueryWorkerThreads;

  /**
   * @param config configuration for initializing resource manager
   */
  public ResourceManager(PinotConfiguration config) {
    numQueryRunnerThreads = config.getProperty(QUERY_RUNNER_CONFIG_KEY, DEFAULT_QUERY_RUNNER_THREADS);
    numQueryWorkerThreads = config.getProperty(QUERY_WORKER_CONFIG_KEY, DEFAULT_QUERY_WORKER_THREADS);

    LOGGER.info("Initializing with {} query runner threads and {} worker threads", numQueryRunnerThreads,
        numQueryWorkerThreads);
    // pqr -> pinot query runner (to give short names)
    ThreadFactory queryRunnerFactory = new ThreadFactoryBuilder().setDaemon(false)
        .setPriority(QUERY_RUNNER_THREAD_PRIORITY).setNameFormat("pqr-%d").build();
    queryRunners =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numQueryRunnerThreads, queryRunnerFactory));

    // pqw -> pinot query workers
    ThreadFactory queryWorkersFactory =
        new ThreadFactoryBuilder().setDaemon(false).setPriority(Thread.NORM_PRIORITY).setNameFormat("pqw-%d").build();
    queryWorkers =
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numQueryWorkerThreads, queryWorkersFactory));
  }

  public void stop() {
    queryWorkers.shutdownNow();
    queryRunners.shutdownNow();
  }

  /**
   * Total number of query runner threads. Query runner threads are 'main'
   * threads executing the query.
   * @return
   */
  final public int getNumQueryRunnerThreads() {
    return numQueryRunnerThreads;
  }

  /**
   * Total number of query worker threads. Query worker threads are a pool of threads
   * for parallel processing of a query.
   * @return
   */
  final public int getNumQueryWorkerThreads() {
    return numQueryWorkerThreads;
  }

  /**
   * Returns executor service for running queries.
   * @return
   */
  final public ListeningExecutorService getQueryRunners() {
    return queryRunners;
  }

  @VisibleForTesting
  final public ExecutorService getQueryWorkers() {
    return queryWorkers;
  }

  /**
   * Get the executor service for running the query. The provided executor
   * service limits the number of resources available for executing query
   * as per the configured policy
   * @param query
   * @param accountant Accountant for a scheduler group
   * @return
   */
  public abstract QueryExecutorService getExecutorService(ServerQueryRequest query,
      SchedulerGroupAccountant accountant);

  /**
   * Hard limit on number of threads for a scheduler group.
   * A group may not be allotted threads more than this for query execution.
   * @return number of threads
   */
  public abstract int getTableThreadsHardLimit();

  /**
   * Soft limit on the number of threads for a scheduler group.
   * Queries from a scheduler group will be de-prioritized if the group
   * is using more than the soft limit.
   * @return number of threads
   */
  public abstract int getTableThreadsSoftLimit();

  /**
   * Check if the query for a scheduler group can get required resources for scheduling
   * @param accountant resource accounting information for a group
   * @return
   */
  public boolean canSchedule(SchedulerGroupAccountant accountant) {
    return accountant.totalReservedThreads() < getTableThreadsHardLimit();
  }
}
