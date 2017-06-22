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
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.metrics.ServerQueryPhase;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.common.query.ServerQueryRequest;
import com.linkedin.pinot.common.query.context.TimerContext;
import com.linkedin.pinot.common.request.InstanceRequest;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.datatable.DataTableImplV2;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
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
  // set the main query runner priority higher than NORM but lower than MAX
  // because if a query is complete we want to deserialize and return response as soon
  // as possible
  private static final int QUERY_RUNNER_THREAD_PRIORITY = 7;
  public static final int DEFAULT_QUERY_RUNNER_THREADS;
  public static final int DEFAULT_QUERY_WORKER_THREADS;
  protected final ServerMetrics serverMetrics;

  protected int numQueryRunnerThreads;
  protected int numQueryWorkerThreads;

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

  protected final QueryExecutor queryExecutor;

  static {
    int numCores = Runtime.getRuntime().availableProcessors();
    // arbitrary...but not completely arbitrary
    DEFAULT_QUERY_RUNNER_THREADS = numCores;
    DEFAULT_QUERY_WORKER_THREADS = 2 * numCores;
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
  public QueryScheduler(@Nonnull Configuration schedulerConfig, QueryExecutor queryExecutor,
      @Nonnull ServerMetrics serverMetrics) {
    Preconditions.checkNotNull(schedulerConfig);
    Preconditions.checkNotNull(queryExecutor);
    Preconditions.checkNotNull(serverMetrics);

    this.serverMetrics = serverMetrics;

    numQueryRunnerThreads = schedulerConfig.getInt(QUERY_RUNNER_CONFIG_KEY, DEFAULT_QUERY_RUNNER_THREADS);
    numQueryWorkerThreads = schedulerConfig.getInt(QUERY_WORKER_CONFIG_KEY, DEFAULT_QUERY_WORKER_THREADS);
    LOGGER.info("Initializing with {} query runner threads and {} worker threads", numQueryRunnerThreads,
        numQueryWorkerThreads);
    // pqr -> pinot query runner (to give short names)
    ThreadFactory queryRunnerFactory = new ThreadFactoryBuilder().setDaemon(false)
        .setPriority(QUERY_RUNNER_THREAD_PRIORITY)
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

  public QueryScheduler(@Nullable QueryExecutor queryExecutor, @Nonnull ServerMetrics serverMetrics) {
    this(new PropertiesConfiguration(), queryExecutor, serverMetrics);
  }

  public abstract @Nonnull ListenableFuture<byte[]> submit(@Nullable ServerQueryRequest queryRequest);

  public abstract String name();

  public abstract void start();

  public @Nonnull QueryExecutor getQueryExecutor() {
    return queryExecutor;
  }

  public ExecutorService getWorkerExecutorService() { return queryWorkers; }

  protected Callable<byte[]> getQueryCallable(final ServerQueryRequest request, final ExecutorService e) {
    return new Callable<byte[]>() {
      @Override
      public byte[] call()
          throws Exception {
        return processQueryAndSerialize(request, e);
      }
    };
  }

  protected ListenableFutureTask<byte[]> getQueryFutureTask(final ServerQueryRequest request) {
    return ListenableFutureTask.create(getQueryCallable(request, getWorkerExecutorService()));
  }

  protected ListenableFutureTask<byte[]> getQueryFutureTask(final ServerQueryRequest request, ExecutorService e) {
    return ListenableFutureTask.create(getQueryCallable(request, e));
  }

  protected byte[] processQueryAndSerialize(final ServerQueryRequest request, final ExecutorService e) {
    DataTable result;
    try {
      result = queryExecutor.processQuery(request, e);
    } catch (Throwable t) {
      // this is called iff queryTask fails with unhandled exception
      serverMetrics.addMeteredGlobalValue(ServerMeter.UNCAUGHT_EXCEPTIONS, 1);
      result = new DataTableImplV2();
      result.addException(QueryException.INTERNAL_ERROR);
    }
    byte[] responseData = serializeDataTable(request, result);
    TimerContext timerContext = request.getTimerContext();
    LOGGER.info("Processed requestId={},table={},reqSegments={},prunedToSegmentCount={},totalExecMs={},totalTimeMs={},broker={},sched={}",
        request.getInstanceRequest().getRequestId(),
        request.getTableName(),
        request.getInstanceRequest().getSearchSegments().size(),
        request.getSegmentCountAfterPruning(),
        timerContext.getPhaseDurationMs(ServerQueryPhase.QUERY_PROCESSING),
        timerContext.getPhaseDurationMs(ServerQueryPhase.TOTAL_QUERY_TIME),
        request.getBrokerId(),
        name());
    return responseData;
  }



  public static byte[] serializeDataTable(ServerQueryRequest queryRequest, DataTable instanceResponse) {
    byte[] responseByte;

    InstanceRequest instanceRequest = queryRequest.getInstanceRequest();
    ServerMetrics metrics = queryRequest.getServerMetrics();
    TimerContext timerContext = queryRequest.getTimerContext();
    timerContext.startNewPhaseTimer(ServerQueryPhase.RESPONSE_SERIALIZATION);
    long requestId = instanceRequest != null ? instanceRequest.getRequestId() : -1;
    String brokerId = instanceRequest != null ? instanceRequest.getBrokerId() : "null";

    try {
      if (instanceResponse == null) {
        LOGGER.warn("Instance response is null for requestId: {}, brokerId: {}", requestId, brokerId);
        responseByte = new byte[0];
      } else {
        responseByte = instanceResponse.toBytes();
      }
    } catch (Exception e) {
      metrics.addMeteredGlobalValue(ServerMeter.RESPONSE_SERIALIZATION_EXCEPTIONS, 1);
      LOGGER.error("Got exception while serializing response for requestId: {}, brokerId: {}",
          requestId, brokerId, e);
      responseByte = null;
    }
    // we assume these phase timers are guaranteed to be started elsewhere..so ignore potential NPE
    timerContext.getPhaseTimer(ServerQueryPhase.RESPONSE_SERIALIZATION).stopAndRecord();
    timerContext.startNewPhaseTimerAtNs(ServerQueryPhase.TOTAL_QUERY_TIME, timerContext.getQueryArrivalTimeNs());
    timerContext.getPhaseTimer(ServerQueryPhase.TOTAL_QUERY_TIME).stopAndRecord();

    return responseByte;
  }
}
