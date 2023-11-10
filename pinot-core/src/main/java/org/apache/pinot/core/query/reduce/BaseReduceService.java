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
package org.apache.pinot.core.query.reduce;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the base reduce service.
 */
@ThreadSafe
public abstract class BaseReduceService {

  // Set the reducer priority higher than NORM but lower than MAX, because if a query is complete
  // we want to deserialize and return response as soon. This is the same as server side 'pqr' threads.
  protected static final int QUERY_RUNNER_THREAD_PRIORITY = 7;
  // brw -> Shorthand for broker reduce worker threads.
  protected static final String REDUCE_THREAD_NAME_FORMAT = "brw-%d";

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseReduceService.class);

  protected final ExecutorService _reduceExecutorService;
  protected final int _maxReduceThreadsPerQuery;
  protected final int _groupByTrimThresholdCfg;
  protected final int _minGroupTrimSizeCfg;

  public BaseReduceService(PinotConfiguration config) {
    _maxReduceThreadsPerQuery = config.getProperty(CommonConstants.Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY,
        CommonConstants.Broker.DEFAULT_MAX_REDUCE_THREADS_PER_QUERY);
    _groupByTrimThresholdCfg = config.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_GROUPBY_TRIM_THRESHOLD,
        CommonConstants.Broker.DEFAULT_BROKER_GROUPBY_TRIM_THRESHOLD);
    _minGroupTrimSizeCfg = config.getProperty(CommonConstants.Broker.CONFIG_OF_BROKER_MIN_GROUP_TRIM_SIZE,
        CommonConstants.Broker.DEFAULT_BROKER_MIN_GROUP_TRIM_SIZE);

    int numThreadsInExecutorService = Runtime.getRuntime().availableProcessors();
    LOGGER.info("Initializing BrokerReduceService with {} threads, and {} max reduce threads.",
        numThreadsInExecutorService, _maxReduceThreadsPerQuery);

    ThreadFactory reduceThreadFactory =
        new ThreadFactoryBuilder().setDaemon(false).setPriority(QUERY_RUNNER_THREAD_PRIORITY)
            .setNameFormat(REDUCE_THREAD_NAME_FORMAT).build();

    // ExecutorService is initialized with numThreads same as availableProcessors.
    _reduceExecutorService = Executors.newFixedThreadPool(numThreadsInExecutorService, reduceThreadFactory);
  }

  protected static void updateAlias(QueryContext queryContext, BrokerResponseNative brokerResponseNative) {
    ResultTable resultTable = brokerResponseNative.getResultTable();
    if (resultTable == null) {
      return;
    }
    List<String> aliasList = queryContext.getAliasList();
    if (aliasList.isEmpty()) {
      return;
    }

    String[] columnNames = resultTable.getDataSchema().getColumnNames();
    List<ExpressionContext> selectExpressions = getSelectExpressions(queryContext.getSelectExpressions());
    int numSelectExpressions = selectExpressions.size();
    // For query like `SELECT *`, we skip alias update.
    if (columnNames.length != numSelectExpressions) {
      return;
    }
    for (int i = 0; i < numSelectExpressions; i++) {
      String alias = aliasList.get(i);
      if (alias != null) {
        columnNames[i] = alias;
      }
    }
  }

  protected static List<ExpressionContext> getSelectExpressions(List<ExpressionContext> selectExpressions) {
    // NOTE: For DISTINCT queries, need to extract the arguments as the SELECT expressions
    if (selectExpressions.size() == 1 && selectExpressions.get(0).getType() == ExpressionContext.Type.FUNCTION
        && selectExpressions.get(0).getFunction().getFunctionName().equals("distinct")) {
      return selectExpressions.get(0).getFunction().getArguments();
    }
    return selectExpressions;
  }

  protected void shutDown() {
    _reduceExecutorService.shutdownNow();
  }
}
