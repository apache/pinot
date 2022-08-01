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
package org.apache.pinot.core.operator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.ThreadTimer;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InstanceResponseOperator extends BaseOperator<InstanceResponseBlock> {

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceResponseOperator.class);
  private static final String EXPLAIN_NAME = "INSTANCE_RESPONSE";

  private final BaseCombineOperator _combineOperator;
  private final List<IndexSegment> _indexSegments;
  private final List<FetchContext> _fetchContexts;
  private final int _fetchContextSize;
  private final QueryContext _queryContext;
  private final ServerMetrics _serverMetrics;

  public InstanceResponseOperator(BaseCombineOperator combinedOperator, List<IndexSegment> indexSegments,
      List<FetchContext> fetchContexts, QueryContext queryContext, ServerMetrics serverMetrics) {
    _combineOperator = combinedOperator;
    _indexSegments = indexSegments;
    _fetchContexts = fetchContexts;
    _fetchContextSize = fetchContexts.size();
    _queryContext = queryContext;
    _serverMetrics = serverMetrics;
  }

  /*
   * Derive systemActivitiesCpuTimeNs from totalWallClockTimeNs, multipleThreadCpuTimeNs, mainThreadCpuTimeNs,
   * and numServerThreads.
   *
   * For example, let's divide query processing into 4 phases:
   * - phase 1: single thread (main thread) preparing. Time used: T1
   * - phase 2: N threads processing segments in parallel, each thread use time T2
   * - phase 3: system activities (GC/OS paging). Time used: T3
   * - phase 4: single thread (main thread) merging intermediate results blocks. Time used: T4
   *
   * Then we have following equations:
   * - mainThreadCpuTimeNs = T1 + T4
   * - multipleThreadCpuTimeNs = T2 * N
   * - totalWallClockTimeNs = T1 + T2 + T3 + T4 = mainThreadCpuTimeNs + T2 + T3
   * - systemActivitiesCpuTimeNs = T3 = totalWallClockTimeNs - mainThreadCpuTimeNs - T2
   */
  public static long calSystemActivitiesCpuTimeNs(long totalWallClockTimeNs, long multipleThreadCpuTimeNs,
      long mainThreadCpuTimeNs, int numServerThreads) {
    double perMultipleThreadCpuTimeNs = multipleThreadCpuTimeNs * 1.0 / numServerThreads;
    long systemActivitiesCpuTimeNs =
        Math.round(totalWallClockTimeNs - mainThreadCpuTimeNs - perMultipleThreadCpuTimeNs);
    // systemActivitiesCpuTimeNs should not be negative, this is just a defensive check
    return Math.max(systemActivitiesCpuTimeNs, 0);
  }

  @Override
  protected InstanceResponseBlock getNextBlock() {
    IntermediateResultsBlock intermediateResultsBlock;
    InstanceResponseBlock instanceResponseBlock;

    if (ThreadTimer.isThreadCpuTimeMeasurementEnabled()) {
      long startWallClockTimeNs = System.nanoTime();

      ThreadTimer mainThreadTimer = new ThreadTimer();
      intermediateResultsBlock = getCombinedResults();
      instanceResponseBlock = new InstanceResponseBlock(intermediateResultsBlock);
      long mainThreadCpuTimeNs = mainThreadTimer.getThreadTimeNs();

      long totalWallClockTimeNs = System.nanoTime() - startWallClockTimeNs;
      /*
       * If/when the threadCpuTime based instrumentation is done for other parts of execution (planning, pruning etc),
       * we will have to change the wallClockTime computation accordingly. Right now everything under
       * InstanceResponseOperator is the one that is instrumented with threadCpuTime.
       */
      long multipleThreadCpuTimeNs = intermediateResultsBlock.getExecutionThreadCpuTimeNs();
      int numServerThreads = intermediateResultsBlock.getNumServerThreads();
      long systemActivitiesCpuTimeNs =
          calSystemActivitiesCpuTimeNs(totalWallClockTimeNs, multipleThreadCpuTimeNs, mainThreadCpuTimeNs,
              numServerThreads);

      long threadCpuTimeNs = mainThreadCpuTimeNs + multipleThreadCpuTimeNs;
      Map<String, String> responseMetaData = instanceResponseBlock.getInstanceResponseDataTable().getMetadata();
      responseMetaData.put(MetadataKey.THREAD_CPU_TIME_NS.getName(), String.valueOf(threadCpuTimeNs));
      responseMetaData
          .put(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName(), String.valueOf(systemActivitiesCpuTimeNs));
    } else {
      intermediateResultsBlock = getCombinedResults();
      instanceResponseBlock = new InstanceResponseBlock(intermediateResultsBlock);
    }

    // TODO: Remove this once the SelectionOrderByOperator is modified to throw an exception to catch cases where
    //       an MV column (identifier or via transform) is present on the order-by list
    logAndEmitMetricForQueryHasMVSelectionOrderBy(intermediateResultsBlock);
    return instanceResponseBlock;
  }

  private void logAndEmitMetricForQueryHasMVSelectionOrderBy(IntermediateResultsBlock intermediateResultsBlock) {
    if (!intermediateResultsBlock.isQueryHasMVSelectionOrderBy()) {
      return;
    }

    String tableName = _queryContext.getTableName();
    if (_serverMetrics != null) {
      _serverMetrics.addMeteredTableValue(tableName, ServerMeter.QUERY_HAS_MV_SELECTION_ORDER_BY, 1);
    }
    LOGGER.warn("Table {} has MV column in ORDER BY. Expressions: {}", tableName,
        _queryContext.getOrderByExpressions());
  }

  private IntermediateResultsBlock getCombinedResults() {
    try {
      prefetchAll();
      return _combineOperator.nextBlock();
    } finally {
      releaseAll();
    }
  }

  private void prefetchAll() {
    for (int i = 0; i < _fetchContextSize; i++) {
      _indexSegments.get(i).prefetch(_fetchContexts.get(i));
    }
  }

  private void releaseAll() {
    for (int i = 0; i < _fetchContextSize; i++) {
      _indexSegments.get(i).release(_fetchContexts.get(i));
    }
  }


  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_combineOperator);
  }
}
