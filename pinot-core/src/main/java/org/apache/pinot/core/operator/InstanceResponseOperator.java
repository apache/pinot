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
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.trace.Tracing;


public class InstanceResponseOperator extends BaseOperator<InstanceResponseBlock> {
  private static final String EXPLAIN_NAME = "INSTANCE_RESPONSE";

  protected final BaseCombineOperator<?> _combineOperator;
  protected final List<IndexSegment> _indexSegments;
  protected final List<FetchContext> _fetchContexts;
  protected final int _fetchContextSize;
  protected final QueryContext _queryContext;

  public InstanceResponseOperator(BaseCombineOperator<?> combineOperator, List<IndexSegment> indexSegments,
      List<FetchContext> fetchContexts, QueryContext queryContext) {
    _combineOperator = combineOperator;
    _indexSegments = indexSegments;
    _fetchContexts = fetchContexts;
    _fetchContextSize = fetchContexts.size();
    _queryContext = queryContext;
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
    if (ThreadResourceUsageProvider.isThreadCpuTimeMeasurementEnabled()) {
      long startWallClockTimeNs = System.nanoTime();

      ThreadResourceUsageProvider mainThreadResourceUsageProvider = new ThreadResourceUsageProvider();
      BaseResultsBlock resultsBlock = getCombinedResults();
      InstanceResponseBlock instanceResponseBlock = new InstanceResponseBlock(resultsBlock);
      long mainThreadCpuTimeNs = mainThreadResourceUsageProvider.getThreadTimeNs();

      long totalWallClockTimeNs = System.nanoTime() - startWallClockTimeNs;
      /*
       * If/when the threadCpuTime based instrumentation is done for other parts of execution (planning, pruning etc),
       * we will have to change the wallClockTime computation accordingly. Right now everything under
       * InstanceResponseOperator is the one that is instrumented with threadCpuTime.
       */
      long multipleThreadCpuTimeNs = resultsBlock.getExecutionThreadCpuTimeNs();
      int numServerThreads = resultsBlock.getNumServerThreads();
      long systemActivitiesCpuTimeNs =
          calSystemActivitiesCpuTimeNs(totalWallClockTimeNs, multipleThreadCpuTimeNs, mainThreadCpuTimeNs,
              numServerThreads);

      long threadCpuTimeNs = mainThreadCpuTimeNs + multipleThreadCpuTimeNs;
      instanceResponseBlock.addMetadata(MetadataKey.THREAD_CPU_TIME_NS.getName(), String.valueOf(threadCpuTimeNs));
      instanceResponseBlock.addMetadata(MetadataKey.SYSTEM_ACTIVITIES_CPU_TIME_NS.getName(),
          String.valueOf(systemActivitiesCpuTimeNs));

      return instanceResponseBlock;
    } else {
      return new InstanceResponseBlock(getCombinedResults());
    }
  }

  private BaseResultsBlock getCombinedResults() {
    try {
      prefetchAll();
      return _combineOperator.nextBlock();
    } catch (EarlyTerminationException e) {
      Exception killedErrorMsg = Tracing.getThreadAccountant().getErrorStatus();
      return new ExceptionResultsBlock(new QueryCancelledException(
          "Cancelled while combining results" + (killedErrorMsg == null ? StringUtils.EMPTY : " " + killedErrorMsg),
          e));
    } finally {
      releaseAll();
    }
  }

  public void prefetchAll() {
    for (int i = 0; i < _fetchContextSize; i++) {
      _indexSegments.get(i).prefetch(_fetchContexts.get(i));
    }
  }

  public void releaseAll() {
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
