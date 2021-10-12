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

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.query.request.context.ThreadTimer;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;


public class InstanceResponseOperator extends BaseOperator<InstanceResponseBlock> {
  private static final String OPERATOR_NAME = "InstanceResponseOperator";
  private static final String EXPLAIN_NAME = "INSTANCE_RESPONSE";

  private final BaseCombineOperator _combineOperator;
  private final List<IndexSegment> _indexSegments;
  private final List<FetchContext> _fetchContexts;
  private final int _fetchContextSize;

  public InstanceResponseOperator(BaseCombineOperator combinedOperator, List<IndexSegment> indexSegments,
      List<FetchContext> fetchContexts) {
    _combineOperator = combinedOperator;
    _indexSegments = indexSegments;
    _fetchContexts = fetchContexts;
    _fetchContextSize = fetchContexts.size();
  }

  @Override
  protected InstanceResponseBlock getNextBlock() {
    if (ThreadTimer.isThreadCpuTimeMeasurementEnabled()) {
      long startWallClockTimeNs = System.nanoTime();
      IntermediateResultsBlock intermediateResultsBlock = getCombinedResults();
      InstanceResponseBlock instanceResponseBlock = new InstanceResponseBlock(intermediateResultsBlock);
      long totalWallClockTimeNs = System.nanoTime() - startWallClockTimeNs;

      /*
       * If/when the threadCpuTime based instrumentation is done for other parts of execution (planning, pruning etc),
       * we will have to change the wallClockTime computation accordingly. Right now everything under
       * InstanceResponseOperator is the one that is instrumented with threadCpuTime.
       */
      long multipleThreadCpuTimeNs = intermediateResultsBlock.getExecutionThreadCpuTimeNs();
      int numServerThreads = intermediateResultsBlock.getNumServerThreads();
      long totalThreadCpuTimeNs =
          calTotalThreadCpuTimeNs(totalWallClockTimeNs, multipleThreadCpuTimeNs, numServerThreads);

      instanceResponseBlock.getInstanceResponseDataTable().getMetadata()
          .put(MetadataKey.THREAD_CPU_TIME_NS.getName(), String.valueOf(totalThreadCpuTimeNs));
      return instanceResponseBlock;
    } else {
      return new InstanceResponseBlock(getCombinedResults());
    }
  }

  private IntermediateResultsBlock getCombinedResults() {
    try {
      prefetchAll();
      return _combineOperator.nextBlock();
    } finally {
      releaseAll();
    }
  }

  /*
   * Calculate totalThreadCpuTimeNs based on totalWallClockTimeNs, multipleThreadCpuTimeNs, and numServerThreads.
   * System activities time such as OS paging, GC, context switching are not captured by totalThreadCpuTimeNs.
   * For example, let's divide query processing into 4 phases:
   * - phase 1: single thread preparing. Time used: T1
   * - phase 2: N threads processing segments in parallel, each thread use time T2
   * - phase 3: GC/OS paging. Time used: T3
   * - phase 4: single thread merging intermediate results blocks. Time used: T4
   *
   * Then we have following equations:
   * - singleThreadCpuTimeNs = T1 + T4
   * - multipleThreadCpuTimeNs = T2 * N
   * - totalWallClockTimeNs = T1 + T2 + T3 + T4 = singleThreadCpuTimeNs + T2 + T3
   * - totalThreadCpuTimeNsWithoutSystemActivities = T1 + T2 * N + T4 = singleThreadCpuTimeNs + T2 * N
   * - systemActivitiesTimeNs = T3 = (totalWallClockTimeNs - totalThreadCpuTimeNsWithoutSystemActivities) + T2 * (N - 1)
   *
   * Thus:
   * totalThreadCpuTimeNsWithSystemActivities = totalThreadCpuTimeNsWithoutSystemActivities + systemActivitiesTimeNs
   * = totalThreadCpuTimeNsWithoutSystemActivities + T3
   * = totalThreadCpuTimeNsWithoutSystemActivities + (totalWallClockTimeNs -
   * totalThreadCpuTimeNsWithoutSystemActivities) + T2 * (N - 1)
   * = totalWallClockTimeNs + T2 * (N - 1)
   * = totalWallClockTimeNs + (multipleThreadCpuTimeNs / N) * (N - 1)
   */
  public static long calTotalThreadCpuTimeNs(long totalWallClockTimeNs, long multipleThreadCpuTimeNs,
      int numServerThreads) {
    double perThreadCpuTimeNs = multipleThreadCpuTimeNs * 1.0 / numServerThreads;
    return Math.round(totalWallClockTimeNs + perThreadCpuTimeNs * (numServerThreads - 1));
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
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public String getExplainPlanName() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Arrays.asList(_combineOperator);
  }
}
