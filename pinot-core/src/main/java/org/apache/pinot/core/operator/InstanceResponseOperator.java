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

import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.DataTable.MetadataKey;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;


public class InstanceResponseOperator extends BaseOperator<InstanceResponseBlock> {
  private static final String OPERATOR_NAME = "InstanceResponseOperator";

  private final Operator _operator;

  public InstanceResponseOperator(Operator combinedOperator) {
    _operator = combinedOperator;
  }

  @Override
  protected InstanceResponseBlock getNextBlock() {
    long startWallClockTimeNs = System.nanoTime();
    IntermediateResultsBlock intermediateResultsBlock = (IntermediateResultsBlock) _operator.nextBlock();
    InstanceResponseBlock instanceResponseBlock = new InstanceResponseBlock(intermediateResultsBlock);
    DataTable dataTable = instanceResponseBlock.getInstanceResponseDataTable();
    long endWallClockTimeNs = System.nanoTime();

    long multipleThreadCpuTimeNs = intermediateResultsBlock.getExecutionThreadCpuTimeNs();
    /*
     * If/when the threadCpuTime based instrumentation is done for other parts of execution (planning, pruning etc),
     * we will have to change the wallClockTime computation accordingly. Right now everything under
     * InstanceResponseOperator is the one that is instrumented with threadCpuTime.
     */
    long totalWallClockTimeNs = endWallClockTimeNs - startWallClockTimeNs;

    int numServerThreads = intermediateResultsBlock.getNumServerThreads();
    long totalThreadCpuTimeNs =
        calTotalThreadCpuTimeNs(totalWallClockTimeNs, multipleThreadCpuTimeNs, numServerThreads);

    dataTable.getMetadata().put(MetadataKey.THREAD_CPU_TIME_NS.getName(), String.valueOf(totalThreadCpuTimeNs));

    return instanceResponseBlock;
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

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
