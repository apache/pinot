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
package org.apache.pinot.core.operator.combine;

import java.util.List;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;


@SuppressWarnings("rawtypes")
public class CombineOperatorUtils {
  private CombineOperatorUtils() {
  }

  /**
   * Use at most 10 or half of the processors threads for each query. If there are less than 2 processors, use 1 thread.
   * <p>NOTE: Runtime.getRuntime().availableProcessors() may return value < 2 in container based environment, e.g.
   *          Kubernetes.
   */
  public static final int MAX_NUM_THREADS_PER_QUERY =
      Math.max(1, Math.min(10, Runtime.getRuntime().availableProcessors() / 2));

  /**
   * Returns the number of tasks for the query execution. The tasks can be assigned to multiple execution threads so
   * that they can run in parallel. The parallelism is bounded by the task count.
   */
  public static int getNumTasksForQuery(int numOperators, int maxExecutionThreads) {
    if (maxExecutionThreads > 0) {
      return Math.min(numOperators, maxExecutionThreads);
    } else {
      return Math.min(numOperators, MAX_NUM_THREADS_PER_QUERY);
    }
  }

  /**
   * Sets the execution statistics into the results block.
   */
  public static void setExecutionStatistics(IntermediateResultsBlock resultsBlock, List<Operator> operators,
      long threadCpuTimeNs, int numServerThreads) {
    int numSegmentsProcessed = operators.size();
    int numSegmentsMatched = 0;
    long numDocsScanned = 0;
    long numEntriesScannedInFilter = 0;
    long numEntriesScannedPostFilter = 0;
    long numTotalDocs = 0;
    for (Operator operator : operators) {
      ExecutionStatistics executionStatistics = operator.getExecutionStatistics();
      if (executionStatistics.getNumDocsScanned() > 0) {
        numSegmentsMatched++;
      }
      numDocsScanned += executionStatistics.getNumDocsScanned();
      numEntriesScannedInFilter += executionStatistics.getNumEntriesScannedInFilter();
      numEntriesScannedPostFilter += executionStatistics.getNumEntriesScannedPostFilter();
      numTotalDocs += executionStatistics.getNumTotalDocs();
    }
    resultsBlock.setNumSegmentsProcessed(numSegmentsProcessed);
    resultsBlock.setNumSegmentsMatched(numSegmentsMatched);
    resultsBlock.setNumDocsScanned(numDocsScanned);
    resultsBlock.setNumEntriesScannedInFilter(numEntriesScannedInFilter);
    resultsBlock.setNumEntriesScannedPostFilter(numEntriesScannedPostFilter);
    resultsBlock.setNumTotalDocs(numTotalDocs);
    resultsBlock.setExecutionThreadCpuTimeNs(threadCpuTimeNs);
    resultsBlock.setNumServerThreads(numServerThreads);
  }
}
