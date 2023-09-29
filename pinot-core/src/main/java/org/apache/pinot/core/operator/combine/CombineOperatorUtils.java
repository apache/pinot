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
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.segment.spi.MutableSegment;


@SuppressWarnings("rawtypes")
public class CombineOperatorUtils {
  private CombineOperatorUtils() {
  }

  /**
   * Sets the execution statistics into the results block.
   */
  public static void setExecutionStatistics(BaseResultsBlock resultsBlock, List<Operator> operators,
      long threadCpuTimeNs, int numServerThreads) {
    int numSegmentsProcessed = operators.size();
    int numSegmentsMatched = 0;
    int numConsumingSegmentsProcessed = 0;
    int numConsumingSegmentsMatched = 0;
    long numDocsScanned = 0;
    long numEntriesScannedInFilter = 0;
    long numEntriesScannedPostFilter = 0;
    long numTotalDocs = 0;
    for (Operator operator : operators) {
      ExecutionStatistics executionStatistics = operator.getExecutionStatistics();
      if (executionStatistics.getNumDocsScanned() > 0) {
        numSegmentsMatched++;
      }

      // TODO: Check all operators and properly implement the getIndexSegment.
      if (operator.getIndexSegment() != null && operator.getIndexSegment() instanceof MutableSegment) {
        numConsumingSegmentsProcessed += 1;
        if (executionStatistics.getNumDocsScanned() > 0) {
          numConsumingSegmentsMatched++;
        }
      }

      numDocsScanned += executionStatistics.getNumDocsScanned();
      numEntriesScannedInFilter += executionStatistics.getNumEntriesScannedInFilter();
      numEntriesScannedPostFilter += executionStatistics.getNumEntriesScannedPostFilter();
      numTotalDocs += executionStatistics.getNumTotalDocs();
    }
    resultsBlock.setNumSegmentsProcessed(numSegmentsProcessed);
    resultsBlock.setNumSegmentsMatched(numSegmentsMatched);
    resultsBlock.setNumConsumingSegmentsProcessed(numConsumingSegmentsProcessed);
    resultsBlock.setNumConsumingSegmentsMatched(numConsumingSegmentsMatched);
    resultsBlock.setNumDocsScanned(numDocsScanned);
    resultsBlock.setNumEntriesScannedInFilter(numEntriesScannedInFilter);
    resultsBlock.setNumEntriesScannedPostFilter(numEntriesScannedPostFilter);
    resultsBlock.setNumTotalDocs(numTotalDocs);
    resultsBlock.setExecutionThreadCpuTimeNs(threadCpuTimeNs);
    resultsBlock.setNumServerThreads(numServerThreads);
  }
}
