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

import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Selection;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombineService</code> class provides the utility methods to combine {@link IntermediateResultsBlock}s.
 */
@SuppressWarnings({"ConstantConditions", "unchecked"})
public class CombineService {
  private CombineService() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(CombineService.class);

  public static void mergeTwoBlocks(BrokerRequest brokerRequest, IntermediateResultsBlock mergedBlock,
      IntermediateResultsBlock blockToMerge) {
    // Combine processing exceptions.
    List<ProcessingException> mergedProcessingExceptions = mergedBlock.getProcessingExceptions();
    List<ProcessingException> processingExceptionsToMerge = blockToMerge.getProcessingExceptions();
    if (mergedProcessingExceptions == null) {
      mergedBlock.setProcessingExceptions(processingExceptionsToMerge);
    } else if (processingExceptionsToMerge != null) {
      mergedProcessingExceptions.addAll(processingExceptionsToMerge);
    }

    // Combine result.
    if (brokerRequest.isSetAggregationsInfo()) {
      // Combine aggregation result.

      if (!brokerRequest.isSetGroupBy()) {
        // Combine aggregation only result.

        // Might be null if caught exception during query execution.
        List<Object> aggregationResultToMerge = blockToMerge.getAggregationResult();
        if (aggregationResultToMerge == null) {
          // No data in block to merge.
          return;
        }

        AggregationFunction[] mergedAggregationFunctions = mergedBlock.getAggregationFunctions();
        if (mergedAggregationFunctions == null) {
          // No data in merged block.
          mergedBlock.setAggregationFunctions(blockToMerge.getAggregationFunctions());
          mergedBlock.setAggregationResults(aggregationResultToMerge);
        } else {
          // Merge two blocks.
          List<Object> mergedAggregationResult = mergedBlock.getAggregationResult();
          int numAggregationFunctions = mergedAggregationFunctions.length;
          for (int i = 0; i < numAggregationFunctions; i++) {
            mergedAggregationResult.set(i,
                mergedAggregationFunctions[i].merge(mergedAggregationResult.get(i), aggregationResultToMerge.get(i)));
          }
        }
      } else {
        // Combine aggregation group-by result, which should not come into CombineService.
        throw new UnsupportedOperationException();
      }
    } else {
      // Combine selection result.

      // Data schema will be null if exceptions caught during query processing.
      // Result set size will be zero if no row matches the predicate.
      DataSchema mergedBlockSchema = mergedBlock.getDataSchema();
      DataSchema blockToMergeSchema = blockToMerge.getDataSchema();
      Collection<Object[]> mergedBlockResultSet = mergedBlock.getSelectionResult();
      Collection<Object[]> blockToMergeResultSet = blockToMerge.getSelectionResult();

      if (mergedBlockSchema == null || mergedBlockResultSet.size() == 0) {
        // No data in merged block.

        // If block to merge schema is not null, set its data schema and result to the merged block.
        if (blockToMergeSchema != null) {
          mergedBlock.setDataSchema(blockToMergeSchema);
          mergedBlock.setSelectionResult(blockToMergeResultSet);
        }
      } else {
        // Some data in merged block.

        Selection selection = brokerRequest.getSelections();
        boolean isSelectionOrderBy = selection.isSetSelectionSortSequence();
        int selectionSize = selection.getSize();

        // No need to merge if already got enough rows for selection only.
        if (!isSelectionOrderBy && (mergedBlockResultSet.size() == selectionSize)) {
          return;
        }

        // Merge only if there are data in block to merge.
        if (blockToMergeSchema != null && blockToMergeResultSet.size() > 0) {
          if (mergedBlockSchema.isTypeCompatibleWith(blockToMergeSchema)) {
            // Two blocks are mergeable.

            // Upgrade the merged block schema if necessary.
            mergedBlockSchema.upgradeToCover(blockToMergeSchema);

            // Merge two blocks.
            if (isSelectionOrderBy) {
              // Combine selection order-by.
              SelectionOperatorUtils
                  .mergeWithOrdering((PriorityQueue<Object[]>) mergedBlockResultSet, blockToMergeResultSet,
                      selection.getOffset() + selectionSize);
            } else {
              // Combine selection only.
              SelectionOperatorUtils.mergeWithoutOrdering(mergedBlockResultSet, blockToMergeResultSet, selectionSize);
            }
            mergedBlock.setSelectionResult(mergedBlockResultSet);
          } else {
            // Two blocks are not mergeable.

            String errorMessage = "Data schema inconsistency between merged block schema: " + mergedBlockSchema
                + " and block to merge schema: " + blockToMergeSchema + ", drop block to merge";
            LOGGER.info(errorMessage);
            mergedBlock.addToProcessingExceptions(
                QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, errorMessage));
          }
        }
      }
    }
  }
}
