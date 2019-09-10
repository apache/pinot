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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.ConcurrentIndexedTable;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombineGroupByOrderByOperator</code> class is the operator to combine aggregation results with group-by and order by.
 */
// TODO: this class has a lot of duplication with {@link CombineGroupByOperator}.
// These 2 classes can be combined into one
// For the first iteration of Order By support, these will be separate
public class CombineGroupByOrderByOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombineGroupByOrderByOperator.class);
  private static final String OPERATOR_NAME = "CombineGroupByOrderByOperator";

  // Use a higher limit for groups stored across segments. For most cases, most groups from each segment should be the
  // same, thus the total number of groups across segments should be equal or slightly higher than the number of groups
  // in each segment. We still put a limit across segments to protect cases where data is very skewed across different
  // segments.
  private static final int INTER_SEGMENT_NUM_GROUPS_LIMIT_FACTOR = 2;

  private final List<Operator> _operators;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;
  private final int _interSegmentNumGroupsLimit;
  private Lock _initLock;
  private DataSchema _dataSchema;
  private ConcurrentIndexedTable _indexedTable;

  public CombineGroupByOrderByOperator(List<Operator> operators, BrokerRequest brokerRequest,
      ExecutorService executorService, long timeOutMs, int innerSegmentNumGroupsLimit) {
    Preconditions.checkArgument(
        brokerRequest.isSetAggregationsInfo() && brokerRequest.isSetGroupBy() && brokerRequest.isSetOrderBy());

    _operators = operators;
    _brokerRequest = brokerRequest;
    _executorService = executorService;
    _timeOutMs = timeOutMs;
    _interSegmentNumGroupsLimit =
        (int) Math.min((long) innerSegmentNumGroupsLimit * INTER_SEGMENT_NUM_GROUPS_LIMIT_FACTOR, Integer.MAX_VALUE);
    _initLock = new ReentrantLock();
    _indexedTable = new ConcurrentIndexedTable();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Combines the group-by result blocks from underlying operators and returns a merged and trimmed group-by
   * result block.
   * <ul>
   *   <li>
   *     Concurrently merge group-by results from multiple result blocks into {@link org.apache.pinot.core.data.table.IndexedTable}
   *   </li>
   *   <li>
   *     Set all exceptions encountered during execution into the merged result block
   *   </li>
   * </ul>
   */
  @Override
  protected IntermediateResultsBlock getNextBlock() {
    int numOperators = _operators.size();
    CountDownLatch operatorLatch = new CountDownLatch(numOperators);

    int numAggregationFunctions = _brokerRequest.getAggregationsInfoSize();
    int numGroupBy = _brokerRequest.getGroupBy().getExpressionsSize();
    ConcurrentLinkedQueue<ProcessingException> mergedProcessingExceptions = new ConcurrentLinkedQueue<>();

    Future[] futures = new Future[numOperators];
    for (int i = 0; i < numOperators; i++) {
      int index = i;
      futures[i] = _executorService.submit(new TraceRunnable() {
        @SuppressWarnings("unchecked")
        @Override
        public void runJob() {
          AggregationGroupByResult aggregationGroupByResult;

          try {
            IntermediateResultsBlock intermediateResultsBlock =
                (IntermediateResultsBlock) _operators.get(index).nextBlock();

            _initLock.lock();
            try {
              if (_dataSchema == null) {
                _dataSchema = intermediateResultsBlock.getDataSchema();
                _indexedTable.init(_dataSchema, _brokerRequest.getAggregationsInfo(), _brokerRequest.getOrderBy(),
                    _interSegmentNumGroupsLimit, false);
              }
            } finally {
              _initLock.unlock();
            }

            // Merge processing exceptions.
            List<ProcessingException> processingExceptionsToMerge = intermediateResultsBlock.getProcessingExceptions();
            if (processingExceptionsToMerge != null) {
              mergedProcessingExceptions.addAll(processingExceptionsToMerge);
            }

            // Merge aggregation group-by result.
            aggregationGroupByResult = intermediateResultsBlock.getAggregationGroupByResult();
            if (aggregationGroupByResult != null) {
              // Iterate over the group-by keys, for each key, update the group-by result in the indexedTable.
              Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
              while (groupKeyIterator.hasNext()) {
                GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
                String[] stringKey = groupKey._stringKey.split(GroupKeyGenerator.DELIMITER);
                Object[] objectKey = new Object[numGroupBy];
                for (int i = 0; i < stringKey.length; i++) {
                  DataSchema.ColumnDataType columnDataType = _dataSchema.getColumnDataType(i);
                  switch (columnDataType) {

                    case INT:
                      objectKey[i] = Integer.valueOf(stringKey[i]);
                      break;
                    case LONG:
                      objectKey[i] = Long.valueOf(stringKey[i]);
                      break;
                    case FLOAT:
                      objectKey[i] = Float.valueOf(stringKey[i]);
                      break;
                    case DOUBLE:
                      objectKey[i] = Double.valueOf(stringKey[i]);
                      break;
                    case STRING:
                      objectKey[i] = stringKey[i];
                      break;
                    case BYTES:
                      objectKey[i] = BytesUtils.toBytes(stringKey[i]);
                      break;
                  }
                }
                Object[] values = new Object[numAggregationFunctions];
                for (int i = 0; i < numAggregationFunctions; i++) {
                  values[i] = aggregationGroupByResult.getResultForKey(groupKey, i);
                }

                Record record = new Record(new Key(objectKey), values);
                _indexedTable.upsert(record);
              }
            }
          } catch (Exception e) {
            LOGGER.error("Exception processing CombineGroupByOrderBy for index {}, operator {}", index,
                _operators.get(index).getClass().getName(), e);
            mergedProcessingExceptions.add(QueryException.getException(QueryException.QUERY_EXECUTION_ERROR, e));
          }

          operatorLatch.countDown();
        }
      });
    }

    try {
      boolean opCompleted = operatorLatch.await(_timeOutMs, TimeUnit.MILLISECONDS);
      if (!opCompleted) {
        // If this happens, the broker side should already timed out, just log the error and return
        String errorMessage = "Timed out while combining group-by results after " + _timeOutMs + "ms";
        LOGGER.error(errorMessage);
        return new IntermediateResultsBlock(new TimeoutException(errorMessage));
      }

      _indexedTable.finish();
      IntermediateResultsBlock mergedBlock = new IntermediateResultsBlock(_indexedTable);

      // Set the processing exceptions.
      if (!mergedProcessingExceptions.isEmpty()) {
        mergedBlock.setProcessingExceptions(new ArrayList<>(mergedProcessingExceptions));
      }

      // Set the execution statistics.
      ExecutionStatistics executionStatistics = new ExecutionStatistics();
      for (Operator operator : _operators) {
        ExecutionStatistics executionStatisticsToMerge = operator.getExecutionStatistics();
        if (executionStatisticsToMerge != null) {
          executionStatistics.merge(executionStatisticsToMerge);
        }
      }
      mergedBlock.setNumDocsScanned(executionStatistics.getNumDocsScanned());
      mergedBlock.setNumEntriesScannedInFilter(executionStatistics.getNumEntriesScannedInFilter());
      mergedBlock.setNumEntriesScannedPostFilter(executionStatistics.getNumEntriesScannedPostFilter());
      mergedBlock.setNumSegmentsProcessed(executionStatistics.getNumSegmentsProcessed());
      mergedBlock.setNumSegmentsMatched(executionStatistics.getNumSegmentsMatched());
      mergedBlock.setNumTotalRawDocs(executionStatistics.getNumTotalRawDocs());

      if (_indexedTable.size() >= _interSegmentNumGroupsLimit) {
        mergedBlock.setNumGroupsLimitReached(true);
      }

      return mergedBlock;
    } catch (Exception e) {
      return new IntermediateResultsBlock(e);
    } finally {
      // Cancel all ongoing jobs
      for (Future future : futures) {
        if (!future.isDone()) {
          future.cancel(true);
        }
      }
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
