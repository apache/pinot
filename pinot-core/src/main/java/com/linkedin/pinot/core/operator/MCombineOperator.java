/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.linkedin.pinot.core.trace.TraceCallable;
import com.linkedin.pinot.core.trace.TraceRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOnlyOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOrderByOperator;
import com.linkedin.pinot.core.query.aggregation.CombineService;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;


/**
 * MCombineOperator will take the arguments below:
 *  1. BrokerRequest;
 *  2. Parallelism Parameters:
 *      ExecutorService;
 *  3. All the Inner-Segment Operators:
 *      For now only four types:
 *          {@link MSelectionOnlyOperator}
 *          {@link MSelectionOrderByOperator}
 *          {@link MAggregationOperator}
 *          {@link MAggregationGroupByOperator}
 *      Number of Operators is based on the pruned segments:
 *          one segment to one Operator.
 *
 *
 */
public class MCombineOperator extends BaseOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(MCombineOperator.class);

  private final List<Operator> _operators;
  private final boolean _isParallel;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private long _timeOutMs;
  //Make this configurable
  //These two control the parallelism on a per query basis, depending on the number of segments to process
  private static int MAX_THREADS_PER_QUERY = 5;
  private static int MIN_SEGMENTS_PER_THREAD = 10;

  private IntermediateResultsBlock _mergedBlock;

  public MCombineOperator(List<Operator> retOperators, BrokerRequest brokerRequest) {
    _operators = retOperators;
    _isParallel = false;
    _brokerRequest = brokerRequest;
    _executorService = null;
  }

  public MCombineOperator(List<Operator> retOperators, ExecutorService executorService, long timeOutMs,
      BrokerRequest brokerRequest) {
    _operators = retOperators;
    _executorService = executorService;
    _brokerRequest = brokerRequest;
    _timeOutMs = timeOutMs;
    if (_executorService != null) {
      _isParallel = true;
    } else {
      _isParallel = false;
    }
  }

  @Override
  public boolean open() {
    for (Operator op : _operators) {
      op.open();
    }
    return true;
  }


  @Override
  public Block getNextBlock() {
    final long startTime = System.currentTimeMillis();
    if (_isParallel) {
      final long queryEndTime = System.currentTimeMillis() + _timeOutMs;
      int numGroups = Math.min(MAX_THREADS_PER_QUERY, (_operators.size() + MIN_SEGMENTS_PER_THREAD - 1) / MIN_SEGMENTS_PER_THREAD);

      final List<List<Operator>> operatorGroups = new ArrayList<List<Operator>>(numGroups);
      for (int i = 0; i < numGroups; i++) {
        operatorGroups.add(new ArrayList<Operator>());
      }
      for (int i = 0; i < _operators.size(); i++) {
        operatorGroups.get(i % numGroups).add(_operators.get(i));
      }
      final BlockingQueue<Block> blockingQueue = new ArrayBlockingQueue<Block>(operatorGroups.size());
      // Submit operators.
      for (final List<Operator> operatorGroup : operatorGroups) {
        _executorService.submit(new TraceRunnable() {
          @Override
          public void runJob() {
            IntermediateResultsBlock mergedBlock = null;
            try {
              for (Operator operator : operatorGroup) {
                IntermediateResultsBlock blockToMerge = (IntermediateResultsBlock) operator.nextBlock();
                if (mergedBlock == null) {
                  mergedBlock = blockToMerge;
                } else {
                  CombineService.mergeTwoBlocks(_brokerRequest, mergedBlock, blockToMerge);
                }
              }
            } catch (Exception e) {
              LOGGER.error("exception in the MCombine operator ", e);
              mergedBlock = new IntermediateResultsBlock(e);
            }
            if (blockingQueue != null) {
              blockingQueue.offer(mergedBlock);
            }
          }
        });
      }
      LOGGER
          .debug("Submitting operators to be run in parallel and it took:" + (System.currentTimeMillis() - startTime));

      // Submit merger job:
      Future<IntermediateResultsBlock> mergedBlockFuture =
          _executorService.submit(new TraceCallable<IntermediateResultsBlock>() {
            @Override
            public IntermediateResultsBlock callJob() throws Exception {
              int mergedBlocksNumber = 0;
              IntermediateResultsBlock mergedBlock = null;
              while ((queryEndTime > System.currentTimeMillis()) && (mergedBlocksNumber < operatorGroups.size())) {
                if (mergedBlock == null) {
                  mergedBlock =
                      (IntermediateResultsBlock) blockingQueue.poll(queryEndTime - System.currentTimeMillis(),
                          TimeUnit.MILLISECONDS);
                  if (mergedBlock != null) {
                    mergedBlocksNumber++;
                  }
                  LOGGER.debug("Got response from operator 0 after: {}", (System.currentTimeMillis() - startTime));
                } else {
                  IntermediateResultsBlock blockToMerge =
                      (IntermediateResultsBlock) blockingQueue.poll(queryEndTime - System.currentTimeMillis(),
                          TimeUnit.MILLISECONDS);
                  if (blockToMerge != null) {
                    try {
                      LOGGER.debug("Got response from operator {} after: {}", mergedBlocksNumber,
                          (System.currentTimeMillis() - startTime));
                      CombineService.mergeTwoBlocks(_brokerRequest, mergedBlock, blockToMerge);
                      LOGGER.debug("Merged response from operator {} after: {}", mergedBlocksNumber,
                          (System.currentTimeMillis() - startTime));
                    } catch (Exception e) {
                      mergedBlock.getExceptions().add(
                          QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
                    }
                    mergedBlocksNumber++;
                  }
                }
              }
              return mergedBlock;
            }
          });

      // Get merge results.
      try {
        _mergedBlock = mergedBlockFuture.get(queryEndTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOGGER.error("InterruptedException ", e);
        if (_mergedBlock == null) {
          _mergedBlock = new IntermediateResultsBlock(e);
        }
        List<ProcessingException> exceptions = _mergedBlock.getExceptions();
        if (exceptions == null) {
          exceptions = new ArrayList<ProcessingException>();
        }
        exceptions.add(QueryException.getException(QueryException.FUTURE_CALL_ERROR, e));
        _mergedBlock.setExceptionsList(exceptions);
      } catch (ExecutionException e) {
        LOGGER.error("Execution Exception", e);
        if (_mergedBlock == null) {
          _mergedBlock = new IntermediateResultsBlock(e);
        }
        List<ProcessingException> exceptions = _mergedBlock.getExceptions();
        if (exceptions == null) {
          exceptions = new ArrayList<ProcessingException>();
        }
        exceptions.add(QueryException.getException(QueryException.MERGE_RESPONSE_ERROR, e));
        _mergedBlock.setExceptionsList(exceptions);
      } catch (TimeoutException e) {
        LOGGER.error("TimeoutException ", e);
        if (_mergedBlock == null) {
          _mergedBlock = new IntermediateResultsBlock(e);
        }
        List<ProcessingException> exceptions = _mergedBlock.getExceptions();
        if (exceptions == null) {
          exceptions = new ArrayList<ProcessingException>();
        }
        exceptions.add(QueryException.getException(QueryException.EXECUTION_TIMEOUT_ERROR, e));
        _mergedBlock.setExceptionsList(exceptions);
      }

    } else {
      for (Operator operator : _operators) {
        if ((operator instanceof MAggregationOperator) || (operator instanceof MSelectionOrderByOperator)
            || (operator instanceof MSelectionOnlyOperator) || (operator instanceof MAggregationGroupByOperator)
            || (operator instanceof MCombineOperator)) {
          IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
          if (_mergedBlock == null) {
            _mergedBlock = block;
          } else {
            CombineService.mergeTwoBlocks(_brokerRequest, _mergedBlock, block);
          }
        } else {
          throw new UnsupportedOperationException("Unsupported Operator to be processed in MResultOperator : "
              + operator);
        }
      }
    }
    if ((_brokerRequest.getAggregationsInfoSize() > 0) && (_brokerRequest.getGroupBy() != null)
        && (_brokerRequest.getGroupBy().getColumnsSize() > 0)) {
      trimToSize(_brokerRequest, _mergedBlock);
    }

    return _mergedBlock;
  }

  private void trimToSize(BrokerRequest brokerRequest, IntermediateResultsBlock mergedBlock) {
    AggregationGroupByOperatorService aggregationGroupByOperatorService =
        new AggregationGroupByOperatorService(brokerRequest.getAggregationsInfo(), brokerRequest.getGroupBy());
    aggregationGroupByOperatorService.trimToSize(mergedBlock.getAggregationGroupByOperatorResult());

  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOperatorName() {
    return "MCombineOperator";
  }

  @Override
  public boolean close() {
    for (Operator op : _operators) {
      op.close();
    }
    return true;
  }

}
