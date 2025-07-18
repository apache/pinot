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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.AcquireReleaseColumnsSegmentOperator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.operator.blocks.results.ExceptionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Combine operator for aggregation queries.
 */
@SuppressWarnings({"rawtypes"})
public class DistinctCountCombineOperator extends BaseSingleBlockCombineOperator<AggregationResultsBlock> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistinctCountCombineOperator.class);
  private static final String EXPLAIN_NAME = "COMBINE_DISTINCT_COUNT";

  private final CountDownLatch _operatorLatch;
  private ConcurrentHashMap.KeySetView _distinctSet;
  private DataType _dataType;

  public DistinctCountCombineOperator(List<Operator> operators, QueryContext queryContext,
      ExecutorService executorService) {
    super(null, operators, queryContext, executorService);
    _operatorLatch = new CountDownLatch(_numTasks);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected void processSegments() {
    int operatorId;
    while (_processingException.get() == null && (operatorId = _nextOperatorId.getAndIncrement()) < _numOperators) {
      Operator operator = _operators.get(operatorId);
      AggregationResultsBlock resultsBlock;
      try {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).acquire();
        }
        resultsBlock = (AggregationResultsBlock) operator.nextBlock();
      } catch (RuntimeException e) {
        throw wrapOperatorException(operator, e);
      } finally {
        if (operator instanceof AcquireReleaseColumnsSegmentOperator) {
          ((AcquireReleaseColumnsSegmentOperator) operator).release();
        }
      }
      // TODO: Do not construct Set for segment result, directly add values to the concurrent set.
      Set set = (Set) resultsBlock.getResults().get(0);
      synchronized (this) {
        if (_distinctSet == null) {
          _distinctSet = ConcurrentHashMap.newKeySet(HashUtil.getHashMapCapacity(set.size()));
          if (set instanceof IntSet) {
            _dataType = DataType.INT;
          } else if (set instanceof LongSet) {
            _dataType = DataType.LONG;
          } else if (set instanceof FloatSet) {
            _dataType = DataType.FLOAT;
          } else if (set instanceof DoubleSet) {
            _dataType = DataType.DOUBLE;
          } else {
            Preconditions.checkState(set instanceof ObjectSet, "Unsupported set type: %s", set.getClass());
          }
        }
      }
      _distinctSet.addAll(set);
    }
  }

  @Override
  public void onProcessSegmentsException(Throwable t) {
    _processingException.compareAndSet(null, t);
  }

  @Override
  protected void onProcessSegmentsFinish() {
    _operatorLatch.countDown();
  }

  @Override
  public BaseResultsBlock mergeResults()
      throws Exception {
    long timeoutMs = _queryContext.getEndTimeMs() - System.currentTimeMillis();
    boolean opCompleted = _operatorLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    if (!opCompleted) {
      // If this happens, the broker side should already timed out, just log the error and return
      String errorMessage =
          String.format("Timed out while combining distinct count results after %dms, queryContext = %s", timeoutMs,
              _queryContext);
      LOGGER.error(errorMessage);
      return new ExceptionResultsBlock(new TimeoutException(errorMessage));
    }

    Throwable processingException = _processingException.get();
    if (processingException != null) {
      if (processingException instanceof BadQueryRequestException) {
        return new ExceptionResultsBlock(QueryException.QUERY_VALIDATION_ERROR, processingException);
      }
      return new ExceptionResultsBlock(processingException);
    }

    Object result;
    if (_queryContext.isServerReturnFinalResult() || _dataType == null) {
      result = _distinctSet;
    } else {
      switch (_dataType) {
        case INT:
          result = new IntOpenHashSet(_distinctSet);
          break;
        case LONG:
          result = new LongOpenHashSet(_distinctSet);
          break;
        case FLOAT:
          result = new FloatOpenHashSet(_distinctSet);
          break;
        case DOUBLE:
          result = new DoubleOpenHashSet(_distinctSet);
          break;
        default:
          throw new IllegalStateException();
      }
    }
    return new AggregationResultsBlock(_queryContext.getAggregationFunctions(), List.of(result), _queryContext);
  }
}
