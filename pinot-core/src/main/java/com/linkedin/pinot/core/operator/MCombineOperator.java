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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOperator;
import com.linkedin.pinot.core.query.aggregation.CombineService;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;


/**
 * MCombineOperator will take the arguments below:
 *  1. BrokerRequest;
 *  2. Parallelism Parameters:
 *      ExecutorService;
 *  3. All the Inner-Segment Operators:
 *      For now only three types:
 *          USelectionOperator, UAggregationOperator
 *          and UAggregationAndSelectionOperator
 *      Number of Operators is based on the pruned segments:
 *          one segment to one Operator.
 *
 * @author xiafu
 *
 */
public class MCombineOperator implements Operator {

  private final List<Operator> _operators;
  private final boolean _isParallel;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private long _timeOutMs;

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
  public Block nextBlock() {
    if (_isParallel) {
      long queryEndTime = System.currentTimeMillis() + _timeOutMs;

      @SuppressWarnings("rawtypes")
      List<Future> blocks = new ArrayList<Future>();
      for (final Operator operator : _operators) {
        blocks.add(_executorService.submit(new Callable<Block>() {
          @Override
          public Block call() throws Exception {
            return operator.nextBlock();
          }
        }));
      }
      try {
        _mergedBlock =
            (IntermediateResultsBlock) blocks.get(0).get(queryEndTime - System.currentTimeMillis(),
                TimeUnit.MILLISECONDS);
        for (int i = 1; i < blocks.size(); ++i) {
          CombineService.mergeTwoBlocks(
              _brokerRequest,
              _mergedBlock,
              ((IntermediateResultsBlock) blocks.get(i).get(queryEndTime - System.currentTimeMillis(),
                  TimeUnit.MILLISECONDS)));
        }
      } catch (InterruptedException e) {
        if (_mergedBlock == null) {
          _mergedBlock = new IntermediateResultsBlock(e);
        }
        List<ProcessingException> exceptions = _mergedBlock.getExceptions();
        if (exceptions == null) {
          exceptions = new ArrayList<ProcessingException>();
        }
        ProcessingException exception = QueryException.FUTURE_CALL_ERROR.deepCopy();
        exception.setMessage(e.getMessage());
        exceptions.add(exception);
        _mergedBlock.setExceptionsList(exceptions);
      } catch (ExecutionException e) {
        if (_mergedBlock == null) {
          _mergedBlock = new IntermediateResultsBlock(e);
        }
        List<ProcessingException> exceptions = _mergedBlock.getExceptions();
        if (exceptions == null) {
          exceptions = new ArrayList<ProcessingException>();
        }
        ProcessingException exception = QueryException.QUERY_EXECUTION_ERROR.deepCopy();
        exception.setMessage(e.getMessage());
        exceptions.add(exception);
        _mergedBlock.setExceptionsList(exceptions);
      } catch (TimeoutException e) {
        if (_mergedBlock == null) {
          _mergedBlock = new IntermediateResultsBlock(e);
        }
        List<ProcessingException> exceptions = _mergedBlock.getExceptions();
        if (exceptions == null) {
          exceptions = new ArrayList<ProcessingException>();
        }
        ProcessingException exception = QueryException.EXECUTION_TIMEOUT_ERROR.deepCopy();
        exception.setMessage(e.getMessage());
        exceptions.add(exception);
        _mergedBlock.setExceptionsList(exceptions);
      }
    } else {
      for (Operator operator : _operators) {
        if ((operator instanceof MAggregationOperator) || (operator instanceof MSelectionOperator)
            || (operator instanceof MAggregationGroupByOperator) || (operator instanceof MCombineOperator)) {
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
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    for (Operator op : _operators) {
      op.close();
    }
    return true;
  }

}
