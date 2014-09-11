package com.linkedin.pinot.core.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.query.aggregation.CombineService;


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

  private IntermediateResultsBlock _mergedBlock;

  public MCombineOperator(List<Operator> retOperators, BrokerRequest brokerRequest) {
    _operators = retOperators;
    _isParallel = false;
    _brokerRequest = brokerRequest;
    _executorService = null;
  }

  public MCombineOperator(List<Operator> retOperators, ExecutorService executorService, BrokerRequest brokerRequest) {
    _operators = retOperators;
    _executorService = executorService;
    _brokerRequest = brokerRequest;
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
      System.out.println("Setting up jobs in parallel!");

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
        _mergedBlock = (IntermediateResultsBlock) blocks.get(0).get(150000, TimeUnit.MILLISECONDS);
        for (int i = 1; i < blocks.size(); ++i) {
          CombineService.mergeTwoBlocks(_brokerRequest, _mergedBlock,
              ((IntermediateResultsBlock) blocks.get(i).get(150000, TimeUnit.MILLISECONDS)));
        }
      } catch (InterruptedException e) {
        if (_mergedBlock == null) {
          _mergedBlock = new IntermediateResultsBlock(e);
        }
        List<ProcessingException> exceptions = _mergedBlock.getExceptions();
        if (exceptions == null) {
          exceptions = new ArrayList<ProcessingException>();
        }
        ProcessingException exception = new ProcessingException();
        exception.setErrorCode(270);
        exception.setMessage("Query execution gets InterruptedException");
        exception.setStackTrace(e.getStackTrace());
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
        ProcessingException exception = new ProcessingException();
        exception.setErrorCode(260);
        exception.setMessage("Query execution gets ExecutionException");
        exception.setStackTrace(e.getStackTrace());
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
        ProcessingException exception = new ProcessingException();
        exception.setErrorCode(250);
        exception.setMessage("Query execution gets TimeoutException");
        exception.setStackTrace(e.getStackTrace());
        exceptions.add(exception);
        _mergedBlock.setExceptionsList(exceptions);
      }
    } else {
      for (Operator operator : _operators) {
        if ((operator instanceof MAggregationOperator) || (operator instanceof USelectionOperator)
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
    return _mergedBlock;
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
