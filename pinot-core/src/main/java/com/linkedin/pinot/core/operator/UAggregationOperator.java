package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.block.aggregation.IntermediateResultsBlock;
import com.linkedin.pinot.core.block.aggregation.MatchEntireSegmentBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.AggregationService;
import com.linkedin.pinot.core.query.aggregation.CombineLevel;
import com.linkedin.pinot.core.query.aggregation.CombineService;


/**
 * This UAggregationAndSelectionOperator will take care of applying a request
 * with both aggregation and selection to one IndexSegment.
 *
 * @author xiafu
 *
 */
public class UAggregationOperator implements Operator {

  private final Operator _filterOperators;
  private final IndexSegment _indexSegment;
  private final BrokerRequest _brokerRequest;

  private AggregationService _aggregationService = null;
  private BlockDocIdIterator _currentBlockDocIdIterator;

  public UAggregationOperator(IndexSegment indexSegment, BrokerRequest brokerRequest, Operator filterOperator) {
    _brokerRequest = brokerRequest;
    _indexSegment = indexSegment;
    _filterOperators = filterOperator;
    _aggregationService = new AggregationService(AggregationFunctionFactory.getAggregationFunction(_brokerRequest));
  }

  public UAggregationOperator(IndexSegment indexSegment, BrokerRequest brokerRequest) {
    _indexSegment = indexSegment;
    _brokerRequest = brokerRequest;
    _filterOperators = null;
    _aggregationService = new AggregationService(AggregationFunctionFactory.getAggregationFunction(_brokerRequest));
  }

  @Override
  public boolean open() {
    if (_filterOperators != null) {
      _filterOperators.open();
    }
    return true;
  }

  @Override
  public Block nextBlock() {

    final long startTime = System.currentTimeMillis();
    int nextDoc = 0;
    Block nextBlock = null;
    if (_filterOperators == null) {
      nextBlock = new MatchEntireSegmentBlock(_indexSegment.getSegmentMetadata().getTotalDocs());
    } else {
      nextBlock = _filterOperators.nextBlock();
    }
    _currentBlockDocIdIterator = nextBlock.getBlockDocIdSet().iterator();
    nextDoc = getNextDoc(nextBlock, nextDoc);
    while (nextDoc != Constants.EOF) {
      _aggregationService.mapDoc(nextDoc, _indexSegment);
      nextDoc = getNextDoc(nextBlock, nextDoc);
    }
    _aggregationService.finializeMap(_indexSegment);
    final IntermediateResultsBlock resultBlock =
        new IntermediateResultsBlock(_aggregationService.getAggregationFunctionList(), CombineService.combine(
            _aggregationService.getAggregationFunctionList(), _aggregationService.getAggregationResultsList(),
            CombineLevel.SEGMENT));
    resultBlock.setNumDocsScanned(_aggregationService.getNumDocsScanned());
    resultBlock.setTotalDocs(_indexSegment.getSegmentMetadata().getTotalDocs());
    resultBlock.setTimeUsedMs(System.currentTimeMillis() - startTime);
    return resultBlock;
  }

  private int getNextDoc(Block nextBlock, int nextDoc) {
    while (_currentBlockDocIdIterator == null || (nextDoc = _currentBlockDocIdIterator.next()) == Constants.EOF) {
      if (_filterOperators != null) {
        nextBlock = _filterOperators.nextBlock();
      } else {
        if (nextDoc == Constants.EOF) {
          nextBlock = null;
        }
      }
      if (nextBlock == null) {
        return Constants.EOF;
      }
      _currentBlockDocIdIterator = nextBlock.getBlockDocIdSet().iterator();
    }
    return nextDoc;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    if (_filterOperators != null) {
      _filterOperators.close();
    }
    return true;
  }

}
