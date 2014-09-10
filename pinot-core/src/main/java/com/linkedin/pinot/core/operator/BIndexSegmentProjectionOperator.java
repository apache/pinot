package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.block.aggregation.ColumnarReaderBlock;
import com.linkedin.pinot.core.block.aggregation.MatchEntireSegmentBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


public class BIndexSegmentProjectionOperator implements Operator {

  private final Operator _filterOperators;
  private final IndexSegment _indexSegment;
  private BlockDocIdIterator _currentBlockDocIdIterator;
  private Block _currentBlock;
  private int _currentDoc = 0;
  private final int _maxDocPerAggregation = 5000;
  private int[] _docIds;
  private int _pos = 0;
  private int _searchableDocIdSize = 0;

  public BIndexSegmentProjectionOperator(Operator filterOperators, IndexSegment indexSegment) {
    _docIds = new int[_maxDocPerAggregation];
    _filterOperators = filterOperators;
    _indexSegment = indexSegment;
    if (_filterOperators == null) {
      _currentBlock = new MatchEntireSegmentBlock(_indexSegment.getSegmentMetadata().getTotalDocs());
    } else {
      _currentBlock = _filterOperators.nextBlock();
    }
    _currentBlockDocIdIterator = _currentBlock.getBlockDocIdSet().iterator();
    _currentDoc = 0;
  }

  @Override
  public boolean open() {
    _filterOperators.open();
    return true;
  }

  @Override
  public Block nextBlock() {
    _pos = 0;
    getNextDoc();
    while (_currentDoc != Constants.EOF) {
      _docIds[_pos++] = _currentDoc;
      if (_pos == _maxDocPerAggregation) {
        _searchableDocIdSize = _pos;
        return new ColumnarReaderBlock(_indexSegment, _docIds, _maxDocPerAggregation);
      }
      getNextDoc();
    }
    if (_pos > 0) {
      _searchableDocIdSize = _pos;
      return new ColumnarReaderBlock(_indexSegment, _docIds, _pos);
    }
    return null;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  public Block getCurrentBlock() {
    return _currentBlock;
  }

  private int getNextDoc() {
    while ((_currentBlockDocIdIterator == null) || ((_currentDoc = _currentBlockDocIdIterator.next()) == Constants.EOF)) {
      if (_filterOperators != null) {
        _currentBlock = _filterOperators.nextBlock();
      } else {
        if (_currentDoc == Constants.EOF) {
          _currentBlock = null;
        }
      }
      if (_currentBlock == null) {
        return Constants.EOF;
      }
      _currentBlockDocIdIterator = _currentBlock.getBlockDocIdSet().iterator();
    }
    return _currentDoc;
  }

  @Override
  public boolean close() {
    _filterOperators.close();
    return true;
  }

  public Block[] getDataBlock(String[] columns) {
    Block[] blocks = new Block[columns.length];
    for (int i = 0; i < columns.length; ++i) {
      if (columns[i] == null) {
        blocks[i] = new ColumnarReaderBlock(_indexSegment, _docIds, _searchableDocIdSize);
      } else {
        blocks[i] = new ColumnarReaderBlock(_indexSegment.getColumnarReader(columns[i]), _docIds, _searchableDocIdSize);
      }
    }
    return blocks;
  }

  public long getCurrentBlockSize() {
    return _searchableDocIdSize;
  }

}
