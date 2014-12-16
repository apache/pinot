package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.block.query.MatchEntireSegmentBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * BDocIdSetOperator will take a filter Operator and get the matched docId set.
 * Internally, cached a given size of docIds, so this Operator could be replicated
 * for many ColumnarReaderDataSource.
 *
 * @author xiafu
 *
 */
public class BDocIdSetOperator implements Operator {

  private final Operator _filterOperators;
  private final IndexSegment _indexSegment;
  private BlockDocIdIterator _currentBlockDocIdIterator;
  private Block _currentBlock;
  private DocIdSetBlock _currentDocIdSetBlock;
  private int _currentDoc = 0;
  private final int _maxSizeOfdocIdSet;
  private final int[] _docIds;
  private int _pos = 0;
  private int _searchableDocIdSize = 0;

  public BDocIdSetOperator(Operator filterOperators, IndexSegment indexSegment, int maxSizeOfdocIdSet) {
    _maxSizeOfdocIdSet = maxSizeOfdocIdSet;
    _docIds = new int[_maxSizeOfdocIdSet];
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
      if (_pos == _maxSizeOfdocIdSet) {
        _searchableDocIdSize = _pos;
        _currentDocIdSetBlock = new DocIdSetBlock(_indexSegment, _docIds, _pos);
        return _currentDocIdSetBlock;
      }
      getNextDoc();
    }
    if (_pos > 0) {
      _searchableDocIdSize = _pos;
      _currentDocIdSetBlock = new DocIdSetBlock(_indexSegment, _docIds, _pos);
      return _currentDocIdSetBlock;
    }
    _currentDocIdSetBlock = null;
    return _currentDocIdSetBlock;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  public Block getCurrentBlock() {
    return _currentBlock;
  }

  public DocIdSetBlock getCurrentDocIdSetBlock() {
    return _currentDocIdSetBlock;
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

  public long getCurrentBlockSize() {
    return _searchableDocIdSize;
  }

}
