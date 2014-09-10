package com.linkedin.pinot.core.block.aggregation;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;


public class ColumnarReaderBlock implements Block {

  private IndexSegment _indexSegment;
  private int[] _docIdSet;
  private int _searchableLength;
  private ColumnarReader _columnarReader;

  public ColumnarReaderBlock(IndexSegment indexSegment, int[] docIdSet, int searchableLength) {
    _indexSegment = indexSegment;
    _docIdSet = docIdSet;
    _searchableLength = searchableLength;
  }

  public ColumnarReaderBlock(ColumnarReader columnarReader, int[] docIds, int searchableDocIdSize) {
    _columnarReader = columnarReader;
    _docIdSet = docIds;
    _searchableLength = searchableDocIdSize;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    return true;
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new BlockValSet() {
      @Override
      public BlockValIterator iterator() {
        return new ColumnarReaderBlockValIterator(_columnarReader, _docIdSet, _searchableLength);
      }
    };
  }

  public BlockValSet getBlockValueSet(final String columnName) {
    return new BlockValSet() {
      @Override
      public BlockValIterator iterator() {
        return new ColumnarReaderBlockValIterator(_indexSegment.getColumnarReader(columnName), _docIdSet,
            _searchableLength);
      }
    };
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetBlock() {
    throw new UnsupportedOperationException();
  }

}
