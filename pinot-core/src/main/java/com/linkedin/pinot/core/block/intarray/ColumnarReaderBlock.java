package com.linkedin.pinot.core.block.intarray;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;


public class ColumnarReaderBlock implements Block {

  private final DocIdSetBlock _docIdSetBlock;
  private final ColumnarReader _columnarReader;
  private final Dictionary<?> _dictionary;

  public ColumnarReaderBlock(DocIdSetBlock docIdSetBlock, ColumnarReader columnarReader, Dictionary<?> dictionary) {
    _docIdSetBlock = docIdSetBlock;
    _columnarReader = columnarReader;
    _dictionary = dictionary;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public BlockId getId() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockValSet getBlockValueSet() {
    return new BlockValSet() {

      @Override
      public BlockValIterator iterator() {
        return new ColumnarReaderBlockValIterator(_columnarReader, _docIdSetBlock.getDocIdSet(),
            _docIdSetBlock.getSearchableLength());
      }

      @Override
      public DataType getValueType() {
        return null;
      }

      @Override
      public int getIntValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public long getLongValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public float getFloatValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public double getDoubleValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public String getStringValueAt(int dictionaryId) {
        // TODO Auto-generated method stub
        return null;
      }

      @Override
      public int getDictionaryId(int docId) {
        // TODO Auto-generated method stub
        return 0;
      }
    };
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BlockMetadata getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

  public Dictionary getDictionary() {
    return _dictionary;
  }

}
