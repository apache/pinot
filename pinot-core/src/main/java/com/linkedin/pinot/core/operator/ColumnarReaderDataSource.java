package com.linkedin.pinot.core.operator;

import com.linkedin.pinot.core.block.intarray.ColumnarReaderBlock;
import com.linkedin.pinot.core.block.intarray.DocIdSetBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnMetadata;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;


/**
 * This is a ColumnarReaderDataSource. The only place ColumnarReader will be called.
 *
 * @author xiafu
 *
 */
public class ColumnarReaderDataSource implements DataSource {

  private final ColumnarReader _columnarReader;
  private final Dictionary<?> _dictionary;
  private final ColumnMetadata _columnMetadata;
  private final UReplicatedDocIdSetOperator _replicatedDocIdSetOperator;

  public ColumnarReaderDataSource(ColumnarReader columnarReader, Dictionary<?> dictionary,
      ColumnMetadata columnMetadata, Operator docIdSetOperator) {
    _columnarReader = columnarReader;
    _dictionary = dictionary;
    _columnMetadata = columnMetadata;
    _replicatedDocIdSetOperator = (UReplicatedDocIdSetOperator) docIdSetOperator;
  }

  @Override
  public boolean open() {
    _replicatedDocIdSetOperator.open();
    return true;
  }

  @Override
  public Block nextBlock() {
    final Block block = _replicatedDocIdSetOperator.nextBlock();
    if (block == null) {
      return null;
    }
    return new ColumnarReaderBlock((DocIdSetBlock) block, _columnarReader, _dictionary);
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean close() {
    _replicatedDocIdSetOperator.close();
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    throw new UnsupportedOperationException();
  }

  public int getIntegerValue(int docId) {
    return _columnarReader.getIntegerValue(docId);
  }

  public long getLongValue(int docId) {
    return _columnarReader.getLongValue(docId);
  }

  public float getFloatValue(int docId) {
    return _columnarReader.getFloatValue(docId);
  }

  public double getDoubleValue(int docId) {
    return _columnarReader.getDoubleValue(docId);
  }

  public String getStringValue(int docId) {
    return _columnarReader.getStringValue(docId);
  }

  public Object getRawValue(int docId) {
    return _columnarReader.getRawValue(docId);
  }

  public int getDictionaryId(int docId) {
    return _columnarReader.getDictionaryId(docId);
  }

  public Dictionary getDictionary() {
    return _dictionary;
  }

}
