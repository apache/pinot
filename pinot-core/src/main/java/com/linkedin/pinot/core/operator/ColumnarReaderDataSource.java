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


public class ColumnarReaderDataSource implements DataSource {

  private final ColumnarReader _columnarReader;
  private final Dictionary<?> _dictionary;
  private final ColumnMetadata _columnMetadata;
  private final BIndexSegmentProjectionOperator _projectionOperator;

  public ColumnarReaderDataSource(ColumnarReader columnarReader, Dictionary<?> dictionary,
      ColumnMetadata columnMetadata, Operator op) {
    _columnarReader = columnarReader;
    _dictionary = dictionary;
    _columnMetadata = columnMetadata;
    _projectionOperator = (BIndexSegmentProjectionOperator) op;
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public Block nextBlock() {
    Block block = _projectionOperator.getCurrentDocIdSetBlock();
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
    _projectionOperator.close();
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    throw new UnsupportedOperationException();
  }

}
