package com.linkedin.pinot.core.query.utils;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.columnar.readers.ColumnarReader;
import com.linkedin.pinot.core.operator.DataSource;


public class SimpleIndexDataSource implements DataSource {

  private final ColumnarReader _columnarReader;
  private int _blockId = 0;
  private final long _size;

  public SimpleIndexDataSource(ColumnarReader columnarReader, long numRecords) {
    _columnarReader = columnarReader;
    _size = numRecords;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    return nextBlock(new BlockId(_blockId++));
  }

  @Override
  public Block nextBlock(BlockId blockId) {
    int id = blockId.getId();
    if (id > 0) {
      return null;
    }
    return new SimpleDataBlock(_columnarReader, _size);
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    return true;
  }

}
