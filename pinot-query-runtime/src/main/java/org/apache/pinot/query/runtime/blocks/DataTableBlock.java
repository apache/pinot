package org.apache.pinot.query.runtime.blocks;

import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.BlockDocIdValueSet;
import org.apache.pinot.core.common.BlockMetadata;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.InstanceResponseBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataTableBlock implements Block {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceResponseBlock.class);

  private DataTable _dataTable;

  public DataTableBlock(DataTable dataTable) {
    _dataTable = dataTable;
  }

  public DataTable getDataTable() {
    return _dataTable;
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
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
}
