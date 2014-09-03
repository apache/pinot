package com.linkedin.pinot.core.block.aggregation;

import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;


/**
 * InstanceResponseBlock is just a holder to get InstanceResponse from InstanceResponseBlock.
 * 
 * @author xiafu
 *
 */
public class InstanceResponseBlock implements Block {
  private DataTable _instanceResponseDataTable;
  private final IntermediateResultsBlock _intermediateResultsBlock;

  public InstanceResponseBlock(Block block) {
    _intermediateResultsBlock = (IntermediateResultsBlock) block;
    try {
      _instanceResponseDataTable = _intermediateResultsBlock.getDataTable();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public DataTable getInstanceResponseDataTable() {
    return _instanceResponseDataTable;
  }

  public byte[] getInstanceResponseBytes() throws Exception {
    return _instanceResponseDataTable.toBytes();
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException();
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
