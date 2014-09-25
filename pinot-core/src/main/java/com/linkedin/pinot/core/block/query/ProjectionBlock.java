package com.linkedin.pinot.core.block.query;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.BDocIdSetOperator;
import com.linkedin.pinot.core.operator.DataSource;


/**
 * ProjectionBlock holds a column name to Block Map.
 * It provides DocIdSetBlock and DataBlock for a given column.
 * 
 * @author xiafu
 *
 */
public class ProjectionBlock implements Block {

  private final Map<String, Block> _blockMap = new HashMap<String, Block>();
  private final Block _docIdSetBlock;

  public ProjectionBlock(BDocIdSetOperator docIdSetOperator, Map<String, DataSource> columnToDataSourceMap) {
    _docIdSetBlock = docIdSetOperator.nextBlock();
    _blockMap.put("_docIdSet", _docIdSetBlock);
    for (String column : columnToDataSourceMap.keySet()) {
      _blockMap.put(column, columnToDataSourceMap.get(column).nextBlock());
    }
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
    // TODO Auto-generated method stub
    return null;
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

  public Block getBlock(String column) {
    return _blockMap.get(column);
  }

  public Block getDocIdSetBlock() {
    return _docIdSetBlock;
  }
}
