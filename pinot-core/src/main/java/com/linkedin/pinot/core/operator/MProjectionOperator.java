package com.linkedin.pinot.core.operator;

import java.util.Map;

import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;


/**
 * MProjectionOperator will call nextBlock then return a ProjectionBlock.
 * 
 * @author xiafu
 *
 */
public class MProjectionOperator implements DataSource {

  private final BDocIdSetOperator _docIdSetOperator;
  private final Map<String, DataSource> _columnToDataSourceMap;
  private ProjectionBlock _currentBlock = null;

  public MProjectionOperator(Map<String, DataSource> dataSourceMap, BDocIdSetOperator docIdSetOperator) {
    this._docIdSetOperator = docIdSetOperator;
    this._columnToDataSourceMap = dataSourceMap;
  }

  @Override
  public boolean open() {
    for (String column : _columnToDataSourceMap.keySet()) {
      _columnToDataSourceMap.get(column).open();
    }
    return true;
  }

  @Override
  public boolean close() {
    for (String column : _columnToDataSourceMap.keySet()) {
      _columnToDataSourceMap.get(column).close();
    }
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    return false;
  }

  @Override
  public Block nextBlock() {
    _currentBlock = new ProjectionBlock(_docIdSetOperator, _columnToDataSourceMap);
    if (_currentBlock.getDocIdSetBlock() == null) {
      return null;
    }
    return _currentBlock;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException("Not supported in MProjectionOperator!");
  }

  public ProjectionBlock getCurrentBlock() {
    return _currentBlock;
  }

  public Dictionary getDictionary(String column) {
    if (_columnToDataSourceMap.get(column) instanceof ColumnarReaderDataSource) {
      return ((ColumnarReaderDataSource) _columnToDataSourceMap.get(column)).getDictionary();
    } else {
      throw new UnsupportedOperationException("Not support getDictionary for DataSource: "
          + _columnToDataSourceMap.get(column));
    }

  }
}
