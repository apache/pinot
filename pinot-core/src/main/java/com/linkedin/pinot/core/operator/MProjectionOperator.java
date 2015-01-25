package com.linkedin.pinot.core.operator;

import java.util.Map;

import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;


/**
 * MProjectionOperator will call nextBlock then return a ProjectionBlock.
 *
 * @author xiafu
 *
 */
public class MProjectionOperator implements DataSource {

  private final BReusableFilteredDocIdSetOperator _docIdSetOperator;
  private final Map<String, DataSource> _columnToDataSourceMap;
  private ProjectionBlock _currentBlock = null;

  public MProjectionOperator(Map<String, DataSource> dataSourceMap, BReusableFilteredDocIdSetOperator docIdSetOperator) {
    _docIdSetOperator = docIdSetOperator;
    _columnToDataSourceMap = dataSourceMap;
  }

  @Override
  public boolean open() {
    for (final String column : _columnToDataSourceMap.keySet()) {
      _columnToDataSourceMap.get(column).open();
    }
    return true;
  }

  @Override
  public boolean close() {
    for (final String column : _columnToDataSourceMap.keySet()) {
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

  //  public DictionaryReader getDictionary(String column) {
  //    if (_columnToDataSourceMap.get(column) instanceof ColumnDataSourceImpl) {
  //      return ((ColumnDataSourceImpl) _columnToDataSourceMap.get(column)).getDictionary();
  //    } else {
  //      throw new UnsupportedOperationException("Not support getDictionary for DataSource: "
  //          + _columnToDataSourceMap.get(column));
  //    }
  //  }

  public DataSource getDataSource(String column) {
    return _columnToDataSourceMap.get(column);
  }
}
