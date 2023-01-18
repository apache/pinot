package org.apache.pinot.core.data.manager.offline;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class InMemoryDimensionTable implements DimensionTable{
  public InMemoryDimensionTable(DataTable dataTable, List<Integer> primaryKeyColumns){
    _schema = dataTable.getDataSchema();
    for(int idx: primaryKeyColumns){
      _primaryKeyColumns.add(_schema.getColumnName(idx));
    }
    String[] columnNames = _schema.getColumnNames();
    for(int i = 0; i < columnNames.length; ++i){
      _indexMap.put(columnNames[i], i);
    }
    int numRows = dataTable.getNumberOfRows();
    int numCols = columnNames.length;
    for(int i = 0; i < numRows; ++i){
      for(int j = 0; j < numCols; ++j){
        
      }
    }
  }

  @Override
  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  @Override
  public GenericRow get(PrimaryKey pk) {
    return null;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public FieldSpec getFieldSpecFor(String columnName) {
    return null;
  }

  @Override
  public void close()
      throws IOException {

  }

  DataTable _dataTable;
  DataSchema _schema;

  List<String> _primaryKeyColumns;
  HashMap<String, Integer> _indexMap;

  HashMap<PrimaryKey, GenericRow> _rows;
}
