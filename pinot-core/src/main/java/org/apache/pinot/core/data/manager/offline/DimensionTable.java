package org.apache.pinot.core.data.manager.offline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


class DimensionTable {

  private Map<PrimaryKey, GenericRow> _lookupTable = new HashMap<>();
  private Schema _tableSchema;
  private List<String> _primaryKeyColumns;

  public void populate(Schema tableSchema, List<String> primaryKeyColumns) {
    _tableSchema = tableSchema;
    _primaryKeyColumns = primaryKeyColumns;
  }

  public void populate(Map<PrimaryKey, GenericRow> lookupTable, Schema tableSchema, List<String> primaryKeyColumns) {
    _lookupTable = lookupTable;
    _tableSchema = tableSchema;
    _primaryKeyColumns = primaryKeyColumns;
  }

  public List<String> getPrimaryKeyColumns() {
    return _primaryKeyColumns;
  }

  public GenericRow get(PrimaryKey pk) {
    return _lookupTable.get(pk);
  }

  public FieldSpec getFieldSpecFor(String columnName) {
    return _tableSchema.getFieldSpecFor(columnName);
  }
}
