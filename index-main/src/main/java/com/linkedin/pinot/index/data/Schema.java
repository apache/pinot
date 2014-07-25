package com.linkedin.pinot.index.data;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.index.data.FieldSpec.DataType;
import com.linkedin.pinot.index.data.FieldSpec.FieldType;


/**
 * Schema is defined for each column. To describe the details information of columns.
 * Three types of information are provided.
 * 1. the data type of this column: int, long, double...
 * 2. if this column is a single value column or a multi-value column.
 * 3. the real world business logic: dimensions, metrics and timeStamps.
 * Different indexing and query strategies are used for different data schema types.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class Schema {
  private Map<String, FieldSpec> _schema = new HashMap<String, FieldSpec>();

  public Schema() {
  }

  public void addSchema(String columnName, FieldSpec fieldSpec) {

    if (columnName != null && fieldSpec != null) {
      _schema.put(columnName, fieldSpec);
    }
  }

  public void removeSchema(String columnName) {
    if (_schema.containsKey(columnName)) {
      _schema.remove(columnName);
    }
  }

  public String toString() {
    String toString = new String();
    for (String column : _schema.keySet()) {
      toString += "< " + column + " : " + _schema.get(column).toString() + " >\n";
    }
    return toString;
  }

  public boolean isExisted(String columnName) {
    return _schema.containsKey(columnName);
  }

  public boolean isSingleValueColumn(String columnName) {
    return _schema.get(columnName).isSingleValueField();
  }

  public DataType getDataType(String columnName) {
    return _schema.get(columnName).getDataType();
  }

  public FieldType getFieldType(String columnName) {
    return _schema.get(columnName).getFieldType();
  }

  public Collection<String> getColumnNames() {
    return _schema.keySet();
  }

  public int size() {
    return _schema.size();
  }

  public String getDelimeter(String columnName) {
    return _schema.get(columnName).getDelimeter();
  }

  public FieldSpec getFieldSpecFor(String column) {
    return _schema.get(column);
  }

  public Collection<FieldSpec> getAllFieldSpecs() {
    return _schema.values();
  }
}
