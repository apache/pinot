package com.linkedin.pinot.index.data;

import java.util.HashMap;
import java.util.Map;


/**
 * Pinot Schema is defined for each column. To specify which columns are dimensions, metrics and timeStamps.
 * Different indexing and query strategies are used for different data schema types.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class Schema {
  private Map<String, SchemaType> _schema = null;

  public Schema() {
    initSchema();
  }

  /**
   * 
   */
  public void initSchema() {
    _schema = new HashMap<String, SchemaType>();
  }

  public void addSchema(String columnName, SchemaType schemaType) {
    if (_schema == null) {
      initSchema();
    }
    if (columnName != null && schemaType != null) {
      _schema.put(columnName, schemaType);
    }
  }

  public void addSchema(String columnName) {
    if (_schema == null) {
      initSchema();
    }
    if (columnName != null) {
      _schema.put(columnName, SchemaType.unknown);
    }
  }

  public void removeSchema(String columnName) {
    if (_schema == null) {
      initSchema();
    }
    if (_schema.containsKey(columnName)) {
      _schema.remove(columnName);
    }
  }

  public String toString() {
    String toString = new String();
    for (String column : _schema.keySet()) {
      toString += "< " + column + " : " + _schema.get(column) + " >\n";
    }
    return toString;
  }

  /**
   * SchemaType is used to demonstrate the real world business logic for a column.
   * 
   */
  public enum SchemaType {
    unknown,
    dimension,
    metric,
    timestamp
  }
}
