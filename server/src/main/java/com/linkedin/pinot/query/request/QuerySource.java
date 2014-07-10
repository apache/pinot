package com.linkedin.pinot.query.request;

/**
 * QuerySource describes the resource and table to be queried.
 * The format of a sourceName is [resourceName.tableName]. E.g. for source name midas.jymbii,
 * its resource name is midas and table name is jymbii.
 *  
 * Resource name is required and table name is optional.
 * 
 * @author xiafu
 *
 */
public class QuerySource {

  private String _resourceName = null;
  private String _tableName = null;

  public String getResourceName() {
    return _resourceName;
  }

  public void setResourceName(String _resourceName) {
    this._resourceName = _resourceName;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String _tableName) {
    this._tableName = _tableName;
  }

  public void fromDataSourceString(String sourceName) {
    int indexOfDot = sourceName.indexOf(".");
    if (indexOfDot > 0) {
      _resourceName = sourceName.substring(0, indexOfDot);
      _tableName = sourceName.substring(indexOfDot + 1, sourceName.length());
    } else {
      _resourceName = sourceName;
      _tableName = null;
    }
  }

  @Override
  public String toString() {
    if (_tableName == null || _tableName.length() == 0) {
      return _resourceName;
    } else {
      return _resourceName + "." + _tableName;
    }
  }
}
