package com.linkedin.pinot.core.data;

import java.util.HashMap;
import java.util.Map;


/**
 * A plain implementation of RowEvent based on HashMap.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class GenericRow implements RowEvent {
  private Map<String, Object> _fieldMap = new HashMap<String, Object>();

  @Override
  public void init(Map<String, Object> field) {
    _fieldMap = field;
  }

  @Override
  public String[] getFieldNames() {
    return _fieldMap.keySet().toArray(new String[_fieldMap.size()]);
  }

  @Override
  public Object getValue(String fieldName) {
    return _fieldMap.get(fieldName);
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    for (String key : _fieldMap.keySet())
      b.append(key + " : " + _fieldMap.get(key) + ", ");
    return b.toString();
  }
}
