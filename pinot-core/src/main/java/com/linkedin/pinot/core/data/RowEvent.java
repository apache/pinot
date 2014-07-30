package com.linkedin.pinot.core.data;

import java.util.Map;


/**
 * The interface for a row based record usually comes with dimensions, metrics and timestamps.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface RowEvent {

  public void init(Map<String, Object> field);

  public String[] getFieldNames();

  public Object getValue(String fieldName);
}
