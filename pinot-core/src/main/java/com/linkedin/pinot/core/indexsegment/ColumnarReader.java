package com.linkedin.pinot.core.indexsegment;

/**
 * ColumnarReader is a random reader for a particular column.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */

public interface ColumnarReader {
  public int getIntegerValue(int docId);

  public long getLongValue(int docId);

  public float getFloatValue(int docId);

  public double getDoubleValue(int docId);

  public String getStringValue(int docId);

  Object getRawValue(int docId);

}
