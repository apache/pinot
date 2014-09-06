package com.linkedin.pinot.core.indexsegment.columnar.readers;

import com.linkedin.pinot.common.data.FieldSpec.DataType;


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

  public Object getRawValue(int docId);

  public int getDictionaryId(int docId);

  public DataType getDataType();

  public String getStringValueFromDictId(int dictId);

}
