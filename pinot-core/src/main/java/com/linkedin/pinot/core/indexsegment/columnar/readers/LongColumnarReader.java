package com.linkedin.pinot.core.indexsegment.columnar.readers;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 3, 2014
 */

public class LongColumnarReader implements ColumnarReader{
  final Dictionary<Long> dict;
  final IntArray forwardIndex;

  @SuppressWarnings("unchecked")
  public LongColumnarReader(Dictionary<?> dictionary, IntArray intArray) {
    dict = (Dictionary<Long>) dictionary;
    forwardIndex = intArray;
  }

  @Override
  public int getIntegerValue(int docId) {
    return dict.getRaw(forwardIndex.getInt(docId)).intValue();
  }

  @Override
  public long getLongValue(int docId) {
    return dict.getRaw(forwardIndex.getInt(docId)).longValue();
  }

  @Override
  public float getFloatValue(int docId) {
    return dict.getRaw(forwardIndex.getInt(docId)).floatValue();
  }

  @Override
  public double getDoubleValue(int docId) {
    return dict.getRaw(forwardIndex.getInt(docId)).doubleValue();
  }

  @Override
  public String getStringValue(int docId) {
    return dict.getString(forwardIndex.getInt(docId));
  }

  @Override
  public Object getRawValue(int docId) {
    return dict.getRaw(forwardIndex.getInt(docId));
  }

  @Override
  public int getDictionaryId(int docId) {
    return forwardIndex.getInt(docId);
  }

  @Override
  public DataType getDataType() {
    return DataType.LONG;
  }


}
