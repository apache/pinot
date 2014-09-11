package com.linkedin.pinot.core.indexsegment.columnar.readers;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 3, 2014
 */

public class IntColumnarReader implements ColumnarReader {
  final Dictionary<Integer> dict;
  final IntArray forwardIndex;

  @SuppressWarnings("unchecked")
  public IntColumnarReader(Dictionary<?> dictionary, IntArray intArray) {
    dict = (Dictionary<Integer>) dictionary;
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
    return new Float(dict.getRaw(forwardIndex.getInt(docId)).intValue()).floatValue();
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
    return DataType.INT;
  }

  @Override
  public String getStringValueFromDictId(int dictId) {
    return dict.getString(dictId);
  }
}
