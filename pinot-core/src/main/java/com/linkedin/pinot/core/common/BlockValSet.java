package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec.DataType;

public interface BlockValSet {

  BlockValIterator iterator();
  DataType getValueType();

  int getDictionaryId(int docId);

  // methods are on dictionaryId
  int getIntValueAt(int dictionaryId);
  long getLongValueAt(int dictionaryId);
  float getFloatValueAt(int dictionaryId);
  double getDoubleValueAt(int dictionaryId);
  String getStringValueAt(int dictionaryId);
}
