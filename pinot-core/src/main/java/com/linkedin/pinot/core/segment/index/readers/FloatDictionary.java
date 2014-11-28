package com.linkedin.pinot.core.segment.index.readers;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 14, 2014
 */

public class FloatDictionary extends DictionaryReader {

  public FloatDictionary(File dictFile, ColumnMetadata metadata, ReadMode loadMode) throws IOException {
    super(dictFile, metadata.getCardinality(), Float.SIZE / 8, loadMode == ReadMode.mmap);
  }

  @Override
  public int indexOf(Object rawValue) {
    Float lookup ;

    if (rawValue instanceof String) {
      lookup = new Float(Float.parseFloat((String)rawValue));
    } else {
      lookup = (Float) rawValue;
    }
    return floatIndexOf(lookup.floatValue());
  }

  @Override
  public Float get(int dictionaryId) {
    return new Float(getFloat(dictionaryId));
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return new Float(getFloat(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return new Double(getFloat(dictionaryId));
  }

  @Override
  public String toString(int dictionaryId) {
    return new Float(getFloat(dictionaryId)).toString();
  }

}
