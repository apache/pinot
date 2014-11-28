package com.linkedin.pinot.core.segment.index.readers;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 14, 2014
 */

public class IntDictionary extends DictionaryReader {

  public IntDictionary(File dictFile, ColumnMetadata metadata, ReadMode mode) throws IOException {
    super(dictFile, metadata.getCardinality(), Integer.SIZE/8, mode == ReadMode.mmap);
  }

  @Override
  public int indexOf(Object rawValue) {
    Integer lookup;
    if (rawValue instanceof String) {
      lookup = Integer.parseInt((String)rawValue);
    } else {
      lookup = (Integer) rawValue;
    }

    return intIndexOf(lookup.intValue());
  }

  @Override
  public Integer get(int dictionaryId) {
    return new Integer(getInt(dictionaryId));
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return new Long(getInt(dictionaryId));
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return new Double(getInt(dictionaryId));
  }

  @Override
  public String toString(int dictionaryId) {
    return new Integer(getInt(dictionaryId)).toString();
  }

}
