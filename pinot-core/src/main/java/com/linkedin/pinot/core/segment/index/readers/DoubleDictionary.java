package com.linkedin.pinot.core.segment.index.readers;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 14, 2014
 */

public class DoubleDictionary extends DictionaryReader  {

  public DoubleDictionary(File dictFile, SegmentMetadataImpl columnMetadata, ReadMode loadMode) throws IOException {
    super(dictFile, columnMetadata.getCardinality(), Double.SIZE/8, loadMode == ReadMode.mmap);
  }

  @Override
  public int indexOf(Object rawValue) {
    Double lookup;

    if (rawValue instanceof String) {
      lookup = new Double(Double.parseDouble((String)rawValue));
    } else {
      lookup = (Double)rawValue;
    }
    return doubleIndexOf(lookup.doubleValue());
  }

  @Override
  public Double get(int dictionaryId) {
    return new Double(getDouble(dictionaryId));
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return (new Double(getDouble(dictionaryId))).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return new Double(getDouble(dictionaryId));
  }

  @Override
  public String toString(int dictionaryId) {
    return (new Double(getDouble(dictionaryId))).toString();
  }

}
