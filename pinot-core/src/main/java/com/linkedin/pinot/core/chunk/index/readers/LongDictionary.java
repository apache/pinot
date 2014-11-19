package com.linkedin.pinot.core.chunk.index.readers;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.chunk.index.ChunkColumnMetadata;

/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 14, 2014
 */

public class LongDictionary extends DictionaryReader{

  public LongDictionary(File dictFile, ChunkColumnMetadata metadata, ReadMode loadMode) throws IOException {
    super(dictFile, metadata.getCardinality(), Long.SIZE/8, loadMode == ReadMode.mmap);
  }

  @Override
  public int indexOf(Object rawValue) {
    Long lookup ;
    if (rawValue instanceof String) {
      lookup = new Long(Long.parseLong((String)rawValue));
    } else {
      lookup = (Long) rawValue;
    }
    return longIndexOf(lookup.longValue());
  }

  @Override
  public Long get(int dictionaryId) {
    return new Long(getLong(dictionaryId));
  }

  @Override
  public long getLongValue(int dictionaryId) {
    return new Long(getLong(dictionaryId)).longValue();
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    return new Double(getLong(dictionaryId));
  }

  @Override
  public String toString(int dictionaryId) {
    return new Long(getLong(dictionaryId)).toString();
  }

}
