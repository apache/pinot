package com.linkedin.pinot.core.chunk.index.readers;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.datasource.ChunkColumnMetadata;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 14, 2014
 */

public class DoubleDictionary extends AbstractDictionaryReader<Double> {

  public DoubleDictionary(File dictFile, ChunkColumnMetadata columnMetadata, ReadMode loadMode) throws IOException {
    super(dictFile, columnMetadata.getCardinality(), Double.SIZE/8, loadMode == ReadMode.mmap);
  }

  @Override
  public int indexOf(Double rawValue) {
    return doubleIndexOf(rawValue.doubleValue());
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
