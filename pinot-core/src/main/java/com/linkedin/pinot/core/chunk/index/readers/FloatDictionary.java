package com.linkedin.pinot.core.chunk.index.readers;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.datasource.ChunkColumnMetadata;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 14, 2014
 */

public class FloatDictionary extends AbstractDictionaryReader<Float> {

  public FloatDictionary(File dictFile, ChunkColumnMetadata metadata, ReadMode loadMode) throws IOException {
    super(dictFile, metadata.getCardinality(), Float.SIZE / 8, loadMode == ReadMode.mmap);
  }

  @Override
  public int indexOf(Float dictionatyId) {
    return floatIndexOf(dictionatyId.floatValue());
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
