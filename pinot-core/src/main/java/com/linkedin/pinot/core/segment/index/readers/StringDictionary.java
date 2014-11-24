package com.linkedin.pinot.core.segment.index.readers;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 14, 2014
 */

public class StringDictionary extends DictionaryReader {
  private final int lengthofMaxEntry;

  public StringDictionary(File dictFile, SegmentMetadataImpl metadata, ReadMode mode) throws IOException {
    super(dictFile, metadata.getCardinality(), metadata.getStringColumnMaxLength(), mode == ReadMode.mmap);
    lengthofMaxEntry = metadata.getStringColumnMaxLength();
  }

  @Override
  public int indexOf(Object rawValue) {
    final String lookup = rawValue.toString();
    final int differenceInLength = lengthofMaxEntry - lookup.length();
    final StringBuilder bld = new StringBuilder();
    for (int i = 0; i < differenceInLength; i++) {
      bld.append(V1Constants.Str.STRING_PAD_CHAR);
    }
    bld.append(lookup);
    return stringIndexOf(bld.toString());
  }

  @Override
  public String get(int dictionaryId) {
    return StringUtils.remove(getString(dictionaryId), String.valueOf(V1Constants.Str.STRING_PAD_CHAR));
  }

  @Override
  public long getLongValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to long");
  }

  @Override
  public double getDoubleValue(int dictionaryId) {
    throw new RuntimeException("cannot converted string to double");
  }

  @Override
  public String toString(int dictionaryId) {
    return StringUtils.remove(getString(dictionaryId), String.valueOf(V1Constants.Str.STRING_PAD_CHAR));
  }
}
