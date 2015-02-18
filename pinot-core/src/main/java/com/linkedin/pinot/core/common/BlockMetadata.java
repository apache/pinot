package com.linkedin.pinot.core.common;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.segment.index.loader.Loaders.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;

public interface BlockMetadata {

  int getSize();

  //additional info about the docIdSet
  int getLength();

  int getStartDocId();

  int getEndDocId();

  //DocId set properties

  boolean isSorted();

  boolean isSparse();

  boolean hasInvertedIndex();

  boolean hasDictionary();

  boolean isSingleValue();

  com.linkedin.pinot.core.segment.index.readers.Dictionary getDictionary();

  int maxNumberOfMultiValues();

  DataType getDataType();

  //boolean getForwardIndexCompressionType();

  //boolean getInvertedIndexCompressionType();

}
