package com.linkedin.pinot.core.segment.index.column;

import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.SortedInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class SortedSVColumnIndexContainer extends ColumnIndexContainer {

  private final String column;
  private final ColumnMetadata columnMetadata;
  private final FixedByteWidthRowColDataFileReader indexFileReader;
  private final ImmutableDictionaryReader dictionaryReader;
  private final InvertedIndexReader invertedIndexReader;

  public SortedSVColumnIndexContainer(String column, ColumnMetadata columnMetadata,
      FixedByteWidthRowColDataFileReader indexFileReader, ImmutableDictionaryReader dictionaryReader) {
    this.column = column;
    this.columnMetadata = columnMetadata;
    this.indexFileReader = indexFileReader;
    this.dictionaryReader = dictionaryReader;
    this.invertedIndexReader = new SortedInvertedIndexReader(indexFileReader);
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return invertedIndexReader;
  }

  @Override
  public DataFileReader getForwardIndex() {
    return null;
  }

  @Override
  public ImmutableDictionaryReader getDictionary() {
    return dictionaryReader;
  }

  @Override
  public ColumnMetadata getColumnMetadata() {
    return columnMetadata;
  }

  @Override
  public boolean unload() throws Exception {
    indexFileReader.close();
    dictionaryReader.close();
    return true;
  }

}
