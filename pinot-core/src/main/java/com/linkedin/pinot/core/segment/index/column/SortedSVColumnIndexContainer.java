/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.column;

import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.index.reader.impl.FixedByteWidthRowColDataFileReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.SortedInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.SortedForwardIndexReader;


public class SortedSVColumnIndexContainer extends ColumnIndexContainer {

  private final String column;
  private final ColumnMetadata columnMetadata;
  private final FixedByteWidthRowColDataFileReader indexFileReader;
  private final ImmutableDictionaryReader dictionaryReader;
  private final InvertedIndexReader invertedIndexReader;
  private final SortedForwardIndexReader forwardIndexReader;

  public SortedSVColumnIndexContainer(String column, ColumnMetadata columnMetadata,
      FixedByteWidthRowColDataFileReader indexFileReader, ImmutableDictionaryReader dictionaryReader) {
    this.column = column;
    this.columnMetadata = columnMetadata;
    this.indexFileReader = indexFileReader;
    this.dictionaryReader = dictionaryReader;
    this.invertedIndexReader = new SortedInvertedIndexReader(indexFileReader);
    this.forwardIndexReader = new SortedForwardIndexReader(indexFileReader, columnMetadata.getTotalDocs());
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return invertedIndexReader;
  }

  @Override
  public DataFileReader getForwardIndex() {
    return forwardIndexReader;
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
    forwardIndexReader.close();
    return true;
  }

}
