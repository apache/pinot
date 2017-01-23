/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.impl.FixedByteSingleValueMultiColReader;
import com.linkedin.pinot.core.io.reader.impl.SortedForwardIndexReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.SortedInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class SortedSVColumnIndexContainer extends ColumnIndexContainer {

  private final String column;
  private final ColumnMetadata columnMetadata;
  private final FixedByteSingleValueMultiColReader indexFileReader;
  private final ImmutableDictionaryReader dictionaryReader;
  private final InvertedIndexReader invertedIndexReader;
  private final SortedForwardIndexReader forwardIndexReader;

  public SortedSVColumnIndexContainer(String column, ColumnMetadata columnMetadata,
      FixedByteSingleValueMultiColReader indexFileReader, ImmutableDictionaryReader dictionaryReader) {
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
  public boolean hasDictionary() {
    return columnMetadata.hasDictionary();
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
