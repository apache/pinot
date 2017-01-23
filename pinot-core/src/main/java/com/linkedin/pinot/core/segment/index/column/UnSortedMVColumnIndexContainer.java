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

import com.linkedin.pinot.core.segment.index.readers.BitmapInvertedIndexReader;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


public class UnSortedMVColumnIndexContainer extends ColumnIndexContainer {

  private final String column;
  private final ColumnMetadata columnMetadata;
  private final SingleColumnMultiValueReader indexReader;
  private final ImmutableDictionaryReader dictionary;
  private final BitmapInvertedIndexReader invertedIndexReader;

  public UnSortedMVColumnIndexContainer(String column, ColumnMetadata columnMetadata,
      SingleColumnMultiValueReader indexReader, ImmutableDictionaryReader dictionary) {
    this(column, columnMetadata, indexReader, dictionary, null);
  }

  public UnSortedMVColumnIndexContainer(String column, ColumnMetadata columnMetadata,
      SingleColumnMultiValueReader indexReader, ImmutableDictionaryReader dictionary,
      BitmapInvertedIndexReader invertedIndex) {
    this.column = column;
    this.columnMetadata = columnMetadata;
    this.indexReader = indexReader;
    this.dictionary = dictionary;
    this.invertedIndexReader = invertedIndex;
  }

  @Override
  public InvertedIndexReader getInvertedIndex() {
    return invertedIndexReader;
  }

  @Override
  public DataFileReader getForwardIndex() {
    return indexReader;
  }

  @Override
  public boolean hasDictionary() {
    return columnMetadata.hasDictionary();
  }

  @Override
  public ImmutableDictionaryReader getDictionary() {
    return dictionary;
  }

  @Override
  public ColumnMetadata getColumnMetadata() {
    return columnMetadata;
  }

  @Override
  public boolean unload() throws Exception {
    indexReader.close();
    dictionary.close();
    if (invertedIndexReader != null) {
      invertedIndexReader.close();
    }
    return true;
  }

}
