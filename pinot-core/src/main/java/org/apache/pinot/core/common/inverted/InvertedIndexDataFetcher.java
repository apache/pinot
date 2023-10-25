/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.common.inverted;

import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class InvertedIndexDataFetcher implements InvertedDataFetcher {
  private final String _column;
  private final ColumnReader _columnValueReader;

  /**
   * Constructor for DataFetcher.
   */
  public InvertedIndexDataFetcher(String column, DataSource dataSource) {
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    BitmapInvertedIndexReader indexReader = (BitmapInvertedIndexReader) dataSource.getInvertedIndex();
    ColumnReader columnReader =
        new ColumnReader(dataSourceMetadata.getDataType(), dataSourceMetadata.isSingleValue(),
            indexReader, dataSource.getDictionary());
    _columnValueReader = columnReader;
    _column = column;
  }

  @Override
  public boolean supportsDictId() {
    return true;
  }

  public Object[] getValues() {
    return _columnValueReader.getDictValues();
  }

  public ImmutableRoaringBitmap getDocIds(int dictId) {
    return _columnValueReader.getDocIds(dictId);
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(Object value) {
    throw new UnsupportedOperationException("");
  }

  private class ColumnReader {
    final BitmapInvertedIndexReader _indexReader;
    final Dictionary _dictionary;
    final FieldSpec.DataType _storedType;
    final boolean _singleValue;

    ColumnReader(
        FieldSpec.DataType storedType,
        boolean isSingleValue,
        BitmapInvertedIndexReader indexReader,
        Dictionary dictionary) {
      _indexReader = indexReader;
      _dictionary = dictionary;
      _storedType = storedType;
      _singleValue = isSingleValue;
    }

    Object[] getDictValues() {
      Object[] result = new Object[_dictionary.length()];
      for (int dictId = 0; dictId < _dictionary.length(); dictId++) {
        result[dictId] = _dictionary.get(dictId);
      }
      return result;
    }

    ImmutableRoaringBitmap getDocIds(int dictId) {
      ImmutableRoaringBitmap roaringBitmap = _indexReader.getDocIds(dictId);
      return roaringBitmap;
    }
  }
}
