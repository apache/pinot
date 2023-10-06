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
package org.apache.pinot.core.common;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.DictionaryIdBasedBitmapProvider;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class InvertedIndexDataFetcher {
  // Thread local (reusable) buffer for single-valued column dictionary Ids
  private static final ThreadLocal<int[]> THREAD_LOCAL_DICT_IDS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  // TODO: Figure out a way to close the reader context within the ColumnValueReader
  //       ChunkReaderContext should be closed explicitly to release the off-heap buffer
  private final Map<String, ColumnReader> _columnValueReaderMap;

  /**
   * Constructor for DataFetcher.
   *
   * @param dataSourceMap Map from column to data source
   */
  public InvertedIndexDataFetcher(Map<String, DataSource> dataSourceMap) {
    _columnValueReaderMap = new HashMap<>();
    int maxNumValuesPerMVEntry = 0;
    for (Map.Entry<String, DataSource> entry : dataSourceMap.entrySet()) {
      String column = entry.getKey();
      DataSource dataSource = entry.getValue();
      DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
      IndexReader indexReader = dataSource.getInvertedIndex();
      Preconditions.checkState(indexReader instanceof DictionaryIdBasedBitmapProvider);
      ColumnReader columnReader =
          new ColumnReader(dataSourceMetadata.getDataType(), dataSourceMetadata.isSingleValue(),
              (DictionaryIdBasedBitmapProvider) indexReader, dataSource.getDictionary());
      _columnValueReaderMap.put(column, columnReader);
      if (!dataSourceMetadata.isSingleValue()) {
        maxNumValuesPerMVEntry = Math.max(maxNumValuesPerMVEntry, dataSourceMetadata.getMaxNumValuesPerMVEntry());
      }
    }
  }

  public int getCardinality(String column) {
    return _columnValueReaderMap.get(column)._dictionary.length();
  }

  public Object[] getValues(String column) {
    return _columnValueReaderMap.get(column).getDictValues();
  }

  public ImmutableRoaringBitmap getDocIds(String column, int dictId) {
    return _columnValueReaderMap.get(column).getDocIds(dictId);
  }

  private class ColumnReader {
    final DictionaryIdBasedBitmapProvider _indexReader;
    final Dictionary _dictionary;
    final FieldSpec.DataType _storedType;
    final boolean _singleValue;

    ColumnReader(
        FieldSpec.DataType storedType,
        boolean isSingleValue,
        DictionaryIdBasedBitmapProvider indexReader,
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
