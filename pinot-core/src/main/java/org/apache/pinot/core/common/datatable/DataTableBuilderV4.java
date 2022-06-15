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
package org.apache.pinot.core.common.datatable;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.DataTableFactory;
import org.apache.pinot.core.common.NullBitmapUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * Version 4 of the {@link org.apache.pinot.core.common.DataTableBuilder}
 * It extends the DataTableBuilderV2/V3 and provides a override to a single String dictionary encoding.
 */
public class DataTableBuilderV4 extends DataTableBuilderV2V3 {
  private static final String DEFAULT_DICTIONARY_MAP_KEY = "DEFAULT";

  private int _numDictionaryEntries;

  public DataTableBuilderV4(DataSchema dataSchema) {
    super(dataSchema);
  }

  @Override
  public void setNullRowIds(RoaringBitmap nullBitmap)
      throws IOException {
    int version = DataTableFactory.getCurrentDataTableVersion();
    Preconditions.checkState(version >= DataTableFactory.VERSION_4, "Null bitmap only supported after DataTable V4");
    NullBitmapUtils.setNullRowIds(nullBitmap, _fixedSizeDataByteArrayOutputStream,
        _variableSizeDataByteArrayOutputStream);
  }

  @Override
  public void setColumn(int colId, String value) {
    Map<String, Integer> dictionary = _dictionaryMap.get(DEFAULT_DICTIONARY_MAP_KEY);
    if (dictionary == null) {
      dictionary = new HashMap<>();
      _dictionaryMap.put(DEFAULT_DICTIONARY_MAP_KEY, dictionary);
      _reverseDictionaryMap.put(DEFAULT_DICTIONARY_MAP_KEY, new HashMap<>());
    }

    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    Integer dictId = dictionary.get(value);
    if (dictId == null) {
      dictId = _numDictionaryEntries;
      _numDictionaryEntries++;
      dictionary.put(value, dictId);
      _reverseDictionaryMap.get(DEFAULT_DICTIONARY_MAP_KEY).put(dictId, value);
    }
    _currentRowDataByteBuffer.putInt(dictId);
  }

  @Override
  public void setColumn(int colId, String[] values)
      throws IOException {
    _currentRowDataByteBuffer.position(_columnOffsets[colId]);
    _currentRowDataByteBuffer.putInt(_variableSizeDataByteArrayOutputStream.size());
    _currentRowDataByteBuffer.putInt(values.length);

    Map<String, Integer> dictionary = _dictionaryMap.get(DEFAULT_DICTIONARY_MAP_KEY);
    if (dictionary == null) {
      dictionary = new HashMap<>();
      _dictionaryMap.put(DEFAULT_DICTIONARY_MAP_KEY, dictionary);
      _reverseDictionaryMap.put(DEFAULT_DICTIONARY_MAP_KEY, new HashMap<>());
    }

    for (String value : values) {
      Integer dictId = dictionary.get(value);
      if (dictId == null) {
        dictId = _numDictionaryEntries;
        _numDictionaryEntries++;
        dictionary.put(value, dictId);
        _reverseDictionaryMap.get(DEFAULT_DICTIONARY_MAP_KEY).put(dictId, value);
      }
      _variableSizeDataOutputStream.writeInt(dictId);
    }
  }

  @Override
  public DataTable build() {
    int version = DataTableFactory.getCurrentDataTableVersion();
    switch (version) {
      case DataTableFactory.VERSION_2:
        return new DataTableImplV2(_numRows, _dataSchema, _reverseDictionaryMap,
            _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray());
      case DataTableFactory.VERSION_3:
        return new DataTableImplV3(_numRows, _dataSchema, _reverseDictionaryMap,
            _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray());
      case DataTableFactory.VERSION_4:
        return new DataTableImplV4(_numRows, _dataSchema, _reverseDictionaryMap,
            _fixedSizeDataByteArrayOutputStream.toByteArray(), _variableSizeDataByteArrayOutputStream.toByteArray(),
            _numDictionaryEntries);
      default:
        throw new IllegalStateException("Unexpected value: " + version);
    }
  }
}
